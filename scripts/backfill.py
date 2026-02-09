"""Full historical backfill for specified stations."""

import argparse
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Backfill station data")
    parser.add_argument(
        "--stations",
        required=True,
        help='Comma-separated GHCN station IDs, or "all" for full registry',
    )
    args = parser.parse_args()

    from extreme_temps.config import STATIONS_JSON
    from extreme_temps.db.connection import get_connection
    from extreme_temps.db.schema import create_all_tables
    from extreme_temps.ingest.stations import seed_stations, load_station_registry
    from extreme_temps.ingest.orchestrator import ingest_station_full
    from extreme_temps.compute.climatology import compute_climatology_quantiles
    from extreme_temps.compute.records import compute_all_records
    from extreme_temps.compute.rolling_windows import compute_recent_windows

    conn = get_connection()
    create_all_tables(conn)
    seed_stations(conn)

    # Determine which stations to backfill
    if args.stations.lower() == "all":
        registry = load_station_registry()
        station_ids = [(s["station_id"], s.get("wban")) for s in registry]
    else:
        station_ids = [(sid.strip(), None) for sid in args.stations.split(",")]

    logger.info("Backfilling %d station(s)", len(station_ids))

    for station_id, wban in station_ids:
        logger.info("--- %s ---", station_id)

        # 1. Ingest full history
        result = ingest_station_full(conn, station_id, wban=wban)
        logger.info("Ingest: %d rows (%s)", result.rows_inserted, result.source)
        if result.errors:
            logger.warning("Errors: %s", result.errors)

        if result.rows_inserted == 0:
            continue

        # 2. Compute climatology quantiles for key window sizes
        for w in [1, 3, 5, 7, 10, 14, 21, 28, 30, 45, 60, 90]:
            n = compute_climatology_quantiles(conn, station_id, "tavg_c", w, 7)
            logger.info("Climatology w=%d: %d rows", w, n)

        # 3. Compute records
        n = compute_all_records(conn, station_id)
        logger.info("Records: %d rows", n)

        # 4. Compute recent rolling windows
        n = compute_recent_windows(conn, station_id)
        logger.info("Recent windows: %d rows", n)

        # 5. Compute latest insights for home page (all window sizes)
        from extreme_temps.compute.latest_insights import compute_latest_insights_multi
        insights = compute_latest_insights_multi(conn, station_id)
        logger.info("Latest insights: %d windows computed", len(insights))

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
