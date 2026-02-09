"""Management endpoints for data ingestion and maintenance."""

from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Depends
import duckdb

from extreme_temps.api.deps import get_db
from extreme_temps.ingest.orchestrator import ingest_all_stations_incremental
from extreme_temps.compute.latest_insights import compute_latest_insight

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/refresh")
def refresh_all(
    db: duckdb.DuckDBPyConnection = Depends(get_db),
) -> dict:
    """Incremental ingest for all stations, then recompute latest insights.

    Steps:
    1. Fetch new GHCN Daily data + Open-Meteo gap-fill for every active station.
    2. Recompute precomputed latest insights for the home page.
    """
    t0 = time.time()

    # Step 1: Incremental ingest
    results = ingest_all_stations_incremental(db)
    total_rows = sum(r.rows_inserted for r in results)
    ingest_errors = [
        {"station_id": r.station_id, "errors": r.errors}
        for r in results
        if r.errors
    ]
    t1 = time.time()

    # Step 2: Recompute latest insights
    station_ids = [r.station_id for r in results]
    computed = 0
    compute_errors = 0
    for sid in station_ids:
        try:
            result = compute_latest_insight(db, sid)
            if result:
                computed += 1
        except Exception:
            logger.exception("Failed to compute latest insight for %s", sid)
            compute_errors += 1
    t2 = time.time()

    return {
        "ingest": {
            "stations": len(results),
            "rows_inserted": total_rows,
            "errors": ingest_errors,
            "duration_s": round(t1 - t0, 1),
        },
        "compute": {
            "insights_computed": computed,
            "errors": compute_errors,
            "duration_s": round(t2 - t1, 1),
        },
        "total_duration_s": round(t2 - t0, 1),
    }
