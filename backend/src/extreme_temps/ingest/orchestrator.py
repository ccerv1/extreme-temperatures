"""Ingestion orchestrator — coordinates data fetching and DuckDB loading.

GHCN Daily is the primary source. GSOD is an optional secondary gap-filler.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
import logging

import duckdb
import pandas as pd

from extreme_temps.db.queries import (
    get_station,
    upsert_daily_observations,
    update_station_coverage,
    get_station_date_range,
)
from extreme_temps.ingest.ghcn_daily import fetch_ghcn_daily
from extreme_temps.ingest.gsod import fetch_gsod
from extreme_temps.ingest.open_meteo import fetch_open_meteo

logger = logging.getLogger(__name__)


@dataclass
class IngestResult:
    station_id: str
    rows_inserted: int = 0
    source: str = ""
    errors: list[str] = field(default_factory=list)


def ingest_station_full(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    wban: str | None = None,
) -> IngestResult:
    """Full historical backfill for one station.

    1. Fetch all GHCN Daily history (primary).
    2. Optionally fetch GSOD for gap-filling if wban is provided.
    3. Upsert into fact_station_day (GHCN rows take priority).
    4. Update dim_station coverage stats.
    """
    result = IngestResult(station_id=station_id)

    # Step 1: GHCN Daily (primary)
    ghcn_df = fetch_ghcn_daily(station_id)
    if ghcn_df.empty:
        result.errors.append("No GHCN Daily data returned")
    else:
        count = upsert_daily_observations(conn, station_id, ghcn_df, source="ghcn_daily")
        result.rows_inserted += count
        result.source = "ghcn_daily"
        logger.info("Inserted %d GHCN Daily rows for %s", count, station_id)

    # Step 2: GSOD gap-fill (optional, secondary)
    if wban:
        _fill_gaps_from_gsod(conn, station_id, wban, result)

    # Step 3: Open-Meteo fill for recent days (GHCN has 3-5 day lag)
    station = get_station(conn, station_id)
    if station is not None:
        _fill_recent_from_open_meteo(conn, station_id, station, result)

    # Step 4: Update coverage stats
    update_station_coverage(conn, station_id)

    return result


def ingest_station_incremental(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
) -> IngestResult:
    """Incremental update — fetch only new data since last observation.

    1. Determine last_obs_date from dim_station.
    2. Fetch GHCN Daily from last_obs_date to today.
    3. Upsert new rows.
    4. Update dim_station metadata.
    """
    result = IngestResult(station_id=station_id)

    station = get_station(conn, station_id)
    if station is None:
        result.errors.append(f"Station {station_id} not found in dim_station")
        return result

    _, last_date = get_station_date_range(conn, station_id)

    # Fetch from day after last observation (or all if no data yet)
    start = None
    if last_date is not None:
        start = last_date  # overlap by 1 day to catch updates

    ghcn_df = fetch_ghcn_daily(station_id, start_date=start)
    if ghcn_df.empty:
        logger.info("No new GHCN data for %s since %s", station_id, start)
    else:
        count = upsert_daily_observations(conn, station_id, ghcn_df, source="ghcn_daily")
        result.rows_inserted = count
        result.source = "ghcn_daily"
        logger.info("Incremental: inserted %d rows for %s", count, station_id)

    # Fill recent gap with Open-Meteo (GHCN has 3-5 day lag)
    _fill_recent_from_open_meteo(conn, station_id, station, result)

    update_station_coverage(conn, station_id)
    return result


def ingest_all_stations_incremental(conn: duckdb.DuckDBPyConnection) -> list[IngestResult]:
    """Run incremental ingest for all active stations."""
    stations = conn.execute(
        "SELECT station_id FROM dim_station WHERE is_active = TRUE"
    ).fetchdf()

    results = []
    for station_id in stations["station_id"]:
        try:
            r = ingest_station_incremental(conn, station_id)
            results.append(r)
        except Exception:
            logger.exception("Failed incremental ingest for %s", station_id)
            results.append(IngestResult(station_id=station_id, errors=["Exception during ingest"]))

    return results


def _fill_gaps_from_gsod(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    wban: str,
    result: IngestResult,
) -> None:
    """Fill date gaps using GSOD as secondary source."""
    # Determine date range and gaps
    first_date, last_date = get_station_date_range(conn, station_id)
    if first_date is None or last_date is None:
        return

    start_year = first_date.year
    end_year = last_date.year

    gsod_df = fetch_gsod(wban, start_year, end_year)
    if gsod_df.empty:
        return

    # Find dates that are missing from GHCN
    existing = conn.execute(
        "SELECT DISTINCT obs_date FROM fact_station_day WHERE station_id = ?",
        [station_id],
    ).fetchdf()

    existing_dates = set(existing["obs_date"].tolist())
    gsod_df = gsod_df[~gsod_df["obs_date"].isin(existing_dates)]

    if gsod_df.empty:
        return

    count = upsert_daily_observations(conn, station_id, gsod_df, source="gsod")
    result.rows_inserted += count
    if result.source:
        result.source += "+gsod"
    else:
        result.source = "gsod"
    logger.info("Gap-filled %d GSOD rows for %s", count, station_id)


def _fill_recent_from_open_meteo(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    station: dict,
    result: IngestResult,
) -> None:
    """Fill the GHCN lag (last few days) using Open-Meteo.

    Always re-fetches at least the last 2 days of Open-Meteo data so that
    the previous day gets updated with final end-of-day values. Caps at
    yesterday (today's data is partial). Only overwrites existing Open-Meteo
    rows, never GHCN or GSOD.
    """
    from datetime import timedelta

    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        return

    yesterday = date.today() - timedelta(days=1)

    # Find the last date with authoritative (GHCN/GSOD) data
    ghcn_last = conn.execute(
        "SELECT MAX(obs_date) FROM fact_station_day "
        "WHERE station_id = ? AND source IN ('ghcn_daily', 'gsod')",
        [station_id],
    ).fetchone()
    ghcn_last_date = ghcn_last[0] if ghcn_last and ghcn_last[0] else None
    if ghcn_last_date and hasattr(ghcn_last_date, 'date'):
        ghcn_last_date = ghcn_last_date.date()

    if ghcn_last_date is None:
        return

    # Fetch from day after last GHCN date through yesterday
    start = ghcn_last_date + timedelta(days=1)
    if start > yesterday:
        return  # GHCN is already up to yesterday

    om_df = fetch_open_meteo(float(lat), float(lon), start, yesterday)
    if om_df.empty:
        return

    # Filter out dates that have GHCN/GSOD data (authoritative sources)
    ghcn_dates = conn.execute(
        "SELECT DISTINCT obs_date FROM fact_station_day "
        "WHERE station_id = ? AND source IN ('ghcn_daily', 'gsod')",
        [station_id],
    ).fetchdf()
    ghcn_date_set = set(ghcn_dates["obs_date"].tolist())
    om_df = om_df[~om_df["obs_date"].isin(ghcn_date_set)]

    if om_df.empty:
        return

    # INSERT OR REPLACE: overwrites stale Open-Meteo rows with fresh values
    count = upsert_daily_observations(conn, station_id, om_df, source="open_meteo")
    result.rows_inserted += count
    if result.source:
        result.source += "+open_meteo"
    else:
        result.source = "open_meteo"
    logger.info("Open-Meteo filled %d recent rows for %s", count, station_id)
