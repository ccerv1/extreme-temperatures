"""Precompute latest insight for the home page.

Computes a single insight per station using the most recent available data
and stores it in fact_station_latest_insight for fast retrieval.
"""

from __future__ import annotations

from datetime import timedelta
import logging

import duckdb
import pandas as pd

from extreme_temps.compute.rolling_windows import compute_rolling_window
from extreme_temps.compute.climatology import get_percentile_for_value_from_quantiles
from extreme_temps.compute.severity import (
    Severity,
    Direction,
    classify_severity,
    classify_direction,
)
from extreme_temps.compute.statements import generate_insight
from extreme_temps.db.queries import (
    get_station,
    get_climatology_quantiles,
    upsert_latest_insight,
)

logger = logging.getLogger(__name__)


def compute_latest_insight(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    window_days: int = 7,
    metric: str = "tavg_c",
) -> dict | None:
    """Compute and store the latest insight for a station.

    Finds the most recent date with data, tries progressively earlier dates
    (to handle GHCN lag), then runs the full insight pipeline.

    Returns the insight dict, or None if no data available.
    """
    station = get_station(conn, station_id)
    if station is None:
        logger.warning("Station %s not found", station_id)
        return None

    # Find latest date with data
    result = conn.execute(
        "SELECT MAX(obs_date) FROM fact_station_day WHERE station_id = ?",
        [station_id],
    ).fetchone()

    if result is None or result[0] is None:
        return None

    latest_date = result[0]
    if hasattr(latest_date, "date"):
        latest_date = latest_date.date()

    # Try latest date, then progressively earlier (up to 7 days back)
    for i in range(8):
        end_date = latest_date - timedelta(days=i)

        window = compute_rolling_window(conn, station_id, end_date, window_days, metric)
        if window is None:
            continue

        value = window["value"]
        coverage_ratio = window["coverage_ratio"]

        # Look up precomputed climatology
        end_doy = end_date.timetuple().tm_yday
        quantiles = get_climatology_quantiles(conn, station_id, metric, window_days, end_doy)

        percentile = None
        if quantiles is not None:
            percentile = get_percentile_for_value_from_quantiles(quantiles, float(value))

        # Coverage years from station metadata
        coverage_years = station.get("coverage_years")
        if coverage_years is not None and pd.notna(coverage_years):
            coverage_years = int(coverage_years)
        else:
            coverage_years = 0

        # First year from quantiles or station
        first_year_val = station.get("first_obs_date")
        if first_year_val is not None and not (isinstance(first_year_val, float) and pd.isna(first_year_val)):
            first_year = first_year_val.year if hasattr(first_year_val, "year") else int(first_year_val)
        else:
            first_year = 2000

        if quantiles and quantiles.get("first_year") is not None:
            first_year = int(quantiles["first_year"])

        # Classify
        if percentile is not None:
            severity = classify_severity(percentile, coverage_years, coverage_ratio=coverage_ratio)
            direction = classify_direction(percentile, metric)
        else:
            severity = Severity.INSUFFICIENT_DATA
            direction = Direction.NEUTRAL

        # Generate statements
        primary, supporting = generate_insight(
            window_days=window_days,
            value_c=value,
            percentile=percentile or 50.0,
            severity=severity,
            direction=direction,
            coverage_years=coverage_years,
            first_year=first_year,
        )

        row = {
            "station_id": station_id,
            "end_date": end_date,
            "window_days": window_days,
            "metric": metric,
            "value": round(value, 2) if value is not None else None,
            "percentile": round(percentile, 1) if percentile is not None else None,
            "severity": severity.value,
            "direction": direction.value,
            "primary_statement": primary,
            "supporting_line": supporting,
            "coverage_years": coverage_years,
            "first_year": first_year,
        }

        upsert_latest_insight(conn, row)
        logger.info(
            "Latest insight for %s: %s (%s) end_date=%s",
            station_id, severity.value, direction.value, end_date,
        )
        return row

    logger.warning("No valid window found for %s", station_id)
    return None
