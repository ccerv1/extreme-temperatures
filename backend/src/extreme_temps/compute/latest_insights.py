"""Precompute latest insights for the home page.

Computes insights for multiple window sizes per station using the most recent
available data and stores them in fact_station_latest_insight for fast retrieval.
"""

from __future__ import annotations

from datetime import date, timedelta
import logging

import duckdb
import pandas as pd

from extreme_temps.compute.rolling_windows import compute_rolling_window
from extreme_temps.compute.climatology import (
    get_percentile_for_value_from_quantiles,
    compute_quantiles_for_doy,
)
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

DEFAULT_WINDOW_SIZES = [1, 7, 14, 30]


def compute_latest_insights_multi(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    window_sizes: list[int] | None = None,
    metric: str = "tavg_c",
    since_year: int | None = None,
) -> list[dict]:
    """Compute and store latest insights for multiple window sizes.

    Args:
        conn: DuckDB connection.
        station_id: GHCN station ID.
        window_sizes: List of window sizes to compute. Defaults to [1, 7, 14, 30].
        metric: Metric to compute insights for.
        since_year: If set, use on-the-fly quantiles from this year onward.
                    Defaults to current_year - 24 (25-year window).

    Returns:
        List of insight dicts that were successfully computed and stored.
    """
    if window_sizes is None:
        window_sizes = DEFAULT_WINDOW_SIZES

    if since_year is None:
        since_year = date.today().year - 24

    station = get_station(conn, station_id)
    if station is None:
        logger.warning("Station %s not found", station_id)
        return []

    # Find latest date with data
    result = conn.execute(
        "SELECT MAX(obs_date) FROM fact_station_day WHERE station_id = ?",
        [station_id],
    ).fetchone()

    if result is None or result[0] is None:
        return []

    latest_date = result[0]
    if hasattr(latest_date, "date"):
        latest_date = latest_date.date()

    # Cap at yesterday — today's data may be partial
    yesterday = date.today() - timedelta(days=1)
    if latest_date > yesterday:
        latest_date = yesterday

    results = []
    for window_days in window_sizes:
        row = _compute_single_window(
            conn, station_id, station, latest_date,
            window_days, metric, since_year,
        )
        if row is not None:
            results.append(row)

    return results


def compute_latest_insight(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    window_days: int = 7,
    metric: str = "tavg_c",
) -> dict | None:
    """Compute and store a single latest insight (backward-compat wrapper).

    Delegates to the multi-window function with a single window size and
    default 25-year comparison.
    """
    results = compute_latest_insights_multi(
        conn, station_id, window_sizes=[window_days], metric=metric,
    )
    return results[0] if results else None


def _compute_single_window(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    station: dict,
    latest_date: date,
    window_days: int,
    metric: str,
    since_year: int,
) -> dict | None:
    """Compute insight for a single window size, trying progressively earlier dates."""
    for i in range(8):
        end_date = latest_date - timedelta(days=i)

        window = compute_rolling_window(conn, station_id, end_date, window_days, metric)
        if window is None:
            continue

        value = window["value"]
        coverage_ratio = window["coverage_ratio"]

        end_doy = end_date.timetuple().tm_yday

        # Try on-the-fly quantiles with since_year filter first
        quantiles = compute_quantiles_for_doy(
            conn, station_id, metric, window_days, end_doy, since_year,
        )
        # Fall back to precomputed climatology if on-the-fly fails
        if quantiles is None:
            quantiles = get_climatology_quantiles(conn, station_id, metric, window_days, end_doy)

        percentile = None
        if quantiles is not None:
            percentile = get_percentile_for_value_from_quantiles(quantiles, float(value))

        # Coverage years: count from since_year
        coverage_years = date.today().year - since_year + 1

        first_year = since_year

        # Classify — skip coverage-years downgrade since since_year is always set
        if percentile is not None:
            severity = classify_severity(percentile, coverage_ratio=coverage_ratio)
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
            since_year=since_year,
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
            "since_year": since_year,
        }

        upsert_latest_insight(conn, row)
        logger.info(
            "Latest insight for %s w=%d: %s (%s) end_date=%s",
            station_id, window_days, severity.value, direction.value, end_date,
        )
        return row

    logger.warning("No valid window found for %s w=%d", station_id, window_days)
    return None
