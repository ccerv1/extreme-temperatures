"""Rolling window computation.

Computes rolling averages/sums over configurable window sizes and stores
results in fact_station_window_recent.
"""

from __future__ import annotations

from datetime import date, timedelta
import logging

import duckdb
import pandas as pd
import numpy as np

from extreme_temps.config import WINDOW_DAYS

logger = logging.getLogger(__name__)


def compute_rolling_window(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    end_date: date,
    window_days: int,
    metric: str = "tavg_c",
) -> dict | None:
    """Compute a single rolling window value for one station/date/window.

    Returns dict with: start_date, end_date, value, coverage_ratio
    or None if insufficient data.
    """
    start_date = end_date - timedelta(days=window_days - 1)

    result = conn.execute(f"""
        SELECT
            AVG({metric}) AS value,
            COUNT({metric}) AS n_valid,
            ? AS n_expected
        FROM fact_station_day
        WHERE station_id = ?
          AND obs_date BETWEEN ? AND ?
    """, [window_days, station_id, start_date, end_date]).fetchone()

    if result is None or result[1] == 0:
        return None

    value, n_valid, n_expected = result
    coverage = n_valid / n_expected

    return {
        "start_date": start_date,
        "end_date": end_date,
        "value": round(value, 4) if value is not None else None,
        "coverage_ratio": round(coverage, 4),
    }


def compute_recent_windows(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    lookback_days: int = 400,
) -> int:
    """Compute all rolling windows for recent dates and store in DB.

    Computes for each (window_days, end_date) combination over the last
    lookback_days. Returns count of rows stored.
    """
    _, last_date = conn.execute(
        "SELECT MIN(obs_date), MAX(obs_date) FROM fact_station_day WHERE station_id = ?",
        [station_id],
    ).fetchone()

    if last_date is None:
        return 0

    # Normalize to date (DuckDB may return Timestamp or date)
    if hasattr(last_date, 'date'):
        last_date = last_date.date()

    earliest = last_date - timedelta(days=lookback_days)

    # Fetch all daily data in one shot
    daily = conn.execute("""
        SELECT obs_date, tavg_c, tmin_c, tmax_c, prcp_mm
        FROM fact_station_day
        WHERE station_id = ? AND obs_date >= ?
        ORDER BY obs_date
    """, [station_id, earliest - timedelta(days=max(WINDOW_DAYS))]).fetchdf()

    if daily.empty:
        return 0

    daily = daily.set_index("obs_date").sort_index()

    rows = []
    for w in WINDOW_DAYS:
        tavg_roll = daily["tavg_c"].rolling(w, min_periods=1).mean()
        tmin_roll = daily["tmin_c"].rolling(w, min_periods=1).mean()
        tmax_roll = daily["tmax_c"].rolling(w, min_periods=1).mean()
        prcp_roll = daily["prcp_mm"].rolling(w, min_periods=1).sum()
        count_roll = daily["tavg_c"].rolling(w, min_periods=1).count()

        for end_dt in daily.index:
            end_dt_date = end_dt.date() if hasattr(end_dt, 'date') else end_dt
            if end_dt_date < earliest:
                continue
            start_dt = end_dt_date - timedelta(days=w - 1)
            rows.append({
                "window_days": w,
                "end_date": end_dt_date,
                "start_date": start_dt,
                "tavg_c_mean": round(tavg_roll[end_dt], 4) if pd.notna(tavg_roll[end_dt]) else None,
                "tmin_c_mean": round(tmin_roll[end_dt], 4) if pd.notna(tmin_roll[end_dt]) else None,
                "tmax_c_mean": round(tmax_roll[end_dt], 4) if pd.notna(tmax_roll[end_dt]) else None,
                "prcp_mm_sum": round(prcp_roll[end_dt], 4) if pd.notna(prcp_roll[end_dt]) else None,
                "coverage_ratio": round(count_roll[end_dt] / w, 4),
            })

    if not rows:
        return 0

    from extreme_temps.db.queries import upsert_window_aggregates
    df = pd.DataFrame(rows)
    count = upsert_window_aggregates(conn, station_id, df)
    logger.info("Stored %d recent window rows for %s", count, station_id)
    return count


def find_all_time_extremes(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    metric: str = "tavg_c",
) -> list[dict]:
    """Find all-time extreme windows (hottest/coldest) for every window size.

    Returns list of record dicts suitable for upsert_station_records.
    """
    daily = conn.execute("""
        SELECT obs_date, {} as value
        FROM fact_station_day
        WHERE station_id = ?
        ORDER BY obs_date
    """.format(metric), [station_id]).fetchdf()

    if daily.empty:
        return []

    daily = daily.set_index("obs_date").sort_index()
    first_year = daily.index.min().year
    last_year = daily.index.max().year
    n_years = last_year - first_year + 1

    records = []
    for w in WINDOW_DAYS:
        if len(daily) < w:
            continue

        rolling = daily["value"].rolling(w, min_periods=w).mean()
        rolling = rolling.dropna()

        if rolling.empty:
            continue

        # Highest
        max_idx = rolling.idxmax()
        max_val = rolling[max_idx]
        records.append({
            "metric_id": metric,
            "window_days": w,
            "record_type": "highest",
            "value": round(float(max_val), 4),
            "start_date": max_idx - timedelta(days=w - 1),
            "end_date": max_idx,
            "n_years_considered": n_years,
        })

        # Lowest
        min_idx = rolling.idxmin()
        min_val = rolling[min_idx]
        records.append({
            "metric_id": metric,
            "window_days": w,
            "record_type": "lowest",
            "value": round(float(min_val), 4),
            "start_date": min_idx - timedelta(days=w - 1),
            "end_date": min_idx,
            "n_years_considered": n_years,
        })

    return records
