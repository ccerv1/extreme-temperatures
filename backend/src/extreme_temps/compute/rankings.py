"""Historical ranking computation.

Computes per-year rankings for seasonal and all-time extreme comparisons.
"""

from __future__ import annotations

from datetime import date, timedelta
import logging

import duckdb
import numpy as np
import pandas as pd

from extreme_temps.config import DOY_WINDOW_HALFWIDTH

logger = logging.getLogger(__name__)


def _c_to_f(c: float) -> float:
    return round(c * 9 / 5 + 32, 1)


def compute_seasonal_rankings(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    end_date: date,
    window_days: int,
    metric: str = "tavg_c",
    since_year: int | None = None,
    doy_window_halfwidth: int = DOY_WINDOW_HALFWIDTH,
) -> dict | None:
    """Rank the current period against the same time of year in every historical year.

    For each year in the range, computes the rolling N-day average for the DOY
    matching end_date (using DOY +/- halfwidth to find the best match).
    Returns a ranked list sorted by value.

    Args:
        conn: DuckDB connection.
        station_id: Station identifier.
        end_date: End date of the current period.
        window_days: Rolling window size.
        metric: Metric column name.
        since_year: If set, only include years >= this.
        doy_window_halfwidth: DOY smoothing window half-width.

    Returns:
        Dict with rankings list, current_rank, total_years, direction.
        None if insufficient data.
    """
    target_doy = end_date.timetuple().tm_yday
    current_year = end_date.year

    # Fetch all daily data
    params = [station_id]
    year_filter = ""
    if since_year is not None:
        year_filter = "AND YEAR(obs_date) >= ?"
        params.append(since_year)

    daily = conn.execute(f"""
        SELECT obs_date, {metric} AS value
        FROM fact_station_day
        WHERE station_id = ?
          AND {metric} IS NOT NULL
          {year_filter}
        ORDER BY obs_date
    """, params).fetchdf()

    if daily.empty:
        return None

    daily["obs_date"] = pd.to_datetime(daily["obs_date"])

    # Compute rolling average if window > 1
    if window_days > 1:
        daily = daily.set_index("obs_date").sort_index()
        daily["value"] = daily["value"].rolling(window_days, min_periods=window_days).mean()
        daily = daily.dropna().reset_index()

    daily["doy"] = daily["obs_date"].dt.dayofyear
    daily["year"] = daily["obs_date"].dt.year

    # For each year, find the value closest to the target DOY
    # Use the DOY window to get the best representative value
    per_year = []
    for year, group in daily.groupby("year"):
        # Find values within DOY window of target
        doy_diff = (group["doy"] - target_doy).abs()
        # Handle year wrap (e.g., target DOY 5, actual DOY 362 -> diff should be 9, not 357)
        doy_diff = doy_diff.where(doy_diff <= 183, 366 - doy_diff)
        within_window = group[doy_diff <= doy_window_halfwidth]

        if within_window.empty:
            continue

        # Pick the value with DOY closest to target
        closest_idx = doy_diff[within_window.index].idxmin()
        value = within_window.loc[closest_idx, "value"]
        per_year.append({
            "year": int(year),
            "value_c": round(float(value), 2),
        })

    if not per_year:
        return None

    # Determine direction from current year's percentile position
    current_entry = next((e for e in per_year if e["year"] == current_year), None)
    if current_entry is None:
        return None

    current_value = current_entry["value_c"]
    values = [e["value_c"] for e in per_year]
    below_count = sum(1 for v in values if v < current_value)
    percentile = below_count / len(values) * 100

    # Sort direction: if cold (low percentile), sort ascending (coldest first)
    ascending = percentile <= 50
    direction = "cold" if percentile <= 50 else "warm"
    per_year.sort(key=lambda e: e["value_c"], reverse=not ascending)

    # Add rank, Fahrenheit values, and delta
    current_value_f = _c_to_f(current_value)
    for i, entry in enumerate(per_year):
        entry["rank"] = i + 1
        entry["value_f"] = _c_to_f(entry["value_c"])
        entry["delta_f"] = round(entry["value_f"] - current_value_f, 1)
        entry["is_current"] = entry["year"] == current_year

    current_rank = next(e["rank"] for e in per_year if e["is_current"])

    return {
        "rankings": per_year,
        "current_rank": current_rank,
        "total_years": len(per_year),
        "direction": direction,
    }


def compute_extremes_rankings(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    end_date: date,
    window_days: int,
    metric: str = "tavg_c",
    direction: str = "cold",
    since_year: int | None = None,
    doy_window_halfwidth: int = DOY_WINDOW_HALFWIDTH,
) -> dict | None:
    """Rank the current period against the most extreme N-day period for this season in each year.

    For each year, finds the single most extreme (min for cold, max for warm)
    rolling N-day average within the same seasonal DOY window as end_date.

    Args:
        conn: DuckDB connection.
        station_id: Station identifier.
        end_date: End date of the current period.
        window_days: Rolling window size.
        metric: Metric column name.
        direction: "cold" (find yearly minimums) or "warm" (find yearly maximums).
        since_year: If set, only include years >= this.
        doy_window_halfwidth: Half-width for seasonal DOY filtering.

    Returns:
        Dict with rankings list (including dates), current_rank, total_years, direction.
        None if insufficient data.
    """
    current_year = end_date.year
    target_doy = end_date.timetuple().tm_yday

    params = [station_id]
    year_filter = ""
    if since_year is not None:
        year_filter = "AND YEAR(obs_date) >= ?"
        params.append(since_year)

    daily = conn.execute(f"""
        SELECT obs_date, {metric} AS value
        FROM fact_station_day
        WHERE station_id = ?
          AND {metric} IS NOT NULL
          {year_filter}
        ORDER BY obs_date
    """, params).fetchdf()

    if daily.empty:
        return None

    daily["obs_date"] = pd.to_datetime(daily["obs_date"])
    daily = daily.set_index("obs_date").sort_index()

    # Compute rolling average
    if window_days > 1:
        daily["value"] = daily["value"].rolling(window_days, min_periods=window_days).mean()
        daily = daily.dropna()

    # Filter to same seasonal DOY window
    daily["doy"] = daily.index.dayofyear
    doy_diff = (daily["doy"] - target_doy).abs()
    doy_diff = doy_diff.where(doy_diff <= 183, 366 - doy_diff)
    daily = daily[doy_diff <= doy_window_halfwidth]

    if daily.empty:
        return None

    daily["year"] = daily.index.year

    # For each year, find the most extreme period within the seasonal window
    per_year = []
    for year, group in daily.groupby("year"):
        if direction == "cold":
            extreme_idx = group["value"].idxmin()
        else:
            extreme_idx = group["value"].idxmax()

        extreme_val = group.loc[extreme_idx, "value"]
        extreme_end = extreme_idx
        extreme_start = extreme_end - timedelta(days=window_days - 1)

        per_year.append({
            "year": int(year),
            "value_c": round(float(extreme_val), 2),
            "start_date": extreme_start.strftime("%Y-%m-%d") if hasattr(extreme_start, "strftime") else str(extreme_start),
            "end_date": extreme_end.strftime("%Y-%m-%d") if hasattr(extreme_end, "strftime") else str(extreme_end),
        })

    if not per_year:
        return None

    # Sort: cold = ascending (coldest first), warm = descending (hottest first)
    ascending = direction == "cold"
    per_year.sort(key=lambda e: e["value_c"], reverse=not ascending)

    # Find current entry
    current_entry = next((e for e in per_year if e["year"] == current_year), None)
    if current_entry is None:
        return None

    current_value_f = _c_to_f(current_entry["value_c"])

    # Add rank, Fahrenheit, delta
    for i, entry in enumerate(per_year):
        entry["rank"] = i + 1
        entry["value_f"] = _c_to_f(entry["value_c"])
        entry["delta_f"] = round(entry["value_f"] - current_value_f, 1)
        entry["is_current"] = entry["year"] == current_year

    current_rank = next(e["rank"] for e in per_year if e["is_current"])

    return {
        "rankings": per_year,
        "current_rank": current_rank,
        "total_years": len(per_year),
        "direction": direction,
    }
