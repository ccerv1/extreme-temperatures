"""Climatology quantile computation.

Computes percentile distributions by day-of-year for severity classification.
Generalizes the notebook's percentile-by-day-offset logic.
"""

from __future__ import annotations

from datetime import timedelta
import logging

import duckdb
import numpy as np
import pandas as pd

from extreme_temps.config import DOY_WINDOW_HALFWIDTH, WINDOW_DAYS

logger = logging.getLogger(__name__)

QUANTILES = [0.02, 0.10, 0.25, 0.50, 0.75, 0.90, 0.98]
QUANTILE_NAMES = ["p02", "p10", "p25", "p50", "p75", "p90", "p98"]


def compute_climatology_quantiles(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    metric_id: str = "tavg_c",
    window_days: int = 1,
    doy_window_halfwidth: int = DOY_WINDOW_HALFWIDTH,
) -> int:
    """Compute and store climatology quantiles for all days of year.

    For window_days == 1:
        For each DOY (1-366), gather all historical values within
        [DOY - halfwidth, DOY + halfwidth], compute quantiles.

    For window_days > 1:
        First compute rolling averages, then gather by DOY.

    Returns number of rows stored.
    """
    # Fetch all daily data for this station
    daily = conn.execute(f"""
        SELECT obs_date, {metric_id} AS value
        FROM fact_station_day
        WHERE station_id = ?
          AND {metric_id} IS NOT NULL
        ORDER BY obs_date
    """, [station_id]).fetchdf()

    if daily.empty:
        return 0

    daily["obs_date"] = pd.to_datetime(daily["obs_date"])

    if window_days > 1:
        # Compute rolling average
        daily = daily.set_index("obs_date").sort_index()
        daily["value"] = daily["value"].rolling(window_days, min_periods=window_days).mean()
        daily = daily.dropna().reset_index()

    daily["doy"] = daily["obs_date"].dt.dayofyear
    daily["year"] = daily["obs_date"].dt.year

    first_year = int(daily["year"].min())
    last_year = int(daily["year"].max())

    rows = []
    for target_doy in range(1, 367):
        # Gather values within the DOY window (wrapping around year boundary)
        mask = _doy_within_window(daily["doy"], target_doy, doy_window_halfwidth)
        subset = daily.loc[mask, "value"].dropna()

        if len(subset) < 10:
            continue

        quantile_values = np.quantile(subset.values, QUANTILES)

        row = {
            "window_days": window_days,
            "end_doy": target_doy,
            "doy_window_halfwidth": doy_window_halfwidth,
            "n_samples": len(subset),
            "first_year": first_year,
            "last_year": last_year,
        }
        for name, val in zip(QUANTILE_NAMES, quantile_values):
            row[name] = round(float(val), 4)

        rows.append(row)

    if not rows:
        return 0

    from extreme_temps.db.queries import upsert_climatology_quantiles
    df = pd.DataFrame(rows)
    count = upsert_climatology_quantiles(conn, station_id, metric_id, df)
    logger.info(
        "Stored %d climatology rows for %s/%s/w%d",
        count, station_id, metric_id, window_days,
    )
    return count


def get_percentile_for_value(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    metric_id: str,
    window_days: int,
    end_doy: int,
    value: float,
    doy_window_halfwidth: int = DOY_WINDOW_HALFWIDTH,
) -> float | None:
    """Compute the percentile of a value against stored climatology.

    Uses linear interpolation between stored quantile breakpoints.
    Returns percentile (0-100) or None if no climatology available.
    """
    from extreme_temps.db.queries import get_climatology_quantiles

    q = get_climatology_quantiles(
        conn, station_id, metric_id, window_days, end_doy, doy_window_halfwidth,
    )
    if q is None:
        return None

    # Build quantile lookup: percentile -> value
    breakpoints = [
        (2, q["p02"]),
        (10, q["p10"]),
        (25, q["p25"]),
        (50, q["p50"]),
        (75, q["p75"]),
        (90, q["p90"]),
        (98, q["p98"]),
    ]

    # Filter out None values
    breakpoints = [(p, v) for p, v in breakpoints if v is not None and pd.notna(v)]
    if not breakpoints:
        return None

    # Below lowest breakpoint
    if value <= breakpoints[0][1]:
        return breakpoints[0][0] * (value / breakpoints[0][1]) if breakpoints[0][1] != 0 else 0.0

    # Above highest breakpoint
    if value >= breakpoints[-1][1]:
        top_p = breakpoints[-1][0]
        return top_p + (100 - top_p) * 0.5  # conservative estimate

    # Linear interpolation between breakpoints
    for i in range(len(breakpoints) - 1):
        p_low, v_low = breakpoints[i]
        p_high, v_high = breakpoints[i + 1]
        if v_low <= value <= v_high:
            if v_high == v_low:
                return (p_low + p_high) / 2.0
            frac = (value - v_low) / (v_high - v_low)
            return p_low + frac * (p_high - p_low)

    return 50.0  # fallback


def compute_quantiles_for_doy(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    metric_id: str,
    window_days: int,
    end_doy: int,
    since_year: int,
    doy_window_halfwidth: int = DOY_WINDOW_HALFWIDTH,
) -> dict | None:
    """Compute quantiles on-the-fly for a single DOY, filtered by year range.

    Returns dict with p02-p98, n_samples, first_year, last_year, or None if <10 samples.
    """
    daily = conn.execute(f"""
        SELECT obs_date, {metric_id} AS value
        FROM fact_station_day
        WHERE station_id = ?
          AND {metric_id} IS NOT NULL
          AND YEAR(obs_date) >= ?
        ORDER BY obs_date
    """, [station_id, since_year]).fetchdf()

    if daily.empty:
        return None

    daily["obs_date"] = pd.to_datetime(daily["obs_date"])

    if window_days > 1:
        daily = daily.set_index("obs_date").sort_index()
        daily["value"] = daily["value"].rolling(window_days, min_periods=window_days).mean()
        daily = daily.dropna().reset_index()

    daily["doy"] = daily["obs_date"].dt.dayofyear
    daily["year"] = daily["obs_date"].dt.year

    mask = _doy_within_window(daily["doy"], end_doy, doy_window_halfwidth)
    subset = daily.loc[mask, "value"].dropna()

    if len(subset) < 10:
        return None

    quantile_values = np.quantile(subset.values, QUANTILES)
    result = {
        "n_samples": len(subset),
        "first_year": int(daily["year"].min()),
        "last_year": int(daily["year"].max()),
    }
    for name, val in zip(QUANTILE_NAMES, quantile_values):
        result[name] = round(float(val), 4)

    return result


def compute_quantiles_for_doy_range(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    metric_id: str,
    window_days: int,
    doys: list[int],
    since_year: int,
    doy_window_halfwidth: int = DOY_WINDOW_HALFWIDTH,
) -> dict[int, dict | None]:
    """Compute quantiles on-the-fly for multiple DOYs in one pass.

    Fetches data once and loops over DOYs. Returns dict keyed by DOY.
    """
    daily = conn.execute(f"""
        SELECT obs_date, {metric_id} AS value
        FROM fact_station_day
        WHERE station_id = ?
          AND {metric_id} IS NOT NULL
          AND YEAR(obs_date) >= ?
        ORDER BY obs_date
    """, [station_id, since_year]).fetchdf()

    if daily.empty:
        return {doy: None for doy in doys}

    daily["obs_date"] = pd.to_datetime(daily["obs_date"])

    if window_days > 1:
        daily = daily.set_index("obs_date").sort_index()
        daily["value"] = daily["value"].rolling(window_days, min_periods=window_days).mean()
        daily = daily.dropna().reset_index()

    daily["doy"] = daily["obs_date"].dt.dayofyear
    daily["year"] = daily["obs_date"].dt.year

    first_year = int(daily["year"].min())
    last_year = int(daily["year"].max())

    results: dict[int, dict | None] = {}
    for target_doy in doys:
        mask = _doy_within_window(daily["doy"], target_doy, doy_window_halfwidth)
        subset = daily.loc[mask, "value"].dropna()

        if len(subset) < 10:
            results[target_doy] = None
            continue

        quantile_values = np.quantile(subset.values, QUANTILES)
        row = {
            "n_samples": len(subset),
            "first_year": first_year,
            "last_year": last_year,
        }
        for name, val in zip(QUANTILE_NAMES, quantile_values):
            row[name] = round(float(val), 4)
        results[target_doy] = row

    return results


def get_percentile_for_value_from_quantiles(
    quantiles: dict,
    value: float,
) -> float | None:
    """Compute percentile of a value against a quantiles dict.

    Same interpolation logic as get_percentile_for_value() but takes
    a pre-computed quantiles dict instead of querying the DB.
    """
    breakpoints = [
        (2, quantiles.get("p02")),
        (10, quantiles.get("p10")),
        (25, quantiles.get("p25")),
        (50, quantiles.get("p50")),
        (75, quantiles.get("p75")),
        (90, quantiles.get("p90")),
        (98, quantiles.get("p98")),
    ]

    breakpoints = [(p, v) for p, v in breakpoints if v is not None and pd.notna(v)]
    if not breakpoints:
        return None

    if value <= breakpoints[0][1]:
        return breakpoints[0][0] * (value / breakpoints[0][1]) if breakpoints[0][1] != 0 else 0.0

    if value >= breakpoints[-1][1]:
        top_p = breakpoints[-1][0]
        return top_p + (100 - top_p) * 0.5

    for i in range(len(breakpoints) - 1):
        p_low, v_low = breakpoints[i]
        p_high, v_high = breakpoints[i + 1]
        if v_low <= value <= v_high:
            if v_high == v_low:
                return (p_low + p_high) / 2.0
            frac = (value - v_low) / (v_high - v_low)
            return p_low + frac * (p_high - p_low)

    return 50.0


def _doy_within_window(doy_series: pd.Series, target_doy: int, halfwidth: int) -> pd.Series:
    """Return boolean mask for DOYs within [target - halfwidth, target + halfwidth].

    Handles wrapping around year boundaries (e.g., DOY 1 is near DOY 365).
    """
    low = target_doy - halfwidth
    high = target_doy + halfwidth

    if low >= 1 and high <= 366:
        return (doy_series >= low) & (doy_series <= high)
    elif low < 1:
        # Wraps around start of year
        return (doy_series >= (low + 366)) | (doy_series <= high)
    else:
        # Wraps around end of year
        return (doy_series >= low) | (doy_series <= (high - 366))
