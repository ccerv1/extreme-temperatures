"""Time series endpoint for charting."""

from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
import duckdb
import pandas as pd

from extreme_temps.api.deps import get_db
from extreme_temps.api.schemas import SeriesResponse, SeriesPoint
from extreme_temps.db.queries import get_climatology_quantiles
from extreme_temps.compute.climatology import (
    compute_quantiles_for_doy_range,
    get_percentile_for_value_from_quantiles,
)

router = APIRouter()


@router.get("/window", response_model=SeriesResponse)
def get_window_series(
    station_id: str = Query(...),
    window_days: int = Query(7, ge=1, le=365),
    metric: str = Query("tavg_c"),
    start_date: date = Query(...),
    end_date: date = Query(...),
    since_year: int | None = Query(None, ge=1850, le=2100),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
) -> SeriesResponse:
    """Return rolling time series with climatology bands for charting."""
    # Fetch daily data with enough lookback for the rolling window
    lookback_start = start_date - timedelta(days=window_days - 1)

    daily = db.execute(f"""
        SELECT obs_date, {metric} AS value
        FROM fact_station_day
        WHERE station_id = ?
          AND obs_date BETWEEN ? AND ?
        ORDER BY obs_date
    """, [station_id, lookback_start, end_date]).fetchdf()

    if daily.empty:
        raise HTTPException(404, "No data available for this range")

    daily["obs_date"] = pd.to_datetime(daily["obs_date"])
    daily = daily.set_index("obs_date").sort_index()

    # Compute rolling average
    rolling = daily["value"].rolling(window_days, min_periods=window_days).mean()

    # Collect DOYs we need climatology for
    needed_doys: list[int] = []
    for dt in pd.date_range(start_date, end_date, freq="D"):
        if dt in rolling.index and not pd.isna(rolling[dt]):
            needed_doys.append(dt.timetuple().tm_yday)

    # Batch-fetch on-the-fly quantiles if since_year is set
    if since_year is not None and needed_doys:
        unique_doys = list(set(needed_doys))
        doy_quantiles = compute_quantiles_for_doy_range(
            db, station_id, metric, window_days, unique_doys, since_year,
        )
    else:
        doy_quantiles = None

    # Build series points with climatology bands
    points = []
    for dt in pd.date_range(start_date, end_date, freq="D"):
        if dt not in rolling.index:
            continue

        val = rolling[dt]
        if pd.isna(val):
            continue

        dt_date = dt.date()
        doy = dt.timetuple().tm_yday

        if doy_quantiles is not None:
            q = doy_quantiles.get(doy)
        else:
            q = get_climatology_quantiles(db, station_id, metric, window_days, doy)

        pctl = None
        if q is not None:
            pctl = get_percentile_for_value_from_quantiles(q, float(val))
            if pctl is not None:
                pctl = round(pctl, 1)

        points.append(SeriesPoint(
            end_date=dt_date,
            value=round(float(val), 2),
            percentile=pctl,
            p10=q["p10"] if q else None,
            p25=q["p25"] if q else None,
            p50=q["p50"] if q else None,
            p75=q["p75"] if q else None,
            p90=q["p90"] if q else None,
        ))

    return SeriesResponse(
        station_id=station_id,
        window_days=window_days,
        metric=metric,
        series=points,
        since_year=since_year,
    )
