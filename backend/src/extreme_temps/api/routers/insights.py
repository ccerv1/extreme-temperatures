"""Insight endpoint — the core product output."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
import duckdb
import pandas as pd

from extreme_temps.api.deps import get_db
from extreme_temps.api.schemas import (
    InsightResponse,
    NormalBand,
    DataQuality,
    RecordInfo,
)
from extreme_temps.compute.rolling_windows import compute_rolling_window
from extreme_temps.compute.climatology import (
    get_percentile_for_value,
    compute_quantiles_for_doy,
    get_percentile_for_value_from_quantiles,
)
from extreme_temps.compute.severity import classify_severity, classify_direction
from extreme_temps.compute.records import check_record_proximity
from extreme_temps.compute.statements import generate_insight
from extreme_temps.db.queries import get_climatology_quantiles, get_station

router = APIRouter()


@router.get("/window", response_model=InsightResponse)
def get_window_insight(
    station_id: str = Query(...),
    end_date: date = Query(...),
    window_days: int = Query(7, ge=1, le=365),
    metric: str = Query("tavg_c"),
    since_year: int | None = Query(None, ge=1850, le=2100),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
) -> InsightResponse:
    """Get a severity-classified insight for a rolling window.

    Orchestrates: rolling value -> percentile -> severity -> statement.
    """
    station = get_station(db, station_id)
    if station is None:
        raise HTTPException(404, f"Station {station_id} not found")

    # 1. Compute the rolling window value
    window = compute_rolling_window(db, station_id, end_date, window_days, metric)
    if window is None:
        raise HTTPException(404, "No data available for this window")

    value = window["value"]
    coverage_ratio = window["coverage_ratio"]

    # 2. Look up climatology and compute percentile
    end_doy = end_date.timetuple().tm_yday

    if since_year is not None:
        # On-the-fly computation for filtered year range
        quantiles = compute_quantiles_for_doy(
            db, station_id, metric, window_days, end_doy, since_year,
        )
        if quantiles is not None:
            percentile = get_percentile_for_value_from_quantiles(quantiles, value)
        else:
            percentile = None

        # Compute filtered coverage: count distinct years with data in range
        filtered_stats = db.execute("""
            SELECT COUNT(DISTINCT YEAR(obs_date)) as n_years,
                   MIN(YEAR(obs_date)) as first_year
            FROM fact_station_day
            WHERE station_id = ?
              AND YEAR(obs_date) >= ?
        """, [station_id, since_year]).fetchone()
        if filtered_stats and filtered_stats[0]:
            coverage_years_override = int(filtered_stats[0])
            first_year_override = int(filtered_stats[1])
        else:
            coverage_years_override = 0
            first_year_override = since_year
    else:
        # Default: use precomputed climatology
        percentile = get_percentile_for_value(db, station_id, metric, window_days, end_doy, value)
        quantiles = get_climatology_quantiles(db, station_id, metric, window_days, end_doy)
        coverage_years_override = None
        first_year_override = None

    # 3. Get quantile band for normal range
    normal_band = None
    n_samples = None
    first_year = station.get("first_obs_date")
    if first_year is not None and not (isinstance(first_year, float) and pd.isna(first_year)):
        first_year_int = first_year.year if hasattr(first_year, 'year') else int(first_year)
    else:
        first_year_int = 2000

    if quantiles:
        normal_band = NormalBand(p25=quantiles["p25"], p75=quantiles["p75"])
        n_samples = quantiles.get("n_samples")
        fy = quantiles.get("first_year")
        if fy is not None:
            first_year_int = int(fy)

    # 4. Classify severity
    coverage_years = station.get("coverage_years")
    if coverage_years is not None and pd.notna(coverage_years):
        coverage_years = int(coverage_years)
    else:
        coverage_years = 0

    # Apply filtered overrides when since_year is active
    if coverage_years_override is not None:
        coverage_years = coverage_years_override
    if first_year_override is not None:
        first_year_int = first_year_override

    if percentile is not None:
        # Skip coverage-years downgrade when since_year is explicitly set —
        # the user intentionally chose that comparison window.
        cov_years_for_severity = None if since_year is not None else coverage_years
        severity = classify_severity(percentile, cov_years_for_severity, coverage_ratio=coverage_ratio)
        direction = classify_direction(percentile, metric)
    else:
        from extreme_temps.compute.severity import Severity, Direction
        severity = Severity.INSUFFICIENT_DATA
        direction = Direction.NEUTRAL
        percentile = None

    # 5. Check for records
    record_info = None
    if value is not None:
        rec = check_record_proximity(db, station_id, metric, window_days, value)
        if rec:
            record_info = RecordInfo(**rec)

    # 6. Generate statements
    primary, supporting = generate_insight(
        window_days=window_days,
        value_c=value,
        percentile=percentile or 50.0,
        severity=severity,
        direction=direction,
        coverage_years=coverage_years,
        first_year=first_year_int,
        record_info=rec if rec else None,
        since_year=since_year,
    )

    return InsightResponse(
        station_id=station_id,
        end_date=end_date,
        window_days=window_days,
        metric=metric,
        primary_statement=primary,
        supporting_line=supporting,
        value=round(value, 2) if value is not None else None,
        severity=severity.value,
        percentile=round(percentile, 1) if percentile is not None else None,
        normal_band=normal_band,
        data_quality=DataQuality(
            coverage_years=coverage_years,
            first_year=first_year_int,
            coverage_ratio=coverage_ratio,
            n_samples=n_samples,
            since_year=since_year,
        ),
        record_info=record_info,
        since_year=since_year,
    )
