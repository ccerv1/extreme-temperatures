"""Rankings endpoints — seasonal and all-time extremes."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
import duckdb

from extreme_temps.api.deps import get_db
from extreme_temps.api.schemas import SeasonalRankingResponse, ExtremesRankingResponse
from extreme_temps.compute.rankings import compute_seasonal_rankings, compute_extremes_rankings

router = APIRouter()


@router.get("/seasonal", response_model=SeasonalRankingResponse)
def get_seasonal_rankings(
    station_id: str = Query(...),
    end_date: date = Query(...),
    window_days: int = Query(7, ge=1, le=365),
    metric: str = Query("tavg_c"),
    since_year: int | None = Query(None, ge=1850, le=2100),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
) -> SeasonalRankingResponse:
    """Rank the current period against the same time of year across history."""
    result = compute_seasonal_rankings(
        db, station_id, end_date, window_days, metric, since_year,
    )
    if result is None:
        raise HTTPException(404, "Insufficient data for seasonal rankings")
    return SeasonalRankingResponse(**result)


@router.get("/extremes", response_model=ExtremesRankingResponse)
def get_extremes_rankings(
    station_id: str = Query(...),
    end_date: date = Query(...),
    window_days: int = Query(7, ge=1, le=365),
    metric: str = Query("tavg_c"),
    direction: str = Query("cold"),
    since_year: int | None = Query(None, ge=1850, le=2100),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
) -> ExtremesRankingResponse:
    """Rank the current period against the most extreme period from any time in each year."""
    if direction not in ("cold", "warm"):
        raise HTTPException(400, "direction must be 'cold' or 'warm'")
    result = compute_extremes_rankings(
        db, station_id, end_date, window_days, metric, direction, since_year,
    )
    if result is None:
        raise HTTPException(404, "Insufficient data for extremes rankings")
    return ExtremesRankingResponse(**result)
