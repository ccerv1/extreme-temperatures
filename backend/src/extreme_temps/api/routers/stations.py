"""Station discovery endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
import duckdb

from extreme_temps.api.deps import get_db
from extreme_temps.api.schemas import StationResponse
from extreme_temps.db.queries import find_nearby_stations, get_station

router = APIRouter()


@router.get("/nearby", response_model=list[StationResponse])
def get_nearby(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    radius_km: float = Query(50.0, description="Search radius in km"),
    limit: int = Query(10, ge=1, le=50),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[StationResponse]:
    """Find weather stations near a coordinate."""
    rows = find_nearby_stations(db, lat, lon, radius_km, limit)
    return [StationResponse(**_clean_station(r)) for r in rows]


@router.get("/{station_id}", response_model=StationResponse)
def get_station_detail(
    station_id: str,
    db: duckdb.DuckDBPyConnection = Depends(get_db),
) -> StationResponse:
    """Get details for a specific station."""
    station = get_station(db, station_id)
    if station is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Station {station_id} not found")
    return StationResponse(**_clean_station(station))


def _clean_station(row: dict) -> dict:
    """Convert DB row to schema-compatible dict, handling pandas/numpy types."""
    import pandas as pd
    import math
    cleaned = {}
    for k, v in row.items():
        # Catch pd.NA, pd.NaT, np.nan, None — must come before type checks
        try:
            is_missing = v is None or pd.isna(v)
        except (ValueError, TypeError):
            is_missing = False
        if is_missing:
            cleaned[k] = None
        elif isinstance(v, pd.Timestamp):
            cleaned[k] = v.date()
        elif hasattr(v, 'item'):  # numpy scalar
            cleaned[k] = v.item()
        else:
            cleaned[k] = v
    return cleaned
