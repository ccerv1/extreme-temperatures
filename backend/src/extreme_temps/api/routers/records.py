"""Records endpoint."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
import duckdb
import pandas as pd

from extreme_temps.api.deps import get_db
from extreme_temps.api.schemas import RecordResponse
from extreme_temps.db.queries import get_station_records

router = APIRouter()


@router.get("/", response_model=list[RecordResponse])
def get_records(
    station_id: str = Query(...),
    metric: str = Query("tavg_c"),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[RecordResponse]:
    """Return all-time records for a station across all window sizes."""
    records_df = get_station_records(db, station_id, metric)

    if records_df.empty:
        return []

    results = []
    for _, row in records_df.iterrows():
        results.append(RecordResponse(
            station_id=station_id,
            metric=metric,
            window_days=int(row["window_days"]),
            record_type=row["record_type"],
            value=round(float(row["value"]), 2),
            start_date=pd.Timestamp(row["start_date"]).date() if isinstance(row["start_date"], pd.Timestamp) else row["start_date"],
            end_date=pd.Timestamp(row["end_date"]).date() if isinstance(row["end_date"], pd.Timestamp) else row["end_date"],
            n_years=int(row["n_years_considered"]),
        ))

    return results
