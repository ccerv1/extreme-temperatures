"""Precomputed latest insights for the home page."""

from __future__ import annotations

from fastapi import APIRouter, Depends
import duckdb

from extreme_temps.api.deps import get_db
from extreme_temps.api.schemas import LatestInsightItem
from extreme_temps.db.queries import get_all_latest_insights

router = APIRouter()


@router.get("/latest", response_model=list[LatestInsightItem])
def get_latest_insights(
    db: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[LatestInsightItem]:
    """Return precomputed latest insights for all stations."""
    rows = get_all_latest_insights(db)
    return [LatestInsightItem(**row) for row in rows]
