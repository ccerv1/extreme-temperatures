"""Precomputed latest insights for the home page."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Query
import duckdb

from extreme_temps.api.deps import get_db
from extreme_temps.api.schemas import LatestInsightItem
from extreme_temps.db.queries import get_all_latest_insights
from extreme_temps.compute.latest_insights import compute_latest_insights_multi

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/latest", response_model=list[LatestInsightItem])
def get_latest_insights(
    window_days: int | None = Query(None, description="Filter by window size in days"),
    db: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[LatestInsightItem]:
    """Return precomputed latest insights for all stations.

    If window_days is provided, returns only insights for that window size.
    Otherwise returns all insights (multiple per station).
    """
    rows = get_all_latest_insights(db, window_days=window_days)
    return [LatestInsightItem(**row) for row in rows]


@router.post("/compute-latest")
def trigger_compute_latest(
    db: duckdb.DuckDBPyConnection = Depends(get_db),
) -> dict:
    """Trigger recomputation of latest insights for all active stations."""
    station_ids = [
        r[0]
        for r in db.execute(
            "SELECT station_id FROM dim_station WHERE is_active = TRUE ORDER BY station_id"
        ).fetchall()
    ]
    computed = 0
    errors = 0
    for sid in station_ids:
        try:
            results = compute_latest_insights_multi(db, sid)
            computed += len(results)
        except Exception:
            logger.exception("Failed to compute latest insights for %s", sid)
            errors += 1
    return {"computed": computed, "errors": errors, "total_stations": len(station_ids)}
