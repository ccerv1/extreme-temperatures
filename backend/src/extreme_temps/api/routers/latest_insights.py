"""Precomputed latest insights for the home page."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, BackgroundTasks
import duckdb

from extreme_temps.api.deps import get_db
from extreme_temps.api.schemas import LatestInsightItem
from extreme_temps.db.queries import get_all_latest_insights
from extreme_temps.compute.latest_insights import compute_latest_insight

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/latest", response_model=list[LatestInsightItem])
def get_latest_insights(
    db: duckdb.DuckDBPyConnection = Depends(get_db),
) -> list[LatestInsightItem]:
    """Return precomputed latest insights for all stations."""
    rows = get_all_latest_insights(db)
    return [LatestInsightItem(**row) for row in rows]


@router.post("/compute-latest")
def trigger_compute_latest(
    background_tasks: BackgroundTasks,
    db: duckdb.DuckDBPyConnection = Depends(get_db),
) -> dict:
    """Trigger recomputation of latest insights for all active stations."""
    station_ids = [
        r[0]
        for r in db.execute(
            "SELECT station_id FROM dim_station WHERE is_active = TRUE ORDER BY station_id"
        ).fetchall()
    ]
    # Run synchronously so the cursor stays open
    computed = 0
    errors = 0
    for sid in station_ids:
        try:
            result = compute_latest_insight(db, sid)
            if result:
                computed += 1
        except Exception:
            logger.exception("Failed to compute latest insight for %s", sid)
            errors += 1
    return {"computed": computed, "errors": errors, "total_stations": len(station_ids)}
