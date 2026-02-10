"""Management endpoints for data ingestion and maintenance."""

from __future__ import annotations

import logging
import threading
import time

from fastapi import APIRouter, Depends, Request
import duckdb

from extreme_temps.api.deps import get_db
from extreme_temps.ingest.orchestrator import ingest_all_stations_incremental
from extreme_temps.compute.latest_insights import compute_latest_insights_multi

logger = logging.getLogger(__name__)
router = APIRouter()

# Simple in-memory status for the background refresh job
_refresh_status: dict = {"running": False, "last_result": None}
_refresh_lock = threading.Lock()


def _run_refresh(conn: duckdb.DuckDBPyConnection) -> None:
    """Background worker: ingest + recompute (runs in a separate thread)."""
    global _refresh_status
    t0 = time.time()
    try:
        cursor = conn.cursor()

        # Step 1: Incremental ingest
        results = ingest_all_stations_incremental(cursor)
        total_rows = sum(r.rows_inserted for r in results)
        ingest_errors = [
            {"station_id": r.station_id, "errors": r.errors}
            for r in results if r.errors
        ]
        t1 = time.time()
        logger.info(
            "Ingest complete: %d stations, %d rows, %d errors in %.0fs",
            len(results), total_rows, len(ingest_errors), t1 - t0,
        )

        # Step 2: Recompute latest insights (all window sizes)
        station_ids = [r.station_id for r in results]
        computed = 0
        compute_errors = 0
        for sid in station_ids:
            try:
                rows = compute_latest_insights_multi(cursor, sid)
                computed += len(rows)
            except Exception:
                logger.exception("Failed to compute latest insights for %s", sid)
                compute_errors += 1
        t2 = time.time()
        logger.info(
            "Compute complete: %d insights, %d errors in %.0fs",
            computed, compute_errors, t2 - t1,
        )

        cursor.close()

        with _refresh_lock:
            _refresh_status["last_result"] = {
                "status": "completed",
                "ingest": {
                    "stations": len(results),
                    "rows_inserted": total_rows,
                    "errors": ingest_errors,
                    "duration_s": round(t1 - t0, 1),
                },
                "compute": {
                    "insights_computed": computed,
                    "errors": compute_errors,
                    "duration_s": round(t2 - t1, 1),
                },
                "total_duration_s": round(t2 - t0, 1),
            }
    except Exception:
        logger.exception("Refresh failed")
        with _refresh_lock:
            _refresh_status["last_result"] = {
                "status": "failed",
                "error": "See server logs",
                "duration_s": round(time.time() - t0, 1),
            }
    finally:
        with _refresh_lock:
            _refresh_status["running"] = False


@router.post("/refresh")
def trigger_refresh(request: Request) -> dict:
    """Start background data refresh (ingest + recompute).

    Returns immediately. Poll GET /manage/refresh-status for progress.
    """
    with _refresh_lock:
        if _refresh_status["running"]:
            return {"status": "already_running"}
        _refresh_status["running"] = True
        _refresh_status["last_result"] = None

    conn = request.app.state.db
    thread = threading.Thread(target=_run_refresh, args=(conn,), daemon=True)
    thread.start()

    return {"status": "started"}


@router.get("/refresh-status")
def get_refresh_status() -> dict:
    """Check the status of the last refresh job."""
    with _refresh_lock:
        return {
            "running": _refresh_status["running"],
            "last_result": _refresh_status["last_result"],
        }


@router.get("/last-updated")
def get_last_updated(db: duckdb.DuckDBPyConnection = Depends(get_db)) -> dict:
    """Return the timestamp of the most recent data computation."""
    row = db.execute(
        "SELECT MAX(computed_at) AS last_updated FROM fact_station_latest_insight"
    ).fetchone()
    return {"last_updated": row[0].isoformat() if row and row[0] else None}
