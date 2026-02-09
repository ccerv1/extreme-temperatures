"""FastAPI application factory."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from extreme_temps.db.connection import get_connection
from extreme_temps.db.schema import create_all_tables


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize DuckDB on startup, close on shutdown."""
    conn = get_connection()
    create_all_tables(conn)
    app.state.db = conn
    yield
    conn.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Extreme Temperatures API",
        version="0.2.0",
        description="Historical weather extremes analysis",
        lifespan=lifespan,
    )

    # CORS for frontend dev
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from extreme_temps.api.routers import stations, insights, series, records, rankings, latest_insights

    app.include_router(stations.router, prefix="/stations", tags=["stations"])
    app.include_router(insights.router, prefix="/insights", tags=["insights"])
    app.include_router(latest_insights.router, prefix="/insights", tags=["insights"])
    app.include_router(series.router, prefix="/series", tags=["series"])
    app.include_router(records.router, prefix="/records", tags=["records"])
    app.include_router(rankings.router, prefix="/rankings", tags=["rankings"])

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app
