"""FastAPI dependency injection."""

from __future__ import annotations

from typing import Generator

from fastapi import Request
import duckdb


def get_db(request: Request) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Provide a per-request DuckDB cursor (thread-safe)."""
    cursor = request.app.state.db.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
