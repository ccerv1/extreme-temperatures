"""DuckDB connection management."""

import duckdb
from pathlib import Path

from extreme_temps.config import DB_PATH


def get_connection(db_path: Path | str = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Return a file-backed DuckDB connection."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def get_memory_connection() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection (for testing)."""
    return duckdb.connect(":memory:")
