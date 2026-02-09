"""Shared test fixtures."""

import pytest
import duckdb
import pandas as pd
from datetime import date

from extreme_temps.db.schema import create_all_tables


@pytest.fixture
def db():
    """In-memory DuckDB with all tables created."""
    conn = duckdb.connect(":memory:")
    create_all_tables(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_station() -> dict:
    """A sample station for testing."""
    return {
        "station_id": "USW00094728",
        "wban": "94728",
        "name": "New York City Central Park",
        "lat": 40.7789,
        "lon": -73.9692,
        "elevation_m": 47.5,
    }


@pytest.fixture
def sample_daily_df() -> pd.DataFrame:
    """Sample daily observations (10 days, Jan 1-10 2024)."""
    dates = pd.date_range("2024-01-01", periods=10, freq="D")
    return pd.DataFrame({
        "obs_date": dates.date,
        "tmin_c": [-5.0, -3.0, -2.0, 0.0, 1.0, -1.0, -4.0, -6.0, -3.0, -1.0],
        "tmax_c": [2.0, 4.0, 5.0, 7.0, 8.0, 6.0, 3.0, 1.0, 4.0, 6.0],
        "tavg_c": [-1.5, 0.5, 1.5, 3.5, 4.5, 2.5, -0.5, -2.5, 0.5, 2.5],
        "prcp_mm": [0.0, 2.5, 0.0, 5.0, 0.0, 1.0, 0.0, 0.0, 3.0, 0.0],
    })
