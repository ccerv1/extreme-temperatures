"""Tests for the ingestion orchestrator."""

from datetime import date
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from extreme_temps.db.schema import create_all_tables
from extreme_temps.db.queries import upsert_station, get_station, get_daily_observations
from extreme_temps.ingest.orchestrator import (
    ingest_station_full,
    ingest_station_incremental,
)


@pytest.fixture
def db_with_station(db, sample_station):
    """DB with schema and one station seeded."""
    upsert_station(db, sample_station)
    return db


def _fake_ghcn_df() -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=5, freq="D")
    return pd.DataFrame({
        "obs_date": dates.date,
        "tmin_c": [-5.0, -3.0, -2.0, 0.0, 1.0],
        "tmax_c": [2.0, 4.0, 5.0, 7.0, 8.0],
        "tavg_c": [-1.5, 0.5, 1.5, 3.5, 4.5],
        "prcp_mm": [0.0, 2.5, 0.0, 5.0, 0.0],
    })


@patch("extreme_temps.ingest.orchestrator.fetch_ghcn_daily")
def test_ingest_full_basic(mock_fetch, db_with_station):
    mock_fetch.return_value = _fake_ghcn_df()

    result = ingest_station_full(db_with_station, "USW00094728")

    assert result.rows_inserted == 5
    assert result.source == "ghcn_daily"
    assert len(result.errors) == 0

    # Verify data landed in DB
    obs = get_daily_observations(db_with_station, "USW00094728", date(2024, 1, 1), date(2024, 1, 5))
    assert len(obs) == 5


@patch("extreme_temps.ingest.orchestrator.fetch_ghcn_daily")
def test_ingest_full_updates_coverage(mock_fetch, db_with_station):
    mock_fetch.return_value = _fake_ghcn_df()

    ingest_station_full(db_with_station, "USW00094728")

    station = get_station(db_with_station, "USW00094728")
    assert station["last_ingest_at"] is not None


@patch("extreme_temps.ingest.orchestrator.fetch_ghcn_daily")
def test_ingest_full_empty_data(mock_fetch, db_with_station):
    mock_fetch.return_value = pd.DataFrame(columns=["obs_date", "tmin_c", "tmax_c", "tavg_c", "prcp_mm"])

    result = ingest_station_full(db_with_station, "USW00094728")

    assert result.rows_inserted == 0
    assert "No GHCN Daily data" in result.errors[0]


@patch("extreme_temps.ingest.orchestrator.fetch_ghcn_daily")
def test_ingest_incremental(mock_fetch, db_with_station):
    # First: seed some existing data
    from extreme_temps.db.queries import upsert_daily_observations
    initial = _fake_ghcn_df()
    upsert_daily_observations(db_with_station, "USW00094728", initial)

    # Incremental fetch returns 3 new days
    new_data = pd.DataFrame({
        "obs_date": [date(2024, 1, 6), date(2024, 1, 7), date(2024, 1, 8)],
        "tmin_c": [2.0, 3.0, 1.0],
        "tmax_c": [9.0, 10.0, 8.0],
        "tavg_c": [5.5, 6.5, 4.5],
        "prcp_mm": [0.0, 0.0, 1.0],
    })
    mock_fetch.return_value = new_data

    result = ingest_station_incremental(db_with_station, "USW00094728")

    assert result.rows_inserted == 3

    # Total should be 8
    obs = get_daily_observations(db_with_station, "USW00094728", date(2024, 1, 1), date(2024, 1, 8))
    assert len(obs) == 8


@patch("extreme_temps.ingest.orchestrator.fetch_ghcn_daily")
def test_ingest_incremental_station_not_found(mock_fetch, db):
    result = ingest_station_incremental(db, "NONEXISTENT")

    assert "not found" in result.errors[0]
    mock_fetch.assert_not_called()
