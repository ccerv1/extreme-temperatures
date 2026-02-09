"""Tests for precomputed latest insights."""

from datetime import date

import duckdb
import numpy as np
import pandas as pd
import pytest

from extreme_temps.db.schema import create_all_tables
from extreme_temps.db.queries import (
    upsert_station,
    upsert_daily_observations,
    upsert_latest_insight,
    get_all_latest_insights,
    update_station_coverage,
)
from extreme_temps.compute.climatology import compute_climatology_quantiles
from extreme_temps.compute.latest_insights import compute_latest_insight


@pytest.fixture
def db_with_station():
    """In-memory DB with a station and 10 years of daily data."""
    conn = duckdb.connect(":memory:")
    create_all_tables(conn)

    upsert_station(conn, {
        "station_id": "USW00094728",
        "wban": "94728",
        "name": "New York City Central Park",
        "lat": 40.7789,
        "lon": -73.9692,
        "elevation_m": 47.5,
    })

    # 10 years of synthetic daily data
    all_rows = []
    for year in range(2014, 2024):
        dates = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
        np.random.seed(year)
        doy = dates.dayofyear
        noise = np.random.normal(0, 3, len(dates))
        tavg = 10 + 15 * np.sin((doy - 80) * 2 * np.pi / 365) + noise
        df = pd.DataFrame({
            "obs_date": dates.date,
            "tmin_c": (tavg - 5).round(2),
            "tmax_c": (tavg + 5).round(2),
            "tavg_c": tavg.round(2),
            "prcp_mm": np.maximum(0, np.random.normal(2, 3, len(dates))).round(2),
        })
        all_rows.append(df)

    combined = pd.concat(all_rows, ignore_index=True)
    upsert_daily_observations(conn, "USW00094728", combined)
    update_station_coverage(conn, "USW00094728")

    # Compute climatology for window=3 (used by compute_latest_insight)
    compute_climatology_quantiles(conn, "USW00094728", "tavg_c", 3, 7)

    yield conn
    conn.close()


class TestComputeLatestInsight:
    def test_returns_valid_dict(self, db_with_station):
        result = compute_latest_insight(db_with_station, "USW00094728")
        assert result is not None
        assert result["station_id"] == "USW00094728"
        assert result["window_days"] == 3
        assert result["metric"] == "tavg_c"
        assert result["severity"] in ["normal", "unusual", "very_unusual", "extreme", "insufficient_data"]
        assert result["direction"] in ["warm", "cold", "neutral"]
        assert result["primary_statement"]
        assert result["supporting_line"]
        assert isinstance(result["end_date"], date)

    def test_returns_none_for_missing_station(self, db_with_station):
        result = compute_latest_insight(db_with_station, "NONEXISTENT")
        assert result is None

    def test_stores_in_db(self, db_with_station):
        compute_latest_insight(db_with_station, "USW00094728")
        rows = get_all_latest_insights(db_with_station)
        assert len(rows) == 1
        assert rows[0]["station_id"] == "USW00094728"

    def test_upsert_replaces(self, db_with_station):
        compute_latest_insight(db_with_station, "USW00094728")
        compute_latest_insight(db_with_station, "USW00094728")
        rows = get_all_latest_insights(db_with_station)
        assert len(rows) == 1  # Should still be just one row


class TestLatestInsightQueries:
    def test_upsert_and_get(self, db):
        row = {
            "station_id": "TEST001",
            "end_date": date(2024, 1, 10),
            "window_days": 3,
            "metric": "tavg_c",
            "value": -1.5,
            "percentile": 15.2,
            "severity": "unusual",
            "direction": "cold",
            "primary_statement": "This 3-day period is cold.",
            "supporting_line": "Colder than 85% of historical 3-day periods.",
            "coverage_years": 10,
            "first_year": 2014,
        }
        upsert_latest_insight(db, row)
        results = get_all_latest_insights(db)
        assert len(results) == 1
        assert results[0]["station_id"] == "TEST001"
        assert results[0]["severity"] == "unusual"

    def test_empty_table(self, db):
        results = get_all_latest_insights(db)
        assert results == []
