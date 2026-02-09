"""API endpoint tests using TestClient with in-memory DuckDB."""

from datetime import date, timedelta

import duckdb
import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from extreme_temps.api.app import create_app
from extreme_temps.db.schema import create_all_tables
from extreme_temps.db.queries import (
    upsert_station,
    upsert_daily_observations,
    upsert_station_records,
    update_station_coverage,
)
from extreme_temps.compute.climatology import compute_climatology_quantiles


@pytest.fixture
def app_with_data():
    """Create a FastAPI app with in-memory DB and seeded test data."""
    app = create_app()

    # Override with in-memory DB
    conn = duckdb.connect(":memory:")
    create_all_tables(conn)
    app.state.db = conn

    # Seed station
    upsert_station(conn, {
        "station_id": "USW00094728",
        "wban": "94728",
        "name": "New York City Central Park",
        "lat": 40.7789,
        "lon": -73.9692,
        "elevation_m": 47.5,
    })

    # Seed 10 years of daily data (enough for climatology)
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

    # Compute climatology for window=1 and window=7
    compute_climatology_quantiles(conn, "USW00094728", "tavg_c", 1, 7)
    compute_climatology_quantiles(conn, "USW00094728", "tavg_c", 3, 7)
    compute_climatology_quantiles(conn, "USW00094728", "tavg_c", 7, 7)

    # Seed some records
    upsert_station_records(conn, "USW00094728", [
        {
            "metric_id": "tavg_c",
            "window_days": 7,
            "record_type": "highest",
            "value": 32.0,
            "start_date": date(2023, 7, 15),
            "end_date": date(2023, 7, 21),
            "n_years_considered": 10,
        },
        {
            "metric_id": "tavg_c",
            "window_days": 7,
            "record_type": "lowest",
            "value": -15.0,
            "start_date": date(2015, 2, 10),
            "end_date": date(2015, 2, 16),
            "n_years_considered": 10,
        },
    ])

    client = TestClient(app, raise_server_exceptions=True)
    yield client
    conn.close()


class TestHealthEndpoint:
    def test_health(self, app_with_data):
        resp = app_with_data.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


class TestStationsEndpoints:
    def test_nearby(self, app_with_data):
        resp = app_with_data.get("/stations/nearby", params={"lat": 40.78, "lon": -73.97})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["station_id"] == "USW00094728"
        assert data[0]["distance_km"] < 5

    def test_nearby_no_results(self, app_with_data):
        resp = app_with_data.get("/stations/nearby", params={"lat": 0.0, "lon": 0.0})
        assert resp.status_code == 200
        assert resp.json() == []

    def test_station_detail(self, app_with_data):
        resp = app_with_data.get("/stations/USW00094728")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "New York City Central Park"

    def test_station_not_found(self, app_with_data):
        resp = app_with_data.get("/stations/NONEXISTENT")
        assert resp.status_code == 404


class TestInsightsEndpoint:
    def test_basic_insight(self, app_with_data):
        resp = app_with_data.get("/insights/window", params={
            "station_id": "USW00094728",
            "end_date": "2023-07-15",
            "window_days": 7,
            "metric": "tavg_c",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["station_id"] == "USW00094728"
        assert data["window_days"] == 7
        assert data["primary_statement"]
        assert data["supporting_line"]
        assert data["severity"] in ["normal", "a_bit", "unusual", "extreme", "insufficient_data"]
        assert data["data_quality"]["coverage_years"] >= 1

    def test_insight_station_not_found(self, app_with_data):
        resp = app_with_data.get("/insights/window", params={
            "station_id": "NONEXISTENT",
            "end_date": "2023-07-15",
            "window_days": 7,
        })
        assert resp.status_code == 404


class TestRecordsEndpoint:
    def test_get_records(self, app_with_data):
        resp = app_with_data.get("/records/", params={
            "station_id": "USW00094728",
            "metric": "tavg_c",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2  # highest + lowest
        types = {r["record_type"] for r in data}
        assert types == {"highest", "lowest"}

    def test_records_empty(self, app_with_data):
        resp = app_with_data.get("/records/", params={
            "station_id": "USW00094728",
            "metric": "prcp_mm",  # no records seeded for this
        })
        assert resp.status_code == 200
        assert resp.json() == []


class TestSeriesEndpoint:
    def test_basic_series(self, app_with_data):
        resp = app_with_data.get("/series/window", params={
            "station_id": "USW00094728",
            "window_days": 7,
            "metric": "tavg_c",
            "start_date": "2023-07-01",
            "end_date": "2023-07-31",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["station_id"] == "USW00094728"
        assert data["window_days"] == 7
        assert len(data["series"]) > 0
        # Each point should have a value and climatology bands
        point = data["series"][0]
        assert "value" in point
        assert "p25" in point
        assert "p75" in point

    def test_series_no_data(self, app_with_data):
        resp = app_with_data.get("/series/window", params={
            "station_id": "USW00094728",
            "window_days": 7,
            "metric": "tavg_c",
            "start_date": "1900-01-01",
            "end_date": "1900-01-31",
        })
        assert resp.status_code == 404


class TestLatestInsightsEndpoint:
    def test_empty(self, app_with_data):
        resp = app_with_data.get("/insights/latest")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_with_precomputed_data(self, app_with_data):
        from extreme_temps.compute.latest_insights import compute_latest_insights_multi
        app = app_with_data.app
        compute_latest_insights_multi(app.state.db, "USW00094728")

        resp = app_with_data.get("/insights/latest")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1  # Multiple windows per station
        assert data[0]["station_id"] == "USW00094728"
        assert data[0]["severity"] in ["normal", "a_bit", "unusual", "extreme", "insufficient_data"]
        assert data[0]["primary_statement"]
        assert data[0]["direction"] in ["warm", "cold", "neutral"]

    def test_filter_by_window_days(self, app_with_data):
        from extreme_temps.compute.latest_insights import compute_latest_insights_multi
        app = app_with_data.app
        compute_latest_insights_multi(app.state.db, "USW00094728")

        resp = app_with_data.get("/insights/latest", params={"window_days": 7})
        assert resp.status_code == 200
        data = resp.json()
        for item in data:
            assert item["window_days"] == 7
