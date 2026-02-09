"""Tests for typed query functions."""

from datetime import date

import pandas as pd

from extreme_temps.db.queries import (
    upsert_station,
    get_station,
    update_station_coverage,
    find_nearby_stations,
    upsert_daily_observations,
    get_daily_observations,
    get_station_date_range,
    upsert_window_aggregates,
    upsert_climatology_quantiles,
    get_climatology_quantiles,
    upsert_station_records,
    get_station_records,
)


# ---------------------------------------------------------------------------
# dim_station
# ---------------------------------------------------------------------------

class TestStationQueries:
    def test_upsert_and_get_station(self, db, sample_station):
        upsert_station(db, sample_station)
        result = get_station(db, "USW00094728")
        assert result is not None
        assert result["name"] == "New York City Central Park"
        assert result["lat"] == 40.7789
        assert result["is_active"] is True

    def test_get_nonexistent_station(self, db):
        result = get_station(db, "NONEXISTENT")
        assert result is None

    def test_upsert_overwrites(self, db, sample_station):
        upsert_station(db, sample_station)
        sample_station["name"] = "Updated Name"
        upsert_station(db, sample_station)
        result = get_station(db, "USW00094728")
        assert result["name"] == "Updated Name"

    def test_update_station_coverage(self, db, sample_station, sample_daily_df):
        upsert_station(db, sample_station)
        upsert_daily_observations(db, "USW00094728", sample_daily_df)
        update_station_coverage(db, "USW00094728")

        station = get_station(db, "USW00094728")
        assert pd.Timestamp(station["first_obs_date"]).date() == date(2024, 1, 1)
        assert pd.Timestamp(station["last_obs_date"]).date() == date(2024, 1, 10)
        assert station["completeness_temp_pct"] == 100.0  # all rows have tavg_c

    def test_find_nearby_stations(self, db, sample_station):
        upsert_station(db, sample_station)
        # Search from a point 10km away from Central Park
        results = find_nearby_stations(db, lat=40.85, lon=-73.95, radius_km=50.0)
        assert len(results) == 1
        assert results[0]["station_id"] == "USW00094728"
        assert results[0]["distance_km"] < 10

    def test_find_nearby_no_results(self, db, sample_station):
        upsert_station(db, sample_station)
        # Search from London — should find nothing within 50km
        results = find_nearby_stations(db, lat=51.5, lon=-0.12, radius_km=50.0)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# fact_station_day
# ---------------------------------------------------------------------------

class TestDailyObservationQueries:
    def test_upsert_and_get(self, db, sample_daily_df):
        count = upsert_daily_observations(db, "USW00094728", sample_daily_df)
        assert count == 10

        result = get_daily_observations(db, "USW00094728", date(2024, 1, 1), date(2024, 1, 10))
        assert len(result) == 10
        assert result.iloc[0]["tavg_c"] == -1.5

    def test_upsert_empty_df(self, db):
        empty = pd.DataFrame(columns=["obs_date", "tmin_c", "tmax_c", "tavg_c", "prcp_mm"])
        count = upsert_daily_observations(db, "USW00094728", empty)
        assert count == 0

    def test_source_column(self, db, sample_daily_df):
        upsert_daily_observations(db, "USW00094728", sample_daily_df, source="gsod")
        result = get_daily_observations(db, "USW00094728", date(2024, 1, 1), date(2024, 1, 10))
        assert all(result["source"] == "gsod")

    def test_upsert_replaces_existing(self, db, sample_daily_df):
        upsert_daily_observations(db, "USW00094728", sample_daily_df)

        # Upsert a single overlapping day with different value
        update = pd.DataFrame({
            "obs_date": [date(2024, 1, 1)],
            "tmin_c": [-10.0],
            "tmax_c": [10.0],
            "tavg_c": [0.0],
            "prcp_mm": [0.0],
        })
        upsert_daily_observations(db, "USW00094728", update)

        result = get_daily_observations(db, "USW00094728", date(2024, 1, 1), date(2024, 1, 1))
        assert result.iloc[0]["tavg_c"] == 0.0
        assert result.iloc[0]["tmin_c"] == -10.0

    def test_date_range_filter(self, db, sample_daily_df):
        upsert_daily_observations(db, "USW00094728", sample_daily_df)
        result = get_daily_observations(db, "USW00094728", date(2024, 1, 3), date(2024, 1, 5))
        assert len(result) == 3

    def test_get_station_date_range(self, db, sample_daily_df):
        upsert_daily_observations(db, "USW00094728", sample_daily_df)
        first, last = get_station_date_range(db, "USW00094728")
        assert first == date(2024, 1, 1)
        assert last == date(2024, 1, 10)

    def test_get_station_date_range_empty(self, db):
        first, last = get_station_date_range(db, "NONEXISTENT")
        assert first is None
        assert last is None


# ---------------------------------------------------------------------------
# fact_station_window_recent
# ---------------------------------------------------------------------------

class TestWindowAggregateQueries:
    def test_upsert_window_aggregates(self, db):
        df = pd.DataFrame({
            "window_days": [7, 7, 30],
            "end_date": [date(2024, 1, 7), date(2024, 1, 8), date(2024, 1, 30)],
            "start_date": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 1)],
            "tavg_c_mean": [1.0, 1.5, 2.0],
            "tmin_c_mean": [-2.0, -1.5, -1.0],
            "tmax_c_mean": [4.0, 4.5, 5.0],
            "prcp_mm_sum": [10.0, 8.0, 25.0],
            "coverage_ratio": [1.0, 1.0, 0.97],
        })
        count = upsert_window_aggregates(db, "USW00094728", df)
        assert count == 3

        result = db.execute(
            "SELECT * FROM fact_station_window_recent WHERE station_id = 'USW00094728'"
        ).fetchdf()
        assert len(result) == 3


# ---------------------------------------------------------------------------
# dim_climatology_quantiles
# ---------------------------------------------------------------------------

class TestClimatologyQueries:
    def test_upsert_and_get_quantiles(self, db):
        df = pd.DataFrame({
            "window_days": [1, 1],
            "end_doy": [1, 2],
            "doy_window_halfwidth": [7, 7],
            "p02": [-15.0, -14.0],
            "p10": [-10.0, -9.0],
            "p25": [-5.0, -4.0],
            "p50": [0.0, 1.0],
            "p75": [5.0, 6.0],
            "p90": [10.0, 11.0],
            "p98": [15.0, 16.0],
            "n_samples": [100, 100],
            "first_year": [1900, 1900],
            "last_year": [2023, 2023],
        })
        count = upsert_climatology_quantiles(db, "USW00094728", "tavg_c", df)
        assert count == 2

        result = get_climatology_quantiles(db, "USW00094728", "tavg_c", 1, 1, 7)
        assert result is not None
        assert result["p50"] == 0.0
        assert result["n_samples"] == 100

    def test_get_nonexistent_quantiles(self, db):
        result = get_climatology_quantiles(db, "FAKE", "tavg_c", 1, 1, 7)
        assert result is None


# ---------------------------------------------------------------------------
# dim_station_records
# ---------------------------------------------------------------------------

class TestRecordQueries:
    def test_upsert_and_get_records(self, db):
        records = [
            {
                "metric_id": "tavg_c",
                "window_days": 7,
                "record_type": "highest",
                "value": 35.2,
                "start_date": date(2023, 7, 10),
                "end_date": date(2023, 7, 16),
                "n_years_considered": 100,
            },
            {
                "metric_id": "tavg_c",
                "window_days": 7,
                "record_type": "lowest",
                "value": -20.1,
                "start_date": date(1934, 2, 5),
                "end_date": date(1934, 2, 11),
                "n_years_considered": 100,
            },
        ]
        count = upsert_station_records(db, "USW00094728", records)
        assert count == 2

        result = get_station_records(db, "USW00094728", "tavg_c")
        assert len(result) == 2

    def test_get_records_all_metrics(self, db):
        records = [
            {
                "metric_id": "tavg_c",
                "window_days": 1,
                "record_type": "highest",
                "value": 40.0,
                "start_date": date(2023, 7, 20),
                "end_date": date(2023, 7, 20),
                "n_years_considered": 80,
            },
            {
                "metric_id": "tmax_c",
                "window_days": 1,
                "record_type": "highest",
                "value": 42.0,
                "start_date": date(2023, 7, 20),
                "end_date": date(2023, 7, 20),
                "n_years_considered": 80,
            },
        ]
        upsert_station_records(db, "USW00094728", records)
        result = get_station_records(db, "USW00094728")
        assert len(result) == 2

    def test_upsert_empty_records(self, db):
        count = upsert_station_records(db, "USW00094728", [])
        assert count == 0
