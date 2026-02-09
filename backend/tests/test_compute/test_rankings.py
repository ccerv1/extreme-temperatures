"""Tests for historical ranking computation."""

from datetime import date

import pandas as pd
import numpy as np

from extreme_temps.db.queries import upsert_daily_observations
from extreme_temps.compute.rankings import compute_seasonal_rankings, compute_extremes_rankings


def _seed_multi_year_data(db, station_id: str, n_years: int = 30):
    """Insert n_years of synthetic daily data with seasonal pattern + noise."""
    all_rows = []
    for year in range(2024 - n_years, 2024):
        dates = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
        doy = dates.dayofyear
        np.random.seed(year)
        noise = np.random.normal(0, 3, len(dates))
        tavg = 10 + 15 * np.sin((doy - 80) * 2 * np.pi / 365) + noise
        tmin = tavg - 5
        tmax = tavg + 5

        df = pd.DataFrame({
            "obs_date": dates.date,
            "tmin_c": tmin.round(2),
            "tmax_c": tmax.round(2),
            "tavg_c": tavg.round(2),
            "prcp_mm": np.maximum(0, np.random.normal(2, 3, len(dates))).round(2),
        })
        all_rows.append(df)

    combined = pd.concat(all_rows, ignore_index=True)
    upsert_daily_observations(db, station_id, combined)
    return combined


class TestComputeSeasonalRankings:
    def test_basic_ranking(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        result = compute_seasonal_rankings(
            db, "TEST001", end_date=date(2023, 7, 15),
            window_days=7, metric="tavg_c",
        )
        assert result is not None
        assert "rankings" in result
        assert "current_rank" in result
        assert "total_years" in result
        assert "direction" in result
        assert len(result["rankings"]) > 0
        # Rankings should be sorted by value
        values = [r["value_c"] for r in result["rankings"]]
        assert values == sorted(values) or values == sorted(values, reverse=True)

    def test_ranking_has_current_year(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        result = compute_seasonal_rankings(
            db, "TEST001", end_date=date(2023, 7, 15),
            window_days=7, metric="tavg_c",
        )
        current_entries = [r for r in result["rankings"] if r.get("is_current")]
        assert len(current_entries) == 1
        assert current_entries[0]["year"] == 2023

    def test_since_year_filter(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        result_all = compute_seasonal_rankings(
            db, "TEST001", end_date=date(2023, 7, 15),
            window_days=7, metric="tavg_c",
        )
        result_filtered = compute_seasonal_rankings(
            db, "TEST001", end_date=date(2023, 7, 15),
            window_days=7, metric="tavg_c", since_year=2014,
        )
        assert result_filtered["total_years"] < result_all["total_years"]
        assert result_filtered["total_years"] <= 10

    def test_ranking_fields(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        result = compute_seasonal_rankings(
            db, "TEST001", end_date=date(2023, 7, 15),
            window_days=14, metric="tavg_c",
        )
        entry = result["rankings"][0]
        assert "rank" in entry
        assert "year" in entry
        assert "value_c" in entry
        assert "value_f" in entry
        assert "delta_f" in entry


class TestComputeExtremesRankings:
    def test_basic_cold(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        result = compute_extremes_rankings(
            db, "TEST001", end_date=date(2023, 7, 15),
            window_days=7, metric="tavg_c", direction="cold",
        )
        assert result is not None
        assert "rankings" in result
        assert "current_rank" in result
        assert len(result["rankings"]) > 0
        # Cold direction: sorted ascending (coldest first)
        values = [r["value_c"] for r in result["rankings"]]
        assert values == sorted(values)

    def test_basic_warm(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        result = compute_extremes_rankings(
            db, "TEST001", end_date=date(2023, 7, 15),
            window_days=7, metric="tavg_c", direction="warm",
        )
        assert result is not None
        values = [r["value_c"] for r in result["rankings"]]
        assert values == sorted(values, reverse=True)

    def test_has_dates(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        result = compute_extremes_rankings(
            db, "TEST001", end_date=date(2023, 7, 15),
            window_days=14, metric="tavg_c", direction="cold",
        )
        entry = result["rankings"][0]
        assert "start_date" in entry
        assert "end_date" in entry

    def test_since_year_filter(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        result_all = compute_extremes_rankings(
            db, "TEST001", end_date=date(2023, 7, 15),
            window_days=7, metric="tavg_c", direction="cold",
        )
        result_filtered = compute_extremes_rankings(
            db, "TEST001", end_date=date(2023, 7, 15),
            window_days=7, metric="tavg_c", direction="cold",
            since_year=2014,
        )
        assert result_filtered["total_years"] < result_all["total_years"]

    def test_current_year_marked(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        result = compute_extremes_rankings(
            db, "TEST001", end_date=date(2023, 7, 15),
            window_days=7, metric="tavg_c", direction="cold",
        )
        current = [r for r in result["rankings"] if r.get("is_current")]
        assert len(current) == 1
        assert current[0]["year"] == 2023
