"""Tests for climatology quantile computation."""

from datetime import date

import pandas as pd
import numpy as np

from extreme_temps.db.queries import upsert_daily_observations
from extreme_temps.compute.climatology import (
    compute_climatology_quantiles,
    get_percentile_for_value,
    compute_quantiles_for_doy,
    compute_quantiles_for_doy_range,
    get_percentile_for_value_from_quantiles,
    _doy_within_window,
)


def _seed_multi_year_data(db, station_id: str, n_years: int = 30):
    """Insert n_years of synthetic daily data with seasonal pattern + noise."""
    all_rows = []
    for year in range(2024 - n_years, 2024):
        dates = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")
        doy = dates.dayofyear
        # Seasonal pattern + random noise
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


class TestDoyWithinWindow:
    def test_normal_range(self):
        doy = pd.Series([1, 5, 10, 15, 20])
        mask = _doy_within_window(doy, target_doy=10, halfwidth=5)
        assert mask.tolist() == [False, True, True, True, False]

    def test_wrap_start_of_year(self):
        doy = pd.Series([360, 365, 1, 3, 10])
        mask = _doy_within_window(doy, target_doy=1, halfwidth=3)
        # Should include DOY 364-366 and 1-4
        assert mask.tolist() == [False, True, True, True, False]

    def test_wrap_end_of_year(self):
        doy = pd.Series([360, 364, 365, 1, 5])
        mask = _doy_within_window(doy, target_doy=365, halfwidth=3)
        # Should include 362-366 and 1-2
        assert mask.tolist() == [False, True, True, True, False]


class TestComputeClimatologyQuantiles:
    def test_basic_computation(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)

        count = compute_climatology_quantiles(
            db, "TEST001", metric_id="tavg_c", window_days=1, doy_window_halfwidth=7
        )

        # Should have ~366 rows (one per DOY, some may be skipped if < 10 samples)
        assert count >= 360
        assert count <= 366

    def test_quantile_ordering(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        compute_climatology_quantiles(db, "TEST001", "tavg_c", 1, 7)

        # Check that quantiles are properly ordered for a mid-year DOY
        q = db.execute("""
            SELECT p02, p10, p25, p50, p75, p90, p98
            FROM dim_climatology_quantiles
            WHERE station_id = 'TEST001' AND metric_id = 'tavg_c'
              AND window_days = 1 AND end_doy = 180
        """).fetchone()

        assert q is not None
        # p02 <= p10 <= p25 <= p50 <= p75 <= p90 <= p98
        for i in range(len(q) - 1):
            assert q[i] <= q[i + 1], f"Quantile ordering violated at position {i}"

    def test_summer_warmer_than_winter(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        compute_climatology_quantiles(db, "TEST001", "tavg_c", 1, 7)

        # DOY ~180 (July) should have higher p50 than DOY ~1 (January)
        summer = db.execute("""
            SELECT p50 FROM dim_climatology_quantiles
            WHERE station_id = 'TEST001' AND end_doy = 180 AND window_days = 1
        """).fetchone()
        winter = db.execute("""
            SELECT p50 FROM dim_climatology_quantiles
            WHERE station_id = 'TEST001' AND end_doy = 1 AND window_days = 1
        """).fetchone()

        assert summer[0] > winter[0]


class TestGetPercentileForValue:
    def test_median_value(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        compute_climatology_quantiles(db, "TEST001", "tavg_c", 1, 7)

        q = db.execute("""
            SELECT p50 FROM dim_climatology_quantiles
            WHERE station_id = 'TEST001' AND end_doy = 180 AND window_days = 1
        """).fetchone()
        median_val = q[0]

        pct = get_percentile_for_value(db, "TEST001", "tavg_c", 1, 180, median_val)
        assert pct is not None
        assert 45 < pct < 55  # Should be near 50

    def test_no_climatology(self, db):
        pct = get_percentile_for_value(db, "NONEXIST", "tavg_c", 1, 180, 20.0)
        assert pct is None


class TestComputeQuantilesForDoy:
    def test_basic(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        result = compute_quantiles_for_doy(db, "TEST001", "tavg_c", 1, 180, since_year=1994)
        assert result is not None
        assert "p02" in result
        assert "p50" in result
        assert "p98" in result
        assert result["n_samples"] >= 10
        assert result["p02"] <= result["p50"] <= result["p98"]

    def test_filtered_vs_all(self, db):
        """Filtering to recent years should give different results than all years."""
        _seed_multi_year_data(db, "TEST001", n_years=30)
        all_years = compute_quantiles_for_doy(db, "TEST001", "tavg_c", 1, 180, since_year=1994)
        recent = compute_quantiles_for_doy(db, "TEST001", "tavg_c", 1, 180, since_year=2014)
        assert all_years is not None
        assert recent is not None
        assert recent["n_samples"] < all_years["n_samples"]

    def test_insufficient_data(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        # since_year in the future -> no data
        result = compute_quantiles_for_doy(db, "TEST001", "tavg_c", 1, 180, since_year=2030)
        assert result is None

    def test_rolling_window(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        result = compute_quantiles_for_doy(db, "TEST001", "tavg_c", 7, 180, since_year=1994)
        assert result is not None
        assert result["p02"] <= result["p50"] <= result["p98"]


class TestGetPercentileForValueFromQuantiles:
    def test_median(self):
        q = {"p02": 0.0, "p10": 5.0, "p25": 10.0, "p50": 15.0, "p75": 20.0, "p90": 25.0, "p98": 30.0}
        pct = get_percentile_for_value_from_quantiles(q, 15.0)
        assert pct is not None
        assert 49 < pct < 51

    def test_below_range(self):
        q = {"p02": 0.0, "p10": 5.0, "p25": 10.0, "p50": 15.0, "p75": 20.0, "p90": 25.0, "p98": 30.0}
        pct = get_percentile_for_value_from_quantiles(q, -5.0)
        assert pct is not None
        assert pct < 2

    def test_interpolation(self):
        q = {"p02": 0.0, "p10": 10.0, "p25": 20.0, "p50": 30.0, "p75": 40.0, "p90": 50.0, "p98": 60.0}
        # Midpoint between p25 (20.0) and p50 (30.0) should give ~37.5
        pct = get_percentile_for_value_from_quantiles(q, 25.0)
        assert pct is not None
        assert 35 < pct < 40


class TestComputeQuantilesForDoyRange:
    def test_multiple_doys(self, db):
        _seed_multi_year_data(db, "TEST001", n_years=30)
        doys = [1, 90, 180, 270]
        results = compute_quantiles_for_doy_range(db, "TEST001", "tavg_c", 1, doys, since_year=1994)
        assert len(results) == 4
        for doy in doys:
            assert doy in results
            assert results[doy] is not None
            assert results[doy]["p02"] <= results[doy]["p98"]

    def test_consistency_with_single_doy(self, db):
        """Batch and single should give same result for the same DOY."""
        _seed_multi_year_data(db, "TEST001", n_years=30)
        single = compute_quantiles_for_doy(db, "TEST001", "tavg_c", 1, 180, since_year=1994)
        batch = compute_quantiles_for_doy_range(db, "TEST001", "tavg_c", 1, [180], since_year=1994)
        assert single is not None
        assert batch[180] is not None
        assert single["p50"] == batch[180]["p50"]
        assert single["n_samples"] == batch[180]["n_samples"]
