"""Tests for rolling window computation."""

from datetime import date, timedelta

import pandas as pd

from extreme_temps.db.queries import upsert_daily_observations
from extreme_temps.compute.rolling_windows import (
    compute_rolling_window,
    find_all_time_extremes,
)


def _seed_daily_data(db, station_id: str, n_days: int = 365, start: date = date(2024, 1, 1)):
    """Insert n_days of synthetic daily data with a seasonal pattern."""
    import numpy as np
    dates = pd.date_range(start, periods=n_days, freq="D")
    # Sinusoidal pattern: cold in Jan, hot in Jul
    doy = dates.dayofyear
    tavg = 10 + 15 * np.sin((doy - 80) * 2 * 3.14159 / 365)
    tmin = tavg - 5
    tmax = tavg + 5

    df = pd.DataFrame({
        "obs_date": dates.date,
        "tmin_c": tmin.round(2),
        "tmax_c": tmax.round(2),
        "tavg_c": tavg.round(2),
        "prcp_mm": [0.0] * n_days,
    })
    upsert_daily_observations(db, station_id, df)
    return df


class TestComputeRollingWindow:
    def test_single_day_window(self, db):
        _seed_daily_data(db, "TEST001", n_days=10)

        result = compute_rolling_window(db, "TEST001", date(2024, 1, 5), window_days=1)

        assert result is not None
        assert result["coverage_ratio"] == 1.0
        assert result["start_date"] == date(2024, 1, 5)
        assert result["end_date"] == date(2024, 1, 5)

    def test_seven_day_window(self, db):
        _seed_daily_data(db, "TEST001", n_days=10)

        result = compute_rolling_window(db, "TEST001", date(2024, 1, 7), window_days=7)

        assert result is not None
        assert result["start_date"] == date(2024, 1, 1)
        assert result["end_date"] == date(2024, 1, 7)
        assert result["coverage_ratio"] == 1.0

    def test_partial_coverage(self, db):
        # Only 3 days of data but requesting 7-day window
        _seed_daily_data(db, "TEST001", n_days=3)

        result = compute_rolling_window(db, "TEST001", date(2024, 1, 3), window_days=7)

        assert result is not None
        assert result["coverage_ratio"] < 1.0

    def test_no_data(self, db):
        result = compute_rolling_window(db, "NONEXIST", date(2024, 1, 7), window_days=7)
        assert result is None


class TestFindAllTimeExtremes:
    def test_finds_hottest_and_coldest(self, db):
        _seed_daily_data(db, "TEST001", n_days=365)

        records = find_all_time_extremes(db, "TEST001", "tavg_c")

        # Should have 2 records per window size (highest + lowest)
        window_sizes_with_data = [w for w in [1, 3, 5, 7, 10, 14, 21, 28, 30, 45, 60, 75, 90, 180, 365] if w <= 365]
        assert len(records) == 2 * len(window_sizes_with_data)

        # Check that highest > lowest for 7-day window
        w7 = [r for r in records if r["window_days"] == 7]
        highest = [r for r in w7 if r["record_type"] == "highest"][0]
        lowest = [r for r in w7 if r["record_type"] == "lowest"][0]
        assert highest["value"] > lowest["value"]

    def test_no_data(self, db):
        records = find_all_time_extremes(db, "NONEXIST", "tavg_c")
        assert records == []
