"""Tests for statement generation."""

from extreme_temps.compute.severity import Severity, Direction
from extreme_temps.compute.statements import generate_insight


class TestGenerateInsight:
    def test_extreme_cold(self):
        primary, supporting = generate_insight(
            window_days=7,
            value_c=-15.0,
            percentile=1.5,
            severity=Severity.EXTREME,
            direction=Direction.COLD,
            coverage_years=100,
            first_year=1924,
        )
        assert "extremely" in primary.lower()
        assert "cold" in primary.lower()
        assert "week" in primary.lower()
        assert "1924" in supporting

    def test_normal(self):
        primary, supporting = generate_insight(
            window_days=30,
            value_c=12.0,
            percentile=55.0,
            severity=Severity.NORMAL,
            direction=Direction.NEUTRAL,
            coverage_years=80,
            first_year=1944,
        )
        assert "unusual" not in primary.lower() or "not" in primary.lower()
        assert "30-day" in primary.lower()

    def test_record(self):
        primary, supporting = generate_insight(
            window_days=7,
            value_c=35.0,
            percentile=99.5,
            severity=Severity.EXTREME,
            direction=Direction.WARM,
            coverage_years=100,
            first_year=1924,
            record_info={"is_new_record": True, "record_type": "highest"},
        )
        assert "record" in primary.lower()

    def test_single_day_window(self):
        primary, _ = generate_insight(
            window_days=1,
            value_c=5.0,
            percentile=60.0,
            severity=Severity.NORMAL,
            direction=Direction.NEUTRAL,
            coverage_years=50,
            first_year=1974,
        )
        assert "day" in primary.lower()

    def test_deterministic(self):
        """Same inputs should always produce same outputs."""
        kwargs = dict(
            window_days=14,
            value_c=20.0,
            percentile=85.0,
            severity=Severity.UNUSUAL,
            direction=Direction.WARM,
            coverage_years=60,
            first_year=1964,
        )
        a = generate_insight(**kwargs)
        b = generate_insight(**kwargs)
        assert a == b

    def test_precipitation(self):
        primary, supporting = generate_insight(
            window_days=30,
            value_c=150.0,
            percentile=95.0,
            severity=Severity.VERY_UNUSUAL,
            direction=Direction.WET,
            coverage_years=70,
            first_year=1954,
        )
        assert "wet" in primary.lower()
        assert "very wet" in primary.lower()
        assert "Wetter" in supporting

    def test_very_unusual_cold_phrasing(self):
        """'very unusually cold' should become 'very cold'."""
        primary, _ = generate_insight(
            window_days=14,
            value_c=-10.0,
            percentile=5.0,
            severity=Severity.VERY_UNUSUAL,
            direction=Direction.COLD,
            coverage_years=100,
            first_year=1924,
        )
        assert primary == "This 14-day period is very cold."

    def test_unusual_warm_phrasing(self):
        primary, _ = generate_insight(
            window_days=7,
            value_c=30.0,
            percentile=80.0,
            severity=Severity.UNUSUAL,
            direction=Direction.WARM,
            coverage_years=100,
            first_year=1924,
        )
        assert primary == "This week is warm."

    def test_since_year_coverage_in_supporting(self):
        """When since_year is set, supporting line should show filtered coverage."""
        _, supporting = generate_insight(
            window_days=14,
            value_c=-7.0,
            percentile=2.0,
            severity=Severity.EXTREME,
            direction=Direction.COLD,
            coverage_years=25,
            first_year=2001,
            since_year=2001,
        )
        assert "25 years of data" in supporting
        assert "2001" in supporting
