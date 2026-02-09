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
        assert "near normal" in primary.lower()
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
            percentile=90.0,
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
            severity=Severity.UNUSUAL,
            direction=Direction.WET,
            coverage_years=70,
            first_year=1954,
        )
        assert "wet" in primary.lower()
        assert "unusually wet" in primary.lower()
        assert "Wetter" in supporting

    def test_unusual_warm_phrasing(self):
        primary, _ = generate_insight(
            window_days=7,
            value_c=30.0,
            percentile=92.0,
            severity=Severity.UNUSUAL,
            direction=Direction.WARM,
            coverage_years=100,
            first_year=1924,
        )
        assert primary == "This week is unusually warm."

    def test_a_bit_warmer_phrasing(self):
        """A_BIT severity uses comparative form: 'a bit warmer'."""
        primary, _ = generate_insight(
            window_days=7,
            value_c=15.0,
            percentile=75.0,
            severity=Severity.A_BIT,
            direction=Direction.WARM,
            coverage_years=100,
            first_year=1924,
        )
        assert primary == "This week is a bit warmer."

    def test_a_bit_colder_phrasing(self):
        """A_BIT severity uses comparative form: 'a bit colder'."""
        primary, _ = generate_insight(
            window_days=14,
            value_c=-3.0,
            percentile=25.0,
            severity=Severity.A_BIT,
            direction=Direction.COLD,
            coverage_years=100,
            first_year=1924,
        )
        assert primary == "This 14-day period is a bit colder."

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
