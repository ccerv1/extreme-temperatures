"""Tests for severity classification."""

from extreme_temps.compute.severity import (
    Severity,
    Direction,
    classify_severity,
    classify_direction,
)


class TestClassifySeverity:
    def test_normal_range(self):
        assert classify_severity(50.0) == Severity.NORMAL
        assert classify_severity(30.0) == Severity.NORMAL
        assert classify_severity(70.0) == Severity.NORMAL

    def test_unusual_range(self):
        assert classify_severity(15.0) == Severity.UNUSUAL
        assert classify_severity(80.0) == Severity.UNUSUAL
        assert classify_severity(24.9) == Severity.UNUSUAL

    def test_very_unusual_range(self):
        assert classify_severity(5.0) == Severity.VERY_UNUSUAL
        assert classify_severity(95.0) == Severity.VERY_UNUSUAL
        assert classify_severity(2.0) == Severity.VERY_UNUSUAL
        assert classify_severity(9.9) == Severity.VERY_UNUSUAL

    def test_boundary_values_go_to_more_severe(self):
        # At exact boundaries, value goes to the more severe bucket
        assert classify_severity(25.0) == Severity.UNUSUAL
        assert classify_severity(75.0) == Severity.UNUSUAL
        assert classify_severity(10.0) == Severity.VERY_UNUSUAL
        assert classify_severity(90.0) == Severity.VERY_UNUSUAL

    def test_extreme_range(self):
        assert classify_severity(1.0) == Severity.EXTREME
        assert classify_severity(99.0) == Severity.EXTREME
        assert classify_severity(0.0) == Severity.EXTREME
        assert classify_severity(100.0) == Severity.EXTREME
        assert classify_severity(1.9) == Severity.EXTREME

    def test_boundary_exact_2(self):
        # Exactly at 2 should be VERY_UNUSUAL, not EXTREME
        assert classify_severity(2.0) == Severity.VERY_UNUSUAL

    def test_boundary_exact_98(self):
        assert classify_severity(98.0) == Severity.VERY_UNUSUAL

    def test_downgrade_with_low_coverage(self):
        # With only 20 years (< 30 min), extreme should downgrade
        assert classify_severity(1.0, coverage_years=20) == Severity.VERY_UNUSUAL
        assert classify_severity(5.0, coverage_years=20) == Severity.UNUSUAL
        assert classify_severity(15.0, coverage_years=20) == Severity.NORMAL

    def test_no_downgrade_with_sufficient_coverage(self):
        assert classify_severity(1.0, coverage_years=50) == Severity.EXTREME

    def test_normal_not_downgraded_further(self):
        assert classify_severity(50.0, coverage_years=10) == Severity.NORMAL

    def test_downgrade_with_low_coverage_ratio(self):
        # With only 2/7 days of data (0.29 < 0.5), downgrade by one level
        assert classify_severity(1.0, coverage_ratio=0.29) == Severity.VERY_UNUSUAL
        assert classify_severity(5.0, coverage_ratio=0.29) == Severity.UNUSUAL
        assert classify_severity(15.0, coverage_ratio=0.29) == Severity.NORMAL

    def test_no_downgrade_with_good_coverage_ratio(self):
        assert classify_severity(1.0, coverage_ratio=0.8) == Severity.EXTREME

    def test_double_downgrade_low_years_and_ratio(self):
        # Both low coverage years AND low coverage ratio: downgrade twice
        assert classify_severity(1.0, coverage_years=20, coverage_ratio=0.29) == Severity.UNUSUAL


class TestClassifyDirection:
    def test_warm(self):
        assert classify_direction(80.0, "tavg_c") == Direction.WARM

    def test_cold(self):
        assert classify_direction(20.0, "tavg_c") == Direction.COLD

    def test_neutral(self):
        assert classify_direction(50.0, "tavg_c") == Direction.NEUTRAL

    def test_wet(self):
        assert classify_direction(80.0, "prcp_mm") == Direction.WET

    def test_dry(self):
        assert classify_direction(20.0, "prcp_mm") == Direction.DRY
