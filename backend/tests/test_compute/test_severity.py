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
        assert classify_severity(40.0) == Severity.NORMAL
        assert classify_severity(60.0) == Severity.NORMAL

    def test_a_bit_range(self):
        assert classify_severity(20.0) == Severity.A_BIT
        assert classify_severity(80.0) == Severity.A_BIT
        assert classify_severity(30.0) == Severity.A_BIT

    def test_unusual_range(self):
        assert classify_severity(8.0) == Severity.UNUSUAL
        assert classify_severity(92.0) == Severity.UNUSUAL
        assert classify_severity(5.0) == Severity.UNUSUAL
        assert classify_severity(14.0) == Severity.UNUSUAL

    def test_boundary_values_go_to_more_severe(self):
        # At exact boundaries, value goes to the more severe bucket
        assert classify_severity(35.0) == Severity.A_BIT
        assert classify_severity(65.0) == Severity.A_BIT
        assert classify_severity(15.0) == Severity.UNUSUAL
        assert classify_severity(85.0) == Severity.UNUSUAL

    def test_extreme_range(self):
        assert classify_severity(1.0) == Severity.EXTREME
        assert classify_severity(99.0) == Severity.EXTREME
        assert classify_severity(0.0) == Severity.EXTREME
        assert classify_severity(100.0) == Severity.EXTREME
        assert classify_severity(4.9) == Severity.EXTREME

    def test_boundary_exact_5(self):
        # Exactly at 5 should be UNUSUAL, not EXTREME
        assert classify_severity(5.0) == Severity.UNUSUAL

    def test_boundary_exact_95(self):
        assert classify_severity(95.0) == Severity.UNUSUAL

    def test_downgrade_with_low_coverage(self):
        # With only 20 years (< 30 min), extreme should downgrade
        assert classify_severity(1.0, coverage_years=20) == Severity.UNUSUAL
        assert classify_severity(8.0, coverage_years=20) == Severity.A_BIT
        assert classify_severity(20.0, coverage_years=20) == Severity.NORMAL

    def test_no_downgrade_with_sufficient_coverage(self):
        assert classify_severity(1.0, coverage_years=50) == Severity.EXTREME

    def test_normal_not_downgraded_further(self):
        assert classify_severity(50.0, coverage_years=10) == Severity.NORMAL

    def test_downgrade_with_low_coverage_ratio(self):
        # With only 2/7 days of data (0.29 < 0.5), downgrade by one level
        assert classify_severity(1.0, coverage_ratio=0.29) == Severity.UNUSUAL
        assert classify_severity(8.0, coverage_ratio=0.29) == Severity.A_BIT
        assert classify_severity(20.0, coverage_ratio=0.29) == Severity.NORMAL

    def test_no_downgrade_with_good_coverage_ratio(self):
        assert classify_severity(1.0, coverage_ratio=0.8) == Severity.EXTREME

    def test_double_downgrade_low_years_and_ratio(self):
        # Both low coverage years AND low coverage ratio: downgrade twice
        assert classify_severity(1.0, coverage_years=20, coverage_ratio=0.29) == Severity.A_BIT


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
