"""Severity classification based on percentile thresholds.

Implements the PRD severity classification:
    Normal:       >25th to <75th percentile
    Unusual:      >=10th to <=25th or >=75th to <=90th
    Very Unusual: >=2nd to <=10th or >=90th to <=98th
    Extreme:      <2nd or >98th
"""

from __future__ import annotations

from enum import Enum

from extreme_temps.config import MIN_COVERAGE_YEARS


class Severity(str, Enum):
    EXTREME = "extreme"
    VERY_UNUSUAL = "very_unusual"
    UNUSUAL = "unusual"
    NORMAL = "normal"
    INSUFFICIENT_DATA = "insufficient_data"


class Direction(str, Enum):
    WARM = "warm"
    COLD = "cold"
    WET = "wet"
    DRY = "dry"
    NEUTRAL = "neutral"


MIN_COVERAGE_RATIO = 0.5


def classify_severity(
    percentile: float,
    coverage_years: int | None = None,
    min_years: int = MIN_COVERAGE_YEARS,
    coverage_ratio: float | None = None,
) -> Severity:
    """Classify severity from a percentile value (0-100).

    Downgrade by one level if:
    - coverage_years < min_years (short history), or
    - coverage_ratio < MIN_COVERAGE_RATIO (sparse window data).
    """
    raw = _raw_severity(percentile)
    if coverage_years is not None and coverage_years < min_years:
        raw = _downgrade(raw)
    if coverage_ratio is not None and coverage_ratio < MIN_COVERAGE_RATIO:
        raw = _downgrade(raw)
    return raw


def classify_direction(
    percentile: float,
    metric_id: str = "tavg_c",
) -> Direction:
    """Determine the direction (warm/cold/wet/dry) from percentile and metric."""
    if metric_id.startswith("prcp"):
        if percentile > 50:
            return Direction.WET
        elif percentile < 50:
            return Direction.DRY
        return Direction.NEUTRAL

    # Temperature metrics
    if percentile > 50:
        return Direction.WARM
    elif percentile < 50:
        return Direction.COLD
    return Direction.NEUTRAL


def _raw_severity(percentile: float) -> Severity:
    if percentile < 2 or percentile > 98:
        return Severity.EXTREME
    if percentile <= 10 or percentile >= 90:
        return Severity.VERY_UNUSUAL
    if percentile <= 25 or percentile >= 75:
        return Severity.UNUSUAL
    return Severity.NORMAL


_DOWNGRADE_MAP = {
    Severity.EXTREME: Severity.VERY_UNUSUAL,
    Severity.VERY_UNUSUAL: Severity.UNUSUAL,
    Severity.UNUSUAL: Severity.NORMAL,
    Severity.NORMAL: Severity.NORMAL,
    Severity.INSUFFICIENT_DATA: Severity.INSUFFICIENT_DATA,
}


def _downgrade(severity: Severity) -> Severity:
    return _DOWNGRADE_MAP[severity]
