"""Severity classification based on percentile thresholds.

Four-level severity scale:
    Normal:       >35th to <65th percentile
    A Bit:        >=15th to <=35th or >=65th to <=85th
    Unusual:      >=5th to <=15th or >=85th to <=95th
    Extreme:      <5th or >95th
"""

from __future__ import annotations

from enum import Enum

from extreme_temps.config import MIN_COVERAGE_YEARS


class Severity(str, Enum):
    EXTREME = "extreme"
    UNUSUAL = "unusual"
    A_BIT = "a_bit"
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
    if percentile < 5 or percentile > 95:
        return Severity.EXTREME
    if percentile <= 15 or percentile >= 85:
        return Severity.UNUSUAL
    if percentile <= 35 or percentile >= 65:
        return Severity.A_BIT
    return Severity.NORMAL


_DOWNGRADE_MAP = {
    Severity.EXTREME: Severity.UNUSUAL,
    Severity.UNUSUAL: Severity.A_BIT,
    Severity.A_BIT: Severity.NORMAL,
    Severity.NORMAL: Severity.NORMAL,
    Severity.INSUFFICIENT_DATA: Severity.INSUFFICIENT_DATA,
}


def _downgrade(severity: Severity) -> Severity:
    return _DOWNGRADE_MAP[severity]
