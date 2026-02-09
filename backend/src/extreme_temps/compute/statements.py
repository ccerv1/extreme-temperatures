"""Server-side descriptive statement generation.

Produces the primary statement and supporting line for the insight API.
All statements are deterministic for identical inputs.
"""

from __future__ import annotations

from extreme_temps.compute.severity import Severity, Direction


def generate_insight(
    window_days: int,
    value_c: float,
    percentile: float,
    severity: Severity,
    direction: Direction,
    coverage_years: int,
    first_year: int,
    record_info: dict | None = None,
    since_year: int | None = None,
) -> tuple[str, str]:
    """Generate primary_statement and supporting_line.

    Args:
        window_days: Rolling window size in days.
        value_c: The computed metric value in Celsius.
        percentile: Percentile rank (0-100).
        severity: Classified severity.
        direction: Warm/cold/wet/dry direction.
        coverage_years: Years of historical data.
        first_year: Earliest year of data.
        record_info: Optional dict if near/at a record.
        since_year: If set, comparison is limited to this year onward.

    Returns:
        (primary_statement, supporting_line) tuple.
    """
    window_label = _window_label(window_days)

    # Primary statement
    if record_info and record_info.get("is_new_record"):
        primary = f"This {window_label} is the {record_info['record_type']} on record."
    elif severity == Severity.NORMAL:
        primary = f"This {window_label} is near normal."
    elif severity == Severity.INSUFFICIENT_DATA:
        primary = f"Not enough climatology data to classify this {window_label}."
    elif severity == Severity.A_BIT:
        comparative = _direction_comparative(direction)
        primary = f"This {window_label} is a bit {comparative}."
    else:
        severity_word = _severity_adjective(severity)
        direction_word = _direction_adjective(direction)
        if severity_word:
            primary = f"This {window_label} is {severity_word} {direction_word}."
        else:
            primary = f"This {window_label} is {direction_word}."

    # Supporting line
    if percentile <= 50:
        comparison = f"Colder than {100 - percentile:.0f}%"
    else:
        comparison = f"Warmer than {percentile:.0f}%"

    if direction in (Direction.WET, Direction.DRY):
        if percentile <= 50:
            comparison = f"Drier than {100 - percentile:.0f}%"
        else:
            comparison = f"Wetter than {percentile:.0f}%"

    if since_year is not None:
        from datetime import date
        current_year = date.today().year
        range_label = f"{since_year}\u2013{current_year}"
    else:
        range_label = f"since {first_year}"

    supporting = (
        f"{comparison} of historical {window_label}s "
        f"({range_label}, {coverage_years} years of data)."
    )

    return primary, supporting


def _window_label(window_days: int) -> str:
    """Human-readable window label."""
    if window_days == 1:
        return "day"
    elif window_days == 7:
        return "week"
    elif window_days == 30:
        return "30-day period"
    elif window_days == 365:
        return "year"
    else:
        return f"{window_days}-day period"


def _severity_adjective(severity: Severity) -> str:
    return {
        Severity.EXTREME: "extremely",
        Severity.UNUSUAL: "unusually",
        Severity.A_BIT: "",
        Severity.NORMAL: "",
        Severity.INSUFFICIENT_DATA: "",
    }[severity]


def _direction_adjective(direction: Direction) -> str:
    return {
        Direction.WARM: "warm",
        Direction.COLD: "cold",
        Direction.WET: "wet",
        Direction.DRY: "dry",
        Direction.NEUTRAL: "",
    }[direction]


def _direction_comparative(direction: Direction) -> str:
    return {
        Direction.WARM: "warmer",
        Direction.COLD: "colder",
        Direction.WET: "wetter",
        Direction.DRY: "drier",
        Direction.NEUTRAL: "different",
    }[direction]
