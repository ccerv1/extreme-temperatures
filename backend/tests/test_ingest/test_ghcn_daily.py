"""Tests for GHCN Daily fetcher."""

from datetime import date
from unittest.mock import patch

import pandas as pd

from extreme_temps.ingest.ghcn_daily import fetch_ghcn_daily


def _mock_ghcn_csv() -> pd.DataFrame:
    """Simulate the raw CSV returned by NCEI GHCN Daily endpoint."""
    return pd.DataFrame({
        "STATION": ["USW00094728"] * 5,
        "DATE": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
        "TMIN": [-50, -30, -20, 0, 10],      # tenths of C
        "TMAX": [20, 40, 50, 70, 80],         # tenths of C
        "TAVG": [None, None, None, None, None],  # missing — should be computed
        "PRCP": [0, 25, 0, 50, 0],            # tenths of mm
    })


@patch("extreme_temps.ingest.ghcn_daily.pd.read_csv")
def test_fetch_converts_to_celsius(mock_read):
    mock_read.return_value = _mock_ghcn_csv()

    result = fetch_ghcn_daily("USW00094728")

    assert len(result) == 5
    # First row: TMIN=-50 tenths C = -5.0 C, TMAX=20 tenths C = 2.0 C
    assert result.iloc[0]["tmin_c"] == -5.0
    assert result.iloc[0]["tmax_c"] == 2.0


@patch("extreme_temps.ingest.ghcn_daily.pd.read_csv")
def test_fetch_computes_tavg_when_missing(mock_read):
    mock_read.return_value = _mock_ghcn_csv()

    result = fetch_ghcn_daily("USW00094728")

    # TAVG should be computed as (tmin + tmax) / 2
    # Row 0: (-5.0 + 2.0) / 2 = -1.5
    assert result.iloc[0]["tavg_c"] == -1.5


@patch("extreme_temps.ingest.ghcn_daily.pd.read_csv")
def test_fetch_converts_precipitation(mock_read):
    mock_read.return_value = _mock_ghcn_csv()

    result = fetch_ghcn_daily("USW00094728")

    # Row 0: PRCP=0 tenths mm = 0.0 mm
    assert result.iloc[0]["prcp_mm"] == 0.0
    # Row 1: PRCP=25 tenths mm = 2.5 mm
    assert result.iloc[1]["prcp_mm"] == 2.5


@patch("extreme_temps.ingest.ghcn_daily.pd.read_csv")
def test_fetch_date_filter(mock_read):
    mock_read.return_value = _mock_ghcn_csv()

    result = fetch_ghcn_daily(
        "USW00094728",
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 4),
    )

    assert len(result) == 3
    assert result.iloc[0]["obs_date"] == date(2024, 1, 2)


@patch("extreme_temps.ingest.ghcn_daily.pd.read_csv")
def test_fetch_returns_correct_columns(mock_read):
    mock_read.return_value = _mock_ghcn_csv()

    result = fetch_ghcn_daily("USW00094728")

    assert list(result.columns) == ["obs_date", "tmin_c", "tmax_c", "tavg_c", "prcp_mm"]


@patch("extreme_temps.ingest.ghcn_daily.pd.read_csv")
def test_fetch_empty_data(mock_read):
    mock_read.return_value = pd.DataFrame()

    result = fetch_ghcn_daily("USW00094728")

    assert result.empty
    assert list(result.columns) == ["obs_date", "tmin_c", "tmax_c", "tavg_c", "prcp_mm"]


@patch("extreme_temps.ingest.ghcn_daily.pd.read_csv")
def test_fetch_drops_rows_without_temps(mock_read):
    df = _mock_ghcn_csv()
    # Make one row have no temp data
    df.loc[0, "TMIN"] = None
    df.loc[0, "TMAX"] = None
    mock_read.return_value = df

    result = fetch_ghcn_daily("USW00094728")

    assert len(result) == 4  # row 0 dropped
