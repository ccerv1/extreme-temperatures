"""Tests for Open-Meteo fetcher."""

from datetime import date
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from extreme_temps.ingest.open_meteo import fetch_open_meteo


SAMPLE_RESPONSE = {
    "daily": {
        "time": ["2026-02-04", "2026-02-05", "2026-02-06"],
        "temperature_2m_max": [-2.5, -0.1, -0.4],
        "temperature_2m_min": [-6.0, -10.1, -9.3],
        "temperature_2m_mean": [-4.2, -4.9, -4.8],
        "precipitation_sum": [0.0, 0.0, 0.2],
    }
}


@patch("extreme_temps.ingest.open_meteo.requests.get")
def test_fetch_returns_correct_columns(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_RESPONSE
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    df = fetch_open_meteo(40.78, -73.97, date(2026, 2, 4), date(2026, 2, 6))

    assert list(df.columns) == ["obs_date", "tmin_c", "tmax_c", "tavg_c", "prcp_mm"]
    assert len(df) == 3


@patch("extreme_temps.ingest.open_meteo.requests.get")
def test_fetch_celsius_values(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_RESPONSE
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    df = fetch_open_meteo(40.78, -73.97, date(2026, 2, 4), date(2026, 2, 6))

    # Values should pass through as-is (already Celsius)
    assert df.iloc[0]["tmax_c"] == -2.5
    assert df.iloc[0]["tmin_c"] == -6.0
    assert df.iloc[0]["tavg_c"] == -4.2
    assert df.iloc[0]["prcp_mm"] == 0.0


@patch("extreme_temps.ingest.open_meteo.requests.get")
def test_fetch_dates(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_RESPONSE
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    df = fetch_open_meteo(40.78, -73.97, date(2026, 2, 4), date(2026, 2, 6))

    assert df.iloc[0]["obs_date"] == date(2026, 2, 4)
    assert df.iloc[2]["obs_date"] == date(2026, 2, 6)


@patch("extreme_temps.ingest.open_meteo.requests.get")
def test_fetch_empty_response(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"daily": {}}
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    df = fetch_open_meteo(40.78, -73.97, date(2026, 2, 4), date(2026, 2, 6))

    assert df.empty
    assert list(df.columns) == ["obs_date", "tmin_c", "tmax_c", "tavg_c", "prcp_mm"]


@patch("extreme_temps.ingest.open_meteo.requests.get")
def test_fetch_handles_network_error(mock_get):
    mock_get.side_effect = ConnectionError("Network unreachable")

    df = fetch_open_meteo(40.78, -73.97, date(2026, 2, 4), date(2026, 2, 6))

    assert df.empty


@patch("extreme_temps.ingest.open_meteo.requests.get")
def test_fetch_computes_tavg_when_missing(mock_get):
    response = {
        "daily": {
            "time": ["2026-02-04"],
            "temperature_2m_max": [5.0],
            "temperature_2m_min": [-3.0],
            "temperature_2m_mean": [None],
            "precipitation_sum": [0.0],
        }
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = response
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    df = fetch_open_meteo(40.78, -73.97, date(2026, 2, 4), date(2026, 2, 4))

    assert df.iloc[0]["tavg_c"] == 1.0  # (5.0 + -3.0) / 2


@patch("extreme_temps.ingest.open_meteo.requests.get")
def test_fetch_sends_correct_params(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = SAMPLE_RESPONSE
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    fetch_open_meteo(40.78, -73.97, date(2026, 2, 4), date(2026, 2, 6))

    args, kwargs = mock_get.call_args
    params = kwargs.get("params") or args[1] if len(args) > 1 else kwargs["params"]
    assert params["latitude"] == 40.78
    assert params["longitude"] == -73.97
    assert params["temperature_unit"] == "celsius"
    assert params["start_date"] == "2026-02-04"
    assert params["end_date"] == "2026-02-06"
