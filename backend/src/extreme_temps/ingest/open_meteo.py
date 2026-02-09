"""Open-Meteo API fetcher — near-real-time gap-filler.

Fills the 3-5 day lag between GHCN Daily updates and today.
Free API, no key required. Uses coordinates (lat/lon) instead of station IDs.
"""

from __future__ import annotations

from datetime import date
import logging

import pandas as pd
import requests

logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_open_meteo(
    lat: float,
    lon: float,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Fetch recent daily observations from Open-Meteo.

    Args:
        lat: Station latitude.
        lon: Station longitude.
        start_date: Inclusive start date.
        end_date: Inclusive end date.

    Returns:
        DataFrame with columns: obs_date, tmin_c, tmax_c, tavg_c, prcp_mm.
        Empty DataFrame on failure.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum",
        "temperature_unit": "celsius",
        "precipitation_unit": "mm",
        "timezone": "auto",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }

    logger.info("Fetching Open-Meteo: lat=%.4f lon=%.4f %s to %s", lat, lon, start_date, end_date)

    try:
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        logger.exception("Failed to fetch Open-Meteo data")
        return _empty_df()

    daily = data.get("daily")
    if not daily or not daily.get("time"):
        logger.warning("No daily data in Open-Meteo response")
        return _empty_df()

    result = pd.DataFrame({
        "obs_date": [date.fromisoformat(d) for d in daily["time"]],
        "tmin_c": daily.get("temperature_2m_min"),
        "tmax_c": daily.get("temperature_2m_max"),
        "tavg_c": daily.get("temperature_2m_mean"),
        "prcp_mm": daily.get("precipitation_sum"),
    })

    # Compute tavg where missing but tmin/tmax available
    mask = result["tavg_c"].isna() & result["tmin_c"].notna() & result["tmax_c"].notna()
    result.loc[mask, "tavg_c"] = (result.loc[mask, "tmin_c"] + result.loc[mask, "tmax_c"]) / 2.0

    # Drop rows with no temperature data at all
    result = result.dropna(subset=["tmin_c", "tmax_c"], how="all").reset_index(drop=True)

    logger.info("Fetched %d Open-Meteo records", len(result))
    return result


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["obs_date", "tmin_c", "tmax_c", "tavg_c", "prcp_mm"])
