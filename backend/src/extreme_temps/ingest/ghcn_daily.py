"""GHCN Daily data fetcher — primary data source.

Downloads station CSV from NCEI and returns observations in Celsius.
"""

from __future__ import annotations

from datetime import date
import logging

import pandas as pd

from extreme_temps.config import GHCN_BASE_URL

logger = logging.getLogger(__name__)


def fetch_ghcn_daily(
    station_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    """Fetch GHCN Daily observations for a station.

    Args:
        station_id: GHCN station ID (e.g. "USW00094728").
        start_date: Optional inclusive start date filter.
        end_date: Optional inclusive end date filter.

    Returns:
        DataFrame with columns: obs_date, tmin_c, tmax_c, tavg_c, prcp_mm.
        Empty DataFrame if no data or download fails.
    """
    url = f"{GHCN_BASE_URL}{station_id}.csv"
    logger.info("Downloading GHCN Daily: %s", url)

    try:
        df = pd.read_csv(url, low_memory=False)
    except Exception:
        logger.exception("Failed to download %s", url)
        return _empty_df()

    if df.empty:
        return _empty_df()

    df["DATE"] = pd.to_datetime(df["DATE"])

    if start_date is not None:
        df = df[df["DATE"] >= pd.Timestamp(start_date)]
    if end_date is not None:
        df = df[df["DATE"] <= pd.Timestamp(end_date)]

    if df.empty:
        return _empty_df()

    # GHCN stores temps in tenths of °C and precip in tenths of mm
    result = pd.DataFrame({
        "obs_date": df["DATE"].dt.date,
        "tmin_c": pd.to_numeric(df.get("TMIN"), errors="coerce") / 10.0,
        "tmax_c": pd.to_numeric(df.get("TMAX"), errors="coerce") / 10.0,
        "tavg_c": pd.to_numeric(df.get("TAVG"), errors="coerce") / 10.0,
        "prcp_mm": pd.to_numeric(df.get("PRCP"), errors="coerce") / 10.0,
    })

    # Compute tavg where missing but tmin/tmax available
    mask = result["tavg_c"].isna() & result["tmin_c"].notna() & result["tmax_c"].notna()
    result.loc[mask, "tavg_c"] = (result.loc[mask, "tmin_c"] + result.loc[mask, "tmax_c"]) / 2.0

    # Drop rows with no temperature data at all
    result = result.dropna(subset=["tmin_c", "tmax_c"], how="all").reset_index(drop=True)

    logger.info("Fetched %d GHCN Daily records for %s", len(result), station_id)
    return result


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["obs_date", "tmin_c", "tmax_c", "tavg_c", "prcp_mm"])
