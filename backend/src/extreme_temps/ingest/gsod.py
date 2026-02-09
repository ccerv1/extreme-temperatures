"""BigQuery NOAA GSOD fetcher — secondary data source.

Queries GSOD year-tables from BigQuery and returns observations in Celsius.
"""

from __future__ import annotations

import logging

import pandas as pd

from extreme_temps.config import BIGQUERY_DATASET

logger = logging.getLogger(__name__)


def fetch_gsod(
    wban: str,
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Fetch GSOD data from BigQuery for a WBAN station.

    Args:
        wban: WBAN station ID (e.g. "94728").
        start_year: First year to query (inclusive).
        end_year: Last year to query (inclusive).

    Returns:
        DataFrame with columns: obs_date, tmin_c, tmax_c, tavg_c, prcp_mm.
        Empty DataFrame if query fails or no data.
    """
    try:
        from google.cloud import bigquery
    except ImportError:
        logger.error("google-cloud-bigquery not installed; GSOD fetch unavailable")
        return _empty_df()

    # Build UNION ALL across year tables
    selects = []
    for year in range(start_year, end_year + 1):
        selects.append(
            f"SELECT year, mo, da, temp, max, min, prcp "
            f"FROM `{BIGQUERY_DATASET}.gsod{year}` "
            f"WHERE wban='{wban}'"
        )

    sql = "\nUNION ALL\n".join(selects)

    logger.info("Querying BigQuery GSOD for WBAN %s (%d-%d)", wban, start_year, end_year)

    try:
        client = bigquery.Client()
        df = client.query(sql).to_dataframe()
    except Exception:
        logger.exception("BigQuery query failed for WBAN %s", wban)
        return _empty_df()

    if df.empty:
        return _empty_df()

    # Build date column
    df["obs_date"] = pd.to_datetime({
        "year": df["year"],
        "month": df["mo"],
        "day": df["da"],
    }).dt.date

    # GSOD temps are in Fahrenheit; convert to Celsius
    # GSOD uses 9999.9 as missing value
    for col in ["temp", "max", "min"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df.loc[df[col] > 900, col] = None

    df["prcp"] = pd.to_numeric(df["prcp"], errors="coerce")
    df.loc[df["prcp"] > 90, "prcp"] = None  # 99.99 = missing

    result = pd.DataFrame({
        "obs_date": df["obs_date"],
        "tmin_c": (df["min"] - 32) * 5 / 9,
        "tmax_c": (df["max"] - 32) * 5 / 9,
        "tavg_c": (df["temp"] - 32) * 5 / 9,
        "prcp_mm": df["prcp"] * 25.4,  # inches to mm
    })

    result = result.dropna(subset=["tmin_c", "tmax_c"], how="all").reset_index(drop=True)

    logger.info("Fetched %d GSOD records for WBAN %s", len(result), wban)
    return result


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["obs_date", "tmin_c", "tmax_c", "tavg_c", "prcp_mm"])
