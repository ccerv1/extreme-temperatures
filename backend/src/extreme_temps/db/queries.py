"""Typed query functions for the canonical data model."""

from __future__ import annotations

from datetime import date, datetime
from math import radians, cos

import duckdb
import pandas as pd


# ---------------------------------------------------------------------------
# dim_station
# ---------------------------------------------------------------------------

def upsert_station(conn: duckdb.DuckDBPyConnection, station: dict) -> None:
    """Insert or update a station row."""
    conn.execute("""
        INSERT OR REPLACE INTO dim_station (
            station_id, wban, name, lat, lon, elevation_m, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [
        station["station_id"],
        station.get("wban"),
        station["name"],
        station["lat"],
        station["lon"],
        station.get("elevation_m"),
        station.get("is_active", True),
    ])


def get_station(conn: duckdb.DuckDBPyConnection, station_id: str) -> dict | None:
    """Fetch a single station by ID."""
    result = conn.execute(
        "SELECT * FROM dim_station WHERE station_id = ?", [station_id]
    ).fetchdf()
    if result.empty:
        return None
    return result.iloc[0].to_dict()


def update_station_coverage(conn: duckdb.DuckDBPyConnection, station_id: str) -> None:
    """Recompute and update coverage stats for a station from fact_station_day."""
    conn.execute("""
        UPDATE dim_station SET
            first_obs_date = sub.first_obs,
            last_obs_date = sub.last_obs,
            coverage_years = sub.years,
            completeness_temp_pct = sub.temp_pct,
            completeness_prcp_pct = sub.prcp_pct,
            last_ingest_at = current_timestamp
        FROM (
            SELECT
                MIN(obs_date) AS first_obs,
                MAX(obs_date) AS last_obs,
                DATE_DIFF('year', MIN(obs_date), MAX(obs_date)) + 1 AS years,
                ROUND(100.0 * COUNT(tavg_c) / COUNT(*), 1) AS temp_pct,
                ROUND(100.0 * COUNT(prcp_mm) / COUNT(*), 1) AS prcp_pct
            FROM fact_station_day
            WHERE station_id = ?
        ) AS sub
        WHERE dim_station.station_id = ?
    """, [station_id, station_id])


def find_nearby_stations(
    conn: duckdb.DuckDBPyConnection,
    lat: float,
    lon: float,
    radius_km: float = 50.0,
    limit: int = 10,
) -> list[dict]:
    """Find stations within radius using Haversine approximation."""
    # Approximate bounding box for pre-filter
    lat_delta = radius_km / 111.0
    lon_delta = radius_km / (111.0 * max(cos(radians(lat)), 0.01))

    result = conn.execute("""
        SELECT *,
            6371 * ACOS(
                LEAST(1.0,
                    COS(RADIANS(?)) * COS(RADIANS(lat)) *
                    COS(RADIANS(lon) - RADIANS(?)) +
                    SIN(RADIANS(?)) * SIN(RADIANS(lat))
                )
            ) AS distance_km
        FROM dim_station
        WHERE lat BETWEEN ? AND ?
          AND lon BETWEEN ? AND ?
          AND is_active = TRUE
        ORDER BY distance_km
        LIMIT ?
    """, [
        lat, lon, lat,
        lat - lat_delta, lat + lat_delta,
        lon - lon_delta, lon + lon_delta,
        limit,
    ]).fetchdf()

    if result.empty:
        return []
    return result.to_dict("records")


# ---------------------------------------------------------------------------
# fact_station_day
# ---------------------------------------------------------------------------

def upsert_daily_observations(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    df: pd.DataFrame,
    source: str = "ghcn_daily",
) -> int:
    """Insert or replace daily observations from a DataFrame.

    Expected columns: obs_date, tmin_c, tmax_c, tavg_c, prcp_mm
    Returns count of rows upserted.
    """
    if df.empty:
        return 0

    records = df.copy()
    records["station_id"] = station_id
    records["source"] = source
    records["ingested_at"] = datetime.now()

    # Ensure column order
    cols = ["station_id", "obs_date", "tmin_c", "tmax_c", "tavg_c", "prcp_mm", "source", "ingested_at"]
    records = records[cols]

    conn.execute("INSERT OR REPLACE INTO fact_station_day SELECT * FROM records")
    return len(records)


def get_daily_observations(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Retrieve daily observations for a station and date range."""
    return conn.execute("""
        SELECT obs_date, tmin_c, tmax_c, tavg_c, prcp_mm, source
        FROM fact_station_day
        WHERE station_id = ?
          AND obs_date BETWEEN ? AND ?
        ORDER BY obs_date
    """, [station_id, start_date, end_date]).fetchdf()


def get_station_date_range(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
) -> tuple[date | None, date | None]:
    """Return (first_date, last_date) for a station's observations."""
    result = conn.execute("""
        SELECT MIN(obs_date), MAX(obs_date)
        FROM fact_station_day
        WHERE station_id = ?
    """, [station_id]).fetchone()
    if result is None:
        return None, None
    return result[0], result[1]


# ---------------------------------------------------------------------------
# fact_station_window_recent
# ---------------------------------------------------------------------------

def upsert_window_aggregates(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    df: pd.DataFrame,
) -> int:
    """Insert or replace rolling window aggregates.

    Expected columns: window_days, end_date, start_date, tavg_c_mean,
                      tmin_c_mean, tmax_c_mean, prcp_mm_sum, coverage_ratio
    """
    if df.empty:
        return 0

    records = df.copy()
    records["station_id"] = station_id
    records["computed_at"] = datetime.now()

    cols = [
        "station_id", "window_days", "end_date", "start_date",
        "tavg_c_mean", "tmin_c_mean", "tmax_c_mean", "prcp_mm_sum",
        "coverage_ratio", "computed_at",
    ]
    records = records[cols]

    conn.execute("INSERT OR REPLACE INTO fact_station_window_recent SELECT * FROM records")
    return len(records)


# ---------------------------------------------------------------------------
# dim_climatology_quantiles
# ---------------------------------------------------------------------------

def upsert_climatology_quantiles(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    metric_id: str,
    df: pd.DataFrame,
) -> int:
    """Insert or replace climatology quantile rows.

    Expected columns: window_days, end_doy, doy_window_halfwidth,
                      p02, p10, p25, p50, p75, p90, p98,
                      n_samples, first_year, last_year
    """
    if df.empty:
        return 0

    records = df.copy()
    records["station_id"] = station_id
    records["metric_id"] = metric_id
    records["computed_at"] = datetime.now()

    cols = [
        "station_id", "metric_id", "window_days", "end_doy",
        "doy_window_halfwidth", "p02", "p10", "p25", "p50", "p75", "p90", "p98",
        "n_samples", "first_year", "last_year", "computed_at",
    ]
    records = records[cols]

    conn.execute("INSERT OR REPLACE INTO dim_climatology_quantiles SELECT * FROM records")
    return len(records)


def get_climatology_quantiles(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    metric_id: str,
    window_days: int,
    end_doy: int,
    doy_window_halfwidth: int = 7,
) -> dict | None:
    """Fetch climatology quantiles for a specific station/metric/window/DOY."""
    result = conn.execute("""
        SELECT p02, p10, p25, p50, p75, p90, p98, n_samples, first_year, last_year
        FROM dim_climatology_quantiles
        WHERE station_id = ?
          AND metric_id = ?
          AND window_days = ?
          AND end_doy = ?
          AND doy_window_halfwidth = ?
    """, [station_id, metric_id, window_days, end_doy, doy_window_halfwidth]).fetchdf()

    if result.empty:
        return None
    return result.iloc[0].to_dict()


# ---------------------------------------------------------------------------
# dim_station_records
# ---------------------------------------------------------------------------

def upsert_station_records(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    records: list[dict],
) -> int:
    """Insert or replace station record rows.

    Each dict should have: metric_id, window_days, record_type, value,
                           start_date, end_date, n_years_considered
    """
    if not records:
        return 0

    df = pd.DataFrame(records)
    df["station_id"] = station_id
    df["computed_at"] = datetime.now()

    cols = [
        "station_id", "metric_id", "window_days", "record_type",
        "value", "start_date", "end_date", "n_years_considered", "computed_at",
    ]
    df = df[cols]

    conn.execute("INSERT OR REPLACE INTO dim_station_records SELECT * FROM df")
    return len(df)


def get_station_records(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    metric_id: str | None = None,
) -> pd.DataFrame:
    """Fetch all records for a station, optionally filtered by metric."""
    if metric_id:
        return conn.execute("""
            SELECT * FROM dim_station_records
            WHERE station_id = ? AND metric_id = ?
            ORDER BY window_days, record_type
        """, [station_id, metric_id]).fetchdf()
    else:
        return conn.execute("""
            SELECT * FROM dim_station_records
            WHERE station_id = ?
            ORDER BY metric_id, window_days, record_type
        """, [station_id]).fetchdf()


# ---------------------------------------------------------------------------
# fact_station_latest_insight
# ---------------------------------------------------------------------------

def upsert_latest_insight(conn: duckdb.DuckDBPyConnection, row: dict) -> None:
    """Insert or replace a precomputed latest insight for a station."""
    conn.execute("""
        INSERT OR REPLACE INTO fact_station_latest_insight (
            station_id, window_days, end_date, metric, value, percentile,
            severity, direction, primary_statement, supporting_line,
            coverage_years, first_year, since_year, computed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
    """, [
        row["station_id"],
        row["window_days"],
        row["end_date"],
        row["metric"],
        row.get("value"),
        row.get("percentile"),
        row["severity"],
        row["direction"],
        row["primary_statement"],
        row["supporting_line"],
        row.get("coverage_years"),
        row.get("first_year"),
        row.get("since_year"),
    ])


def get_all_latest_insights(
    conn: duckdb.DuckDBPyConnection,
    window_days: int | None = None,
) -> list[dict]:
    """Fetch all precomputed latest insights.

    If window_days is provided, returns only rows for that window size.
    Otherwise returns all rows (multiple per station).
    """
    if window_days is not None:
        df = conn.execute("""
            SELECT station_id, end_date, window_days, metric, value, percentile,
                   severity, direction, primary_statement, supporting_line,
                   coverage_years, first_year, since_year
            FROM fact_station_latest_insight
            WHERE window_days = ?
            ORDER BY station_id
        """, [window_days]).fetchdf()
    else:
        df = conn.execute("""
            SELECT station_id, end_date, window_days, metric, value, percentile,
                   severity, direction, primary_statement, supporting_line,
                   coverage_years, first_year, since_year
            FROM fact_station_latest_insight
            ORDER BY station_id, window_days
        """).fetchdf()
    if df.empty:
        return []
    return df.to_dict("records")
