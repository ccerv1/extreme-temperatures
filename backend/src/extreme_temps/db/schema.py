"""DDL for the canonical data model."""

import duckdb


def create_all_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all tables if they don't already exist."""

    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_station (
            station_id  VARCHAR PRIMARY KEY,
            wban        VARCHAR,
            name        VARCHAR NOT NULL,
            lat         DOUBLE NOT NULL,
            lon         DOUBLE NOT NULL,
            elevation_m DOUBLE,
            first_obs_date DATE,
            last_obs_date  DATE,
            completeness_temp_pct DOUBLE,
            completeness_prcp_pct DOUBLE,
            coverage_years INTEGER,
            quality_score  DOUBLE,
            is_active   BOOLEAN DEFAULT TRUE,
            last_ingest_at TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_station_day (
            station_id  VARCHAR NOT NULL,
            obs_date    DATE NOT NULL,
            tmin_c      DOUBLE,
            tmax_c      DOUBLE,
            tavg_c      DOUBLE,
            prcp_mm     DOUBLE,
            source      VARCHAR DEFAULT 'ghcn_daily',
            ingested_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (station_id, obs_date)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_station_window_recent (
            station_id     VARCHAR NOT NULL,
            window_days    INTEGER NOT NULL,
            end_date       DATE NOT NULL,
            start_date     DATE NOT NULL,
            tavg_c_mean    DOUBLE,
            tmin_c_mean    DOUBLE,
            tmax_c_mean    DOUBLE,
            prcp_mm_sum    DOUBLE,
            coverage_ratio DOUBLE,
            computed_at    TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (station_id, window_days, end_date)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_climatology_quantiles (
            station_id           VARCHAR NOT NULL,
            metric_id            VARCHAR NOT NULL,
            window_days          INTEGER NOT NULL,
            end_doy              INTEGER NOT NULL,
            doy_window_halfwidth INTEGER NOT NULL,
            p02   DOUBLE,
            p10   DOUBLE,
            p25   DOUBLE,
            p50   DOUBLE,
            p75   DOUBLE,
            p90   DOUBLE,
            p98   DOUBLE,
            n_samples   INTEGER,
            first_year  INTEGER,
            last_year   INTEGER,
            computed_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (station_id, metric_id, window_days, end_doy, doy_window_halfwidth)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_station_records (
            station_id  VARCHAR NOT NULL,
            metric_id   VARCHAR NOT NULL,
            window_days INTEGER NOT NULL,
            record_type VARCHAR NOT NULL,
            value       DOUBLE NOT NULL,
            start_date  DATE NOT NULL,
            end_date    DATE NOT NULL,
            n_years_considered INTEGER,
            computed_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (station_id, metric_id, window_days, record_type)
        )
    """)

    # Migrate: old schema had PK (station_id) only; new needs (station_id, window_days)
    _migrate_latest_insight_table(conn)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_station_latest_insight (
            station_id        VARCHAR NOT NULL,
            window_days       INTEGER NOT NULL,
            end_date          DATE NOT NULL,
            metric            VARCHAR NOT NULL,
            value             DOUBLE,
            normal_value      DOUBLE,
            percentile        DOUBLE,
            severity          VARCHAR NOT NULL,
            direction         VARCHAR NOT NULL,
            primary_statement VARCHAR NOT NULL,
            supporting_line   VARCHAR NOT NULL,
            coverage_years    INTEGER,
            first_year        INTEGER,
            since_year        INTEGER,
            computed_at       TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (station_id, window_days)
        )
    """)


def _migrate_latest_insight_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Drop fact_station_latest_insight if it has the old single-column PK."""
    try:
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'fact_station_latest_insight'"
        ).fetchall()
        col_names = {c[0] for c in cols}
        if col_names and ("since_year" not in col_names or "normal_value" not in col_names):
            conn.execute("DROP TABLE fact_station_latest_insight")
    except Exception:
        pass
