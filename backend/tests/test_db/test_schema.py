"""Tests for DuckDB schema creation."""

from extreme_temps.db.schema import create_all_tables


def test_create_all_tables(db):
    """All five tables should exist after creation."""
    tables = db.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchdf()
    expected = {
        "dim_station",
        "fact_station_day",
        "fact_station_window_recent",
        "dim_climatology_quantiles",
        "dim_station_records",
        "fact_station_latest_insight",
    }
    assert set(tables["table_name"].tolist()) == expected


def test_create_tables_idempotent(db):
    """Running create_all_tables twice should not raise."""
    create_all_tables(db)
    tables = db.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchdf()
    assert len(tables) == 6


def test_dim_station_columns(db):
    """dim_station should have the expected columns."""
    cols = db.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'dim_station' ORDER BY ordinal_position"
    ).fetchdf()
    expected = [
        "station_id", "wban", "name", "lat", "lon", "elevation_m",
        "first_obs_date", "last_obs_date",
        "completeness_temp_pct", "completeness_prcp_pct",
        "coverage_years", "quality_score", "is_active", "last_ingest_at",
    ]
    assert cols["column_name"].tolist() == expected


def test_fact_station_day_columns(db):
    """fact_station_day should have the expected columns."""
    cols = db.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'fact_station_day' ORDER BY ordinal_position"
    ).fetchdf()
    expected = [
        "station_id", "obs_date", "tmin_c", "tmax_c", "tavg_c",
        "prcp_mm", "source", "ingested_at",
    ]
    assert cols["column_name"].tolist() == expected


def test_fact_station_day_primary_key(db):
    """Duplicate (station_id, obs_date) should conflict."""
    db.execute("""
        INSERT INTO fact_station_day (station_id, obs_date, tavg_c)
        VALUES ('TEST001', '2024-01-01', 5.0)
    """)
    # INSERT OR REPLACE should work
    db.execute("""
        INSERT OR REPLACE INTO fact_station_day (station_id, obs_date, tavg_c)
        VALUES ('TEST001', '2024-01-01', 6.0)
    """)
    result = db.execute(
        "SELECT tavg_c FROM fact_station_day WHERE station_id = 'TEST001'"
    ).fetchone()
    assert result[0] == 6.0
