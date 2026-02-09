"""Seed dim_station from data/stations.json."""

from extreme_temps.db.connection import get_connection
from extreme_temps.db.schema import create_all_tables
from extreme_temps.ingest.stations import seed_stations


def main():
    conn = get_connection()
    create_all_tables(conn)
    count = seed_stations(conn)
    print(f"Seeded {count} stations.")
    conn.close()


if __name__ == "__main__":
    main()
