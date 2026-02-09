"""Station metadata management."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import duckdb

from extreme_temps.config import STATIONS_JSON
from extreme_temps.db.queries import upsert_station

logger = logging.getLogger(__name__)


def load_station_registry(json_path: Path = STATIONS_JSON) -> list[dict]:
    """Load the curated station list from data/stations.json."""
    with open(json_path) as f:
        return json.load(f)


def seed_stations(conn: duckdb.DuckDBPyConnection, json_path: Path = STATIONS_JSON) -> int:
    """Populate dim_station from the station registry JSON.

    Returns the number of stations upserted.
    """
    stations = load_station_registry(json_path)
    for station in stations:
        upsert_station(conn, station)
    logger.info("Seeded %d stations from %s", len(stations), json_path)
    return len(stations)
