"""Station records computation.

Finds all-time records across all window sizes and checks proximity.
"""

from __future__ import annotations

from datetime import date
import logging

import duckdb

from extreme_temps.compute.rolling_windows import find_all_time_extremes
from extreme_temps.db.queries import upsert_station_records, get_station_records
from extreme_temps.config import METRICS

logger = logging.getLogger(__name__)


def compute_station_records(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    metric_id: str = "tavg_c",
) -> int:
    """Compute and store all-time records for a station/metric.

    Returns count of records stored.
    """
    records = find_all_time_extremes(conn, station_id, metric_id)
    if not records:
        return 0

    count = upsert_station_records(conn, station_id, records)
    logger.info("Stored %d records for %s/%s", count, station_id, metric_id)
    return count


def compute_all_records(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
) -> int:
    """Compute records for all temperature metrics."""
    total = 0
    for metric in ["tavg_c", "tmax_c", "tmin_c"]:
        total += compute_station_records(conn, station_id, metric)
    return total


def check_record_proximity(
    conn: duckdb.DuckDBPyConnection,
    station_id: str,
    metric_id: str,
    window_days: int,
    current_value: float,
) -> dict | None:
    """Check if a current value is near or exceeds an existing record.

    Returns dict with record info if within range, else None.
    """
    records_df = get_station_records(conn, station_id, metric_id)
    if records_df.empty:
        return None

    filtered = records_df[records_df["window_days"] == window_days]
    if filtered.empty:
        return None

    for _, row in filtered.iterrows():
        record_val = row["value"]
        record_type = row["record_type"]

        if record_type == "highest" and current_value >= record_val:
            return {
                "record_type": "highest",
                "record_value": record_val,
                "record_start": row["start_date"],
                "record_end": row["end_date"],
                "is_new_record": True,
            }
        elif record_type == "lowest" and current_value <= record_val:
            return {
                "record_type": "lowest",
                "record_value": record_val,
                "record_start": row["start_date"],
                "record_end": row["end_date"],
                "is_new_record": True,
            }

    return None
