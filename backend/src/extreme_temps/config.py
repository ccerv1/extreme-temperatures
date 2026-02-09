from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "extreme_temps.duckdb"
STATIONS_JSON = DATA_DIR / "stations.json"

# GHCN Daily
GHCN_BASE_URL = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/"
GHCN_STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
GHCN_INVENTORY_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"

# BigQuery (secondary source)
BIGQUERY_DATASET = "bigquery-public-data.noaa_gsod"
BIGQUERY_CUTOFF = "2025-08-27"

# Rolling window sizes (days)
WINDOW_DAYS = [1, 3, 5, 7, 10, 14, 21, 28, 30, 45, 60, 75, 90, 180, 365]

# Severity thresholds (percentile boundaries)
SEVERITY_THRESHOLDS = {
    "extreme_low": 2,
    "very_unusual_low": 10,
    "unusual_low": 25,
    "unusual_high": 75,
    "very_unusual_high": 90,
    "extreme_high": 98,
}

# Minimum years of data required for full severity classification
MIN_COVERAGE_YEARS = 30

# Climatology: half-width of DOY smoothing window (days on each side)
DOY_WINDOW_HALFWIDTH = 7

# Supported metrics
METRICS = ["tavg_c", "tmax_c", "tmin_c", "prcp_mm"]
