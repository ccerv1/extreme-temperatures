"""Configuration constants for the weather fetcher."""

# BigQuery dataset
PROJECT_DATASET = "bigquery-public-data.noaa_gsod"

# Default weather station ID (WBAN)
DEFAULT_WBAN = "94728"  # NYC Central Park (example station)

# Example weather stations (for reference)
EXAMPLE_STATIONS = {
    "94728": "NYC Central Park",
    "94745": "LaGuardia Airport",
    "94789": "JFK Airport",
}
