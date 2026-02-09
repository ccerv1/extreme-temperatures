"""Extreme Temperatures - Weather data fetcher for analyzing temperature extremes."""

__version__ = "0.1.0"

from weather_fetcher.scraper import scrape_weather_data
from weather_fetcher.config import DEFAULT_WBAN, PROJECT_DATASET

__all__ = ["scrape_weather_data", "DEFAULT_WBAN", "PROJECT_DATASET"]
