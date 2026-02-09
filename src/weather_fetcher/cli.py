#!/usr/bin/env python3
"""Unified CLI for fetching weather data."""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from weather_fetcher.fetcher import fetch_weather_data
from weather_fetcher.config import DEFAULT_WBAN


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Fetch complete weather data for a date range',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch NYC Central Park data for 2024
  extreme-temperatures --station 94728 --start-date 2024-01-01 --end-date 2024-12-31

  # Fetch data for a specific month
  extreme-temperatures --station 94728 --start-date 2025-08-01 --end-date 2025-08-31

  # Use different station
  extreme-temperatures --station 94745 --start-date 2020-01-01 --end-date 2020-12-31
        """
    )
    parser.add_argument(
        '--station',
        type=str,
        default=DEFAULT_WBAN,
        help=f'Weather station ID (WBAN). Default: {DEFAULT_WBAN} (NYC Central Park)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        required=True,
        help='Start date (YYYY-MM-DD format)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        required=True,
        help='End date (YYYY-MM-DD format)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data',
        help='Output directory for raw and consolidated files (default: data/)'
    )
    
    args = parser.parse_args()
    
    # Parse dates
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError as e:
        print(f"Error: Invalid date format. Use YYYY-MM-DD format.")
        print(f"  Example: --start-date 2024-01-01 --end-date 2024-12-31")
        sys.exit(1)
    
    if start_date > end_date:
        print("Error: Start date must be before end date")
        sys.exit(1)
    
    # Fetch the data
    df = fetch_weather_data(
        station=args.station,
        start_date=start_date,
        end_date=end_date,
        output_dir=Path(args.output_dir)
    )
    
    return df


if __name__ == '__main__':
    main()
