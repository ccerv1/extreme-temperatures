"""Unified weather data fetcher that combines BigQuery and GHCN Daily sources."""

from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd

from weather_fetcher.scraper import scrape_weather_data, _process_dataframe
from weather_fetcher.ghcn_daily import download_ghcn_daily


def fetch_weather_data(
    station: str,
    start_date: datetime,
    end_date: datetime,
    output_dir: Optional[Path] = None
) -> pd.DataFrame:
    """
    Fetch complete weather data for a date range, using BigQuery first and GHCN Daily to fill gaps.
    
    Args:
        station: Weather station ID (WBAN for BigQuery, USW000<WBAN> for GHCN)
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        output_dir: Directory to save raw and consolidated files (default: data/)
    
    Returns:
        pandas.DataFrame: Complete weather data with columns: date, min, max, avg
    """
    if output_dir is None:
        output_dir = Path('data')
    
    output_dir = Path(output_dir)
    raw_dir = output_dir / 'raw'
    raw_dir.mkdir(parents=True, exist_ok=True)
    
    start_year = start_date.year
    end_year = end_date.year
    
    print("=" * 80)
    print(f"FETCHING WEATHER DATA")
    print("=" * 80)
    print(f"Station: {station}")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print()
    
    # BigQuery data is only available up to 2025-08-27
    bigquery_cutoff = datetime(2025, 8, 27)
    
    # Step 1: Get data from BigQuery (NOAA) - only for dates before 2025-08-27
    print("Step 1: Fetching from BigQuery (NOAA)...")
    print("-" * 80)
    
    # Determine if we need to query BigQuery
    bigquery_end_date = min(end_date, bigquery_cutoff)
    
    if start_date <= bigquery_cutoff:
        # We have dates that could be in BigQuery
        bigquery_start_year = start_date.year
        bigquery_end_year = bigquery_end_date.year
        
        noaa_file = raw_dir / f"noaa_{station}_{start_date.strftime('%Y%m%d')}_{bigquery_end_date.strftime('%Y%m%d')}.csv"
        
        try:
            df_noaa = scrape_weather_data(
                wban=station,
                start_year=bigquery_start_year,
                end_year=bigquery_end_year,
                output_file=str(noaa_file),
                output_format='csv'
            )
            
            # Normalize schema and add source
            if not df_noaa.empty:
                df_noaa = _normalize_schema(df_noaa)
                df_noaa['source'] = 'BigQuery'
                # Filter to exact date range (up to cutoff)
                df_noaa = df_noaa[(df_noaa['date'] >= start_date) & (df_noaa['date'] <= bigquery_end_date)]
                print(f"✓ Retrieved {len(df_noaa):,} records from BigQuery (up to {bigquery_cutoff.date()})")
            else:
                df_noaa = pd.DataFrame(columns=['date', 'min', 'max', 'avg', 'source'])
                print("✗ No data from BigQuery")
        except Exception as e:
            print(f"✗ Error fetching from BigQuery: {e}")
            df_noaa = pd.DataFrame(columns=['date', 'min', 'max', 'avg', 'source'])
    else:
        # All dates are after BigQuery cutoff, skip BigQuery
        print(f"⚠️  Date range is after BigQuery cutoff ({bigquery_cutoff.date()})")
        print("   Skipping BigQuery, will use GHCN Daily only")
        df_noaa = pd.DataFrame(columns=['date', 'min', 'max', 'avg', 'source'])
    
    # Step 2: Identify gaps and fill with GHCN Daily
    print()
    print("Step 2: Checking for gaps and filling with GHCN Daily...")
    print("-" * 80)
    
    # Find missing dates
    date_range = pd.date_range(start_date, end_date, freq='D')
    expected_dates = set(date_range.date)
    
    if not df_noaa.empty:
        noaa_dates = set(df_noaa['date'].dt.date)
        missing_dates = sorted(expected_dates - noaa_dates)
    else:
        # No BigQuery data, all dates are missing (or after cutoff)
        missing_dates = sorted(expected_dates)
    
    # Also include any dates after BigQuery cutoff (2025-08-27)
    dates_after_cutoff = [d for d in expected_dates if d > bigquery_cutoff.date()]
    if dates_after_cutoff:
        missing_dates = sorted(set(missing_dates) | set(dates_after_cutoff))
        print(f"  Note: {len(dates_after_cutoff)} dates are after BigQuery cutoff ({bigquery_cutoff.date()})")
    
    if missing_dates:
        print(f"Found {len(missing_dates):,} missing dates")
        
        # Convert WBAN to GHCN station ID format (USW000<WBAN>)
        ghcn_station = f"USW000{station}"
        
        # Get missing data from GHCN
        gap_start = datetime.combine(missing_dates[0], datetime.min.time())
        gap_end = datetime.combine(missing_dates[-1], datetime.min.time())
        
        ghcn_file = raw_dir / f"ghcn_{station}_{gap_start.strftime('%Y%m%d')}_{gap_end.strftime('%Y%m%d')}.csv"
        
        try:
            df_ghcn = download_ghcn_daily(
                station_id=ghcn_station,
                start_date=gap_start,
                end_date=gap_end,
                output_file=str(ghcn_file),
                output_format='csv'
            )
            
            if not df_ghcn.empty:
                # GHCN Daily returns data with min_temp_f, max_temp_f, temp_f columns
                # Normalize to our schema
                df_ghcn = _normalize_schema(df_ghcn)
                df_ghcn['source'] = 'GHCN Daily'
                # Filter to missing dates only
                df_ghcn = df_ghcn[df_ghcn['date'].dt.date.isin(missing_dates)]
                print(f"✓ Retrieved {len(df_ghcn):,} records from GHCN Daily")
            else:
                df_ghcn = pd.DataFrame(columns=['date', 'min', 'max', 'avg', 'source'])
                print("✗ No data from GHCN Daily")
        except Exception as e:
            print(f"✗ Error fetching from GHCN Daily: {e}")
            df_ghcn = pd.DataFrame(columns=['date', 'min', 'max', 'avg'])
    else:
        print("✓ No gaps found - complete coverage from BigQuery")
        df_ghcn = pd.DataFrame(columns=['date', 'min', 'max', 'avg'])
    
    # Step 3: Combine and consolidate
    print()
    print("Step 3: Consolidating data...")
    print("-" * 80)
    
    all_data = []
    if not df_noaa.empty:
        all_data.append(df_noaa)
    if not df_ghcn.empty:
        all_data.append(df_ghcn)
    
    if not all_data:
        print("✗ No data retrieved")
        return pd.DataFrame(columns=['date', 'min', 'max', 'avg', 'source'])
    
    # Combine and deduplicate (prefer BigQuery over GHCN Daily)
    df_combined = pd.concat(all_data, ignore_index=True)
    df_combined = df_combined.sort_values(['date', 'source']).reset_index(drop=True)
    # Keep first occurrence (BigQuery if both exist for same date)
    df_combined = df_combined.drop_duplicates(subset=['date'], keep='first')
    
    # Filter to exact date range
    df_combined = df_combined[(df_combined['date'] >= start_date) & (df_combined['date'] <= end_date)]
    df_combined = df_combined.sort_values('date').reset_index(drop=True)
    
    # Ensure source column is present and reorder columns
    if 'source' not in df_combined.columns:
        df_combined['source'] = 'Unknown'
    df_combined = df_combined[['date', 'min', 'max', 'avg', 'source']]
    
    # Save consolidated file
    consolidated_dir = output_dir / 'consolidated'
    consolidated_dir.mkdir(parents=True, exist_ok=True)
    consolidated_file = consolidated_dir / f"weather_{station}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
    df_combined.to_csv(consolidated_file, index=False)
    
    # Summary
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    expected_days = (end_date - start_date).days + 1
    actual_days = len(df_combined)
    coverage = (actual_days / expected_days * 100) if expected_days > 0 else 0
    
    print(f"Total records: {actual_days:,} / {expected_days:,} expected ({coverage:.1f}%)")
    print(f"Date range: {df_combined['date'].min().date()} to {df_combined['date'].max().date()}")
    print(f"\nFiles created:")
    print(f"  Raw files: {raw_dir}")
    print(f"  Consolidated: {consolidated_file}")
    
    if coverage == 100:
        print(f"\n✓ Complete coverage!")
    else:
        missing = expected_days - actual_days
        print(f"\n⚠️  Missing {missing:,} days ({missing/expected_days*100:.1f}%)")
    
    return df_combined


def _normalize_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame to standard schema: date, min, max, avg."""
    # Handle different input schemas
    if 'min_temp_f' in df.columns:
        # From scraper.py output
        return pd.DataFrame({
            'date': pd.to_datetime(df['date']),
            'min': df['min_temp_f'],
            'max': df['max_temp_f'],
            'avg': df['temp_f']
        })
    elif 'min' in df.columns and 'max' in df.columns and 'avg' in df.columns:
        # Already normalized
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        return df[['date', 'min', 'max', 'avg']]
    else:
        raise ValueError(f"Unknown schema: {list(df.columns)}")
