"""GHCN Daily data scraper from NCEI raw HTTP directories."""

import requests
from pathlib import Path
from datetime import datetime
import pandas as pd
from typing import Optional

from weather_fetcher.scraper import _process_dataframe, _save_data


GHCN_BASE_URL = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/"


def download_ghcn_daily(station_id: str = "USW00094728",
                        start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None,
                        output_file: Optional[str] = None,
                        output_format: str = 'parquet') -> pd.DataFrame:
    """
    Download GHCN Daily data for a station from NCEI raw HTTP directories.
    
    Args:
        station_id: GHCN station ID (e.g., USW00094728 for Central Park)
        start_date: Filter start date (optional)
        end_date: Filter end date (optional)
        output_file: Output file path (optional)
        output_format: Output format ('parquet', 'csv', or 'json')
    
    Returns:
        pandas.DataFrame: Weather data with columns: date, year, month, day, 
                         temp_f, max_temp_f, min_temp_f
    """
    print(f"Downloading GHCN Daily data for station {station_id}...")
    
    url = f"{GHCN_BASE_URL}{station_id}.csv"
    
    try:
        print(f"  Downloading from: {url}")
        df = pd.read_csv(url, low_memory=False)
        
        if df.empty:
            print("✗ No data found")
            return pd.DataFrame()
        
        print(f"  ✓ Downloaded {len(df):,} records")
        
        # Parse DATE column
        df['DATE'] = pd.to_datetime(df['DATE'])
        
        # Filter by date range if specified
        if start_date:
            df = df[df['DATE'] >= start_date]
        if end_date:
            df = df[df['DATE'] <= end_date]
        
        if df.empty:
            print("✗ No data in specified date range")
            return pd.DataFrame()
        
        # GHCN Daily temperatures are in tenths of degrees Celsius
        # Convert to Fahrenheit: (C * 9/5) + 32, but first divide by 10
        # TMAX, TMIN, TAVG are in tenths of degrees C
        
        # Extract temperature columns
        temp_data = []
        
        for _, row in df.iterrows():
            date = row['DATE']
            
            # Get temperatures (in tenths of degrees C)
            tmax_raw = row.get('TMAX')
            tmin_raw = row.get('TMIN')
            tavg_raw = row.get('TAVG')
            
            # Convert from tenths of degrees C to Fahrenheit
            # Formula: (C / 10 * 9/5) + 32 = (C * 9/50) + 32
            max_temp_f = (tmax_raw * 9 / 50) + 32 if pd.notna(tmax_raw) else None
            min_temp_f = (tmin_raw * 9 / 50) + 32 if pd.notna(tmin_raw) else None
            
            # Use TAVG if available, otherwise calculate average
            if pd.notna(tavg_raw):
                avg_temp_f = (tavg_raw * 9 / 50) + 32
            elif pd.notna(max_temp_f) and pd.notna(min_temp_f):
                avg_temp_f = (max_temp_f + min_temp_f) / 2
            else:
                avg_temp_f = None
            
            # Only include rows with at least some temperature data
            if any([pd.notna(max_temp_f), pd.notna(min_temp_f), pd.notna(avg_temp_f)]):
                temp_data.append({
                    'year': date.year,
                    'mo': date.month,
                    'da': date.day,
                    'temp': avg_temp_f,
                    'max': max_temp_f,
                    'min': min_temp_f
                })
        
        if not temp_data:
            print("✗ No temperature data found")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df_temp = pd.DataFrame(temp_data)
        
        # Process to match our schema
        df_processed = _process_dataframe(df_temp)
        
        print(f"\nRetrieved {len(df_processed)} records")
        print(f"Date range: {df_processed['date'].min().date()} to {df_processed['date'].max().date()}")
        print(f"Average temperature: {df_processed['temp_f'].mean():.1f}°F")
        
        # Save to file if specified
        if output_file:
            _save_data(df_processed, output_file, output_format)
        
        return df_processed
        
    except Exception as e:
        print(f"✗ Error downloading data: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
