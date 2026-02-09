"""Weather data scraper module for querying NOAA GSOD data from BigQuery."""

from pathlib import Path
from google.cloud import bigquery
import pandas as pd

from weather_fetcher.config import PROJECT_DATASET


def scrape_weather_data(wban, start_year, end_year, output_file=None, output_format='parquet'):
    """
    Scrape weather data from BigQuery for a given station and year range.
    
    Args:
        wban: Weather station ID (WBAN)
        start_year: Starting year (inclusive)
        end_year: Ending year (inclusive)
        output_file: Output file path (optional)
        output_format: Output format ('parquet', 'csv', or 'json'). Default: 'parquet'
    
    Returns:
        pandas.DataFrame: Weather data with columns: date, year, month, day, 
                         temp_f, max_temp_f, min_temp_f
    """
    print(f"Scraping weather data for WBAN {wban} from {start_year} to {end_year}...")
    
    # Build SQL query
    selects = []
    for year in range(start_year, end_year + 1):
        selects.append(f"""
        SELECT year, mo, da, temp, max, min
        FROM `{PROJECT_DATASET}.gsod{year}`
        WHERE wban='{wban}'
        """.strip())
    
    sql = "\nUNION ALL\n".join(selects)
    
    # Query BigQuery
    print("Querying BigQuery...")
    client = bigquery.Client()
    job = client.query(sql)
    df = job.to_dataframe()
    
    # Clean and process data
    if not df.empty:
        df = _process_dataframe(df)
        
        print(f"Retrieved {len(df)} records")
        print(f"\nFirst few records:")
        print(df.head())
        print(f"\nDate range: {df['date'].min()} to {df['date'].max()}")
        print(f"Average temperature: {df['temp_f'].mean():.1f}°F")
        
        # Save to file if specified
        if output_file:
            _save_data(df, output_file, output_format)
    else:
        print("No data found for the specified parameters.")
    
    return df


def _process_dataframe(df):
    """Process and clean the raw DataFrame from BigQuery."""
    # Convert date columns to datetime
    df['date'] = pd.to_datetime({
        'year': df['year'],
        'month': df['mo'],
        'day': df['da']
    })
    df = df.sort_values('date')
    
    # Rename columns for clarity
    df = df.rename(columns={
        'mo': 'month',
        'da': 'day',
        'temp': 'temp_f',
        'max': 'max_temp_f',
        'min': 'min_temp_f'
    })
    
    # Reorder columns
    df = df[['date', 'year', 'month', 'day', 'temp_f', 'max_temp_f', 'min_temp_f']]
    
    return df


def _save_data(df, output_file, output_format):
    """Save DataFrame to file in the specified format."""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if output_format.lower() == 'parquet':
        df.to_parquet(output_path, index=False, engine='pyarrow')
        file_size = output_path.stat().st_size
        print(f"\nData saved to {output_path} ({file_size / 1024:.2f} KB)")
    elif output_format.lower() == 'csv':
        df.to_csv(output_path, index=False)
        file_size = output_path.stat().st_size
        print(f"\nData saved to {output_path} ({file_size / 1024:.2f} KB)")
    elif output_format.lower() == 'json':
        df.to_json(output_path, orient='records', date_format='iso')
        file_size = output_path.stat().st_size
        print(f"\nData saved to {output_path} ({file_size / 1024:.2f} KB)")
    else:
        print(f"Unknown format: {output_format}. Data not saved.")
