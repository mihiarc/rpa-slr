"""
Simplified HTF data assignment module.
Works with the existing imputation structure and flood data.

Notes on HTF Data:
- Missing days (NaN) indicate periods before a tide station was installed
- Zero flood days are valid measurements indicating no flooding occurred
- Flood days have generally increased in recent years due to sea level rise
- Early years often have legitimate zero flood days, not missing data

IMPORTANT - Flood Severity:
    The 'flood_days' column in all outputs represents MINOR flood days only.
    Major and moderate flood events from the NOAA API are intentionally excluded.
    See src/noaa/historical/historical_htf_processor.py for rationale.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Optional, Generator
import numpy as np
import gc
import psutil
from tqdm import tqdm

from src.config import ASSIGNMENT_SETTINGS

logger = logging.getLogger(__name__)

def log_memory_usage():
    """Log current memory usage."""
    process = psutil.Process()
    memory_gb = process.memory_info().rss / 1024 / 1024 / 1024
    logger.info(f"Current memory usage: {memory_gb:.2f} GB")

def optimize_dtypes(df: pd.DataFrame, copy: bool = False) -> pd.DataFrame:
    """Simple data type optimization focusing on numeric columns.

    Args:
        df: DataFrame to optimize
        copy: If True, create a copy before modifying. Default False for performance.

    Returns:
        Optimized DataFrame (same object if copy=False)
    """
    if copy:
        df = df.copy()

    # Handle numeric columns
    numeric_cols = {
        'year': np.int16,
        'flood_days': np.float32,
        'missing_days': np.float32,
        'weight': np.float32
    }

    for col, dtype in numeric_cols.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)

    return df

def process_in_chunks(
    htf_df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    chunk_size: int = 100,
    start_year: int = ASSIGNMENT_SETTINGS['historical']['start_year']
) -> Generator[pd.DataFrame, None, None]:
    """Process HTF data in chunks to manage memory usage.

    Notes:
        - Missing days indicate periods before station installation
        - Zero flood days are valid measurements (no flooding)
        - We keep all records from start_year onwards, as zeros are valid data

    Args:
        htf_df: Historical HTF data
        mapping_df: Gauge-county mapping with weights
        chunk_size: Number of stations to process at once
        start_year: Start year for analysis (inclusive)
    """
    # Keep only necessary columns FIRST (before any filtering to reduce memory)
    htf_cols = ['station_id', 'year', 'flood_days', 'missing_days']
    mapping_cols = ['station_id', 'county_fips', 'weight', 'region']

    htf_df = htf_df[htf_cols]
    mapping_df = mapping_df[mapping_cols]

    # Filter by year (use query for efficiency on large dataframes)
    htf_df = htf_df[htf_df['year'] >= start_year]
    logger.info(f"Processing {len(htf_df)} records from {start_year} onwards")

    # Log data completeness
    total_records = len(htf_df)
    if total_records > 0:
        missing_records = (htf_df['missing_days'] == 365).sum()
        zero_flood_records = (htf_df['flood_days'] == 0).sum()
        logger.info(f"Data overview:")
        logger.info(f"  - Total records: {total_records}")
        logger.info(f"  - Records with no floods: {zero_flood_records} ({zero_flood_records/total_records*100:.1f}%)")
        logger.info(f"  - Records before station installation: {missing_records} ({missing_records/total_records*100:.1f}%)")

    # Optimize dtypes ONCE before chunking (modifies in place)
    optimize_dtypes(htf_df)
    optimize_dtypes(mapping_df)

    # Get unique stations
    stations = htf_df['station_id'].unique()

    # Process stations in chunks
    for i in range(0, len(stations), chunk_size):
        chunk_stations = set(stations[i:i + chunk_size])

        # Filter data for current chunk using isin (already optimized types)
        chunk_htf = htf_df[htf_df['station_id'].isin(chunk_stations)]
        chunk_mapping = mapping_df[mapping_df['station_id'].isin(chunk_stations)]

        # Process chunk - merge without additional copies
        merged_df = pd.merge(
            chunk_mapping,
            chunk_htf,
            on='station_id',
            how='left'
        )
        
        # Calculate weighted values
        # Note: missing_days of 365 indicate no station data, but flood_days of 0 are valid measurements
        merged_df['weighted_flood_days'] = merged_df['flood_days'] * merged_df['weight']
        merged_df['weighted_missing_days'] = merged_df['missing_days'] * merged_df['weight']
        
        # Group by county and year
        county_htf = merged_df.groupby(['county_fips', 'year']).agg({
            'weighted_flood_days': 'sum',
            'weighted_missing_days': 'sum',
            'weight': 'sum',
            'region': 'first'
        }).reset_index()
        
        # Calculate final values with division by zero protection
        # Counties with zero total weight (no valid station data) will get NaN
        with np.errstate(divide='ignore', invalid='ignore'):
            county_htf['flood_days'] = np.where(
                county_htf['weight'] > 0,
                (county_htf['weighted_flood_days'] / county_htf['weight']).astype(np.float32),
                np.nan
            )
            county_htf['missing_days'] = np.where(
                county_htf['weight'] > 0,
                (county_htf['weighted_missing_days'] / county_htf['weight']).astype(np.float32),
                np.nan
            )

        # Log warning if any counties have no valid data
        zero_weight_count = (county_htf['weight'] == 0).sum()
        if zero_weight_count > 0:
            logger.warning(f"{zero_weight_count} county-year records have zero weight (no station data)")
            county_htf = county_htf[county_htf['weight'] > 0]
        
        # Drop intermediate columns
        county_htf = county_htf.drop(columns=[
            'weighted_flood_days', 'weighted_missing_days', 'weight'
        ])
        
        yield county_htf
        
        # Clean up memory
        del chunk_htf, chunk_mapping, merged_df, county_htf
        gc.collect()

def calculate_county_htf(
    htf_df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    chunk_size: int = 100,
    start_year: int = ASSIGNMENT_SETTINGS['historical']['start_year']
) -> pd.DataFrame:
    """Calculate county-level HTF values using weighted station data.
    
    Notes:
        - Zero flood days are valid measurements indicating no flooding
        - Missing days (365) indicate periods before station installation
        - Early years often have legitimate zero flood days due to lower sea levels
    """
    logger.info("Calculating county-level HTF values")
    log_memory_usage()
    
    # Process data in chunks
    results = []
    for chunk_result in tqdm(
        process_in_chunks(htf_df, mapping_df, chunk_size, start_year),
        desc="Processing stations in chunks",
        total=len(htf_df['station_id'].unique()) // chunk_size + 1
    ):
        results.append(chunk_result)
        
        # Log memory usage periodically
        if len(results) % 10 == 0:
            log_memory_usage()
    
    # Combine results
    county_htf = pd.concat(results, ignore_index=True)
    
    # Final aggregation if needed
    county_htf = county_htf.groupby(['county_fips', 'year', 'region']).agg({
        'flood_days': 'mean',
        'missing_days': 'mean'
    }).reset_index()
    
    # Ensure final dtypes (in-place for efficiency)
    optimize_dtypes(county_htf)
    
    # Log completion statistics
    logger.info(f"Calculated HTF values for {county_htf['county_fips'].nunique()} counties")
    logger.info(f"Year range: {county_htf['year'].min()} - {county_htf['year'].max()}")
    
    # Calculate trend statistics
    early_years = county_htf[county_htf['year'] <= 1980]['flood_days'].mean()
    recent_years = county_htf[county_htf['year'] >= 2010]['flood_days'].mean()
    logger.info(f"\nTrend Analysis:")
    logger.info(f"Average flood days (1970-1980): {early_years:.2f}")
    logger.info(f"Average flood days (2010-2024): {recent_years:.2f}")
    logger.info(f"Relative increase: {((recent_years/early_years)-1)*100:.1f}%")
    
    log_memory_usage()
    return county_htf

def process_htf_assignment(
    mapping_path: str | Path,
    historical_path: str | Path,
    output_dir: str | Path
) -> None:
    """
    Process HTF data assignment using simplified approach.
    
    Args:
        mapping_path: Path to gauge-county mapping file
        historical_path: Path to historical HTF data directory
        output_dir: Directory to save outputs
    """
    from .data_loader import load_gauge_county_mapping, load_htf_data, validate_gauge_coverage
    
    logger.info("Starting simplified HTF assignment process")
    log_memory_usage()
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load data
    mapping_df = load_gauge_county_mapping(mapping_path)
    htf_df = load_htf_data(historical_path)
    
    # Validate gauge coverage
    validate_gauge_coverage(mapping_df, htf_df)
    
    # Calculate county-level HTF values
    county_htf = calculate_county_htf(htf_df, mapping_df)
    
    # Clean up memory before saving
    del htf_df, mapping_df
    gc.collect()
    log_memory_usage()
    
    # Save results in chunks
    chunk_size = 500  # Smaller chunks for saving
    for i in range(0, len(county_htf), chunk_size):
        chunk = county_htf.iloc[i:i + chunk_size]
        output_file = output_dir / f"county_htf_values_{i//chunk_size}.parquet"
        chunk.to_parquet(output_file, compression='snappy')
    
    logger.info(f"Saved county HTF values in chunks to {output_dir}")
    
    # Display summary statistics
    logger.info("\nSummary Statistics:")
    logger.info(f"Total counties: {county_htf['county_fips'].nunique()}")
    logger.info(f"Year range: {county_htf['year'].min()} - {county_htf['year'].max()}")
    logger.info("\nStatistics by region:")
    region_stats = county_htf.groupby('region').agg({
        'county_fips': 'nunique',
        'flood_days': ['mean', 'max']
    }).round(2)
    region_stats.columns = ['num_counties', 'avg_flood_days', 'max_flood_days']
    logger.info(region_stats.to_string())
    
    logger.info("\nFlood days statistics:")
    logger.info(county_htf['flood_days'].describe().round(3).to_string())
    
    # Display sample of results with better formatting
    logger.info("\nSample of county HTF values:")
    sample_df = county_htf.head()
    sample_df['flood_days'] = sample_df['flood_days'].round(2)
    sample_df['missing_days'] = sample_df['missing_days'].round(1)
    logger.info(sample_df.to_string(index=False)) 