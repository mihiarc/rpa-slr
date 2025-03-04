"""
Main entry point for historical HTF data assignment.
Processes minor flood events from 1970-2024 by region.
"""

from pathlib import Path
from typing import Optional
import logging
import pandas as pd

from src.config import ASSIGNMENT_SETTINGS, NOAA_HISTORICAL_DIR, IMPUTATION_DIR
from src.assignment.historical.data_loader import HistoricalDataLoader
from src.assignment.historical.aggregator import HistoricalAggregator

logger = logging.getLogger(__name__)

def process_historical_htf(
    region: str,
    input_dir: str | Path,
    output_dir: str | Path,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None
) -> None:
    """Process historical HTF data for a specific region.
    
    Args:
        region: Region to process (e.g., 'gulf_coast', 'west_coast')
        input_dir: Base directory containing input data
        output_dir: Directory to save outputs
        start_year: Optional start year (defaults to config setting)
        end_year: Optional end year (defaults to config setting)
    """
    # Use config defaults if not specified
    settings = ASSIGNMENT_SETTINGS['historical']
    start_year = start_year or settings['start_year']
    end_year = end_year or settings['end_year']
    
    logger.info(f"Processing historical HTF data for region: {region}")
    logger.info(f"Time range: {start_year}-{end_year}")
    
    # Initialize paths
    input_dir = Path(input_dir)
    output_dir = Path(output_dir) / region
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get imputation structure for region
    imputation_files = list(IMPUTATION_DIR.glob(f"imputation_structure_{region}_*.parquet"))
    if not imputation_files:
        raise FileNotFoundError(f"No imputation structure found for region: {region}")
    imputation_file = sorted(imputation_files)[-1]  # Use most recent
    
    try:
        # Load imputation structure
        logger.info(f"Loading imputation structure from {imputation_file}")
        imputation_df = pd.read_parquet(imputation_file)
        
        # Load historical data for all stations in the region
        logger.info("Loading historical station data")
        station_data = []
        for station_id in imputation_df['station_id'].unique():
            station_file = NOAA_HISTORICAL_DIR / region / f"station_{station_id}.parquet"
            if station_file.exists():
                df = pd.read_parquet(station_file)
                # Filter to specified years and minor floods only
                df = df[
                    (df['year'] >= start_year) & 
                    (df['year'] <= end_year)
                ].copy()
                # Only keep minor flood counts
                df['flood_days'] = df['minCount']
                df = df.drop(columns=['majCount', 'modCount', 'minCount'])
                station_data.append(df)
        
        if not station_data:
            raise ValueError(f"No historical data found for region: {region}")
        
        station_df = pd.concat(station_data, ignore_index=True)
        
        # Create aggregator
        aggregator = HistoricalAggregator(
            require_same_region=ASSIGNMENT_SETTINGS['common']['require_same_region'],
            require_same_subregion=ASSIGNMENT_SETTINGS['common']['require_same_subregion'].get(region, 
                                                                                               ASSIGNMENT_SETTINGS['common']['require_same_subregion']['default'])
        )
        
        # Aggregate flood days by county
        county_htf = aggregator.aggregate_by_county(
            imputation_df=imputation_df,
            station_data=station_df
        )
        
        # Verify county-year level aggregation
        expected_columns = {'county_fips', 'year', 'region', 'flood_days', 
                          'n_stations', 'n_reference_points', 'completeness'}
        missing_cols = expected_columns - set(county_htf.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns in output: {missing_cols}")
        
        # Verify no duplicate county-year combinations
        duplicates = county_htf.groupby(['county_fips', 'year']).size().reset_index(name='count')
        duplicates = duplicates[duplicates['count'] > 1]
        if not duplicates.empty:
            raise ValueError(f"Found duplicate county-year combinations: {duplicates}")
        
        # Save results in both parquet and CSV formats
        output_base = f"historical_htf_{region}_{start_year}_{end_year}"
        parquet_file = output_dir / f"{output_base}.parquet"
        csv_file = output_dir / f"{output_base}.csv"
        
        # Save parquet (efficient storage, preserves data types)
        county_htf.to_parquet(parquet_file)
        
        # Save CSV (human-readable, widely compatible)
        county_htf.to_csv(csv_file, index=False)
        
        logger.info(f"Saved results to:")
        logger.info(f"  - Parquet: {parquet_file}")
        logger.info(f"  - CSV: {csv_file}")
        
        # Generate summary statistics
        stats = {
            'total_counties': county_htf['county_fips'].nunique(),
            'total_stations': station_df['stnId'].nunique(),
            'year_range': f"{county_htf['year'].min()}-{county_htf['year'].max()}",
            'mean_flood_days': county_htf['flood_days'].mean(),
            'max_flood_days': county_htf['flood_days'].max(),
            'records_per_county': county_htf.groupby('county_fips').size().mean(),
            'total_records': len(county_htf)
        }
        
        logger.info("\nSummary Statistics:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        
    except Exception as e:
        logger.error(f"Error processing region {region}: {str(e)}")
        raise 