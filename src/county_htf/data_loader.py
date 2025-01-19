"""
Data loading utilities for county-level HTF assignment.
"""

import pandas as pd
from pathlib import Path
from typing import Tuple, Dict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_gauge_county_mapping(filepath: str | Path) -> pd.DataFrame:
    """
    Load the gauge-to-county mapping structure with weights.
    
    Args:
        filepath: Path to the mapping parquet file
        
    Returns:
        DataFrame containing the gauge-county relationships and weights
    """
    logger.info(f"Loading gauge-county mapping from {filepath}")
    df = pd.read_parquet(filepath)
    
    required_columns = [
        'county_fips', 'county_name', 'state_fips', 'geometry',
        'gauge_id_1', 'gauge_name_1', 'distance_1', 'weight_1',
        'n_gauges', 'total_weight'
    ]
    
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in mapping file: {missing_cols}")
    
    logger.info(f"Loaded mapping with {len(df)} records covering {df['county_fips'].nunique()} counties")
    return df

def load_historical_htf(filepath: str | Path) -> pd.DataFrame:
    """
    Load historical HTF data.
    
    Args:
        filepath: Path to historical HTF data
        
    Returns:
        DataFrame with historical HTF data
    """
    df = pd.read_parquet(filepath)
    
    required_cols = [
        'station_id', 'station_name', 'year', 
        'major_flood_days', 'moderate_flood_days', 'minor_flood_days',
        'total_flood_days'
    ]
    
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in historical HTF data: {missing}")
    
    return df

def load_projected_htf(filepath: str | Path) -> pd.DataFrame:
    """
    Load and transform projected HTF data.
    
    Args:
        filepath: Path to projected HTF data
        
    Returns:
        DataFrame with projected HTF data
    """
    df = pd.read_parquet(filepath)
    
    required_cols = [
        'station', 'station_name', 'decade',
        'low_scenario', 'intermediate_low_scenario', 'intermediate_scenario',
        'intermediate_high_scenario', 'high_scenario'
    ]
    
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in projected HTF data: {missing}")
    
    # Rename station column to match historical data
    df = df.rename(columns={'station': 'station_id'})
    
    # Convert decade to individual years
    years_df = []
    for _, row in df.iterrows():
        # Create entries for each year in the decade
        for year in range(row['decade'], row['decade'] + 10):
            year_data = {
                'station_id': row['station_id'],
                'station_name': row['station_name'],
                'year': year,
                'low_scenario': row['low_scenario'],
                'intermediate_low_scenario': row['intermediate_low_scenario'],
                'intermediate_scenario': row['intermediate_scenario'],
                'intermediate_high_scenario': row['intermediate_high_scenario'],
                'high_scenario': row['high_scenario']
            }
            years_df.append(year_data)
    
    df_expanded = pd.DataFrame(years_df)
    
    logger.info(
        f"Expanded projected data from {len(df)} decade records to "
        f"{len(df_expanded)} yearly records"
    )
    
    return df_expanded

def load_htf_data(historical_path: str | Path, projected_path: str | Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load both historical and projected HTF datasets.
    
    Args:
        historical_path: Path to historical HTF data
        projected_path: Path to projected HTF data
        
    Returns:
        Tuple of (historical_df, projected_df)
    """
    logger.info("Loading HTF datasets")
    
    historical_df = load_historical_htf(historical_path)
    projected_df = load_projected_htf(projected_path)
    
    logger.info(f"Loaded HTF data - Historical: {len(historical_df)} records, Projected: {len(projected_df)} records")
    return historical_df, projected_df

def validate_gauge_coverage(mapping_df: pd.DataFrame, htf_df: pd.DataFrame, dataset_name: str) -> None:
    """
    Validate that all gauges in the mapping have corresponding HTF data.
    
    Args:
        mapping_df: Gauge-county mapping DataFrame
        htf_df: HTF data DataFrame
        dataset_name: Name of the dataset being validated ('historical' or 'projected')
    """
    # Get all unique gauge IDs from mapping (excluding missing values)
    mapping_gauges = set()
    for i in range(1, 4):
        gauge_col = f'gauge_id_{i}'
        if gauge_col in mapping_df.columns:
            gauges = mapping_df[gauge_col].dropna().unique()
            mapping_gauges.update(gauges)
    
    htf_gauges = set(htf_df['station_id'].unique())
    missing_gauges = mapping_gauges - htf_gauges
    
    if missing_gauges:
        logger.warning(
            f"Found {len(missing_gauges)} gauges in mapping without {dataset_name} HTF data: "
            f"{sorted(missing_gauges)[:5]}{'...' if len(missing_gauges) > 5 else ''}"
        ) 