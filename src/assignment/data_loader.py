"""
Simplified data loading utilities for HTF assignment.
Works with the existing imputation structure and flood data.
"""

import pandas as pd
from pathlib import Path
from typing import Tuple, Dict
import logging

logger = logging.getLogger(__name__)

def load_gauge_county_mapping(filepath: str | Path) -> pd.DataFrame:
    """
    Load the gauge-to-county mapping structure with weights.
    
    Args:
        filepath: Path to the imputation structure parquet file
        
    Returns:
        DataFrame containing the gauge-county relationships and weights
    """
    logger.info(f"Loading gauge-county mapping from {filepath}")
    df = pd.read_parquet(filepath)
    
    # Verify we have the essential columns
    required_columns = [
        'county_fips', 'station_id', 'station_name', 'weight'
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
        filepath: Path to directory containing regional HTF data
        
    Returns:
        DataFrame with historical HTF data
    """
    filepath = Path(filepath)
    
    # Load all regional files
    dfs = []
    for file in filepath.glob("historical_htf_*.parquet"):
        df = pd.read_parquet(file)
        dfs.append(df)
    
    if not dfs:
        raise FileNotFoundError(f"No historical HTF files found in {filepath}")
    
    # Combine all regions
    df = pd.concat(dfs, ignore_index=True)
    
    # Verify we have the required columns
    required_cols = [
        'station_id', 'year', 'flood_days', 'missing_days'
    ]
    
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in historical HTF data: {missing}")
    
    logger.info(f"Loaded {len(df)} historical HTF records")
    return df

def load_htf_data(historical_path: str | Path) -> pd.DataFrame:
    """
    Load historical HTF dataset.
    
    Args:
        historical_path: Path to historical HTF data directory
        
    Returns:
        DataFrame with historical HTF data
    """
    logger.info("Loading HTF dataset")
    historical_df = load_historical_htf(historical_path)
    logger.info(f"Loaded HTF data - {len(historical_df)} records")
    return historical_df

def validate_gauge_coverage(mapping_df: pd.DataFrame, htf_df: pd.DataFrame) -> None:
    """
    Validate gauge coverage by checking which gauges in the mapping have HTF data.
    
    Args:
        mapping_df: DataFrame containing gauge-county mapping
        htf_df: DataFrame containing HTF data
    """
    # Get all gauges from mapping
    mapping_gauges = set(mapping_df['station_id'].unique())
    
    # Check which gauges have HTF data
    htf_gauges = set(htf_df['station_id'].unique())
    missing_gauges = mapping_gauges - htf_gauges
    
    if missing_gauges:
        logger.warning(
            f"Found {len(missing_gauges)} gauges in mapping without HTF data: "
            f"{sorted(list(missing_gauges))[:5]}..."
        ) 