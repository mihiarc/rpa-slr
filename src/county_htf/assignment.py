"""
Core logic for assigning gauge-level HTF data to counties using weighted relationships.
"""

import pandas as pd
import numpy as np
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

def prepare_historical_gauge_data(htf_df: pd.DataFrame, mapping_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare historical gauge data by ensuring all required gauges have data for all years.
    
    Args:
        htf_df: DataFrame containing historical HTF data
        mapping_df: DataFrame containing gauge-county mapping
        
    Returns:
        DataFrame with complete historical gauge data
    """
    years = htf_df['year'].unique()
    
    # Get all gauges from mapping
    mapping_gauges = set()
    for i in range(1, 4):
        gauge_col = f'gauge_id_{i}'
        if gauge_col in mapping_df.columns:
            gauges = mapping_df[gauge_col].dropna().unique()
            mapping_gauges.update(gauges)
    
    # Create complete index of year-gauge combinations
    index = pd.MultiIndex.from_product(
        [years, list(mapping_gauges)],
        names=['year', 'station_id']
    )
    
    # Reindex HTF data to ensure all combinations exist
    complete_df = htf_df.set_index(['year', 'station_id']).reindex(index)
    complete_df = complete_df.fillna(0)  # Assume no floods when data is missing
    
    return complete_df.reset_index()

def prepare_projected_gauge_data(htf_df: pd.DataFrame, mapping_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare projected gauge data.
    
    Args:
        htf_df: DataFrame containing projected HTF data
        mapping_df: DataFrame containing gauge-county mapping
        
    Returns:
        DataFrame with complete projected gauge data
    """
    years = htf_df['year'].unique()
    
    # Get all gauges from mapping
    mapping_gauges = set()
    for i in range(1, 4):
        gauge_col = f'gauge_id_{i}'
        if gauge_col in mapping_df.columns:
            gauges = mapping_df[gauge_col].dropna().unique()
            mapping_gauges.update(gauges)
    
    # Create complete index of year-gauge combinations
    index = pd.MultiIndex.from_product(
        [years, list(mapping_gauges)],
        names=['year', 'station_id']
    )
    
    # For projected data, we'll keep NaN values as they represent missing projections
    complete_df = htf_df.set_index(['year', 'station_id']).reindex(index)
    
    return complete_df.reset_index()

def calculate_historical_county_htf(
    htf_df: pd.DataFrame,
    mapping_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate county-level historical HTF values.
    
    Args:
        htf_df: DataFrame containing historical HTF data
        mapping_df: DataFrame containing gauge-county mapping
        
    Returns:
        DataFrame with county-level historical HTF values
    """
    logger.info("Calculating county-level historical HTF values")
    
    flood_columns = [
        'total_flood_days', 'major_flood_days', 
        'moderate_flood_days', 'minor_flood_days'
    ]
    
    complete_gauge_data = prepare_historical_gauge_data(htf_df, mapping_df)
    county_results = []
    
    for county_fips in mapping_df['county_fips'].unique():
        county_map = mapping_df[mapping_df['county_fips'] == county_fips].iloc[0]
        county_data = []
        
        for i in range(1, county_map['n_gauges'] + 1):
            gauge_id = county_map[f'gauge_id_{i}']
            weight = county_map[f'weight_{i}']
            
            if pd.isna(gauge_id) or pd.isna(weight):
                continue
            
            gauge_data = complete_gauge_data[
                complete_gauge_data['station_id'] == gauge_id
            ].copy()
            
            if len(gauge_data) == 0:
                continue
            
            # Apply weight to flood columns
            for col in flood_columns:
                if col in gauge_data.columns:
                    gauge_data[f'weighted_{col}'] = gauge_data[col] * weight
            
            county_data.append(gauge_data)
        
        if not county_data:
            logger.warning(f"No historical gauge data found for county {county_fips}")
            continue
        
        county_df = pd.concat(county_data)
        weighted_cols = [f'weighted_{col}' for col in flood_columns]
        
        yearly_agg = county_df.groupby('year').agg({
            col: 'sum' for col in weighted_cols
        }).reset_index()
        
        # Rename columns back to original names
        for col in flood_columns:
            yearly_agg = yearly_agg.rename(
                columns={f'weighted_{col}': col}
            )
        
        yearly_agg['county_fips'] = county_fips
        yearly_agg['county_name'] = county_map['county_name']
        yearly_agg['state_fips'] = county_map['state_fips']
        
        county_results.append(yearly_agg)
    
    if not county_results:
        raise ValueError("No valid county results generated for historical data.")
    
    result_df = pd.concat(county_results, ignore_index=True)
    logger.info(
        f"Completed historical HTF calculations for {len(county_results)} counties "
        f"across {result_df['year'].nunique()} years"
    )
    
    return result_df

def calculate_projected_county_htf(
    htf_df: pd.DataFrame,
    mapping_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate county-level projected HTF values.
    
    Args:
        htf_df: DataFrame containing projected HTF data
        mapping_df: DataFrame containing gauge-county mapping
        
    Returns:
        DataFrame with county-level projected HTF values
    """
    logger.info("Calculating county-level projected HTF values")
    
    scenario_columns = [
        'low_scenario', 'intermediate_low_scenario', 
        'intermediate_scenario', 'intermediate_high_scenario',
        'high_scenario'
    ]
    
    complete_gauge_data = prepare_projected_gauge_data(htf_df, mapping_df)
    county_results = []
    
    for county_fips in mapping_df['county_fips'].unique():
        county_map = mapping_df[mapping_df['county_fips'] == county_fips].iloc[0]
        county_data = []
        
        for i in range(1, county_map['n_gauges'] + 1):
            gauge_id = county_map[f'gauge_id_{i}']
            weight = county_map[f'weight_{i}']
            
            if pd.isna(gauge_id) or pd.isna(weight):
                continue
            
            gauge_data = complete_gauge_data[
                complete_gauge_data['station_id'] == gauge_id
            ].copy()
            
            if len(gauge_data) == 0:
                continue
            
            # Apply weight to scenario columns
            for col in scenario_columns:
                if col in gauge_data.columns:
                    gauge_data[f'weighted_{col}'] = gauge_data[col] * weight
            
            county_data.append(gauge_data)
        
        if not county_data:
            logger.warning(f"No projected gauge data found for county {county_fips}")
            continue
        
        county_df = pd.concat(county_data)
        weighted_cols = [f'weighted_{col}' for col in scenario_columns]
        
        yearly_agg = county_df.groupby('year').agg({
            col: 'sum' for col in weighted_cols
        }).reset_index()
        
        # Rename columns back to original names
        for col in scenario_columns:
            yearly_agg = yearly_agg.rename(
                columns={f'weighted_{col}': col}
            )
        
        yearly_agg['county_fips'] = county_fips
        yearly_agg['county_name'] = county_map['county_name']
        yearly_agg['state_fips'] = county_map['state_fips']
        
        county_results.append(yearly_agg)
    
    if not county_results:
        raise ValueError("No valid county results generated for projected data.")
    
    result_df = pd.concat(county_results, ignore_index=True)
    logger.info(
        f"Completed projected HTF calculations for {len(county_results)} counties "
        f"across {result_df['year'].nunique()} years"
    )
    
    return result_df 