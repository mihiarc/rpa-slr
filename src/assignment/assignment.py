"""
Core logic for assigning gauge-level HTF data to counties using weighted relationships.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import logging
from tqdm.auto import tqdm
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import os

logger = logging.getLogger(__name__)

def init_tqdm():
    """Initialize tqdm for child processes."""
    tqdm.set_lock(multiprocessing.RLock())

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

def prepare_gauge_lookup(complete_gauge_data: pd.DataFrame, mapping_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Pre-compute gauge data lookup for faster access.
    
    Args:
        complete_gauge_data: DataFrame with complete gauge data
        mapping_df: DataFrame with gauge-county mapping
        
    Returns:
        Dictionary mapping gauge IDs to their data
    """
    mapping_gauges = set()
    for i in range(1, 4):
        gauge_col = f'gauge_id_{i}'
        if gauge_col in mapping_df.columns:
            gauges = mapping_df[gauge_col].dropna().unique()
            mapping_gauges.update(gauges)
    
    return {
        gauge_id: complete_gauge_data[complete_gauge_data['station_id'] == gauge_id]
        for gauge_id in mapping_gauges
    }

def calculate_county_values(
    county_data: Tuple[pd.Series, pd.DataFrame, Dict[str, pd.DataFrame], List[str], List[int]]
) -> pd.DataFrame:
    """
    Calculate HTF values for a single county using vectorized operations.
    
    Args:
        county_data: Tuple containing (county info, mapping data, gauge lookup, columns, years)
        
    Returns:
        DataFrame with county results
    """
    county, county_mapping, gauge_lookup, value_columns, years = county_data
    gauge_data = []
    has_data = False
    
    # Get gauge data for this county
    if not county_mapping.empty:
        for i in range(1, 4):
            gauge_id = county_mapping.iloc[0].get(f'gauge_id_{i}')
            weight = county_mapping.iloc[0].get(f'weight_{i}', 0)
            
            if pd.notna(gauge_id) and weight > 0:
                gauge_df = gauge_lookup.get(gauge_id)
                if gauge_df is not None and not gauge_df.empty:
                    gauge_data.append((gauge_df, weight))
                    has_data = True
    
    if not has_data:
        # Create empty data with NaN values
        empty_data = pd.DataFrame({
            'year': years,
            'county_fips': county['county_fips'],
            'county_name': county['county_name'],
            'state_fips': county['state_fips'],
            'geometry': county['geometry']
        })
        for col in value_columns:
            empty_data[col] = np.nan
        return empty_data
    
    # Vectorized calculation for all years
    yearly_data = []
    for gauge_df, weight in gauge_data:
        # Process each value column separately
        gauge_values = {}
        for col in value_columns:
            # Get values for this column and weight them
            values = gauge_df.pivot(index='year', columns='station_id', values=col)
            # Sum across stations if multiple exist
            values = values.sum(axis=1) * weight
            gauge_values[col] = values
        yearly_data.append(gauge_values)
    
    # Combine all gauge data
    combined_data = {}
    for col in value_columns:
        # Sum the weighted values from all gauges
        col_data = pd.Series(0, index=years)
        for data in yearly_data:
            col_data = col_data.add(data[col], fill_value=0)
        combined_data[col] = col_data
    
    # Create final county data
    result_data = pd.DataFrame({
        'year': years,
        'county_fips': county['county_fips'],
        'county_name': county['county_name'],
        'state_fips': county['state_fips'],
        'geometry': county['geometry']
    })
    
    # Add calculated columns
    for col in value_columns:
        result_data[col] = combined_data[col].values
    
    return result_data

def calculate_historical_county_htf(
    htf_df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    n_processes: int = None
) -> pd.DataFrame:
    """
    Calculate county-level historical HTF values using parallel processing.
    
    Args:
        htf_df: DataFrame containing historical HTF data
        mapping_df: DataFrame containing gauge-county mapping
        n_processes: Number of processes to use (defaults to CPU count - 1)
        
    Returns:
        DataFrame with county-level historical HTF values
    """
    logger.info("Calculating county-level historical HTF values")
    
    if n_processes is None:
        n_processes = max(1, multiprocessing.cpu_count() - 1)
    
    flood_columns = [
        'total_flood_days', 'major_flood_days', 
        'moderate_flood_days', 'minor_flood_days'
    ]
    
    # Prepare data
    complete_gauge_data = prepare_historical_gauge_data(htf_df, mapping_df)
    years = complete_gauge_data['year'].unique()
    gauge_lookup = prepare_gauge_lookup(complete_gauge_data, mapping_df)
    
    # Get unique counties
    unique_counties = mapping_df[['county_fips', 'county_name', 'state_fips', 'geometry']].drop_duplicates()
    total_counties = len(unique_counties)
    
    # Prepare data for parallel processing
    county_data = [
        (
            county,
            mapping_df[mapping_df['county_fips'] == county['county_fips']],
            gauge_lookup,
            flood_columns,
            years
        )
        for _, county in unique_counties.iterrows()
    ]
    
    # Process counties in parallel with enhanced progress bar
    logger.info(f"Processing {total_counties} counties using {n_processes} processes")
    
    # Initialize progress bar
    pbar = tqdm(
        total=total_counties,
        desc="Processing historical HTF by county",
        unit="county",
        position=0,
        leave=True,
        ncols=100,
        miniters=1
    )
    
    # Process counties in parallel
    county_results = []
    with ProcessPoolExecutor(
        max_workers=n_processes, 
        initializer=init_tqdm
    ) as executor:
        futures = [
            executor.submit(calculate_county_values, data)
            for data in county_data
        ]
        
        for future in futures:
            result = future.result()
            county_results.append(result)
            pbar.update(1)
    
    pbar.close()
    
    # Combine results
    result_df = pd.concat(county_results, ignore_index=True)
    
    # Log summary
    counties_with_data = result_df[flood_columns[0]].notna().groupby(result_df['county_fips']).any()
    counties_without_data = counties_with_data[~counties_with_data].index
    
    if len(counties_without_data) > 0:
        logger.info(f"Found {len(counties_without_data)} counties with no historical gauge data:")
        sample_counties = result_df[
            result_df['county_fips'].isin(counties_without_data.tolist()[:10])
        ][['county_fips', 'county_name', 'state_fips']].drop_duplicates()
        
        for _, county in sample_counties.iterrows():
            logger.info(f"  - {county['county_name']} County (FIPS: {county['county_fips']}, State: {county['state_fips']})")
        
        if len(counties_without_data) > 10:
            logger.info(f"  ... and {len(counties_without_data) - 10} more")
    
    logger.info(
        f"Completed historical HTF calculations for {result_df['county_fips'].nunique()} counties "
        f"across {len(years)} years"
    )
    
    return result_df

def calculate_projected_county_htf(
    htf_df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    n_processes: int = None
) -> pd.DataFrame:
    """
    Calculate county-level projected HTF values using parallel processing.
    
    Args:
        htf_df: DataFrame containing projected HTF data
        mapping_df: DataFrame containing gauge-county mapping
        n_processes: Number of processes to use (defaults to CPU count - 1)
        
    Returns:
        DataFrame with county-level projected HTF values
    """
    logger.info("Calculating county-level projected HTF values")
    
    if n_processes is None:
        n_processes = max(1, multiprocessing.cpu_count() - 1)
    
    scenario_columns = [
        'low_scenario', 'intermediate_low_scenario', 'intermediate_scenario',
        'intermediate_high_scenario', 'high_scenario'
    ]
    
    # Prepare data
    complete_gauge_data = prepare_projected_gauge_data(htf_df, mapping_df)
    years = complete_gauge_data['year'].unique()
    gauge_lookup = prepare_gauge_lookup(complete_gauge_data, mapping_df)
    
    # Get unique counties
    unique_counties = mapping_df[['county_fips', 'county_name', 'state_fips', 'geometry']].drop_duplicates()
    total_counties = len(unique_counties)
    
    # Prepare data for parallel processing
    county_data = [
        (
            county,
            mapping_df[mapping_df['county_fips'] == county['county_fips']],
            gauge_lookup,
            scenario_columns,
            years
        )
        for _, county in unique_counties.iterrows()
    ]
    
    # Process counties in parallel with enhanced progress bar
    logger.info(f"Processing {total_counties} counties using {n_processes} processes")
    
    # Initialize progress bar
    pbar = tqdm(
        total=total_counties,
        desc="Processing projected HTF by county",
        unit="county",
        position=0,
        leave=True,
        ncols=100,
        miniters=1
    )
    
    # Process counties in parallel
    county_results = []
    with ProcessPoolExecutor(
        max_workers=n_processes, 
        initializer=init_tqdm
    ) as executor:
        futures = [
            executor.submit(calculate_county_values, data)
            for data in county_data
        ]
        
        for future in futures:
            result = future.result()
            county_results.append(result)
            pbar.update(1)
    
    pbar.close()
    
    # Combine results
    result_df = pd.concat(county_results, ignore_index=True)
    
    # Log summary
    counties_with_data = result_df[scenario_columns[0]].notna().groupby(result_df['county_fips']).any()
    counties_without_data = counties_with_data[~counties_with_data].index
    
    if len(counties_without_data) > 0:
        logger.info(f"Found {len(counties_without_data)} counties with no projected gauge data:")
        sample_counties = result_df[
            result_df['county_fips'].isin(counties_without_data.tolist()[:10])
        ][['county_fips', 'county_name', 'state_fips']].drop_duplicates()
        
        for _, county in sample_counties.iterrows():
            logger.info(f"  - {county['county_name']} County (FIPS: {county['county_fips']}, State: {county['state_fips']})")
        
        if len(counties_without_data) > 10:
            logger.info(f"  ... and {len(counties_without_data) - 10} more")
    
    logger.info(
        f"Completed projected HTF calculations for {result_df['county_fips'].nunique()} counties "
        f"across {len(years)} years"
    )
    
    return result_df 