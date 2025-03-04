"""
County-level aggregation for historical HTF data.

This module handles the aggregation of station HTF data to county level using
the imputation structure. Supports region-specific processing and configurable
data completeness requirements.

The weighting logic for aggregating HTF (High Tide Flooding) data to county level works as follows:

1. Each reference point represents a segment of coastline. The length of this segment 
   (typically 5km) is used as a weight when aggregating flood statistics.

2. The intuition is that longer coastline segments should have more influence on the 
   county-level statistics than shorter segments. For example:

   - If a county has two reference points, one with 10km of coastline experiencing 
     5 flood days and another with 2km experiencing 1 flood day, the weighted average 
     would be: (10*5 + 2*1)/(10 + 2) = 4.33 flood days
     
   - This is more representative than a simple average of (5+1)/2 = 3 flood days,
     since it accounts for the fact that more of the coastline experiences the 
     higher flood frequency

3. The weighting is implemented using:
   - Segment length calculations for each reference point
   - Weighted averaging when grouping by county
   - Preservation of total coastline length for validation

This weighted approach provides a more accurate representation of flooding impacts
at the county level by accounting for the spatial distribution and extent of 
coastal segments.

"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class HistoricalAggregator:
    """Aggregates historical HTF data to county level."""
    
    def __init__(
        self,
        require_same_region: bool = True,
        require_same_subregion: bool = False
    ):
        """Initialize aggregator with configuration.
        
        Args:
            require_same_region: Whether to require stations and counties in same region
            require_same_subregion: Whether to require stations and counties in same subregion
        """
        self.require_same_region = require_same_region
        self.require_same_subregion = require_same_subregion
        
        logger.info("Initialized historical aggregator with regional filtering only")
        logger.info(f"require_same_region={require_same_region}, require_same_subregion={require_same_subregion}")
    
    def aggregate_by_county(
        self,
        imputation_df: pd.DataFrame,
        station_data: pd.DataFrame
    ) -> pd.DataFrame:
        """Aggregate flood days by county and year.
        
        Args:
            imputation_df: DataFrame with imputation structure
            station_data: DataFrame with station flood days by year
            
        Returns:
            DataFrame with flood days by county and year
        """
        logger.info("Aggregating historical HTF data by county")
        
        # Ensure columns exist
        if 'county_fips' not in imputation_df.columns:
            raise ValueError("Missing required column 'county_fips' in imputation data")
        
        if 'year' not in station_data.columns or 'flood_days' not in station_data.columns:
            raise ValueError("Missing required columns in station data")
        
        # Filter by region/subregion if required
        filtered_df = imputation_df.copy()
        if self.require_same_region:
            filtered_df = filtered_df[
                filtered_df['station_region'] == filtered_df['county_region']
            ]
            logger.info(f"Filtered to {len(filtered_df)} station-county pairs in same region")
        
        if self.require_same_subregion:
            filtered_df = filtered_df[
                filtered_df['station_subregion'] == filtered_df['county_subregion']
            ]
            logger.info(f"Filtered to {len(filtered_df)} station-county pairs in same subregion")
        
        # No minimum weight filter - using all weighted relationships from regional filtering
        if len(filtered_df) == 0:
            logger.warning("No valid station-county relationships found after filtering")
            return pd.DataFrame()
        
        # Join station data to imputation structure
        merged = pd.merge(
            filtered_df,
            station_data,
            on='station_id',
            how='inner'
        )
        
        if len(merged) == 0:
            logger.warning("No matching data between imputation structure and station data")
            return pd.DataFrame()
        
        # Calculate weighted flood days
        merged['weighted_flood_days'] = merged['weight'] * merged['flood_days']
        merged['weighted_completeness'] = merged['weight'] * merged['completeness']
        
        # Group by county and year
        county_data = merged.groupby(['county_fips', 'year', 'region']).agg({
            'weighted_flood_days': 'sum',
            'weighted_completeness': 'sum',
            'weight': 'sum',
            'station_id': 'nunique',
            'reference_id': 'nunique'
        }).reset_index()
        
        # Calculate final values
        county_data.rename(columns={
            'weighted_flood_days': 'flood_days',
            'weighted_completeness': 'completeness',
            'station_id': 'n_stations',
            'reference_id': 'n_reference_points'
        }, inplace=True)
        
        logger.info(f"Generated flood day estimates for {len(county_data)} county-year combinations")
        
        return county_data 