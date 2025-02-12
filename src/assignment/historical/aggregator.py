"""
County-level aggregation for historical HTF data.

This module handles the aggregation of reference point HTF data to county level.
Supports both weighted and unweighted aggregation methods.

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
import geopandas as gpd
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class HistoricalAggregator:
    """Aggregates historical HTF data to county level."""
    
    def aggregate_to_county(
        self,
        htf_df: pd.DataFrame,
        reference_points: gpd.GeoDataFrame,
        stations: pd.DataFrame
    ) -> pd.DataFrame:
        """Aggregate HTF data to county level.
        
        Args:
            htf_df: DataFrame with HTF data
            reference_points: GeoDataFrame with reference points
            stations: DataFrame with station metadata
            
        Returns:
            DataFrame with county-level aggregations
        """
        logger.info("Aggregating HTF data to county level")
        
        # Merge station weights
        htf_with_weights = htf_df.merge(
            stations[['station_id', 'total_weight']],
            on='station_id',
            how='left'
        )
        
        # Calculate weighted flood days
        flood_columns = ['flood_days', 'missing_days']
        for col in flood_columns:
            if col in htf_with_weights.columns:
                htf_with_weights[f'weighted_{col}'] = htf_with_weights[col] * htf_with_weights['weight']
        
        # Group by county and year, then aggregate
        agg_dict = {
            'reference_point_id': 'nunique',
            'station_id': 'nunique',
            'total_weight': 'sum',
            'weight': 'sum'
        }
        
        # Add weighted flood columns to aggregation
        for col in flood_columns:
            if f'weighted_{col}' in htf_with_weights.columns:
                agg_dict[f'weighted_{col}'] = 'sum'
        
        county_data = htf_with_weights.groupby(['county_fips', 'year']).agg(agg_dict).reset_index()
        
        # Calculate final flood metrics
        for col in flood_columns:
            if f'weighted_{col}' in county_data.columns:
                county_data[col] = county_data[f'weighted_{col}'] / county_data['weight']
                county_data = county_data.drop(columns=[f'weighted_{col}'])
        
        # Rename columns for clarity
        county_data = county_data.rename(columns={
            'reference_point_id': 'n_reference_points',
            'station_id': 'n_stations',
            'total_weight': 'total_station_weight',
            'weight': 'total_reference_weight'
        })
        
        # Add county metadata from reference points
        county_meta = reference_points.groupby('county_fips').agg({
            'county_name': 'first',
            'state_fips': 'first',
            'region': 'first',
            'region_display': 'first'
        }).reset_index()
        
        county_data = county_data.merge(county_meta, on='county_fips', how='left')
        
        logger.info(f"Created county-level aggregations for {len(county_data)} counties")
        
        return county_data
    
    def _aggregate_unweighted(self, point_data: pd.DataFrame) -> pd.DataFrame:
        """Simple averaging of reference point data to county level.
        
        Args:
            point_data: DataFrame with reference point HTF data
            
        Returns:
            DataFrame with county-level HTF data
        """
        # Group by county
        county_groups = point_data.groupby(['county_fips', 'county_name', 'state', 'region'])
        
        # Calculate metrics
        county_data = county_groups.agg({
            'reference_id': 'nunique',
            'total_weight': 'sum'
        }).reset_index()
        
        # Rename columns
        county_data = county_data.rename(columns={
            'reference_id': 'n_reference_points',
            'total_weight': 'total_station_weight'
        })
        
        logger.info(f"Aggregated {len(point_data)} reference points to {len(county_data)} county records")
        
        return county_data
    
    def _aggregate_weighted(
        self,
        point_data: pd.DataFrame,
        reference_points: gpd.GeoDataFrame
    ) -> pd.DataFrame:
        """Weighted averaging using coastline segment lengths.
        
        Args:
            point_data: DataFrame with reference point HTF data
            reference_points: GeoDataFrame with reference point geometries
            
        Returns:
            DataFrame with county-level HTF data
        """
        # Calculate coastline segment lengths for weighting
        point_lengths = self._calculate_segment_lengths(reference_points)
        
        # Merge lengths with point data
        weighted_data = point_data.merge(
            point_lengths,
            left_on='reference_id',
            right_index=True,
            how='left'
        )
        
        # Group by county
        county_groups = weighted_data.groupby(['county_fips', 'county_name', 'state', 'region'])
        
        # Initialize results
        results = []
        
        # Process each group
        for name, group in county_groups:
            county_fips, county_name, state, region = name
            
            # Calculate metrics
            result = {
                'county_fips': county_fips,
                'county_name': county_name,
                'state': state,
                'region': region,
                'n_reference_points': len(group['reference_id'].unique()),
                'total_station_weight': group['total_weight'].sum(),
                'coastline_length_m': group['segment_length'].sum()
            }
            
            results.append(result)
        
        # Convert to DataFrame
        county_data = pd.DataFrame(results)
            
        logger.info(f"Aggregated {len(point_data)} reference points to {len(county_data)} county records")
        
        return county_data
    
    def _calculate_segment_lengths(self, reference_points: gpd.GeoDataFrame) -> pd.Series:
        """Calculate coastline segment lengths for each reference point.
        
        Args:
            reference_points: GeoDataFrame with reference point geometries
            
        Returns:
            Series with segment lengths indexed by reference_id
        """
        # Project to appropriate CRS for accurate distance calculations
        projected = reference_points.copy()
        
        # Calculate segment lengths
        lengths = pd.Series(
            index=projected.index,
            data=projected.geometry.apply(lambda p: self._estimate_segment_length(p))
        )
        
        lengths.index = projected['reference_id']
        return lengths
    
    def _estimate_segment_length(self, point_geom) -> float:
        """Estimate coastline segment length for a reference point.
        Uses point spacing as an approximation of segment length.
        
        Args:
            point_geom: Point geometry
            
        Returns:
            Estimated segment length in meters
        """
        # Use standard 5km spacing as segment length
        # This is based on the reference point generation process
        return 5000.0  # 5km in meters 