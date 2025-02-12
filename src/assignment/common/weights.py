"""
Common weight calculation utilities for HTF data assignment.
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path
import logging
from typing import Dict, Optional
import yaml
import numpy as np
from pyproj import CRS, Transformer

from src.config import CONFIG_DIR

logger = logging.getLogger(__name__)

class WeightCalculator:
    """Calculates weights for tide stations based on regional projections."""
    
    def __init__(
        self,
        config_dir: Optional[Path] = None,
        max_distance_km: float = 100.0,
        min_weight: float = 0.1,
        idw_power: float = 2.0
    ):
        """Initialize weight calculator.
        
        Args:
            config_dir: Optional custom config directory path
            max_distance_km: Maximum distance in km for station influence
            min_weight: Minimum weight to consider a station's influence
            idw_power: Power parameter for inverse distance weighting
        """
        self.config_dir = config_dir or CONFIG_DIR
        self.max_distance_km = max_distance_km
        self.min_weight = min_weight
        self.idw_power = idw_power
        
        # Load region configuration
        with open(self.config_dir / "region_mappings.yaml") as f:
            self.region_config = yaml.safe_load(f)
    
    def get_region_projection(self, region: str) -> CRS:
        """Get appropriate projection for a region.
        
        Args:
            region: Name of the region
            
        Returns:
            CRS object for the region's projection
        """
        # Special handling for Alaska
        if region.lower() == "alaska":
            return CRS("EPSG:3338")  # Alaska Albers Equal Area
        
        # Special handling for Hawaii
        elif region.lower() == "hawaii":
            return CRS("EPSG:6628")  # Hawaii Albers Equal Area
        
        # Default to appropriate UTM zone based on region config
        region_info = self.region_config.get(region, {})
        utm_zone = region_info.get("utm_zone")
        
        if utm_zone:
            # Northern hemisphere by default
            epsg = f"EPSG:326{utm_zone:02d}"
            return CRS(epsg)
        
        # Fallback to Web Mercator
        logger.warning(f"No specific projection found for {region}, using Web Mercator")
        return CRS("EPSG:3857")
    
    def calculate_weights(
        self,
        region: str,
        stations: gpd.GeoDataFrame,
        reference_points: gpd.GeoDataFrame
    ) -> pd.DataFrame:
        """Calculate weights for stations relative to reference points.
        
        Args:
            region: Name of the region
            stations: GeoDataFrame of tide stations
            reference_points: GeoDataFrame of reference points
            
        Returns:
            DataFrame with weights for each station-reference point pair
        """
        logger.info(f"Calculating weights for {region}")
        
        # Get appropriate projection
        projection = self.get_region_projection(region)
        
        # Project geometries
        stations_proj = stations.to_crs(projection)
        ref_points_proj = reference_points.to_crs(projection)
        
        # Calculate distances and weights
        weights = []
        for _, ref_point in ref_points_proj.iterrows():
            # Calculate distances to all stations
            distances = stations_proj.geometry.distance(ref_point.geometry)
            
            # Convert to kilometers
            distances_km = distances / 1000
            
            # Filter by maximum distance
            valid_stations = distances_km <= self.max_distance_km
            
            if not valid_stations.any():
                logger.warning(
                    f"No stations within {self.max_distance_km}km of reference point "
                    f"{ref_point['reference_id']}"
                )
                continue
            
            # Calculate inverse distance weights
            raw_weights = (1 / distances_km[valid_stations]) ** self.idw_power
            
            # Normalize weights
            normalized_weights = raw_weights / raw_weights.sum()
            
            # Apply minimum weight threshold
            valid_weights = normalized_weights >= self.min_weight
            
            if not valid_weights.any():
                logger.warning(
                    f"No stations with weight >= {self.min_weight} for reference point "
                    f"{ref_point['reference_id']}"
                )
                continue
            
            # Re-normalize remaining weights
            final_weights = normalized_weights[valid_weights]
            final_weights = final_weights / final_weights.sum()
            
            # Store results
            for station_idx, weight in zip(
                stations_proj.index[valid_stations][valid_weights],
                final_weights
            ):
                weights.append({
                    'reference_id': ref_point['reference_id'],
                    'station_id': stations_proj.loc[station_idx, 'station_id'],
                    'distance_km': distances_km[station_idx],
                    'weight': weight
                })
        
        if not weights:
            logger.error(f"No valid weights calculated for {region}")
            return pd.DataFrame()
        
        return pd.DataFrame(weights) 