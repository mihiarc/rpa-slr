"""
Spatial operations for finding nearest tide gauge stations to reference points.
Handles coordinate transformations and nearest neighbor searches.
This module provides spatial operations for finding the nearest tide gauge stations to coastal reference points.
It is needed because:

1. Accurate Distance Calculations:
   - Converts coordinates from WGS84 (lat/long) to Albers Equal Area projection
   - Albers projection preserves distances, critical for finding truly nearest stations

2. Efficient Nearest Neighbor Search:
   - Uses KD-Trees for fast spatial queries
   - Much more efficient than calculating all pairwise distances

3. Gauge Station Association:
   - Associates multiple gauge stations with each reference point
    - some reference points may not nearby gauges or have HTF data
   - Enables interpolation/averaging of water levels between stations
   - Handles cases where closest station may have missing data

The module is a key component for:
- Preprocessing spatial relationships between coastline reference points and tide gauge stations
- Supporting later water level interpolation and imputation

"""

import geopandas as gpd
import numpy as np
from scipy.spatial import cKDTree
from typing import Tuple, List, Dict
import logging
from tqdm import tqdm
import pyproj
from pathlib import Path
from src.config import CONFIG_DIR
import yaml

logger = logging.getLogger(__name__)

class NearestGaugeFinder:
    """Finds nearest gauge stations for reference points."""
    
    def __init__(self, 
                 target_epsg: int = 5070,  # NAD83 / Conus Albers (equal area)
                 fips_config: Path = CONFIG_DIR / "fips_mappings.yaml"):
        self.target_epsg = target_epsg
        
        # Load region definitions
        with open(fips_config) as f:
            config = yaml.safe_load(f)
            self.regions = config['regions']
    
    def _project_points(self,
                      reference_points: gpd.GeoDataFrame,
                      gauge_stations: gpd.GeoDataFrame) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Project points to target CRS for accurate distance calculations.
        
        Args:
            reference_points: GeoDataFrame of reference points
            gauge_stations: GeoDataFrame of gauge stations
            
        Returns:
            Tuple of projected (reference_points, gauge_stations)
        """
        return (
            reference_points.to_crs(epsg=self.target_epsg),
            gauge_stations.to_crs(epsg=self.target_epsg)
        )
    
    def _extract_coordinates(self,
                          reference_points: gpd.GeoDataFrame,
                          gauge_stations: gpd.GeoDataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """
        Extract coordinates as numpy arrays for KDTree.
        
        Args:
            reference_points: GeoDataFrame of reference points
            gauge_stations: GeoDataFrame of gauge stations
            
        Returns:
            Tuple of coordinate arrays (reference_coords, gauge_coords)
        """
        ref_coords = np.column_stack([
            reference_points.geometry.x,
            reference_points.geometry.y
        ])
        gauge_coords = np.column_stack([
            gauge_stations.geometry.x,
            gauge_stations.geometry.y
        ])
        return ref_coords, gauge_coords
    
    def _get_region_bounds(self, state_fips: str) -> Dict[str, float]:
        """
        Get the bounding box for the region containing the given state.
        
        Args:
            state_fips: State FIPS code
            
        Returns:
            Dictionary with minx, miny, maxx, maxy bounds or None if state not found
        """
        for region, info in self.regions.items():
            if state_fips in info['state_codes']:
                return info['bounds']
        return None
    
    def _filter_gauges_by_region(self,
                               gauge_stations: gpd.GeoDataFrame,
                               state_fips: str) -> gpd.GeoDataFrame:
        """
        Filter gauge stations to only include those within the same region as the state.
        
        Args:
            gauge_stations: GeoDataFrame of all gauge stations
            state_fips: State FIPS code
            
        Returns:
            GeoDataFrame of gauge stations within the region
        """
        bounds = self._get_region_bounds(state_fips)
        if bounds is None:
            return gauge_stations
        
        # Create a mask for gauges within the region bounds
        mask = (
            (gauge_stations.geometry.x >= bounds['minx']) &
            (gauge_stations.geometry.x <= bounds['maxx']) &
            (gauge_stations.geometry.y >= bounds['miny']) &
            (gauge_stations.geometry.y <= bounds['maxy'])
        )
        
        return gauge_stations[mask]

    def find_nearest(self,
                    reference_points: gpd.GeoDataFrame,
                    gauge_stations: gpd.GeoDataFrame) -> List[dict]:
        """
        Find nearest gauge stations for each reference point.
        Gets 4 nearest initially to allow for fallbacks if some don't have HTF data.
        Will return data for 2 nearest gauges that have HTF data.
        
        Args:
            reference_points: GeoDataFrame of reference points
            gauge_stations: GeoDataFrame of gauge stations
            
        Returns:
            List of dictionaries containing point data and nearest gauge information
        """
        gauge_data = []
        
        # Process each reference point
        for i in tqdm(range(len(reference_points)), desc="Finding nearest gauges"):
            point = reference_points.iloc[i]
            point_data = {
                'county_fips': point['county_fips'],
                'county_name': point['county_name'],
                'state_fips': point['state_fips'],
                'geometry': point.geometry
            }
            
            # Filter gauge stations to the point's region
            regional_gauges = self._filter_gauges_by_region(gauge_stations, point['state_fips'])
            
            if len(regional_gauges) == 0:
                # If no gauges in region, fall back to all gauges but log a warning
                print(f"Warning: No gauge stations found in region for {point['county_name']}, {point['state_fips']}")
                regional_gauges = gauge_stations
            
            # Project points for distance calculation
            point_proj, gauges_proj = self._project_points(
                gpd.GeoDataFrame([point], crs=reference_points.crs),
                regional_gauges
            )
            
            # Extract coordinates
            point_coords, gauge_coords = self._extract_coordinates(point_proj, gauges_proj)
            
            # Build KDTree for efficient search
            tree = cKDTree(gauge_coords)
            
            # Find 4 nearest gauges (to allow for fallbacks)
            distances, indices = tree.query(point_coords, k=min(4, len(regional_gauges)))
            
            # Flatten single-point results
            if len(point_coords) == 1:
                distances = distances[0]
                indices = indices[0]
            
            # Store gauge options with their IDs and distances
            backup_gauge_ids = []
            backup_distances = []
            
            for j in range(len(indices)):
                gauge = regional_gauges.iloc[indices[j]]
                backup_gauge_ids.append(gauge['station_id'])
                backup_distances.append(distances[j])
            
            point_data['backup_gauge_ids'] = backup_gauge_ids
            point_data['backup_distances'] = backup_distances
            
            gauge_data.append(point_data)
        
        return gauge_data 

def process_spatial_data(
    region: str,
    region_config: Path = CONFIG_DIR / "region_mappings.yaml"):
    """Process spatial data for imputation.""" 