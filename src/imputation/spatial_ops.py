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
   - Enables interpolation/averaging of water levels between stations
   - Handles cases where closest station may have missing data

The module is a key component for:
- Preprocessing spatial relationships between reference points and gauge stations
- Supporting later water level interpolation and imputation
- Ensuring accurate distance-based weighting in spatial analyses

"""

import geopandas as gpd
import numpy as np
from scipy.spatial import cKDTree
from typing import Tuple, List
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)

class NearestGaugeFinder:
    """Finds nearest tide gauge stations to reference points."""
    
    def __init__(self, 
                 albers_crs: str = (
                     "+proj=aea +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 "
                     "+x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"
                 )):
        self.albers_crs = albers_crs
    
    def _project_points(self, 
                       reference_points: gpd.GeoDataFrame,
                       gauge_stations: gpd.GeoDataFrame
                       ) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """Project points to Albers Equal Area for accurate distance calculations."""
        return (
            reference_points.to_crs(self.albers_crs),
            gauge_stations.to_crs(self.albers_crs)
        )
    
    def _extract_coordinates(self,
                           reference_points: gpd.GeoDataFrame,
                           gauge_stations: gpd.GeoDataFrame
                           ) -> Tuple[np.ndarray, np.ndarray]:
        """Extract x,y coordinates for KDTree."""
        ref_coords = np.vstack((
            reference_points.geometry.x,
            reference_points.geometry.y
        )).T
        
        gauge_coords = np.vstack((
            gauge_stations.geometry.x,
            gauge_stations.geometry.y
        )).T
        
        return ref_coords, gauge_coords
    
    def find_nearest(self,
                    reference_points: gpd.GeoDataFrame,
                    gauge_stations: gpd.GeoDataFrame,
                    k: int = 3,
                    initial_max_distance: float = 50000,
                    max_distance_limit: float = 200000,
                    distance_increment: float = 25000
                    ) -> List[dict]:
        """
        Find k nearest gauge stations for each reference point using adaptive distance thresholds.
        
        Args:
            reference_points: GeoDataFrame of reference points
            gauge_stations: GeoDataFrame of gauge stations
            k: Number of nearest gauges to find
            initial_max_distance: Initial maximum distance to consider (meters)
            max_distance_limit: Absolute maximum distance limit (meters)
            distance_increment: Distance increment for adaptive threshold (meters)
            
        Returns:
            List of dictionaries containing point data and nearest gauge information
        """
        # Project points to equal area projection
        ref_points_proj, gauge_stations_proj = self._project_points(
            reference_points, 
            gauge_stations
        )
        
        # Extract coordinates for KDTree
        ref_coords, gauge_coords = self._extract_coordinates(
            ref_points_proj,
            gauge_stations_proj
        )
        
        # Build KDTree for efficient search
        tree = cKDTree(gauge_coords)
        
        # Track counties needing extended search
        counties_with_extended_distance = set()
        gauge_data = []
        
        # Process each reference point
        for i in tqdm(range(len(reference_points)), desc="Finding nearest gauges"):
            point_data = {
                'county_fips': reference_points.iloc[i]['county_fips'],
                'county_name': reference_points.iloc[i]['county_name'],
                'state_fips': reference_points.iloc[i]['state_fips'],
                'geometry': reference_points.iloc[i].geometry
            }
            
            # Use adaptive distance threshold
            current_max_distance = initial_max_distance
            has_gauge = False
            
            while not has_gauge and current_max_distance <= max_distance_limit:
                # Query KDTree
                distances, indices = tree.query(
                    ref_coords[i:i+1],
                    k=k,
                    distance_upper_bound=current_max_distance
                )
                
                # Process results
                distances = distances[0]
                indices = indices[0]
                valid_mask = distances < current_max_distance
                valid_distances = distances[valid_mask]
                valid_indices = indices[valid_mask]
                
                if len(valid_distances) > 0:
                    has_gauge = True
                    
                    # Log if distance was extended
                    if current_max_distance > initial_max_distance:
                        counties_with_extended_distance.add(
                            (point_data['county_fips'], point_data['county_name'])
                        )
                    
                    # Store gauge information
                    for j, (idx, dist) in enumerate(zip(valid_indices, valid_distances)):
                        gauge = gauge_stations.iloc[idx]
                        point_data.update({
                            f'gauge_id_{j+1}': gauge['station_id'],
                            f'gauge_name_{j+1}': gauge['station_name'],
                            f'distance_{j+1}': dist,
                        })
                else:
                    current_max_distance += distance_increment
            
            # Fill missing gauge slots
            for j in range(len(valid_distances) if has_gauge else 0, k):
                point_data.update({
                    f'gauge_id_{j+1}': None,
                    f'gauge_name_{j+1}': None,
                    f'distance_{j+1}': None,
                })
            
            gauge_data.append(point_data)
        
        # Log counties that required extended distance
        if counties_with_extended_distance:
            logger.warning("\nCounties requiring extended gauge search distance:")
            for county_fips, county_name in sorted(counties_with_extended_distance):
                logger.warning(f"  - {county_name} (FIPS: {county_fips})")
        
        return gauge_data 