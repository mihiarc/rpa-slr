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
        
        # Find 4 nearest gauges for each point (to allow for fallbacks)
        distances, indices = tree.query(ref_coords, k=4)
        
        gauge_data = []
        
        # Process each reference point
        for i in tqdm(range(len(reference_points)), desc="Finding nearest gauges"):
            point_data = {
                'county_fips': reference_points.iloc[i]['county_fips'],
                'county_name': reference_points.iloc[i]['county_name'],
                'state_fips': reference_points.iloc[i]['state_fips'],
                'geometry': reference_points.iloc[i].geometry
            }
            
            # Store all 4 gauge options with their IDs and distances
            backup_gauge_ids = []
            backup_distances = []
            
            for j in range(4):
                gauge = gauge_stations.iloc[indices[i, j]]
                backup_gauge_ids.append(gauge['station_id'])
                backup_distances.append(distances[i, j])
            
            point_data['backup_gauge_ids'] = backup_gauge_ids
            point_data['backup_distances'] = backup_distances
            
            gauge_data.append(point_data)
        
        return gauge_data 