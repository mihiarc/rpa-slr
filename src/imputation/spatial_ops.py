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
from shapely.geometry import box

logger = logging.getLogger(__name__)

class NearestGaugeFinder:
    """Finds nearest gauge stations for reference points."""
    
    def __init__(self, 
                 region_config: Path = CONFIG_DIR / "region_mappings.yaml"):
        # Load region definitions
        with open(region_config) as f:
            self.region_config = yaml.safe_load(f)
            
        # Initialize region-specific projections
        self.region_projections = {
            'alaska': "+proj=aea +lat_1=55 +lat_2=65 +lat_0=50 +lon_0=-154 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs",
            'hawaii': "+proj=aea +lat_1=8 +lat_2=18 +lat_0=13 +lon_0=-157 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs",
            'pacific_islands': "+proj=aea +lat_1=0 +lat_2=20 +lat_0=10 +lon_0=160 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs",
            'puerto_rico': "+proj=aea +lat_1=17 +lat_2=19 +lat_0=18 +lon_0=-66.5 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs",
            'virgin_islands': "+proj=aea +lat_1=17 +lat_2=19 +lat_0=18 +lon_0=-64.75 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs",
            'west_coast': "+proj=aea +lat_1=34 +lat_2=45.5 +lat_0=40 +lon_0=-120 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs",
            # CONUS regions use standard Albers Equal Area
            'default': "EPSG:5070"  # NAD83 / Conus Albers
        }
        
        # Load tide station configurations
        self.tide_stations_dir = CONFIG_DIR / "tide_stations"
        self._load_tide_station_configs()
        
    def _load_tide_station_configs(self):
        """Load tide station configurations for each region."""
        self.region_stations = {}
        self.station_metadata = {}
        
        for region in self.region_config['regions']:
            station_file = self.tide_stations_dir / f"{region}_tide_stations.yaml"
            if station_file.exists():
                with open(station_file) as f:
                    config = yaml.safe_load(f)
                    
                    # Store metadata
                    self.station_metadata[region] = config.get('metadata', {})
                    
                    # Store station information
                    stations = {}
                    for station_id, info in config.get('stations', {}).items():
                        stations[station_id] = {
                            'id': station_id,
                            'name': info['name'],
                            'latitude': info['location']['lat'],
                            'longitude': info['location']['lon'],
                            'sub_region': info.get('region', '')
                        }
                    self.region_stations[region] = stations
                    
                    logger.info(f"Loaded {len(stations)} stations for region {region}")
                    logger.info(f"Source: {self.station_metadata[region].get('source', 'Unknown')}")
                    logger.info(f"Last updated: {self.station_metadata[region].get('last_updated', 'Unknown')}")
            else:
                logger.warning(f"No tide station configuration found for region: {region}")
                self.region_stations[region] = {}
                self.station_metadata[region] = {}

    def _filter_by_region(self,
                         reference_points: gpd.GeoDataFrame,
                         gauge_stations: gpd.GeoDataFrame,
                         region: str) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Filter both reference points and gauge stations to ensure they belong to the same region.
        
        Args:
            reference_points: GeoDataFrame of reference points
            gauge_stations: GeoDataFrame of gauge stations
            region: Region identifier
            
        Returns:
            Tuple of filtered (reference_points, gauge_stations)
        """
        # Get region definition
        if region not in self.region_config['regions']:
            raise ValueError(f"Unknown region: {region}")
            
        region_def = self.region_config['regions'][region]
        
        # Get state codes for the region
        state_codes = region_def['state_codes']
        
        # Filter reference points by state codes
        filtered_points = reference_points[
            reference_points['state_code'].isin(state_codes)
        ].copy()
        
        # Get region bounds
        bounds = region_def['bounds']
        bounds_polygon = box(
            bounds['min_lon'],
            bounds['min_lat'],
            bounds['max_lon'],
            bounds['max_lat']
        )
        bounds_gdf = gpd.GeoDataFrame({'geometry': [bounds_polygon]}, crs="EPSG:4326")
        
        # Further filter points by region bounds
        filtered_points = gpd.sjoin(
            filtered_points,
            bounds_gdf,
            how='inner',
            predicate='within'
        )
        
        # Get station IDs for the region
        region_station_ids = set(self.region_stations[region].keys())
        
        # Filter gauge stations by region's station IDs and bounds
        filtered_stations = gauge_stations[
            gauge_stations['station_id'].isin(region_station_ids)
        ].copy()
        
        # Add sub-region information to filtered stations
        filtered_stations['sub_region'] = filtered_stations['station_id'].map(
            lambda x: self.region_stations[region].get(x, {}).get('sub_region', '')
        )
        
        # Add station names for better logging
        filtered_stations['station_name'] = filtered_stations['station_id'].map(
            lambda x: self.region_stations[region].get(x, {}).get('name', '')
        )
        
        filtered_stations = gpd.sjoin(
            filtered_stations,
            bounds_gdf,
            how='inner',
            predicate='within'
        )
        
        if filtered_points.empty or filtered_stations.empty:
            logger.warning(f"No data found for region {region} after filtering")
        else:
            logger.info(f"Found {len(filtered_stations)} stations in region {region}")
            for sub_region in filtered_stations['sub_region'].unique():
                if sub_region:
                    count = len(filtered_stations[filtered_stations['sub_region'] == sub_region])
                    logger.info(f"  Sub-region {sub_region}: {count} stations")
            
        return filtered_points, filtered_stations

    def _get_region_projection(self, region: str) -> str:
        """Get the appropriate projection for a region."""
        return self.region_projections.get(region, self.region_projections['default'])
        
    def _project_points(self,
                      reference_points: gpd.GeoDataFrame,
                      gauge_stations: gpd.GeoDataFrame,
                      region: str) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Project points to region-appropriate CRS for accurate distance calculations.
        
        Args:
            reference_points: GeoDataFrame of reference points
            gauge_stations: GeoDataFrame of gauge stations
            region: Region identifier
            
        Returns:
            Tuple of projected (reference_points, gauge_stations)
        """
        projection = self._get_region_projection(region)
        return (
            reference_points.to_crs(projection),
            gauge_stations.to_crs(projection)
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
                    gauge_stations: gpd.GeoDataFrame,
                    region: str) -> List[dict]:
        """
        Find nearest gauge stations for each reference point within the same region and subregion.
        
        Args:
            reference_points: GeoDataFrame of reference points
            gauge_stations: GeoDataFrame of gauge stations
            region: Region identifier
            
        Returns:
            List of dictionaries containing point-to-gauge mappings
        """
        # Filter by region first
        ref_points, stations = self._filter_by_region(reference_points, gauge_stations, region)
        
        if ref_points.empty or stations.empty:
            return []
            
        # Project coordinates
        ref_points, stations = self._project_points(ref_points, stations, region)
        
        # Process each subregion separately
        all_mappings = []
        
        # Get unique subregions (including empty string for stations without subregion)
        subregions = stations['sub_region'].unique()
        
        for subregion in subregions:
            # Filter stations for this subregion
            subregion_stations = stations[stations['sub_region'] == subregion].copy()
            
            if len(subregion_stations) == 0:
                continue
                
            # Extract coordinates for KD-tree
            ref_coords, station_coords = self._extract_coordinates(ref_points, subregion_stations)
            
            # Build KD-tree for efficient nearest neighbor search
            tree = cKDTree(station_coords)
            
            # Find k nearest neighbors for each reference point
            # k is min(3, number of available stations) to ensure we don't exceed available stations
            k = min(3, len(subregion_stations))
            distances, indices = tree.query(ref_coords, k=k)
            
            # Convert to meters and create mapping dictionaries
            for i, (dist, idx) in enumerate(zip(distances, indices)):
                point_mappings = {
                    'reference_point_id': ref_points.iloc[i].name,
                    'county_fips': ref_points.iloc[i]['county_fips'],
                    'region': region,
                    'mappings': []
                }
                
                # Handle case where k=1 (only one station available)
                if not isinstance(dist, np.ndarray):
                    dist = [dist]
                    idx = [idx]
                    
                for d, j in zip(dist, idx):
                    station = subregion_stations.iloc[j]
                    point_mappings['mappings'].append({
                        'station_id': station['station_id'],
                        'station_name': station['station_name'],
                        'sub_region': station['sub_region'],
                        'distance_meters': float(d),
                        'weight': 1.0  # Initial weight, will be adjusted by weight calculator
                    })
                
                all_mappings.append(point_mappings)
        
        # Log summary statistics
        if all_mappings:
            logger.info(f"\nGenerated mappings for region {region}:")
            for subregion in subregions:
                subregion_name = subregion if subregion else 'main'
                subregion_mappings = [m for m in all_mappings 
                                    if any(sm['sub_region'] == subregion for sm in m['mappings'])]
                if subregion_mappings:
                    logger.info(f"  Subregion {subregion_name}: {len(subregion_mappings)} reference point mappings")
                    
        return all_mappings

def process_spatial_data(
    region: str,
    region_config: Path = CONFIG_DIR / "region_mappings.yaml"):
    """Process spatial data for imputation.""" 