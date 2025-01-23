"""
Data loading utilities for gauge stations and reference points.
Handles loading and basic validation of input data.
"""

import geopandas as gpd
import pandas as pd
from pathlib import Path
import json
import yaml
import logging
from typing import Optional

from src.config import (
    COASTAL_COUNTIES_FILE,
    REFERENCE_POINTS_FILE,
    TIDE_STATIONS_LIST,
    WGS84_EPSG
)

logger = logging.getLogger(__name__)

class GaugeStationLoader:
    """Loads and validates tide gauge station data."""
    
    def __init__(self, gauge_file: Path = TIDE_STATIONS_LIST):
        self.gauge_file = gauge_file
    
    def load(self) -> gpd.GeoDataFrame:
        """
        Load gauge stations from YAML file.
        
        Returns:
            GeoDataFrame containing gauge stations
        """
        try:
            with open(self.gauge_file) as f:
                gauge_data = yaml.safe_load(f)
            
            # Convert YAML structure to list of records
            stations = []
            for station_id, data in gauge_data['stations'].items():
                stations.append({
                    'station_id': station_id,
                    'station_name': data['name'],
                    'latitude': data['location']['lat'],
                    'longitude': data['location']['lon']
                })
            
            # Create GeoDataFrame
            df = pd.DataFrame(stations)
            gdf = gpd.GeoDataFrame(
                df,
                geometry=gpd.points_from_xy(df.longitude, df.latitude),
                crs="EPSG:4326"
            )
            
            logger.info(f"Loaded {len(gdf)} gauge stations")
            return gdf
            
        except FileNotFoundError:
            logger.error(f"Gauge station file not found: {self.gauge_file}")
            raise
        except Exception as e:
            logger.error(f"Invalid YAML in gauge station file: {self.gauge_file}")
            raise

class ReferencePointLoader:
    """Loads and validates coastal reference points."""
    
    def __init__(self, points_file: Path = REFERENCE_POINTS_FILE):
        self.points_file = points_file
    
    def load(self) -> gpd.GeoDataFrame:
        """
        Load reference points from parquet file.
        
        Returns:
            GeoDataFrame containing reference points
        """
        try:
            points_gdf = gpd.read_parquet(self.points_file)
            logger.info(f"Loaded {len(points_gdf)} reference points")
            return points_gdf
            
        except FileNotFoundError:
            logger.error(f"Reference points file not found: {self.points_file}")
            raise
        except Exception as e:
            logger.error(f"Error loading reference points: {str(e)}")
            raise

class DataLoader:
    """Main data loading interface."""
    
    def __init__(self, 
                 gauge_file: Optional[Path] = None,
                 points_file: Optional[Path] = None):
        self.gauge_loader = GaugeStationLoader(gauge_file or TIDE_STATIONS_LIST)
        self.points_loader = ReferencePointLoader(points_file or REFERENCE_POINTS_FILE)
    
    def load_all(self) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Load both gauge stations and reference points.
        
        Returns:
            Tuple of (gauge_stations, reference_points) as GeoDataFrames
        """
        gauge_stations = self.gauge_loader.load()
        reference_points = self.points_loader.load()
        return gauge_stations, reference_points 