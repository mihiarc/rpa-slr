"""
Data loading utilities for gauge stations and reference points.
Handles loading and basic validation of input data.
"""

import geopandas as gpd
import pandas as pd
from pathlib import Path
import json
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
        Load gauge stations from JSON file and convert to GeoDataFrame.
        
        Returns:
            GeoDataFrame containing gauge stations with geometry
        """
        try:
            # Load JSON file
            with open(self.gauge_file) as f:
                gauge_data = json.load(f)
            
            # Convert to DataFrame
            gauges_df = pd.DataFrame(gauge_data)
            
            # Create geometry column from coordinates
            geometry = gpd.points_from_xy(
                gauges_df['lng'],
                gauges_df['lat']
            )
            
            # Rename id to station_id for consistency
            gauges_df = gauges_df.rename(columns={
                'id': 'station_id',
                'name': 'station_name'
            })
            
            # Create GeoDataFrame
            gauges_gdf = gpd.GeoDataFrame(
                gauges_df,
                geometry=geometry,
                crs=f"EPSG:{WGS84_EPSG}"
            )
            
            logger.info(f"Loaded {len(gauges_gdf)} gauge stations")
            return gauges_gdf
            
        except FileNotFoundError:
            logger.error(f"Gauge station file not found: {self.gauge_file}")
            raise
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in gauge station file: {self.gauge_file}")
            raise
        except Exception as e:
            logger.error(f"Error loading gauge stations: {str(e)}")
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