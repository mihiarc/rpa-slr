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
from typing import Optional, Dict

from src.config import (
    COASTAL_COUNTIES_FILE,
    REFERENCE_POINTS_FILE,
    TIDE_STATIONS_DIR,
    REGION_CONFIG,
    WGS84_EPSG
)

logger = logging.getLogger(__name__)

def get_state_fips_to_code_mapping():
    """Create mapping of FIPS codes to state codes."""
    return {
        '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA',
        '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL',
        '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN',
        '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME',
        '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS',
        '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH',
        '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND',
        '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
        '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT',
        '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI',
        '56': 'WY', '60': 'AS', '66': 'GU', '69': 'MP', '72': 'PR',
        '78': 'VI'
    }

class GaugeStationLoader:
    """Loads and validates tide gauge station data."""
    
    def __init__(self, tide_stations_dir: Path = TIDE_STATIONS_DIR):
        self.tide_stations_dir = tide_stations_dir
        
        # Load region configuration to know which regions to look for
        with open(REGION_CONFIG) as f:
            self.region_config = yaml.safe_load(f)
    
    def load(self) -> gpd.GeoDataFrame:
        """
        Load gauge stations from all regional YAML files.
        
        Returns:
            GeoDataFrame containing all gauge stations
        """
        stations = []
        
        # Load stations from each region's configuration
        for region in self.region_config['regions']:
            station_file = self.tide_stations_dir / f"{region}_tide_stations.yaml"
            
            try:
                with open(station_file) as f:
                    config = yaml.safe_load(f)
                    
                # Process stations in this region
                for station_id, data in config.get('stations', {}).items():
                    stations.append({
                        'station_id': station_id,
                        'station_name': data['name'],
                        'latitude': data['location']['lat'],
                        'longitude': data['location']['lon'],
                        'region': region,
                        'sub_region': data.get('region', '')
                    })
                    
                logger.info(f"Loaded {len(config.get('stations', {}))} stations from {region}")
                
            except FileNotFoundError:
                logger.warning(f"No tide station file found for region: {region}")
            except Exception as e:
                logger.error(f"Error loading tide stations for region {region}: {str(e)}")
                raise
        
        if not stations:
            logger.error("No tide stations loaded from any region")
            raise ValueError("No tide stations available")
            
        # Create GeoDataFrame
        df = pd.DataFrame(stations)
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df.longitude, df.latitude),
            crs=f"EPSG:{WGS84_EPSG}"
        )
        
        logger.info(f"Loaded {len(gdf)} total gauge stations across all regions")
        return gdf

class ReferencePointLoader:
    """Loads and validates coastal reference points."""
    
    def __init__(self, points_file: Path = REFERENCE_POINTS_FILE, region: str = None):
        self.points_file = points_file
        self.region = region
        self.state_fips_to_code = get_state_fips_to_code_mapping()
    
    def load(self) -> gpd.GeoDataFrame:
        """
        Load reference points from parquet file.
        
        If a region is specified, it tries to load region-specific points file.
        
        Returns:
            GeoDataFrame containing reference points
        """
        # If region is specified, try to load region-specific file
        if self.region:
            region_points_file = Path(str(self.points_file).replace(
                "coastal_reference_points.parquet", 
                f"reference_points_{self.region}.parquet"
            ))
            
            # Check if region-specific file exists
            if region_points_file.exists():
                logger.info(f"Using region-specific points file for {self.region}: {region_points_file}")
                self.points_file = region_points_file
            else:
                logger.warning(f"Region-specific points file not found for {self.region}: {region_points_file}")
                logger.warning(f"Using default points file: {self.points_file}")
        
        try:
            points_gdf = gpd.read_parquet(self.points_file)
            logger.info(f"Loaded {len(points_gdf)} reference points")
            
            # Verify required columns
            required_columns = ['county_fips', 'state_fips', 'geometry']
            missing_columns = [col for col in required_columns if col not in points_gdf.columns]
            if missing_columns:
                raise ValueError(f"Reference points file missing required columns: {missing_columns}")
            
            # Add state_code column by mapping from state_fips
            points_gdf['state_code'] = points_gdf['state_fips'].map(self.state_fips_to_code)
            
            # Verify all state FIPS codes were mapped
            unmapped_fips = points_gdf[points_gdf['state_code'].isna()]['state_fips'].unique()
            if len(unmapped_fips) > 0:
                logger.warning(f"Could not map state codes for FIPS codes: {unmapped_fips}")
            
            return points_gdf
            
        except FileNotFoundError:
            logger.error(f"Reference points file not found: {self.points_file}")
            raise
        except Exception as e:
            logger.error(f"Error loading reference points: {str(e)}")
            raise

class DataLoader:
    """Main data loading interface."""
    
    def __init__(self, region: str = None):
        self.gauge_loader = GaugeStationLoader()
        self.points_loader = ReferencePointLoader(region=region)
        self.region = region
    
    def load_gauge_stations(self) -> gpd.GeoDataFrame:
        """Load all gauge stations."""
        return self.gauge_loader.load()
    
    def load_reference_points(self) -> gpd.GeoDataFrame:
        """Load reference points."""
        return self.points_loader.load()
    
    def load_all(self) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
        """
        Load both gauge stations and reference points.
        
        Returns:
            Tuple of (gauge_stations, reference_points) as GeoDataFrames
        """
        gauge_stations = self.load_gauge_stations()
        reference_points = self.load_reference_points()
        return gauge_stations, reference_points 