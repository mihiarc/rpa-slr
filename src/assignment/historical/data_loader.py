"""
Data loading utilities for historical HTF assignment.
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
import yaml

from src.config import CONFIG_DIR

logger = logging.getLogger(__name__)

class HistoricalDataLoader:
    """Loads and prepares historical HTF data with regional context."""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize data loader.
        
        Args:
            config_dir: Optional custom config directory
        """
        self.config_dir = config_dir or CONFIG_DIR
        
        # Load region configuration
        with open(self.config_dir / "region_mappings.yaml") as f:
            self.region_config = yaml.safe_load(f)['regions']
    
    def load_regional_data(
        self,
        region: str,
        htf_data_path: Path,
        reference_points_path: Path,
        flood_data_path: Optional[Path] = None
    ) -> Tuple[pd.DataFrame, gpd.GeoDataFrame, pd.DataFrame]:
        """Load data for a specific region.
        
        Args:
            region: Name of the region
            htf_data_path: Path to imputation structure data
            reference_points_path: Path to reference points data
            flood_data_path: Optional path to HTF flood data
            
        Returns:
            Tuple of (htf_data, reference_points, stations)
        """
        logger.info(f"Loading data for region: {region}")
        
        # Load and filter HTF data
        htf_df = self._load_htf_data(htf_data_path, region)
        logger.info(f"Loaded {len(htf_df)} HTF records for {region}")
        
        # Load flood data if provided
        if flood_data_path is not None and flood_data_path.exists():
            flood_df = pd.read_parquet(flood_data_path)
            flood_df = flood_df[flood_df['region'] == region].copy()
            
            # Ensure flood data has required columns
            required_flood_cols = ['station_id', 'year', 'flood_days', 'missing_days']
            missing = [col for col in required_flood_cols if col not in flood_df.columns]
            if missing:
                raise ValueError(f"Missing required columns in flood data: {missing}")
            
            # Create cross product of HTF data with years from flood data
            years = flood_df['year'].unique()
            htf_df = htf_df.assign(key=1).merge(
                pd.DataFrame({'year': years, 'key': 1}),
                on='key'
            ).drop('key', axis=1)
            
            # Merge flood data with HTF data
            htf_df = htf_df.merge(
                flood_df[required_flood_cols],
                on=['station_id', 'year'],
                how='left'
            )
            logger.info(f"Merged flood data with {len(flood_df)} records")
        
        # Load reference points
        ref_points = self._load_reference_points(reference_points_path, region)
        logger.info(f"Loaded {len(ref_points)} reference points for {region}")
        
        # Create stations DataFrame from HTF data
        stations = self._create_stations_from_htf(htf_df)
        logger.info(f"Created stations DataFrame with {len(stations)} stations")
        
        return htf_df, ref_points, stations
    
    def _load_htf_data(self, filepath: Path, region: str) -> pd.DataFrame:
        """Load historical HTF data for a region.
        
        Args:
            filepath: Path to imputation structure data
            region: Region name
            
        Returns:
            DataFrame with historical HTF data
        """
        df = pd.read_parquet(filepath)
        
        # Filter to region
        region_df = df[df['region'] == region].copy()
        
        # Validate required columns
        required_cols = [
            'reference_point_id',
            'county_fips',
            'station_id',
            'station_name',
            'weight'
        ]
        
        missing = [col for col in required_cols if col not in region_df.columns]
        if missing:
            raise ValueError(f"Missing required columns in imputation data: {missing}")
        
        return region_df
    
    def _load_reference_points(self, filepath: Path, region: str) -> gpd.GeoDataFrame:
        """Load reference points for a region.
        
        Args:
            filepath: Path to reference points data
            region: Region name
            
        Returns:
            GeoDataFrame with reference points
        """
        gdf = gpd.read_parquet(filepath)
        return gdf[gdf['region'] == region].copy()
    
    def _create_stations_from_htf(self, htf_df: pd.DataFrame) -> pd.DataFrame:
        """Create stations DataFrame from HTF data.
        
        Args:
            htf_df: DataFrame with HTF data
            
        Returns:
            DataFrame with stations
        """
        # Get unique stations with their weights
        stations = htf_df.groupby(['station_id', 'station_name'])['weight'].sum().reset_index()
        stations = stations.rename(columns={'weight': 'total_weight'})
        
        return stations 