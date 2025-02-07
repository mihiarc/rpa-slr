"""
Main module for water level imputation at coastal reference points.
Orchestrates the process of:

1. Loading preprocessed spatial relationships from module `spatial_ops.py`
2. Uses module `weight_calculator.py` to calculate weights and adjustments
3. Preparing data structures for the next phase saving to file location `data/processed/imputation/imputation_structure.parquet`

The imputation output is designed to support:
- Mapping of historic water levels to coastal county reference points
- Projection of future water levels to coastal county reference points
- Temporal interpolation between gauge readings
- Handling of missing or incomplete gauge data
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Union
import logging
from datetime import datetime

from src.config import (
    PROCESSED_DATA_DIR,
    OUTPUT_DIR,
    REFERENCE_POINTS_FILE,
    TIDE_STATIONS_LIST
)

from .data_loader import DataLoader
from .spatial_ops import NearestGaugeFinder
from .weight_calculator import WeightCalculator

logger = logging.getLogger(__name__)

class ImputationManager:
    """Manages the imputation of water levels at reference points."""
    
    def __init__(self,
                 reference_points_file: Path = REFERENCE_POINTS_FILE,
                 gauge_stations_file: Path = TIDE_STATIONS_LIST,
                 output_dir: Path = OUTPUT_DIR / "imputation"):
        """
        Initialize imputation manager.
        
        Args:
            reference_points_file: Path to reference points file
            gauge_stations_file: Path to gauge stations file
            output_dir: Directory for output files
        """
        self.reference_points_file = reference_points_file
        self.gauge_stations_file = gauge_stations_file
        self.output_dir = output_dir
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.data_loader = DataLoader(
            gauge_file=gauge_stations_file,
            points_file=reference_points_file
        )
        self.gauge_finder = NearestGaugeFinder()
        self.weight_calculator = WeightCalculator()
        
        # Setup logging
        self._setup_logging()
    
    def _setup_logging(self):
        """Configure logging for imputation process."""
        log_dir = self.output_dir / "logs"
        log_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"imputation_{timestamp}.log"
        
        # Configure root logger
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),  # Console handler
                logging.FileHandler(log_file)  # File handler
            ]
        )
        
        logger.info("Logging configured for both console and file output")
    
    def prepare_imputation_structure(self) -> pd.DataFrame:
        """
        Prepare the imputation structure by finding nearest gauges and calculating weights.
        Preserves all points/counties even if they don't have HTF data available.
        
        Returns:
            DataFrame with imputation structure
        """
        logger.info("Loading input data...")
        gauge_stations, reference_points = self.data_loader.load_all()
        
        # Get list of gauges that have HTF data
        htf_historical = pd.read_parquet("data/processed/historical_htf/historical_htf.parquet")
        htf_projected = pd.read_parquet("data/processed/projected_htf/projected_htf.parquet")
        
        # Combine gauge IDs from both datasets (handling different column names)
        historical_gauges = set(htf_historical['station_id'].unique())
        projected_gauges = set(htf_projected['station'].unique())  # Projected data uses 'station' column
        available_gauges = historical_gauges | projected_gauges
        
        logger.info(f"Found {len(available_gauges)} gauges with HTF data")
        logger.info(f"Historical gauges: {len(historical_gauges)}, Projected gauges: {len(projected_gauges)}")
        
        logger.info("Finding nearest gauges for each reference point...")
        point_data = self.gauge_finder.find_nearest(
            reference_points,
            gauge_stations
        )
        
        logger.info("Calculating inverse distance weights...")
        weighted_points = self.weight_calculator.calculate_for_points(
            point_data,
            available_gauges
        )
        
        # Convert to DataFrame
        df = pd.DataFrame(weighted_points)
        
        # Log coverage statistics
        total_points = len(df)
        points_with_htf = len(df[df['has_htf_data']])
        total_counties = df['county_fips'].nunique()
        counties_with_htf = df[df['has_htf_data']]['county_fips'].nunique()
        
        logger.info(f"Processed {total_points} reference points")
        logger.info(f"Points with HTF data: {points_with_htf} ({100 * points_with_htf / total_points:.1f}%)")
        logger.info(f"Points with 2 gauges: {len(df[df['n_gauges'] == 2])}")
        logger.info(f"Points with 1 gauge: {len(df[df['n_gauges'] == 1])}")
        logger.info(f"Points with no HTF data: {len(df[~df['has_htf_data']])}")
        logger.info(f"Counties with HTF data: {counties_with_htf} of {total_counties} ({100 * counties_with_htf / total_counties:.1f}%)")
        
        if len(df) > 0:
            logger.info(f"Average distance to nearest gauge: {df['distance_1'].mean():.2f} meters")
            has_second = df['distance_2'].notna()
            if has_second.any():
                logger.info(f"Average distance to second nearest gauge: {df.loc[has_second, 'distance_2'].mean():.2f} meters")
        
        return df
    
    def save_imputation_structure(self,
                                df: pd.DataFrame,
                                filename: str = "imputation_structure.parquet") -> Path:
        """
        Save imputation structure to file.
        
        Args:
            df: DataFrame with imputation structure
            filename: Name of output file
            
        Returns:
            Path to saved file
        """
        output_file = self.output_dir / filename
        
        # Convert to GeoDataFrame before saving
        gdf = gpd.GeoDataFrame(df, geometry='geometry')
        
        # Save as parquet with metadata
        gdf.to_parquet(
            output_file,
            compression='snappy',
            index=False
        )
        
        logger.info(f"Saved imputation structure to {output_file}")
        return output_file
    
    def run(self) -> Path:
        """
        Run the complete imputation preparation process.
        
        Returns:
            Path to output file
        """
        logger.info("Starting imputation preparation...")
        
        # Prepare imputation structure
        df = self.prepare_imputation_structure()
        
        # Save results
        output_file = self.save_imputation_structure(df)
        
        logger.info("Imputation preparation complete.")
        
        return output_file

if __name__ == "__main__":
    # Run imputation process
    manager = ImputationManager()
    output_file = manager.run() 