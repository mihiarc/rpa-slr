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

This module can be run directly for a specific region:
  `python -m src.imputation.main --region west_coast`
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
import logging
from datetime import datetime
import yaml
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import argparse
import sys
import os
import traceback

from src.config import (
    CONFIG_DIR,
    PROCESSED_DIR,
    IMPUTATION_DIR,
    IMPUTATION_LOGS_DIR,
    COASTAL_COUNTIES_FILE,
    REFERENCE_POINTS_FILE,
    OUTPUT_DIR,
    TIDE_STATIONS_DIR,
    REGION_CONFIG
)

from .data_loader import DataLoader
from .spatial_ops import NearestGaugeFinder
from .weight_calculator import WeightCalculator

logger = logging.getLogger(__name__)

def process_region(region: str,
                  region_info: dict,
                  reference_points: gpd.GeoDataFrame,
                  gauge_stations: gpd.GeoDataFrame) -> Optional[pd.DataFrame]:
    """
    Process a single region.
    
    Args:
        region: Region identifier
        region_info: Region configuration dictionary
        reference_points: Reference points GeoDataFrame
        gauge_stations: Gauge stations GeoDataFrame
        
    Returns:
        DataFrame containing imputation structure for the region or None if error
    """
    try:
        logger.info(f"\nProcessing region: {region}")
        logger.info(f"States included: {', '.join(region_info['state_codes'])}")
        
        # Initialize components for this region
        gauge_finder = NearestGaugeFinder(region_config=REGION_CONFIG)
        weight_calculator = WeightCalculator(
            max_distance_meters=100000,  # 100km max distance
            power=2,  # inverse distance power
            min_weight=0.1
        )
        
        # Find nearest gauges for reference points in this region
        mappings = gauge_finder.find_nearest(
            reference_points=reference_points,
            gauge_stations=gauge_stations,
            region=region
        )
        
        if not mappings:
            logger.warning(f"No mappings found for region {region}")
            return None
            
        # Calculate weights for gauge stations
        weighted_mappings = weight_calculator.calculate_weights(mappings)
        
        # Convert to DataFrame
        records = []
        for mapping in weighted_mappings:
            for gauge in mapping['mappings']:
                records.append({
                    'reference_point_id': mapping['reference_point_id'],
                    'county_fips': mapping['county_fips'],
                    'region': region,
                    'region_name': region_info['name'],
                    'station_id': gauge['station_id'],
                    'station_name': gauge['station_name'],
                    'sub_region': gauge['sub_region'],
                    'distance_meters': gauge['distance_meters'],
                    'weight': gauge['weight']
                })
                
        df = pd.DataFrame.from_records(records)
        
        # Log statistics with improved clarity
        if not df.empty:
            total_counties = df['county_fips'].nunique()
            total_stations = df['station_id'].nunique()
            total_mappings = len(df)
            
            logger.info(f"\nRegion Summary for {region}:")
            logger.info(f"Total unique counties: {total_counties}")
            logger.info(f"Total tide stations: {total_stations}")
            logger.info(f"Total point-to-station mappings: {total_mappings}")
            logger.info("\nNote: Each county is considered for all subregions, with weights determined by distance")
            
            # Log sub-region statistics with improved clarity
            logger.info("\nSubregion Details:")
            for sub_region in sorted(df['sub_region'].unique()):
                if sub_region:
                    sub_df = df[df['sub_region'] == sub_region]
                    sub_counties = sub_df['county_fips'].nunique()
                    sub_stations = sub_df['station_id'].nunique()
                    avg_distance = sub_df['distance_meters'].mean()
                    avg_weight = sub_df['weight'].mean()
                    
                    logger.info(f"\nSubregion: {sub_region}")
                    logger.info(f"  Available stations: {sub_stations}")
                    logger.info(f"  Counties with mappings: {sub_counties} (all counties in region)")
                    logger.info(f"  Average distance to stations: {avg_distance:,.2f} meters")
                    logger.info(f"  Average station weight: {avg_weight:.4f}")
                    logger.info("  Note: Weights decrease with distance, stations beyond 100km have minimal influence")
            
        return df
        
    except Exception as e:
        logger.error(f"Error processing region {region}: {str(e)}")
        return None

class ImputationManager:
    """
    Manages the imputation process across all regions.
    
    Orchestrates the process of:
    1. Loading reference points and gauge stations
    2. Processing each region to create imputation structure
    3. Saving output to parquet files
    """
    
    def __init__(self, 
                 reference_points_file: Path = REFERENCE_POINTS_FILE,
                 gauge_stations_file: Path = None, 
                 output_dir: Path = IMPUTATION_DIR / "data",
                 region_config: Path = REGION_CONFIG,
                 n_processes: int = None,
                 region: str = None):
        """
        Initialize imputation manager.
        
        Args:
            reference_points_file: Path to reference points parquet file
            gauge_stations_file: Path to gauge stations file (optional)
            output_dir: Directory to save output files
            region_config: Path to region configuration file
            n_processes: Number of processes to use for parallel processing
            region: Specific region to process (if None, process all regions)
        """
        self.reference_points_file = reference_points_file
        self.gauge_stations_file = gauge_stations_file
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.n_processes = n_processes or max(1, mp.cpu_count() - 2)
        self.region = region
        
        # Load region configuration
        with open(region_config) as f:
            config = yaml.safe_load(f)
            self.region_config = config['regions']
            self.metadata = config.get('metadata', {})
            
        # Initialize data loader
        self.data_loader = DataLoader(region=region)
        
        self._setup_logging()
        
        if region:
            logger.info(f"Initialized ImputationManager for region: {region}")
            if region not in self.region_config:
                logger.warning(f"Region '{region}' not found in configuration. Available regions: {', '.join(self.region_config.keys())}")
        else:
            logger.info(f"Initialized ImputationManager with {len(self.region_config)} regions")
            
        logger.info(f"Using {self.n_processes} processes for regional processing")
        logger.info(f"Data source: {self.metadata.get('source', 'Unknown')}")
        logger.info(f"Last updated: {self.metadata.get('last_updated', 'Unknown')}")
    
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

    def save_imputation_structure(self,
                                df: pd.DataFrame,
                                region: str) -> Path:
        """
        Save imputation structure for a region.
        
        Args:
            df: DataFrame containing imputation structure
            region: Region identifier
            
        Returns:
            Path to saved file
        """
        if df is None or df.empty:
            logger.warning(f"No data to save for region {region}")
            return None
            
        # Create output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"imputation_structure_{region}_{timestamp}.parquet"
        output_path = self.output_dir / filename
        
        # Save to parquet
        df.to_parquet(output_path)
        logger.info(f"Saved imputation structure for region {region} to {output_path}")
        
        return output_path

    def run(self) -> Dict[str, Path]:
        """
        Run imputation structure preparation for all regions or a single region.
        
        If a region was specified during initialization, only that region is processed.
        Otherwise, all regions are processed in parallel.
        
        Returns:
            Dictionary mapping region names to output file paths
        """
        output_files = {}
        
        try:
            # Load data once for all regions
            reference_points = self.data_loader.load_reference_points()
            gauge_stations = self.data_loader.load_gauge_stations()
            
            if reference_points is None or reference_points.empty:
                logger.error("Failed to load reference points")
                return output_files
                
            if gauge_stations is None or gauge_stations.empty:
                logger.error("Failed to load gauge stations")
                return output_files
            
            # Process only a specific region if requested
            if self.region:
                if self.region not in self.region_config:
                    logger.error(f"Region '{self.region}' not found in configuration")
                    return output_files
                    
                logger.info(f"Processing single region: {self.region}")
                try:
                    df = process_region(
                        self.region, 
                        self.region_config[self.region],
                        reference_points,
                        gauge_stations
                    )
                    
                    if df is not None:
                        output_path = self.save_imputation_structure(df, self.region)
                        if output_path:
                            output_files[self.region] = output_path
                except Exception as e:
                    logger.error(f"Error processing region {self.region}: {str(e)}")
                    logger.error(traceback.format_exc())
                    
                return output_files
            
            # Process all regions in parallel if no specific region requested
            logger.info("Processing all regions in parallel")
            with ProcessPoolExecutor(max_workers=self.n_processes) as executor:
                # Submit all regions
                future_to_region = {
                    executor.submit(
                        process_region,
                        region,
                        self.region_config[region],
                        reference_points,
                        gauge_stations
                    ): region 
                    for region in self.region_config
                }
                
                # Process results as they complete
                for future in tqdm(as_completed(future_to_region), 
                                 total=len(self.region_config),
                                 desc="Processing regions"):
                    region = future_to_region[future]
                    try:
                        df = future.result()
                        if df is not None:
                            output_path = self.save_imputation_structure(df, region)
                            if output_path:
                                output_files[region] = output_path
                    except Exception as e:
                        logger.error(f"Error processing region {region}: {str(e)}")
            
            return output_files
            
        except Exception as e:
            logger.error(f"Error in imputation process: {str(e)}")
            logger.error(traceback.format_exc())
            return output_files

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run imputation process for a specific region")
    parser.add_argument(
        "--region", 
        type=str,
        required=True,
        help="Region to process (e.g., west_coast, north_atlantic)"
    )
    parser.add_argument(
        "--output-dir", 
        type=str, 
        default=None,
        help="Directory to save output files (defaults to config settings)"
    )
    
    args = parser.parse_args()
    
    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        output_dir = IMPUTATION_DIR / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize and run imputation manager for the specified region
    manager = ImputationManager(
        output_dir=output_dir,
        region=args.region
    )
    
    # Run the imputation process
    output_files = manager.run()
    
    # Print results
    if output_files:
        print(f"\nSuccessfully generated imputation structures for {args.region}:")
        for region, path in output_files.items():
            print(f"  - {region}: {path}")
        sys.exit(0)
    else:
        print(f"\nFailed to generate imputation structure for {args.region}")
        sys.exit(1)