import os
import sys
import logging
import argparse
from datetime import datetime
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import modules
from src.config_manager import load_config, setup_earth_engine, ensure_directories
from src.gee_auth import authenticate_ee
from src.gee_climate_projections import get_us_counties, get_cmip6_collection
from src.county_projections import download_climate_data, process_counties_with_indicators


class PipelineOrchestrator:
    """
    Orchestrates the end-to-end pipeline for county-level climate projections.
    """
    
    def __init__(self, config_path=None):
        """
        Initialize the orchestrator with configuration.
        
        Args:
            config_path (str, optional): Path to configuration file
        """
        # Load configuration
        self.config = load_config(config_path) if config_path else load_config()
        
        # Create output and data directories
        ensure_directories(self.config)
        
        # Setup variables from config
        self.model = self.config.get('earth_engine', {}).get('model', 'ACCESS-CM2')
        self.scenario = self.config.get('climate', {}).get('scenario', 'ssp585')
        self.start_year = self.config.get('data', {}).get('years', {}).get('start', 2040)
        self.end_year = self.config.get('data', {}).get('years', {}).get('end', 2060)
        self.data_dir = self.config.get('data', {}).get('dir', 'data')
        self.output_dir = self.config.get('output', {}).get('dir', 'output')
        self.variables = self.config.get('climate', {}).get('variables', ['tas', 'pr'])
        
        # Initialize Earth Engine authentication status
        self.ee_initialized = False
        
    def initialize_earth_engine(self):
        """
        Initialize authentication with Google Earth Engine.
        
        Returns:
            bool: True if successful
        """
        logger.info("Setting up Earth Engine...")
        self.ee_initialized = setup_earth_engine(self.config)
        return self.ee_initialized
    
    def get_county_boundaries(self):
        """
        Fetch county boundaries from Earth Engine.
        
        Returns:
            ee.FeatureCollection: US counties feature collection
        """
        logger.info("Fetching US county boundaries...")
        counties = get_us_counties()
        return counties
    
    def download_climate_projections(self):
        """
        Download climate projections for all specified variables.
        
        Returns:
            dict: Mapping of variables to downloaded file paths
        """
        if not self.ee_initialized:
            if not self.initialize_earth_engine():
                raise RuntimeError("Failed to initialize Earth Engine")
        
        downloaded_files = {}
        
        for variable in self.variables:
            logger.info(f"Downloading {variable} climate projections...")
            try:
                file_path = download_climate_data(
                    self.model, 
                    self.scenario, 
                    variable, 
                    self.start_year, 
                    self.end_year, 
                    self.data_dir
                )
                downloaded_files[variable] = file_path
                logger.info(f"Downloaded {variable} data to {file_path}")
            except Exception as e:
                logger.error(f"Error downloading {variable} data: {str(e)}")
        
        return downloaded_files
    
    def process_climate_data(self, downloaded_files):
        """
        Process climate data to generate county-level indicators.
        
        Args:
            downloaded_files (dict): Mapping of variables to file paths
            
        Returns:
            dict: Mapping of variables to processed indicators DataFrames
        """
        counties = self.get_county_boundaries()
        results = {}
        
        for variable, file_path in downloaded_files.items():
            try:
                logger.info(f"Processing {variable} indicators...")
                indicators = process_counties_with_indicators(
                    file_path, 
                    variable,
                    counties,
                    self.output_dir
                )
                results[variable] = indicators
                logger.info(f"Successfully processed {variable} indicators")
            except Exception as e:
                logger.error(f"Error processing {variable} indicators: {str(e)}")
        
        return results
    
    def run_pipeline(self, download_only=False):
        """
        Run the complete pipeline.
        
        Args:
            download_only (bool): If True, only download data without processing
            
        Returns:
            bool: True if pipeline completed successfully
        """
        start_time = time.time()
        logger.info("Starting county-level climate projection pipeline...")
        
        try:
            # Step 1: Initialize Earth Engine
            if not self.initialize_earth_engine():
                logger.error("Failed to initialize Earth Engine. Aborting pipeline.")
                return False
            
            # Step 2: Download climate projections
            downloaded_files = self.download_climate_projections()
            
            if not downloaded_files:
                logger.error("No climate data was downloaded. Aborting pipeline.")
                return False
                
            if download_only:
                logger.info("Download-only mode. Skipping processing step.")
                logger.info("Pipeline completed successfully (download only).")
                return True
            
            # Step 3: Process downloaded data
            results = self.process_climate_data(downloaded_files)
            
            # Report results
            for variable, data in results.items():
                if data is not None:
                    logger.info(f"Successfully generated {variable} indicators")
                else:
                    logger.warning(f"Failed to generate {variable} indicators")
            
            elapsed_time = time.time() - start_time
            logger.info(f"Pipeline completed successfully in {elapsed_time:.2f} seconds.")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return False


def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Run the county-level climate projection pipeline")
    parser.add_argument("--config", type=str, help="Path to configuration file")
    parser.add_argument("--download-only", action="store_true", help="Only download data without processing")
    return parser.parse_args()


def main():
    """Main function to run the pipeline."""
    args = parse_arguments()
    
    # Initialize and run the pipeline
    pipeline = PipelineOrchestrator(config_path=args.config)
    success = pipeline.run_pipeline(download_only=args.download_only)
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main() 