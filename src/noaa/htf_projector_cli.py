"""
Command-line script to fetch and save projected high tide flooding (HTF) data from NOAA.

This script uses the ProjectedHTFService to retrieve future HTF projections and saves them in a 
parquet format organized by decade and tide gauge station.
"""

import logging
from pathlib import Path
import sys

from src.noaa.htf_projector import ProjectedHTFService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    # Initialize the service
    try:
        service = ProjectedHTFService()
        logger.info("Initialized ProjectedHTFService")
        
        # Create output directory
        output_dir = Path("output/projected_htf")
        
        # Generate the dataset
        logger.info("Fetching projected HTF data...")
        output_file = service.generate_dataset(output_dir)
        
        # Get and log dataset status
        status = service.get_dataset_status()
        logger.info("Dataset generation complete!")
        logger.info(f"Generated file: {output_file}")
        logger.info(f"Dataset contains data for {status['station_count']} stations")
        logger.info(f"Decade range: {status['decade_range']['min']} to {status['decade_range']['max']}")
        logger.info(f"Data completeness: {status['completeness']*100:.1f}%")
        
    except Exception as e:
        logger.error(f"Error generating dataset: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 