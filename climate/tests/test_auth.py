"""
Simple script to test Google Earth Engine authentication using config_manager.
"""

import sys
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import our modules
sys.path.append('src')
from config_manager import load_config, setup_earth_engine, ensure_directories

def main():
    """Test Google Earth Engine authentication using the config manager."""
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        
        # Ensure required directories exist
        logger.info("Ensuring directories exist...")
        ensure_directories(config)
        
        # Try to authenticate and initialize Earth Engine
        logger.info("Attempting to authenticate with Earth Engine...")
        success = setup_earth_engine(config)
        
        if success:
            logger.info("Authentication and initialization successful! ✅")
            
            # Get project ID from config to confirm
            project_id = config.get('earth_engine', {}).get('project_id', 'Not specified')
            logger.info(f"Connected to Earth Engine project: {project_id}")
            
            # Print a summary of the config being used
            image_collection = config.get('earth_engine', {}).get('image_collection', 'Not specified')
            start_year = config.get('data', {}).get('years', {}).get('start', 'Not specified')
            end_year = config.get('data', {}).get('years', {}).get('end', 'Not specified')
            
            logger.info("Configuration summary:")
            logger.info(f"  - Image collection: {image_collection}")
            logger.info(f"  - Year range: {start_year} to {end_year}")
        else:
            logger.error("Authentication failed! ❌")
            logger.error("Please check your credentials and try again.")
    except Exception as e:
        logger.exception(f"An error occurred: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 