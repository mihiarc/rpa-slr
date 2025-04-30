"""
Utility functions for loading configuration and initializing Earth Engine.
"""

import os
import yaml
from pathlib import Path
import logging
from typing import Dict, Any, Optional

# Local imports
from src.gee_auth import authenticate_ee, initialize_earth_engine

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_CONFIG_PATH = 'config.yml'

def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path (str): Path to the configuration file
        
    Returns:
        Dict[str, Any]: Dictionary with configuration values
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        # Return default configuration
        default_config = {
            'earth_engine': {'project_id': 'ee-chrismihiar', 'image_collection': 'NASA/GDDP-CMIP6'},
            'data': {'years': {'start': 2040, 'end': 2060}},
            'processing': {'chunk_size': 10000, 'max_concurrent_tasks': 3000}
        }
        logger.warning(f"Using default configuration due to error: {str(e)}")
        return default_config

def setup_earth_engine(config: Optional[Dict[str, Any]] = None) -> bool:
    """
    Set up Earth Engine with project from configuration.
    
    Args:
        config (Dict[str, Any], optional): Configuration dictionary.
            If None, loads from default config file.
            
    Returns:
        bool: True if authentication and initialization were successful
    """
    # Load config if not provided
    if config is None:
        config = load_config()
        
    # Get project ID from config
    project_id = config.get('earth_engine', {}).get('project_id')
    
    # Authenticate and initialize Earth Engine
    if not authenticate_ee(project=project_id):
        logger.error("Failed to authenticate with Earth Engine")
        return False
        
    logger.info(f"Successfully set up Earth Engine with project: {project_id}")
    return True
    
def ensure_directories(config: Optional[Dict[str, Any]] = None) -> None:
    """
    Ensure that required directories from config exist.
    
    Args:
        config (Dict[str, Any], optional): Configuration dictionary.
            If None, loads from default config file.
    """
    # Load config if not provided
    if config is None:
        config = load_config()
        
    # Create output directory
    output_dir = config.get('output', {}).get('dir', 'output')
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Ensured output directory exists: {output_dir}")
    
    # Create data directory if needed
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)
    logger.info(f"Ensured data directory exists: {data_dir}")
    
if __name__ == "__main__":
    # Test loading configuration
    config = load_config()
    print(f"Loaded configuration: {config}")
    
    # Test setting up Earth Engine
    success = setup_earth_engine(config)
    print(f"Earth Engine setup result: {success}")
    
    # Test ensuring directories
    ensure_directories(config) 