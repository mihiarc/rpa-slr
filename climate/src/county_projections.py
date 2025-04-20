import os
import sys
import pandas as pd
import numpy as np
import xarray as xr
import ee
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import our modules
sys.path.append('src')
from config_manager import load_config, setup_earth_engine, ensure_directories
from gee_climate_projections import (
    get_us_counties, get_cmip6_collection,
    calculate_county_climate_stats
)
from climate_indicators import (
    convert_ee_to_xarray, calculate_temperature_indicators,
    calculate_precipitation_indicators, aggregate_by_county
)


def download_climate_data(model, scenario, variable, start_year, end_year, output_dir):
    """
    Download climate data from Google Earth Engine for a specific model, scenario, and variable.
    
    Args:
        model (str): Climate model name
        scenario (str): Climate scenario (e.g., 'ssp585', 'ssp245')
        variable (str): Climate variable (e.g., 'tas', 'pr')
        start_year (int): Start year
        end_year (int): End year
        output_dir (str): Directory to save output files
    
    Returns:
        str: Path to the downloaded file
    """
    # Get climate collection
    collection = get_cmip6_collection(model, scenario, variable)
    
    # Filter by date range
    start_date = ee.Date.fromYMD(start_year, 1, 1)
    end_date = ee.Date.fromYMD(end_year, 12, 31)
    filtered = collection.filterDate(start_date, end_date)
    
    # Define output filename
    output_file = os.path.join(
        output_dir, 
        f"{variable}_{model}_{scenario}_{start_year}_{end_year}.nc"
    )
    
    # Create a region of interest (continental US bounding box)
    roi = ee.Geometry.Rectangle([-125, 24, -66, 50])
    
    # Using Earth Engine's native export functionality instead of geemap
    logger.info(f"Downloading {variable} data for {model} {scenario} from {start_year} to {end_year}...")
    
    # For larger regions and time periods, we need to export to Google Drive first
    # This is a simplified example - in practice, this would be more complex
    task = ee.batch.Export.image.toDrive(
        image=filtered.mean(),
        description=f"{variable}_{model}_{scenario}_{start_year}_{end_year}",
        folder='climate_data',
        region=roi,
        scale=10000,  # 10km scale, can be adjusted
        maxPixels=1e9
    )
    
    # Start the export task
    task.start()
    
    logger.info(f"Started export task to Google Drive. Please download the file and save to {output_file}")
    logger.info(f"Task ID: {task.id}")
    
    # For demonstration, assume the file has been downloaded and return the path
    return output_file


def process_counties_with_indicators(climate_data_file, variable, counties, output_dir):
    """
    Process climate data with county boundaries to calculate county-level indicators.
    
    Args:
        climate_data_file (str): Path to climate data file (NetCDF)
        variable (str): Climate variable ('tas' or 'pr')
        counties (ee.FeatureCollection): County boundaries
        output_dir (str): Directory to save output files
    
    Returns:
        pd.DataFrame: DataFrame with county-level indicators
    """
    # Load the climate data
    ds = xr.open_dataset(climate_data_file)
    
    # Calculate indicators based on variable type
    if variable == 'tas':
        indicators = calculate_temperature_indicators(ds)
        output_file = os.path.join(output_dir, "county_temperature_indicators.csv")
    elif variable == 'pr':
        indicators = calculate_precipitation_indicators(ds)
        output_file = os.path.join(output_dir, "county_precipitation_indicators.csv")
    else:
        raise ValueError(f"Unsupported variable: {variable}")
    
    # Convert county feature collection to dictionary of geometries
    # This is a simplification - you would need to implement this function
    county_geometries = convert_counties_to_geometries(counties)
    
    # Aggregate indicators by county
    county_indicators = aggregate_by_county(
        indicators, county_geometries, output_file=output_file
    )
    
    return county_indicators


def convert_counties_to_geometries(counties):
    """
    Convert county feature collection to dictionary of geometries.
    
    Args:
        counties (ee.FeatureCollection): County boundaries
    
    Returns:
        dict: Dictionary mapping county IDs to geometries
    """
    # Get the list of features
    features = counties.getInfo()['features']
    
    # Create a dictionary of geometries
    geometries = {}
    for feature in features:
        county_id = feature['properties']['GEOID']  # Or other appropriate ID
        geometry = feature['geometry']
        geometries[county_id] = geometry
    
    return geometries


def main():
    """Main function to run the county-level climate projection analysis."""
    # Load configuration
    config = load_config()
    
    # Ensure directories exist
    ensure_directories(config)
    
    # Setup Earth Engine
    if not setup_earth_engine(config):
        logger.error("Failed to set up Earth Engine. Exiting.")
        return
    
    # Get county boundaries
    logger.info("Fetching US county boundaries...")
    counties = get_us_counties()
    
    # Get parameters from config
    model = config.get('earth_engine', {}).get('model', 'ACCESS-CM2')
    scenario = config.get('climate', {}).get('scenario', 'ssp585')
    start_year = config.get('data', {}).get('years', {}).get('start', 2040)
    end_year = config.get('data', {}).get('years', {}).get('end', 2060)
    data_dir = config.get('data', {}).get('dir', 'data')
    output_dir = config.get('output', {}).get('dir', 'output')
    
    # Get variables to process
    variables = config.get('climate', {}).get('variables', ['tas', 'pr'])
    
    # Process each variable
    for variable in variables:
        if variable == 'tas':
            logger.info("Processing temperature projections...")
        elif variable == 'pr':
            logger.info("Processing precipitation projections...")
        else:
            logger.warning(f"Skipping unsupported variable: {variable}")
            continue
            
        # Download climate data
        data_file = download_climate_data(
            model, scenario, variable, start_year, end_year, data_dir
        )
        
        # When files are downloaded, process them
        # Commented out since the files need to be manually downloaded from Google Drive
        # indicators = process_counties_with_indicators(
        #     data_file, variable, counties, output_dir
        # )
    
    logger.info("County-level climate projections have been calculated.")
    logger.info(f"Please check the {output_dir} directory for results.")


if __name__ == "__main__":
    main() 