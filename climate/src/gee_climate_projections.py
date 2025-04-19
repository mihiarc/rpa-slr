import ee
import geemap
import xarray as xr
import pandas as pd
import os
from datetime import datetime
import numpy as np
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import the configuration manager
from src.config_manager import load_config, setup_earth_engine


def get_us_counties():
    """
    Get US county boundaries from Google Earth Engine.
    Returns a feature collection of US counties.
    """
    # Load US counties dataset from GEE
    counties = ee.FeatureCollection("TIGER/2018/Counties")
    return counties


def get_cmip6_collection(model='ACCESS-CM2', scenario='ssp585', variable='tas'):
    """
    Get climate projection data from the NASA GDDP-CMIP6 collection.
    
    Args:
        model (str): Climate model name
        scenario (str): Climate scenario (e.g., 'ssp585', 'ssp245')
        variable (str): Climate variable (e.g., 'tas' for temperature, 'pr' for precipitation)
    
    Returns:
        ee.ImageCollection: Collection of climate projection images
    """
    collection = ee.ImageCollection("NASA/GDDP-CMIP6")
    
    # Filter the collection based on parameters
    filtered = collection.filter(
        ee.Filter.And(
            ee.Filter.equals('model', model),
            ee.Filter.equals('scenario', scenario),
            ee.Filter.equals('variable', variable)
        )
    )
    
    return filtered


def calculate_county_climate_stats(counties, climate_collection, start_year, end_year):
    """
    Calculate climate statistics for each county.
    
    Args:
        counties (ee.FeatureCollection): US county boundaries
        climate_collection (ee.ImageCollection): Climate data collection
        start_year (int): Start year for the analysis
        end_year (int): End year for the analysis
    
    Returns:
        ee.FeatureCollection: Counties with climate statistics
    """
    # Filter collection by date range
    start_date = ee.Date.fromYMD(start_year, 1, 1)
    end_date = ee.Date.fromYMD(end_year, 12, 31)
    filtered_collection = climate_collection.filterDate(start_date, end_date)
    
    # Calculate mean values for each county
    def calculate_mean(feature):
        # Calculate mean over the time period for this county
        mean_value = filtered_collection.mean().reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=feature.geometry(),
            scale=10000,  # Adjust scale as needed
            maxPixels=1e9
        )
        
        # Add the mean value as a property to the feature
        return feature.set(mean_value)
    
    counties_with_stats = counties.map(calculate_mean)
    return counties_with_stats


def export_counties_to_csv(counties_with_stats, output_file):
    """
    Export county statistics to a CSV file.
    
    Args:
        counties_with_stats (ee.FeatureCollection): Counties with climate statistics
        output_file (str): Path to output CSV file
    """
    # Convert to a pandas dataframe
    counties_list = counties_with_stats.getInfo()['features']
    
    # Extract properties from each feature
    data = []
    for county in counties_list:
        properties = county['properties']
        data.append(properties)
    
    # Create dataframe and save to CSV
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)
    logger.info(f"Saved county climate statistics to {output_file}")


def main():
    """Main function to run the analysis."""
    # Load configuration
    config = load_config()
    
    # Setup Earth Engine
    if not setup_earth_engine(config):
        logger.error("Failed to authenticate with Earth Engine. Exiting.")
        return
    
    # Get US counties
    logger.info("Fetching US counties...")
    counties = get_us_counties()
    
    # Get parameters from config
    model = config.get('earth_engine', {}).get('model', 'ACCESS-CM2')
    scenario = config.get('climate', {}).get('scenario', 'ssp585')
    variable = config.get('earth_engine', {}).get('variable', 'tas')
    start_year = config.get('data', {}).get('years', {}).get('start', 2040)
    end_year = config.get('data', {}).get('years', {}).get('end', 2060)
    output_dir = config.get('output', {}).get('dir', 'output')
    
    # Get climate projection data
    logger.info(f"Fetching climate projections for model {model}, scenario {scenario}, variable {variable}...")
    climate_collection = get_cmip6_collection(model, scenario, variable)
    
    # Calculate statistics for each county
    logger.info("Calculating county-level climate statistics...")
    counties_with_stats = calculate_county_climate_stats(counties, climate_collection, start_year, end_year)
    
    # Export to CSV
    output_file = os.path.join(output_dir, f"county_climate_{model}_{scenario}_{variable}_{start_year}_{end_year}.csv")
    export_counties_to_csv(counties_with_stats, output_file)


if __name__ == "__main__":
    main() 