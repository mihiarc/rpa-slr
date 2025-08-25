"""
Script to generate coastal counties from predefined list.
Uses county_region_mappings.yaml to define coastal counties by region.
"""

import geopandas as gpd
import pandas as pd
import yaml
from pathlib import Path
import logging
import argparse
import os
import sys
from src.config import (
    CONFIG_DIR,
    PROCESSED_DIR,
    COUNTY_REGION_CONFIG,
    CENSUS_COUNTY_SHAPEFILE,
    COASTAL_COUNTIES_FILE
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Make sure directories exist
os.makedirs(PROCESSED_DIR, exist_ok=True)

def load_county_mappings():
    """Load county-to-region mappings from YAML configuration."""
    logger.info(f"Loading county mappings from {COUNTY_REGION_CONFIG}")
    
    with open(COUNTY_REGION_CONFIG) as f:
        config = yaml.safe_load(f)
    
    return config.get("regions", {})

def load_census_counties():
    """Load county geometries from Census shapefile."""
    logger.info(f"Loading county geometries from {CENSUS_COUNTY_SHAPEFILE}")
    
    if not Path(CENSUS_COUNTY_SHAPEFILE).exists():
        raise FileNotFoundError(f"Census county file not found: {CENSUS_COUNTY_SHAPEFILE}")
    
    counties = gpd.read_file(CENSUS_COUNTY_SHAPEFILE)
    logger.info(f"Loaded {len(counties)} counties from Census data")
    
    return counties

def process_county_mappings(region_config, counties_gdf, region_filter=None):
    """Process county mappings and filter/organize counties.
    
    Args:
        region_config: Dictionary of region configurations
        counties_gdf: GeoDataFrame of all counties from Census
        region_filter: Optional region name to filter results
    
    Returns:
        GeoDataFrame of coastal counties with region information
    """
    # Filter regions if specified
    if region_filter:
        if region_filter not in region_config:
            available_regions = ", ".join(region_config.keys())
            raise ValueError(f"Invalid region: {region_filter}. Available regions: {available_regions}")
        
        regions_to_process = {region_filter: region_config[region_filter]}
    else:
        regions_to_process = region_config
    
    # Process each region and collect counties
    all_coastal_counties = []
    
    for region_name, region_def in regions_to_process.items():
        logger.info(f"Processing region: {region_name}")
        
        region_counties = region_def.get("counties", [])
        if not region_counties:
            logger.warning(f"No counties defined for region: {region_name}")
            continue
        
        # Extract FIPS codes for this region
        county_fips_list = [county.get("fips") for county in region_counties]
        county_names = {county.get("fips"): county.get("name") for county in region_counties}
        
        logger.info(f"Found {len(county_fips_list)} counties in region definition")
        
        # Convert county FIPS to match Census GEOID format if needed
        # Census uses 5-digit GEOID (State FIPS + County FIPS)
        formatted_fips = []
        for fips in county_fips_list:
            if fips and len(fips) == 5:  # Already in correct format
                formatted_fips.append(fips)
            else:
                logger.warning(f"Invalid FIPS code format: {fips}")
        
        # Filter counties by FIPS
        region_counties_gdf = counties_gdf[counties_gdf["GEOID"].isin(formatted_fips)].copy()
        
        if len(region_counties_gdf) == 0:
            logger.warning(f"No counties found in Census data for region: {region_name}")
            logger.warning(f"Check FIPS codes or shapefile data")
            continue
        
        logger.info(f"Found {len(region_counties_gdf)} matching counties in Census data")
        
        # Add region information
        region_counties_gdf["region"] = region_name
        region_counties_gdf["region_display"] = region_def.get("name", region_name)
        
        # Add county names from our mapping
        region_counties_gdf["county_name"] = region_counties_gdf["GEOID"].map(county_names)
        region_counties_gdf["county_fips"] = region_counties_gdf["GEOID"]
        
        # Add to collection
        all_coastal_counties.append(region_counties_gdf)
    
    if not all_coastal_counties:
        logger.error("No coastal counties found for any region")
        return gpd.GeoDataFrame()
    
    # Combine all regions
    coastal_counties = pd.concat(all_coastal_counties, ignore_index=True)
    coastal_counties_gdf = gpd.GeoDataFrame(coastal_counties, geometry="geometry")
    
    # Remove any duplicates (counties that may be in multiple regions)
    coastal_counties_gdf = coastal_counties_gdf.drop_duplicates(subset=["GEOID"])
    
    logger.info(f"Total unique coastal counties: {len(coastal_counties_gdf)}")
    logger.info("Counties by region:")
    logger.info(coastal_counties_gdf["region_display"].value_counts())
    
    return coastal_counties_gdf

def generate_coastal_counties(region_filter=None):
    """Generate coastal counties from predefined list.
    
    Args:
        region_filter: Optional region name to filter processing
    
    Returns:
        GeoDataFrame of coastal counties
    """
    try:
        # Load county-to-region mappings
        region_config = load_county_mappings()
        
        # Load Census county geometries
        counties_gdf = load_census_counties()
        
        # Process county mappings
        coastal_counties = process_county_mappings(region_config, counties_gdf, region_filter)
        
        if coastal_counties.empty:
            logger.error("Failed to generate coastal counties")
            return gpd.GeoDataFrame()
        
        # Generate output filename
        output_file = COASTAL_COUNTIES_FILE
        if region_filter:
            # Create region-specific output file
            output_dir = Path(COASTAL_COUNTIES_FILE).parent
            output_name = f"coastal_counties_{region_filter}.parquet"
            output_file = output_dir / output_name
        
        # Create output directory if it doesn't exist
        os.makedirs(output_file.parent, exist_ok=True)
        
        # Save to file
        coastal_counties.to_parquet(output_file, compression="snappy", index=False)
        logger.info(f"Saved coastal counties to {output_file}")
        
        return coastal_counties
    
    except Exception as e:
        logger.error(f"Error generating coastal counties: {str(e)}")
        raise

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Generate coastal counties from predefined list")
    parser.add_argument("--region", type=str, help="Region to process (e.g., west_coast)")
    return parser.parse_args()

def main():
    """Generate coastal counties from predefined list."""
    # Parse command line arguments
    args = parse_args()
    
    try:
        # Generate coastal counties
        generate_coastal_counties(region_filter=args.region)
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 