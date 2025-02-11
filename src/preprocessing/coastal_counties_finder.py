"""
Script to identify coastal counties based on shoreline intersection.
Uses region definitions from region_mappings.yaml for proper regional processing.
"""

import geopandas as gpd
import pandas as pd
import multiprocessing as mp
from functools import partial
import yaml
from pathlib import Path
import logging
from src.config import (
    CONFIG_DIR,
    SHORELINE_DIR,
    COUNTY_FILE,
    COASTAL_COUNTIES_FILE
)

logger = logging.getLogger(__name__)

def load_region_config():
    """Load region configuration from YAML."""
    with open(CONFIG_DIR / "region_mappings.yaml") as f:
        config = yaml.safe_load(f)
    return config['regions']

def get_state_fips_mapping():
    """Create mapping of state codes to FIPS codes."""
    return {
        'ME': '23', 'NH': '33', 'MA': '25', 'RI': '44', 'CT': '09',
        'NY': '36', 'NJ': '34', 'PA': '42', 'DE': '10', 'MD': '24', 'VA': '51',
        'NC': '37', 'SC': '45', 'GA': '13', 'FL': '12',
        'AL': '01', 'LA': '22', 'MS': '28', 'TX': '48',
        'CA': '06', 'OR': '41', 'WA': '53',
        'AK': '02', 'HI': '15',
        'GU': '66', 'AS': '60', 'MP': '69',
        'VI': '78', 'PR': '72'
    }

def get_region_projection(region_name: str, region_def: dict) -> str:
    """Get the appropriate projection for a region.
    
    Args:
        region_name: Name of the region
        region_def: Region definition from config
    
    Returns:
        Projection string for the region
    """
    # Use region-specific projection if defined in config
    if 'projection' in region_def:
        return region_def['projection']
    
    # Default projections for specific regions
    if region_name == 'alaska':
        return ("+proj=aea +lat_1=55 +lat_2=65 +lat_0=50 +lon_0=-154 "
                "+x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs")
    elif region_name == 'hawaii':
        return ("+proj=aea +lat_1=8 +lat_2=18 +lat_0=13 +lon_0=-157 "
                "+x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs")
    elif region_name == 'pacific_islands':
        return ("+proj=aea +lat_1=0 +lat_2=20 +lat_0=10 +lon_0=160 "
                "+x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs")
    elif region_name == 'puerto_rico':
        return ("+proj=aea +lat_1=17 +lat_2=19 +lat_0=18 +lon_0=-66.5 "
                "+x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs")
    elif region_name == 'virgin_islands':
        return ("+proj=aea +lat_1=17 +lat_2=19 +lat_0=18 +lon_0=-64.75 "
                "+x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs")
    elif region_name == 'west_coast':
        return ("+proj=aea +lat_1=34 +lat_2=45.5 +lat_0=40 +lon_0=-120 "
                "+x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs")
    
    # Default to WGS84 for other regions (CONUS regions use standard Albers Equal Area)
    return "EPSG:4326"

def process_chunk(counties_chunk, ocean_shoreline_union):
    """Process a chunk of counties in parallel."""
    return counties_chunk[counties_chunk.geometry.intersects(ocean_shoreline_union)]

def find_coastal_counties_for_region(shoreline_gdf: gpd.GeoDataFrame, 
                                   counties: gpd.GeoDataFrame, 
                                   region_name: str,
                                   region_def: dict) -> gpd.GeoDataFrame:
    """Find counties that intersect with a specific region's shoreline.
    
    Args:
        shoreline_gdf: GeoDataFrame containing the region's shoreline
        counties: GeoDataFrame containing all counties
        region_name: Name of the region being processed
        region_def: Region definition from config
    """
    logger.info(f"\nProcessing region: {region_name}")
    
    # Get appropriate projection
    projection = get_region_projection(region_name, region_def)
    
    # Convert state codes to FIPS codes
    state_fips_mapping = get_state_fips_mapping()
    state_fips = [state_fips_mapping[state] for state in region_def['state_codes']]
    
    # Filter counties by state FIPS codes
    counties = counties[counties['STATEFP'].isin(state_fips)]
    logger.info(f"Filtered to {len(counties)} counties in region states")
    
    if len(counties) == 0:
        logger.warning(f"No counties found for states in region {region_name}")
        return gpd.GeoDataFrame()
    
    # Transform to region-specific projection
    logger.info(f"Using projection: {projection}")
    shoreline_gdf = shoreline_gdf.to_crs(projection)
    counties = counties.to_crs(projection)
    
    # Create shoreline union
    logger.info("Creating shoreline union...")
    shoreline_union = shoreline_gdf.geometry.union_all()
    
    # Configure parallel processing
    n_cores = mp.cpu_count()
    logger.info(f"System has {n_cores} cores available")
    
    # For optimal performance, use slightly fewer processes
    n_processes = min(50, n_cores - 2)
    
    # Calculate chunk size
    min_chunk_size = 20
    n_counties = len(counties)
    chunk_size = max(min_chunk_size, n_counties // n_processes)
    n_chunks = (n_counties + chunk_size - 1) // chunk_size
    
    logger.info(f"Processing with {n_processes} parallel processes")
    logger.info(f"Splitting {n_counties} counties into {n_chunks} chunks of ~{chunk_size} counties each")
    
    # Split counties into chunks
    chunks = [counties.iloc[i:i + chunk_size] for i in range(0, n_counties, chunk_size)]
    
    # Process chunks in parallel
    logger.info(f"Finding coastal counties using {n_processes} processes...")
    with mp.Pool(processes=n_processes, maxtasksperchild=100) as pool:
        process_func = partial(process_chunk, ocean_shoreline_union=shoreline_union)
        coastal_chunks = pool.map(process_func, chunks)
    
    # Filter out empty chunks and combine results
    coastal_chunks = [chunk for chunk in coastal_chunks if not chunk.empty]
    if not coastal_chunks:
        logger.warning(f"No coastal counties found in region {region_name}")
        return gpd.GeoDataFrame()
        
    coastal_counties = gpd.GeoDataFrame(pd.concat(coastal_chunks, ignore_index=True))
    logger.info(f"Found {len(coastal_counties)} coastal counties in {region_name}")
    
    # Add region information
    coastal_counties['region'] = region_name
    coastal_counties['region_display'] = region_def.get('display_name', region_name.replace('_', ' ').title())
    
    # Convert back to WGS84
    if projection != "EPSG:4326":
        coastal_counties = coastal_counties.to_crs("EPSG:4326")
    
    return coastal_counties

def find_coastal_counties():
    """Find counties that intersect with the coastline."""
    # Load region configuration
    regions_config = load_region_config()
    
    # Load county geometries
    logger.info("Loading county geometries...")
    counties = gpd.read_parquet(COUNTY_FILE)
    
    coastal_counties = []
    
    # Process each region
    for region_name, region_def in regions_config.items():
        logger.info(f"\nProcessing region: {region_name}")
        
        # Load regional shoreline
        region_file = SHORELINE_DIR / f"{region_name}.parquet"
        
        if not region_file.exists():
            logger.warning(f"Shoreline file not found for {region_name}")
            continue
        
        logger.info(f"\nReading {region_name} shoreline data...")
        shoreline = gpd.read_parquet(region_file)
        
        # Find coastal counties for this region
        regional_coastal_counties = find_coastal_counties_for_region(
            shoreline, counties, region_name, region_def
        )
        
        if not regional_coastal_counties.empty:
            coastal_counties.append(regional_coastal_counties)
    
    if not coastal_counties:
        logger.error("No coastal counties found in any region")
        raise ValueError("No coastal counties found")
        
    # Combine results from all regions
    coastal_counties_gdf = gpd.GeoDataFrame(pd.concat(coastal_counties, ignore_index=True))
    
    # Remove duplicates (counties that touch multiple regions)
    coastal_counties_gdf = coastal_counties_gdf.drop_duplicates(subset=['GEOID'])
    
    logger.info(f"\nTotal unique coastal counties found: {len(coastal_counties_gdf)}")
    logger.info("Counties by region:")
    logger.info(coastal_counties_gdf['region_display'].value_counts())
    
    # Save coastal counties
    coastal_counties_gdf.to_parquet(COASTAL_COUNTIES_FILE)
    logger.info(f"\nSaved to {COASTAL_COUNTIES_FILE}")
    
    return coastal_counties_gdf

def main():
    """Find coastal counties from regional shoreline parquet files."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        find_coastal_counties()
    except Exception as e:
        logger.error(f"Error finding coastal counties: {str(e)}")
        raise

if __name__ == "__main__":
    main() 