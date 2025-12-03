"""
Script to split shoreline shapefiles into specific regional groups.
Uses region definitions from region_mappings.yaml.
"""

import geopandas as gpd
from pathlib import Path
import logging
import numpy as np
import pandas as pd
import yaml
from shapely.geometry import box
from src.config import (
    CONFIG_DIR,
    PROCESSED_DIR,
    SHORELINE_DIR
)

# Note: Do not call logging.basicConfig here - let the application configure logging
logger = logging.getLogger(__name__)

def load_region_config():
    """Load region configuration from YAML."""
    with open(CONFIG_DIR / "region_mappings.yaml") as f:
        config = yaml.safe_load(f)
    return config['regions']

def get_feature_centroid(geometry):
    """Get the centroid coordinates of a geometry."""
    try:
        coords = np.array([(x, y) for x, y in geometry.coords])
        return np.mean(coords[:, 0]), np.mean(coords[:, 1])
    except:
        # For MultiLineString geometries
        all_coords = []
        for line in geometry.geoms:
            coords = np.array([(x, y) for x, y in line.coords])
            all_coords.append(coords)
        coords = np.vstack(all_coords)
        return np.mean(coords[:, 0]), np.mean(coords[:, 1])

def create_region_bounds(region_def):
    """Create a GeoDataFrame with region boundary."""
    bounds = region_def['bounds']
    boundary = box(
        bounds['min_lon'],
        bounds['min_lat'],
        bounds['max_lon'],
        bounds['max_lat']
    )
    return gpd.GeoDataFrame({'geometry': [boundary]}, crs="EPSG:4326")

def split_by_region(gdf, region_name, region_def):
    """Split features by region definition."""
    logger.info(f"Processing region: {region_name}")
    
    # Filter by state codes
    state_mask = gdf['FIPS_ALPHA'].isin(region_def['state_codes'])
    state_features = gdf[state_mask].copy()
    
    # Create region boundary
    bounds_gdf = create_region_bounds(region_def)
    
    # Filter by region bounds
    region_features = gpd.sjoin(
        state_features,
        bounds_gdf,
        how='inner',
        predicate='intersects'
    )
    
    logger.info(f"Found {len(region_features)} features in {region_name}")
    return region_features

def split_florida(gdf, regions_config):
    """Special handling for Florida which spans two regions."""
    logger.info("Processing Florida features...")
    
    florida_mask = gdf['FIPS_ALPHA'] == 'FL'
    florida_df = gdf[florida_mask].copy()
    
    # Get region bounds
    sa_bounds = create_region_bounds(regions_config['south_atlantic'])
    gc_bounds = create_region_bounds(regions_config['gulf_coast'])
    
    # Split Florida features by region bounds
    atlantic_features = gpd.sjoin(
        florida_df,
        sa_bounds,
        how='inner',
        predicate='intersects'
    )
    
    gulf_features = gpd.sjoin(
        florida_df,
        gc_bounds,
        how='inner',
        predicate='intersects'
    )
    
    logger.info(f"Florida Atlantic features: {len(atlantic_features)}")
    logger.info(f"Florida Gulf features: {len(gulf_features)}")
    
    return atlantic_features, gulf_features

def split_shoreline():
    """Split shoreline into regional files."""
    # Load region configuration
    regions_config = load_region_config()
    
    # Load shoreline data
    shoreline_file = PROCESSED_DIR / "shoreline.parquet"
    
    if not shoreline_file.exists():
        logger.error(f"Shoreline file not found: {shoreline_file}")
        raise FileNotFoundError(f"Shoreline file not found: {shoreline_file}")
    
    logger.info("Loading shoreline data...")
    shoreline = gpd.read_parquet(shoreline_file)
    
    # Process each region
    for region_name, region_def in regions_config.items():
        logger.info(f"\nProcessing region: {region_name}")
        
        # Get region bounds
        bounds = region_def['bounds']
        
        # Filter shoreline for this region
        region_shoreline = filter_shoreline_for_region(shoreline, bounds)
        
        if region_shoreline.empty:
            logger.warning(f"No shoreline found for region: {region_name}")
            continue
        
        # Save regional shoreline
        output_file = SHORELINE_DIR / f"{region_name}.parquet"
        region_shoreline.to_parquet(output_file)
        logger.info(f"Saved {region_name} shoreline to {output_file}")

if __name__ == "__main__":
    split_shoreline() 