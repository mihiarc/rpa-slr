"""
Generate evenly spaced reference points along the coastline for coastal counties.
Points are spaced 5km apart using region-specific projections for accurate distances.
Uses region definitions from region_mappings.yaml for proper regional processing.
"""

import geopandas as gpd
from shapely.geometry import Point, LineString
from pathlib import Path
import yaml
import logging
from typing import List, Dict
import argparse
import pandas as pd
import os
import sys
from src.config import (
    CONFIG_DIR,
    PROCESSED_DIR,
    SHORELINE_DIR,
    COASTAL_COUNTIES_FILE,
    REFERENCE_POINTS_FILE
)

# Set up logging manually without dependencies
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Point spacing in meters (5km)
POINT_SPACING_M = 5000

# Make sure directories exist
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(REFERENCE_POINTS_FILE.parent, exist_ok=True)

def load_region_config():
    """Load region configuration from YAML."""
    with open(CONFIG_DIR / "region_mappings.yaml") as f:
        config = yaml.safe_load(f)
    return config.get('regions', {})

def get_region_projection(region_name: str, region_def: dict) -> str:
    """Get proper projection for distance calculations in a region.
    
    Args:
        region_name: Name of the region
        region_def: Region definition dictionary
        
    Returns:
        EPSG code for the region's UTM zone
    """
    bounds = region_def.get('bounds', {})
    # Default to center of region for UTM zone calculation
    center_lon = (bounds.get('min_lon', -98) + bounds.get('max_lon', -80)) / 2
    
    # Calculate UTM zone from longitude (simplified)
    utm_zone = int((center_lon + 180) // 6) + 1
    
    # North/South UTM
    center_lat = (bounds.get('min_lat', 25) + bounds.get('max_lat', 45)) / 2
    hem = "north" if center_lat >= 0 else "south"
    
    logger.info(f"Using UTM zone {utm_zone}{hem} for {region_name}")
    
    # EPSG code for UTM zone
    if hem == "north":
        epsg = f"EPSG:{32600 + utm_zone}"
    else:
        epsg = f"EPSG:{32700 + utm_zone}"
    
    return epsg

def create_reference_points(line: LineString, spacing: float = POINT_SPACING_M) -> List[Point]:
    """Create evenly spaced points along a linestring.
    
    Args:
        line: LineString to create points along
        spacing: Spacing between points in projected units (meters)
        
    Returns:
        List of Points spaced evenly along the line
    """
    # Skip if line is empty
    if line.is_empty:
        return []
    
    # Get length of line
    length = line.length
    
    # Calculate number of points to create
    n_points = max(1, int(length / spacing))
    
    # Create evenly spaced points along the line
    points = []
    for i in range(n_points):
        distance = i * spacing
        # Get point at distance along line
        if distance > length:
            break
        point = line.interpolate(distance)
        points.append(point)
    
    return points

def process_region(shoreline_gdf: gpd.GeoDataFrame, 
                  counties: gpd.GeoDataFrame, 
                  region_name: str,
                  region_def: dict) -> List[Dict]:
    """Process a region to create reference points.
    
    Args:
        shoreline_gdf: GeoDataFrame with shoreline geometries for the region
        counties: GeoDataFrame with coastal counties
        region_name: Name of the region
        region_def: Configuration for the region
        
    Returns:
        List of dictionaries with reference point information
    """
    logger.info(f"Processing {len(shoreline_gdf)} shoreline features for {region_name}")
    
    # Get regional projection for accurate distance calculations
    region_proj = get_region_projection(region_name, region_def)
    
    # Get counties for this region
    region_counties = counties[counties['region'] == region_name]
    
    if len(region_counties) == 0:
        logger.warning(f"No coastal counties found for {region_name}")
        return []
        
    logger.info(f"Found {len(region_counties)} counties in {region_name}")
    
    # Reproject shoreline and counties to regional projection
    shoreline_projected = shoreline_gdf.to_crs(region_proj)
    counties_projected = region_counties.to_crs(region_proj)
    
    # Create reference points
    reference_points = []
    
    # Process each shoreline segment
    for i, row in shoreline_projected.iterrows():
        line = row['geometry']
        if not isinstance(line, LineString):
            logger.warning(f"Skipping non-LineString geometry: {type(line)}")
            continue
            
        # Create points along shoreline
        points = create_reference_points(line, POINT_SPACING_M)
        
        if not points:
            continue
            
        # Convert points to GeoSeries for spatial join
        points_gdf = gpd.GeoDataFrame(geometry=points, crs=region_proj)
        
        # Spatial join with counties
        joined = gpd.sjoin(points_gdf, counties_projected, how='left', predicate='within')
        
        # Process each point
        for j, point_row in joined.iterrows():
            # Skip points not within any county
            if pd.isna(point_row.get('index_right')):
                continue
                
            # Create reference point dictionary
            reference_point = {
                'geometry': point_row['geometry'],
                'county_fips': point_row['county_fips'],
                'county_name': point_row['county_name'],
                'region': region_name,
                'region_display': region_def.get('name', region_name)
            }
            
            reference_points.append(reference_point)
    
    logger.info(f"Created {len(reference_points)} reference points for {region_name}")
    return reference_points

def generate_coastal_points(region_filter=None) -> gpd.GeoDataFrame:
    """Generate reference points along the coastline for each coastal county.
    Points are spaced 5km apart using region-specific projections.
    
    Args:
        region_filter: Optional region name to filter processing (e.g., 'west_coast')
        
    Returns:
        GeoDataFrame containing reference points with county and region metadata
    """
    try:
        # Load region configuration
        regions_config = load_region_config()
        
        # Check if region filter is valid
        if region_filter and region_filter not in regions_config:
            available_regions = ", ".join(regions_config.keys())
            raise ValueError(f"Invalid region: {region_filter}. Available regions: {available_regions}")
        
        # Load coastal counties from predefined list
        logger.info("Loading coastal counties...")
        
        # First check if there's a region-specific coastal counties file
        if region_filter:
            region_counties_file = COASTAL_COUNTIES_FILE.parent / f"coastal_counties_{region_filter}.parquet"
            if region_counties_file.exists():
                logger.info(f"Found region-specific coastal counties file: {region_counties_file}")
                counties = gpd.read_parquet(region_counties_file)
            else:
                # Try to use the main coastal counties file
                if not COASTAL_COUNTIES_FILE.exists():
                    logger.warning(f"Coastal counties file not found: {COASTAL_COUNTIES_FILE}")
                    logger.info("Generating coastal counties from predefined list...")
                    # Import and run the predefined coastal counties script
                    from src.preprocessing.predefined_coastal_counties import generate_coastal_counties
                    counties = generate_coastal_counties(region_filter=region_filter)
                else:
                    # Load the main file and filter by region
                    counties = gpd.read_parquet(COASTAL_COUNTIES_FILE)
                    counties = counties[counties['region'] == region_filter]
        else:
            # No region filter, use the main coastal counties file
            if not COASTAL_COUNTIES_FILE.exists():
                logger.warning(f"Coastal counties file not found: {COASTAL_COUNTIES_FILE}")
                logger.info("Generating coastal counties from predefined list...")
                # Import and run the predefined coastal counties script
                from src.preprocessing.predefined_coastal_counties import generate_coastal_counties
                counties = generate_coastal_counties()
            else:
                counties = gpd.read_parquet(COASTAL_COUNTIES_FILE)
        
        if counties.empty:
            logger.error("No coastal counties found")
            return gpd.GeoDataFrame()
        
        logger.info(f"Loaded {len(counties)} coastal counties")
        
        region_gdfs = []
        
        # Process each region from config, or just the filtered region
        regions_to_process = [region_filter] if region_filter else regions_config.keys()
        
        for region_name in regions_to_process:
            region_def = regions_config[region_name]
            logger.info(f"\n{'='*80}")
            logger.info(f"Processing region: {region_name}")
            logger.info(f"{'='*80}")
            
            region_file = SHORELINE_DIR / f"{region_name}.parquet"
            
            if not region_file.exists():
                logger.warning(f"Shoreline file not found for {region_name}: {region_file}")
                continue
            
            # Load regional shoreline
            logger.info(f"Loading shoreline for {region_name}...")
            try:
                shoreline = gpd.read_parquet(region_file)
            except Exception as e:
                logger.error(f"Error loading shoreline for {region_name}: {str(e)}")
                continue
            
            # Process this region
            try:
                region_points = process_region(shoreline, counties, region_name, region_def)
                if not region_points:
                    logger.warning(f"No points generated for {region_name}")
                    continue
                    
                region_gdf = pd.DataFrame(region_points)
                region_gdf = gpd.GeoDataFrame(region_gdf, geometry='geometry', crs="EPSG:4326")
                
                # Log region bounds
                bounds = region_gdf.total_bounds
                logger.info("\nRegion bounds (WGS84):")
                logger.info(f"Longitude min/max: {bounds[0]:.2f}, {bounds[2]:.2f}")
                logger.info(f"Latitude min/max: {bounds[1]:.2f}, {bounds[3]:.2f}")
                
                region_gdfs.append(region_gdf)
                logger.info(f"Added {len(region_gdf)} points for {region_name}")
            except Exception as e:
                logger.error(f"Error processing region {region_name}: {str(e)}")
                continue
        
        # Combine all regions (or just the filtered region if specified)
        if not region_gdfs:
            logger.error("No points generated for any region")
            return gpd.GeoDataFrame()
            
        points_gdf = pd.concat(region_gdfs, ignore_index=True)
        points_gdf = gpd.GeoDataFrame(points_gdf, crs="EPSG:4326")
        
        # Generate output filename based on region filter
        output_file = REFERENCE_POINTS_FILE
        if region_filter:
            # Create region-specific output file
            output_dir = Path(REFERENCE_POINTS_FILE).parent
            output_name = f"reference_points_{region_filter}.parquet"
            output_file = output_dir / output_name
            
        # Create output directory if it doesn't exist
        os.makedirs(output_file.parent, exist_ok=True)
        
        logger.info(f"\nGenerated {len(points_gdf)} total reference points")
        logger.info("\nPoints by region:")
        logger.info(points_gdf['region_display'].value_counts())
        
        # Final verification of bounds
        bounds = points_gdf.total_bounds
        logger.info("\nFinal combined dataset bounds (WGS84):")
        logger.info(f"Longitude min/max: {bounds[0]:.2f}, {bounds[2]:.2f}")
        logger.info(f"Latitude min/max: {bounds[1]:.2f}, {bounds[3]:.2f}")
        
        # Save to file
        points_gdf.to_parquet(output_file, compression='snappy', index=False)
        logger.info(f"\nSaved reference points to {output_file}")
        
        return points_gdf
    except Exception as e:
        logger.error(f"Error in generate_coastal_points: {str(e)}")
        raise

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Generate coastal reference points')
    parser.add_argument('--region', type=str, help='Region to process (e.g., west_coast)')
    return parser.parse_args()

def main():
    """Generate coastal reference points."""
    # Parse command line arguments
    args = parse_args()
    
    try:
        # Generate points for all regions or filtered region
        generate_coastal_points(region_filter=args.region)
    except Exception as e:
        logger.error(f"Error generating coastal points: {str(e)}")
        raise

if __name__ == "__main__":
    main() 