"""
Generate evenly spaced reference points along the coastline for coastal counties.
Points are spaced 5km apart using region-specific projections for accurate distances.
Uses county_region_mappings.yaml for county definitions and region_mappings.yaml for projections.
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

# Note: Do not call logging.basicConfig here - let the application configure logging
logger = logging.getLogger(__name__)

# Point spacing in meters (5km)
POINT_SPACING_M = 5000

# Define paths directly to avoid import issues
PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = PROJECT_ROOT / "output"
COUNTY_SHORELINE_REF_POINTS_DIR = OUTPUT_DIR / "county_shoreline_ref_points"

# Configuration files
REGION_CONFIG_FILE = CONFIG_DIR / "region_mappings.yaml"
COUNTY_REGION_CONFIG = CONFIG_DIR / "county_region_mappings.yaml"

# Input/output paths
CENSUS_COUNTY_SHAPEFILE = DATA_DIR / "input" / "shapefile_county_census" / "tl_2024_us_county.shp"
SHORELINE_INPUT_DIR = DATA_DIR / "input" / "shapefile_shoreline_noaa"
SHORELINE_DIR = PROCESSED_DIR / "regional_shorelines"
COASTAL_COUNTIES_FILE = PROCESSED_DIR / "coastal_counties.parquet"
REFERENCE_POINTS_FILE = COUNTY_SHORELINE_REF_POINTS_DIR / "coastal_reference_points.parquet"

# Shoreline region mapping
SHORELINE_REGION_MAP = {
    "west_coast": "Western",
    "north_atlantic": "North_Atlantic",
    "mid_atlantic": "North_Atlantic",  # Shared with North Atlantic
    "south_atlantic": "Southeast_Caribbean",
    "gulf_coast": "Gulf_Of_Mexico",
    "pacific_islands": "Pacific_Islands",
}

# Make sure directories exist
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(REFERENCE_POINTS_FILE.parent, exist_ok=True)
os.makedirs(SHORELINE_DIR, exist_ok=True)

def load_region_config():
    """Load region configuration from YAML."""
    logger.info(f"Loading region configuration from {REGION_CONFIG_FILE}")
    with open(REGION_CONFIG_FILE) as f:
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

def process_shoreline(region_name):
    """Process shoreline shapefiles for a region and convert to parquet.
    
    Args:
        region_name: Name of the region to process
        
    Returns:
        Path to the processed shoreline parquet file or None if failed
    """
    # Get NOAA shoreline folder name from our mapping
    if region_name not in SHORELINE_REGION_MAP:
        logger.warning(f"No shoreline mapping defined for region: {region_name}")
        return None
        
    noaa_region = SHORELINE_REGION_MAP[region_name]
    shoreline_path = SHORELINE_INPUT_DIR / noaa_region
    
    if not shoreline_path.exists():
        logger.warning(f"Shoreline folder not found: {shoreline_path}")
        return None
        
    logger.info(f"Processing shoreline data from {shoreline_path}")
    
    # Check for shapefiles
    shapefiles = list(shoreline_path.glob("*.shp"))
    if not shapefiles:
        logger.warning(f"No shapefiles found in {shoreline_path}")
        return None
        
    logger.info(f"Found {len(shapefiles)} shoreline shapefiles")
    
    # Load and combine shapefiles
    shoreline_gdfs = []
    for shapefile in shapefiles:
        try:
            logger.info(f"Loading shapefile: {shapefile.name}")
            gdf = gpd.read_file(shapefile)
            
            # Check if it's empty
            if gdf.empty:
                logger.warning(f"Empty shapefile: {shapefile.name}")
                continue
                
            # Make sure it has geometry
            if 'geometry' not in gdf.columns:
                logger.warning(f"No geometry column in shapefile: {shapefile.name}")
                continue
                
            # Keep only geometry columns to reduce size
            gdf = gdf[['geometry']]
            
            shoreline_gdfs.append(gdf)
        except Exception as e:
            logger.error(f"Error loading shapefile {shapefile.name}: {str(e)}")
            continue
    
    if not shoreline_gdfs:
        logger.error("No valid shoreline data loaded")
        return None
        
    # Combine all shapefiles
    combined_shoreline = pd.concat(shoreline_gdfs, ignore_index=True)
    shoreline_gdf = gpd.GeoDataFrame(combined_shoreline, geometry='geometry', crs="EPSG:4326")
    
    # Create output file
    output_file = SHORELINE_DIR / f"{region_name}.parquet"
    shoreline_gdf.to_parquet(output_file, compression='snappy', index=False)
    logger.info(f"Saved processed shoreline to {output_file}")
    
    return output_file

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
        
        # Check if region filter is valid
        if region_filter and region_filter not in region_config:
            available_regions = ", ".join(region_config.keys())
            raise ValueError(f"Invalid region: {region_filter}. Available regions: {available_regions}")
        
        # Filter regions if specified
        if region_filter:
            regions_to_process = {region_filter: region_config[region_filter]}
        else:
            regions_to_process = region_config
        
        # Load Census county geometries
        counties_gdf = load_census_counties()
        
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
        
        # Remove any duplicates
        coastal_counties_gdf = coastal_counties_gdf.drop_duplicates(subset=["GEOID"])
        
        logger.info(f"Total unique coastal counties: {len(coastal_counties_gdf)}")
        
        # Generate output filename
        output_file = COASTAL_COUNTIES_FILE
        if region_filter:
            output_dir = COASTAL_COUNTIES_FILE.parent
            output_name = f"coastal_counties_{region_filter}.parquet"
            output_file = output_dir / output_name
        
        # Create output directory if it doesn't exist
        os.makedirs(output_file.parent, exist_ok=True)
        
        # Save to file
        coastal_counties_gdf.to_parquet(output_file, compression="snappy", index=False)
        logger.info(f"Saved coastal counties to {output_file}")
        
        return coastal_counties_gdf
    
    except Exception as e:
        logger.error(f"Error generating coastal counties: {str(e)}")
        raise

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
        
        # Load coastal counties from file or generate them
        logger.info("Loading coastal counties...")
        
        # First check if there's a region-specific coastal counties file
        if region_filter:
            region_counties_file = COASTAL_COUNTIES_FILE.parent / f"coastal_counties_{region_filter}.parquet"
            if region_counties_file.exists():
                logger.info(f"Found region-specific coastal counties file: {region_counties_file}")
                counties = gpd.read_parquet(region_counties_file)
            else:
                # Try to use the main coastal counties file or generate new ones
                if not COASTAL_COUNTIES_FILE.exists():
                    logger.info("Generating coastal counties from predefined list...")
                    counties = generate_coastal_counties(region_filter=region_filter)
                else:
                    # Load the main file and filter by region
                    counties = gpd.read_parquet(COASTAL_COUNTIES_FILE)
                    counties = counties[counties['region'] == region_filter]
        else:
            # No region filter, use the main coastal counties file or generate new ones
            if not COASTAL_COUNTIES_FILE.exists():
                logger.info("Generating coastal counties from predefined list...")
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
            
            # If shoreline file doesn't exist, try to process it from shapefiles
            if not region_file.exists():
                logger.info(f"Shoreline file not found: {region_file}")
                logger.info(f"Attempting to process shoreline from source shapefiles...")
                
                region_file = process_shoreline(region_name)
                if region_file is None or not region_file.exists():
                    logger.warning(f"Failed to process shoreline for {region_name}")
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