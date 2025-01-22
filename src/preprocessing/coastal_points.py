"""
Generate evenly spaced reference points along the coastline for coastal counties.
Points are spaced 5km apart using an Albers Equal Area projection for accurate distances.
Processes each coastal region separately.
"""

import geopandas as gpd
from shapely.geometry import Point, LineString
from pathlib import Path
import os
from typing import List, Dict
from tqdm import tqdm

# Define the Albers Equal Area projection for accurate distances
ALBERS_CRS = (
    "+proj=aea +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 "
    "+x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"
)

# Point spacing in meters (5km)
POINT_SPACING_M = 5000

def create_reference_points(line: LineString, spacing: float = POINT_SPACING_M) -> List[Point]:
    """
    Create evenly spaced points along a line.
    
    Args:
        line: Input LineString
        spacing: Distance between points in meters
    
    Returns:
        List of Point objects
    """
    # Get line length
    length = line.length
    
    # Calculate number of points (minimum 2)
    num_points = max(2, int(length / spacing))
    
    # Generate points at regular intervals
    points = [
        line.interpolate(i / (num_points - 1), normalized=True)
        for i in range(num_points)
    ]
    
    return points

def process_region(shoreline_gdf: gpd.GeoDataFrame, counties: gpd.GeoDataFrame, region_name: str) -> List[Dict]:
    """
    Process a single region to generate reference points.
    
    Args:
        shoreline_gdf: GeoDataFrame containing the region's shoreline
        counties: GeoDataFrame containing coastal counties for this region
        region_name: Name of the region
        
    Returns:
        List of dictionaries containing point data
    """
    print(f"\nProcessing region: {region_name}")
    reference_points = []
    
    # Process each county with progress bar
    for idx, county in tqdm(counties.iterrows(), total=len(counties), desc=f"Processing {region_name} counties"):
        # Intersect county with coastline
        county_coastline = shoreline_gdf.clip(county.geometry)
        
        if len(county_coastline) == 0:
            continue
            
        # Process each coastline segment
        for _, segment in county_coastline.iterrows():
            geom = segment.geometry
            if not geom or geom.is_empty:
                continue
                
            # Handle both LineString and MultiLineString
            lines = [geom] if isinstance(geom, LineString) else list(geom.geoms)
            
            for line in lines:
                # Generate points for this segment
                points = create_reference_points(line)
                
                # Add points with county and region metadata
                for point in points:
                    reference_points.append({
                        'county_fips': county['GEOID'],
                        'county_name': county['NAME'],
                        'state_fips': county['STATEFP'],
                        'region': region_name,
                        'geometry': point
                    })
    
    return reference_points

def generate_coastal_points() -> gpd.GeoDataFrame:
    """
    Generate reference points along the coastline for each coastal county.
    Points are spaced 5km apart using Albers Equal Area projection.
    Processes each region separately.
    
    Returns:
        GeoDataFrame containing reference points with county and region metadata
    """
    # Define paths
    regional_shorelines_dir = "data/processed/regional_shorelines"
    coastal_counties_file = "data/processed/coastal_counties.parquet"
    reference_points_file = "data/processed/coastal_reference_points.parquet"
    
    # Load coastal counties
    print("Loading coastal counties...")
    counties = gpd.read_parquet(coastal_counties_file)
    
    # Get list of regional parquet files
    region_files = [f for f in os.listdir(regional_shorelines_dir) if f.endswith('.parquet')]
    print(f"\nFound {len(region_files)} regions to process")
    
    all_reference_points = []
    
    # Process each region
    for region_file in region_files:
        region_name = region_file.replace('.parquet', '')
        region_path = os.path.join(regional_shorelines_dir, region_file)
        
        # Load regional shoreline
        print(f"\nLoading {region_name} shoreline...")
        shoreline = gpd.read_parquet(region_path)
        
        # Get counties for this region
        region_counties = counties[counties['region'] == region_name]
        
        # Project to Albers Equal Area for accurate distances
        shoreline_proj = shoreline.to_crs(ALBERS_CRS)
        counties_proj = region_counties.to_crs(ALBERS_CRS)
        
        # Process this region
        region_points = process_region(shoreline_proj, counties_proj, region_name)
        all_reference_points.extend(region_points)
    
    # Create GeoDataFrame from all points
    points_gdf = gpd.GeoDataFrame(all_reference_points, crs=ALBERS_CRS)
    
    # Convert back to WGS 84 for storage
    points_gdf = points_gdf.to_crs(epsg=4326)
    
    print(f"\nGenerated {len(points_gdf)} total reference points")
    print("\nPoints by region:")
    print(points_gdf['region'].value_counts())
    
    # Save to file
    points_gdf.to_parquet(reference_points_file, compression='snappy', index=False)
    print(f"\nSaved reference points to {reference_points_file}")
    
    return points_gdf

if __name__ == "__main__":
    points = generate_coastal_points() 