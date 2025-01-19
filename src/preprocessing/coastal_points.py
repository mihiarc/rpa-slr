"""
Generate evenly spaced reference points along the coastline for coastal counties.
Points are spaced 5km apart using an Albers Equal Area projection for accurate distances.
Only includes ocean coastlines (excludes Great Lakes).
"""

import geopandas as gpd
from shapely.geometry import Point, LineString
from pathlib import Path
from typing import List, Dict
from tqdm import tqdm

from config import (
    COASTAL_COUNTIES_FILE,
    SHORELINE_FILE,
    REFERENCE_POINTS_FILE,
    POINT_SPACING_M
)

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

def generate_coastal_points() -> gpd.GeoDataFrame:
    """
    Generate reference points along the coastline for each coastal county.
    Points are spaced 5km apart using Albers Equal Area projection.
    Uses pre-filtered coastal counties that exclude Great Lakes.
    
    Returns:
        GeoDataFrame containing reference points with county metadata
    """
    # Load input data
    print("Loading input files...")
    counties = gpd.read_parquet(COASTAL_COUNTIES_FILE)
    coastline = gpd.read_parquet(SHORELINE_FILE)
    
    # Ensure both datasets are in WGS 84
    counties = counties.to_crs(epsg=4326)
    coastline = coastline.to_crs(epsg=4326)
    
    # Project to Albers Equal Area for accurate distances
    counties_proj = counties.to_crs(ALBERS_CRS)
    coastline_proj = coastline.to_crs(ALBERS_CRS)
    
    print(f"Processing {len(counties)} coastal counties...")
    reference_points = []
    
    # Process each county with progress bar
    for idx, county in tqdm(counties_proj.iterrows(), total=len(counties_proj), desc="Processing counties"):
        # Intersect county with coastline
        county_coastline = coastline_proj.clip(county.geometry)
        
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
                
                # Add points with county metadata
                for point in points:
                    reference_points.append({
                        'county_fips': county['GEOID'],
                        'county_name': county['NAME'],
                        'state_fips': county['STATEFP'],
                        'geometry': point
                    })
    
    # Create GeoDataFrame from points
    points_gdf = gpd.GeoDataFrame(reference_points, crs=ALBERS_CRS)
    
    # Convert back to WGS 84 for storage
    points_gdf = points_gdf.to_crs(epsg=4326)
    
    print(f"\nGenerated {len(points_gdf)} reference points")
    
    # Save to file
    points_gdf.to_parquet(REFERENCE_POINTS_FILE, compression='snappy', index=False)
    print(f"Saved reference points to {REFERENCE_POINTS_FILE}")
    
    return points_gdf

if __name__ == "__main__":
    points = generate_coastal_points() 