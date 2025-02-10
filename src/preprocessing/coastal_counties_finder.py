import geopandas as gpd
import pandas as pd
import multiprocessing as mp
from functools import partial
import os

# Define Great Lakes region code
GREAT_LAKES_REGION = 'L'  # The code used for Great Lakes regions in the shoreline data

# Define projections for different regions
ALASKA_ALBERS = (
    "+proj=aea +lat_1=55 +lat_2=65 +lat_0=50 +lon_0=-154 "
    "+x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"
)

WEST_COAST_ALBERS = (
    "+proj=aea +lat_1=34 +lat_2=45.5 +lat_0=40 +lon_0=-120 "
    "+x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"
)

def process_chunk(counties_chunk, ocean_shoreline_union):
    """Process a chunk of counties in parallel."""
    return counties_chunk[counties_chunk.geometry.intersects(ocean_shoreline_union)]

def find_coastal_counties_for_region(shoreline_gdf: gpd.GeoDataFrame, counties: gpd.GeoDataFrame, region_name: str) -> gpd.GeoDataFrame:
    """Find counties that intersect with a specific region's shoreline.
    
    Args:
        shoreline_gdf: GeoDataFrame containing the region's shoreline
        counties: GeoDataFrame containing all counties
        region_name: Name of the region being processed
    """
    print(f"\nProcessing region: {region_name}")
    
    # Use appropriate projection based on region
    if region_name == 'Alaska':
        print("Using Alaska Albers Equal Area projection...")
        shoreline_gdf = shoreline_gdf.to_crs(ALASKA_ALBERS)
        counties = counties.to_crs(ALASKA_ALBERS)
    elif region_name == 'West_Coast':
        print("Using West Coast Albers Equal Area projection...")
        shoreline_gdf = shoreline_gdf.to_crs(WEST_COAST_ALBERS)
        counties = counties.to_crs(WEST_COAST_ALBERS)
    else:
        # For other regions, ensure same CRS
        counties = counties.to_crs(shoreline_gdf.crs)
    
    # Create shoreline union
    print("Creating shoreline union...")
    shoreline_union = shoreline_gdf.unary_union
    
    # Split counties into chunks for parallel processing
    n_cores = mp.cpu_count()
    chunk_size = max(1, len(counties) // n_cores)
    chunks = [counties.iloc[i:i + chunk_size] for i in range(0, len(counties), chunk_size)]
    
    # Process chunks in parallel
    print(f"Finding coastal counties using {n_cores} cores...")
    with mp.Pool(n_cores) as pool:
        process_func = partial(process_chunk, ocean_shoreline_union=shoreline_union)
        coastal_chunks = pool.map(process_func, chunks)
    
    # Combine results
    coastal_counties = gpd.GeoDataFrame(pd.concat(coastal_chunks, ignore_index=True))
    print(f"Found {len(coastal_counties)} coastal counties in {region_name}")
    
    # Add region information (convert West_Coast to West Coast for display)
    region_display = region_name.replace('_', ' ')
    coastal_counties['region'] = region_display
    
    # Convert back to original CRS if we used a special projection
    if region_name in ['Alaska', 'West_Coast']:
        coastal_counties = coastal_counties.to_crs(shoreline_gdf.crs)
    
    return coastal_counties

def find_coastal_counties(regional_shorelines_dir: str, counties_path: str, output_path: str) -> gpd.GeoDataFrame:
    """Find counties that intersect with regional shorelines.

    Args:
        regional_shorelines_dir: Directory containing regional shoreline parquet files
        counties_path: Path to the counties parquet file
        output_path: Path where the coastal counties parquet file will be saved
    """
    print("Reading county data...")
    counties = gpd.read_parquet(counties_path)
    
    # Get list of regional parquet files
    region_files = [f for f in os.listdir(regional_shorelines_dir) if f.endswith('.parquet')]
    print(f"\nFound {len(region_files)} regions to process")
    
    all_coastal_counties = []
    
    # Process each region
    for region_file in region_files:
        region_name = region_file.replace('.parquet', '')
        region_path = os.path.join(regional_shorelines_dir, region_file)
        
        print(f"\nReading {region_name} shoreline data...")
        shoreline = gpd.read_parquet(region_path)
        
        # Find coastal counties for this region
        regional_coastal_counties = find_coastal_counties_for_region(shoreline, counties, region_name)
        all_coastal_counties.append(regional_coastal_counties)
    
    # Combine results from all regions
    coastal_counties = gpd.GeoDataFrame(pd.concat(all_coastal_counties, ignore_index=True))
    
    # Remove duplicates (counties that touch multiple regions)
    coastal_counties = coastal_counties.drop_duplicates(subset=['GEOID'])
    
    print(f"\nTotal unique coastal counties found: {len(coastal_counties)}")
    print("Counties by region:")
    print(coastal_counties['region'].value_counts())
    
    # Save results
    coastal_counties.to_parquet(output_path)
    print(f"\nSaved to {output_path}")
    
    return coastal_counties

def main():
    """Find coastal counties from regional shoreline parquet files."""
    regional_shorelines_dir = "data/processed/regional_shorelines"
    county_parquet = "data/processed/county.parquet"
    coastal_counties_parquet = "data/processed/coastal_counties.parquet"
    
    find_coastal_counties(regional_shorelines_dir, county_parquet, coastal_counties_parquet)

if __name__ == "__main__":
    main() 