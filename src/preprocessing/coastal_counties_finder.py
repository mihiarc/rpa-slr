import geopandas as gpd
import pandas as pd
import multiprocessing as mp
from functools import partial

# Define Great Lakes region code
GREAT_LAKES_REGION = 'L'  # The code used for Great Lakes regions in the shoreline data

def process_chunk(counties_chunk, ocean_shoreline_union):
    """Process a chunk of counties in parallel."""
    return counties_chunk[counties_chunk.geometry.intersects(ocean_shoreline_union)]

def find_coastal_counties(shoreline_path: str, counties_path: str, output_path: str) -> gpd.GeoDataFrame:
    """Find counties that intersect with the ocean shoreline, excluding Great Lakes.

    Args:
        shoreline_path: Path to the shoreline parquet file
        counties_path: Path to the counties parquet file
        output_path: Path where the coastal counties parquet file will be saved
    """
    print("Reading data...")
    shoreline = gpd.read_parquet(shoreline_path)
    counties = gpd.read_parquet(counties_path)
    
    # Print region statistics
    print("\nShoreline segments by region:")
    print(shoreline['REGIONS'].value_counts())
    print()
    
    # Ensure same CRS
    counties = counties.to_crs(shoreline.crs)
    
    # Separate Great Lakes and ocean shorelines
    print("Separating Great Lakes and ocean shorelines...")
    great_lakes_shoreline = shoreline[shoreline['REGIONS'] == GREAT_LAKES_REGION]
    ocean_shoreline = shoreline[shoreline['REGIONS'] != GREAT_LAKES_REGION]
    
    print(f"Found {len(great_lakes_shoreline)} Great Lakes shoreline segments and {len(ocean_shoreline)} ocean shoreline segments")
    
    # Pre-filter counties that only touch Great Lakes
    print("Pre-filtering Great Lakes counties...")
    great_lakes_union = great_lakes_shoreline.unary_union
    lakes_counties = counties[counties.geometry.intersects(great_lakes_union)]
    
    # Get counties that don't touch Great Lakes or might touch both
    non_lakes_counties = counties[~counties.index.isin(lakes_counties.index)]
    
    print(f"Filtered out {len(lakes_counties)} Great Lakes counties, processing {len(non_lakes_counties)} remaining counties...")
    
    # Create ocean shoreline union
    print("Creating ocean shoreline union...")
    ocean_shoreline_union = ocean_shoreline.unary_union
    
    # Split remaining counties into chunks for parallel processing
    n_cores = mp.cpu_count()
    chunk_size = max(1, len(non_lakes_counties) // n_cores)
    chunks = [non_lakes_counties.iloc[i:i + chunk_size] for i in range(0, len(non_lakes_counties), chunk_size)]
    
    # Process chunks in parallel
    print(f"Finding coastal counties using {n_cores} cores...")
    with mp.Pool(n_cores) as pool:
        process_func = partial(process_chunk, ocean_shoreline_union=ocean_shoreline_union)
        coastal_chunks = pool.map(process_func, chunks)
    
    # Combine results
    coastal_counties = gpd.GeoDataFrame(pd.concat(coastal_chunks, ignore_index=True))
    print(f"Found {len(coastal_counties)} coastal counties")
    
    # Save results
    coastal_counties.to_parquet(output_path)
    print(f"Saved to {output_path}")
    
    return coastal_counties

def main():
    """Find coastal counties from parquet files."""
    shoreline_parquet = "shoreline.parquet"
    county_parquet = "county.parquet"
    coastal_counties_parquet = "coastal_counties.parquet"
    
    find_coastal_counties(shoreline_parquet, county_parquet, coastal_counties_parquet)

if __name__ == "__main__":
    main() 