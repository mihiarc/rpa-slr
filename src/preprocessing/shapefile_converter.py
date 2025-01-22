import geopandas as gpd
import os

def convert_shapefile_to_parquet(shapefile_path, output_path):
    """Convert a shapefile to parquet format.
    
    Args:
        shapefile_path (str): Path to the input shapefile
        output_path (str): Path where the parquet file will be saved
    """
    # Read the shapefile
    print(f"Reading shapefile: {shapefile_path}")
    gdf = gpd.read_file(shapefile_path)
    
    # Convert to parquet
    print(f"Converting to parquet: {output_path}")
    gdf.to_parquet(output_path)
    print(f"Conversion complete: {output_path}")

def convert_regional_shorefiles(input_dir, output_dir):
    """Convert regional shoreline shapefiles to parquet format.
    
    Args:
        input_dir (str): Directory containing regional shoreline directories
        output_dir (str): Directory where parquet files will be saved
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get list of regional directories
    region_dirs = [d for d in os.listdir(input_dir) 
                  if os.path.isdir(os.path.join(input_dir, d)) and not d.startswith('.')]
    
    print(f"Found {len(region_dirs)} regions to process")
    
    # Process each region
    for region in region_dirs:
        region_path = os.path.join(input_dir, region)
        # Find the .shp file in the region directory
        shp_files = [f for f in os.listdir(region_path) if f.endswith('.shp')]
        
        if not shp_files:
            print(f"No shapefile found in {region}")
            continue
            
        shapefile_path = os.path.join(region_path, shp_files[0])
        output_path = os.path.join(output_dir, f"{region}.parquet")
        
        if not os.path.exists(output_path):
            print(f"\nProcessing region: {region}")
            convert_shapefile_to_parquet(shapefile_path, output_path)
        else:
            print(f"\nSkipping {region} - parquet file already exists")

def convert_shapefiles():
    """Convert both shoreline and county shapefiles to parquet format."""
    # Convert main shoreline shapefile
    shoreline_shp = "data/raw/shapefile_shoreline/us_medium_shoreline.shp"
    shoreline_parquet = "data/processed/shoreline.parquet"
    
    # Convert county shapefile
    county_shp = "data/raw/shapefile_county/tl_2024_us_county.shp"
    county_parquet = "data/processed/county.parquet"
    
    # Convert regional shoreline files
    regional_shoreline_dir = "data/raw/shapefile_shoreline"
    regional_output_dir = "data/processed/regional_shorelines"
    
    # Create processed directory if it doesn't exist
    os.makedirs("data/processed", exist_ok=True)
    
    # Perform conversions if parquet files don't exist
    if not os.path.exists(shoreline_parquet):
        convert_shapefile_to_parquet(shoreline_shp, shoreline_parquet)
    if not os.path.exists(county_parquet):
        convert_shapefile_to_parquet(county_shp, county_parquet)
        
    # Convert regional shorefiles
    convert_regional_shorefiles(regional_shoreline_dir, regional_output_dir)

if __name__ == "__main__":
    convert_shapefiles() 