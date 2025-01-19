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

def convert_shapefiles():
    """Convert both shoreline and county shapefiles to parquet format."""
    # Convert shoreline shapefile
    shoreline_shp = "shapefile_shoreline/us_medium_shoreline.shp"
    shoreline_parquet = "shoreline.parquet"
    
    # Convert county shapefile
    county_shp = "shapefile_county/tl_2024_us_county.shp"
    county_parquet = "county.parquet"
    
    # Perform conversions if parquet files don't exist
    if not os.path.exists(shoreline_parquet):
        convert_shapefile_to_parquet(shoreline_shp, shoreline_parquet)
    if not os.path.exists(county_parquet):
        convert_shapefile_to_parquet(county_shp, county_parquet)

if __name__ == "__main__":
    convert_shapefiles() 