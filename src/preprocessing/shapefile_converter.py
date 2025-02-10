"""
Script to convert shapefiles to parquet format.
"""

import geopandas as gpd
import os
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def convert_shapefile_to_parquet(shapefile_path: Path, output_path: Path):
    """Convert a shapefile to parquet format.
    
    Args:
        shapefile_path: Path to the input shapefile
        output_path: Path where the parquet file will be saved
    """
    # Read the shapefile
    logger.info(f"Reading shapefile: {shapefile_path}")
    gdf = gpd.read_file(shapefile_path)
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert to parquet
    logger.info(f"Converting to parquet: {output_path}")
    gdf.to_parquet(output_path)
    logger.info(f"Conversion complete: {output_path}")

def convert_regional_shorefiles(input_dir: Path, output_dir: Path):
    """Convert regional shoreline shapefiles to parquet format.
    
    Args:
        input_dir: Directory containing regional shoreline directories
        output_dir: Directory where parquet files will be saved
    """
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get list of regional directories
    region_dirs = [d for d in input_dir.iterdir() 
                  if d.is_dir() and not d.name.startswith('.')]
    
    logger.info(f"Found {len(region_dirs)} regions to process")
    
    # Process each region
    for region_path in region_dirs:
        region = region_path.name
        # Find the .shp file in the region directory
        shp_files = list(region_path.glob("*.shp"))
        
        if not shp_files:
            logger.warning(f"No shapefile found in {region}")
            continue
            
        shapefile_path = shp_files[0]
        output_path = output_dir / f"{region}.parquet"
        
        if not output_path.exists():
            logger.info(f"\nProcessing region: {region}")
            convert_shapefile_to_parquet(shapefile_path, output_path)
        else:
            logger.info(f"\nSkipping {region} - parquet file already exists")

def convert_shapefiles():
    """Convert both shoreline and county shapefiles to parquet format."""
    # Set up paths
    data_dir = Path("data")
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"
    
    # Define paths
    shoreline_shp = raw_dir / "shapefile_shoreline" / "us_medium_shoreline.shp"
    shoreline_parquet = processed_dir / "shoreline.parquet"
    
    county_shp = raw_dir / "shapefile_county" / "tl_2024_us_county.shp"
    county_parquet = processed_dir / "county.parquet"
    
    regional_shoreline_dir = raw_dir / "shapefile_shoreline"
    regional_output_dir = processed_dir / "regional_shorelines"
    
    # Create processed directory if it doesn't exist
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Perform conversions if parquet files don't exist
    if not shoreline_shp.exists():
        logger.warning(f"Main shoreline shapefile not found: {shoreline_shp}")
    elif not shoreline_parquet.exists():
        convert_shapefile_to_parquet(shoreline_shp, shoreline_parquet)
    
    if not county_shp.exists():
        logger.warning(f"County shapefile not found: {county_shp}")
    elif not county_parquet.exists():
        convert_shapefile_to_parquet(county_shp, county_parquet)
    
    # Convert regional shorefiles
    convert_regional_shorefiles(regional_shoreline_dir, regional_output_dir)

def main():
    """Run the shapefile conversion process."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        convert_shapefiles()
        logger.info("All shapefile conversions complete")
    except Exception as e:
        logger.error(f"Error converting shapefiles: {str(e)}")
        raise

if __name__ == "__main__":
    main() 