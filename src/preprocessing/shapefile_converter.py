"""
Script to convert shapefiles to parquet format with proper coordinate system handling.
Uses configuration from region_mappings.yaml for regional processing.
"""

import geopandas as gpd
import yaml
from pathlib import Path
import logging
from src.config import (
    CONFIG_DIR,
    RAW_DATA_DIR,
    PROCESSED_DIR,
    COUNTY_FILE,
    SHORELINE_DIR
)

logger = logging.getLogger(__name__)

def load_region_config():
    """Load region configuration from YAML."""
    with open(CONFIG_DIR / "region_mappings.yaml") as f:
        config = yaml.safe_load(f)
    return config['regions']

def convert_shapefile_to_parquet(shapefile_path: Path, output_path: Path, target_crs="EPSG:4326"):
    """Convert a shapefile to parquet format with proper coordinate system.
    
    Args:
        shapefile_path: Path to the input shapefile
        output_path: Path where the parquet file will be saved
        target_crs: Target coordinate reference system (default: EPSG:4326 WGS84)
    """
    # Read the shapefile
    logger.info(f"Reading shapefile: {shapefile_path}")
    gdf = gpd.read_file(shapefile_path)
    
    # Check and transform CRS if needed
    if gdf.crs is None:
        logger.warning(f"No CRS found in {shapefile_path}, assuming {target_crs}")
        gdf.set_crs(target_crs, inplace=True)
    elif gdf.crs != target_crs:
        logger.info(f"Converting from {gdf.crs} to {target_crs}")
        gdf = gdf.to_crs(target_crs)
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert to parquet
    logger.info(f"Converting to parquet: {output_path}")
    gdf.to_parquet(output_path)
    logger.info(f"Conversion complete: {output_path}")
    return gdf

def process_shared_region(shapefile_path: Path, output_dir: Path, regions_config: dict, sub_regions: list):
    """Process a shapefile that contains multiple regions.
    
    Args:
        shapefile_path: Path to the input shapefile
        output_dir: Directory where parquet files will be saved
        regions_config: Region configuration dictionary
        sub_regions: List of sub-regions to extract from this shapefile
    """
    # Read the shapefile once
    gdf = gpd.read_file(shapefile_path)
    
    # Process each sub-region
    for sub_region in sub_regions:
        if sub_region not in regions_config:
            logger.warning(f"No configuration found for sub-region: {sub_region}")
            continue
            
        output_path = output_dir / f"{sub_region}.parquet"
        if output_path.exists():
            logger.info(f"\nSkipping {sub_region} - parquet file already exists")
            continue
            
        logger.info(f"\nProcessing sub-region: {sub_region}")
        bounds = regions_config[sub_region]['bounds']
        
        # Filter by bounds
        mask = (
            (gdf.geometry.bounds.minx >= bounds['min_lon']) &
            (gdf.geometry.bounds.maxx <= bounds['max_lon']) &
            (gdf.geometry.bounds.miny >= bounds['min_lat']) &
            (gdf.geometry.bounds.maxy <= bounds['max_lat'])
        )
        sub_gdf = gdf[mask].copy()
        
        if len(sub_gdf) == 0:
            logger.warning(f"No features found within bounds for {sub_region}")
            continue
            
        # Convert to target CRS if specified
        target_crs = regions_config[sub_region].get('crs', 'EPSG:4326')
        if sub_gdf.crs != target_crs:
            logger.info(f"Converting {sub_region} from {sub_gdf.crs} to {target_crs}")
            sub_gdf = sub_gdf.to_crs(target_crs)
        
        sub_gdf.to_parquet(output_path)
        logger.info(f"Saved {sub_region} to {output_path}")

def convert_regional_shorefiles(input_dir: Path, output_dir: Path):
    """Convert regional shoreline shapefiles to parquet format.
    
    Args:
        input_dir: Directory containing regional shoreline directories
        output_dir: Directory where parquet files will be saved
    """
    # Load region configuration
    regions_config = load_region_config()
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Define shared directory mappings
    shared_regions = {
        'Southeast_Caribbean': ['south_atlantic', 'puerto_rico', 'virgin_islands'],
        'Western': ['west_coast', 'alaska'],
        'North_Atlantic': ['north_atlantic', 'mid_atlantic'],
        'Pacific_Islands': ['pacific_islands', 'hawaii']
    }
    
    # Process shared region directories
    for source_dir, sub_regions in shared_regions.items():
        region_path = input_dir / source_dir
        if not region_path.exists():
            logger.warning(f"Directory not found: {region_path}")
            continue
            
        # Find the shapefile
        shp_files = list(region_path.glob("*.shp"))
        if not shp_files:
            logger.warning(f"No shapefile found in {source_dir}")
            continue
            
        # Process all sub-regions from this source
        process_shared_region(shp_files[0], output_dir, regions_config, sub_regions)
    
    # Process Gulf Coast (single region)
    gulf_path = input_dir / "Gulf_Of_Mexico"
    if gulf_path.exists():
        shp_files = list(gulf_path.glob("*.shp"))
        if shp_files:
            output_path = output_dir / "gulf_coast.parquet"
            if not output_path.exists():
                logger.info("\nProcessing region: gulf_coast")
                convert_shapefile_to_parquet(shp_files[0], output_path)
            else:
                logger.info("\nSkipping gulf_coast - parquet file already exists")

def convert_shapefiles():
    """Convert shapefiles to parquet format."""
    # Convert county shapefile
    logger.info("\nConverting county shapefile...")
    county_gdf = convert_county_shapefile()
    county_gdf.to_parquet(COUNTY_FILE)
    logger.info(f"Saved county data to {COUNTY_FILE}")
    
    # Convert shoreline shapefile
    logger.info("\nConverting shoreline shapefile...")
    shoreline_gdf = convert_shoreline_shapefile()
    shoreline_file = PROCESSED_DIR / "shoreline.parquet"
    shoreline_gdf.to_parquet(shoreline_file)
    logger.info(f"Saved shoreline data to {shoreline_file}")
    
    # Ensure regional shorelines directory exists
    SHORELINE_DIR.mkdir(parents=True, exist_ok=True)

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