"""
Script to split shoreline shapefiles into specific regional groups.
"""

import geopandas as gpd
from pathlib import Path
import logging
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_feature_centroid(geometry):
    """Get the centroid coordinates of a geometry."""
    try:
        coords = np.array([(x, y) for x, y in geometry.coords])
        return np.mean(coords[:, 0]), np.mean(coords[:, 1])
    except:
        # For MultiLineString geometries
        all_coords = []
        for line in geometry.geoms:
            coords = np.array([(x, y) for x, y in line.coords])
            all_coords.append(coords)
        coords = np.vstack(all_coords)
        return np.mean(coords[:, 0]), np.mean(coords[:, 1])

def split_southeast_caribbean(gdf):
    """Split Southeast Caribbean into South Atlantic states, Puerto Rico, and US Virgin Islands."""
    
    # Create mask for Florida's Atlantic coast (east of -81.5 degrees)
    florida_mask = gdf['FIPS_ALPHA'] == 'FL'
    florida_df = gdf[florida_mask].copy()
    
    # Split Florida features based on longitude
    florida_atlantic = []
    florida_other = []
    
    for idx, row in florida_df.iterrows():
        lon, _ = get_feature_centroid(row.geometry)
        if lon > -81.5:  # East coast of Florida
            florida_atlantic.append(idx)
        else:
            florida_other.append(idx)
    
    florida_atlantic_df = florida_df.loc[florida_atlantic]
    logger.info(f"Florida Atlantic coast features: {len(florida_atlantic_df)}")
    
    # South Atlantic states (GA, SC, NC) plus Florida Atlantic coast
    south_atlantic_base = gdf[gdf['FIPS_ALPHA'].isin(['GA', 'SC', 'NC'])]
    south_atlantic = pd.concat([south_atlantic_base, florida_atlantic_df])
    logger.info(f"South Atlantic features (including FL Atlantic): {len(south_atlantic)}")
    
    # Puerto Rico
    puerto_rico = gdf[gdf['FIPS_ALPHA'] == 'PR']
    logger.info(f"Puerto Rico features: {len(puerto_rico)}")
    
    # US Virgin Islands
    virgin_islands = gdf[gdf['FIPS_ALPHA'] == 'VI']
    logger.info(f"US Virgin Islands features: {len(virgin_islands)}")
    
    return {
        'south_atlantic': south_atlantic,
        'puerto_rico': puerto_rico,
        'virgin_islands': virgin_islands
    }

def split_pacific_islands(gdf):
    """Split Pacific Islands into Hawaiian Islands and Other Pacific Islands."""
    
    # Hawaiian Islands
    hawaii = gdf[gdf['FIPS_ALPHA'] == 'HI']
    logger.info(f"Hawaii features: {len(hawaii)}")
    
    # Other Pacific Islands (GU, MP, AS, UM)
    other_pacific = gdf[gdf['FIPS_ALPHA'].isin(['GU', 'MP', 'AS', 'UM'])]
    logger.info(f"Other Pacific features: {len(other_pacific)}")
    
    return {
        'hawaii': hawaii,
        'other_pacific': other_pacific
    }

def main():
    # Set up paths
    input_dir = Path('data/raw/shapefile_shoreline')
    output_dir = Path('data/processed/regional_shorelines')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Process Southeast Caribbean
    logger.info("Processing Southeast Caribbean shapefile...")
    se_caribbean_file = input_dir / 'Southeast_Caribbean' / 'Southeast_Caribbean.shp'
    se_caribbean = gpd.read_file(se_caribbean_file)
    se_splits = split_southeast_caribbean(se_caribbean)
    
    # Process Pacific Islands
    logger.info("Processing Pacific Islands shapefile...")
    pacific_file = input_dir / 'Pacific_Islands' / 'Pacific_Islands.shp'
    pacific = gpd.read_file(pacific_file)
    pacific_splits = split_pacific_islands(pacific)
    
    # Save all splits to parquet files
    splits = {
        'south_atlantic_shoreline': se_splits['south_atlantic'],
        'puerto_rico_shoreline': se_splits['puerto_rico'],
        'virgin_islands_shoreline': se_splits['virgin_islands'],
        'hawaii_shoreline': pacific_splits['hawaii'],
        'other_pacific_shoreline': pacific_splits['other_pacific']
    }
    
    for name, data in splits.items():
        output_file = output_dir / f"{name}.parquet"
        data.to_parquet(output_file)
        logger.info(f"Saved {name} with {len(data)} features to {output_file}")
        
        # Log FIPS distribution for verification
        fips_counts = data['FIPS_ALPHA'].value_counts()
        logger.info(f"FIPS distribution for {name}:")
        for fips, count in fips_counts.items():
            logger.info(f"  {fips}: {count} features")

if __name__ == '__main__':
    main() 