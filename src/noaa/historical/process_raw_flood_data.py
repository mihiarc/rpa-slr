"""
Process raw JSON flood data files into regional parquet files.
Focuses on minor flood events (minCount) from NOAA data.
"""

import json
import pandas as pd
import logging
from pathlib import Path
import yaml
from typing import Dict, List, Optional, Set
from tqdm import tqdm

from src.config import CONFIG_DIR, OUTPUT_DIR

logger = logging.getLogger(__name__)

def load_region_config() -> Dict:
    """Load region configuration from YAML."""
    with open(CONFIG_DIR / "region_mappings.yaml") as f:
        config = yaml.safe_load(f)
    return config['regions']

def get_region_stations(region: str) -> Set[str]:
    """Get set of station IDs for a region.
    
    Args:
        region: Name of the region
        
    Returns:
        Set of station IDs
    """
    # Load imputation structure to get station to region mapping
    imputation_file = OUTPUT_DIR / "imputation" / "imputation_structure_all_regions.parquet"
    if not imputation_file.exists():
        logger.warning(f"Imputation structure not found: {imputation_file}")
        return set()
    
    df = pd.read_parquet(imputation_file)
    return set(df[df['region'] == region]['station_id'].unique())

def process_station_json(file_path: Path) -> pd.DataFrame:
    """Process a single station's JSON file.
    
    Args:
        file_path: Path to JSON file
        
    Returns:
        DataFrame with processed flood data
    """
    # Load JSON data
    with open(file_path) as f:
        data = json.load(f)
    
    # Convert to DataFrame
    records = []
    station_id = file_path.stem  # filename is station ID
    
    for year_data in data:
        record = {
            'station_id': station_id,
            'year': year_data['year'],
            'flood_days': year_data.get('minCount', 0) or 0,  # minor flood days only
            'missing_days': year_data.get('nanCount', 0) or 0
        }
        records.append(record)
    
    return pd.DataFrame(records)

def process_region(
    region: str,
    region_def: Dict,
    raw_data_dir: Path,
    output_dir: Path
) -> Optional[Path]:
    """Process flood data for a specific region.
    
    Args:
        region: Name of the region
        region_def: Region definition from config
        raw_data_dir: Directory containing raw JSON files
        output_dir: Directory to save processed data
        
    Returns:
        Path to output file if successful, None otherwise
    """
    logger.info(f"\nProcessing region: {region}")
    
    # Get stations for this region
    try:
        region_stations = get_region_stations(region)
        logger.info(f"Found {len(region_stations)} stations for {region}")
        
        if not region_stations:
            # Create empty DataFrame with correct structure
            empty_df = pd.DataFrame(columns=[
                'station_id', 'year', 'flood_days', 'missing_days', 'region'
            ])
            output_path = output_dir / f"historical_htf_{region}.parquet"
            empty_df.to_parquet(output_path)
            logger.warning(f"No stations found for {region}, created empty file")
            return output_path
            
    except Exception as e:
        logger.error(f"Error getting stations for {region}: {str(e)}")
        return None
    
    # Get list of JSON files for this region's stations
    json_files = [
        f for f in raw_data_dir.glob("*.json")
        if f.stem in region_stations
    ]
    
    if not json_files:
        # Create empty DataFrame with correct structure
        empty_df = pd.DataFrame(columns=[
            'station_id', 'year', 'flood_days', 'missing_days', 'region'
        ])
        output_path = output_dir / f"historical_htf_{region}.parquet"
        empty_df.to_parquet(output_path)
        logger.warning(f"No JSON files found for {region} stations, created empty file")
        return output_path
    
    # Process each station
    dfs = []
    for file_path in tqdm(json_files, desc=f"Processing {region} stations"):
        try:
            df = process_station_json(file_path)
            if not df.empty:
                df['region'] = region
                dfs.append(df)
        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {str(e)}")
            continue
    
    if not dfs:
        # Create empty DataFrame with correct structure
        empty_df = pd.DataFrame(columns=[
            'station_id', 'year', 'flood_days', 'missing_days', 'region'
        ])
        output_path = output_dir / f"historical_htf_{region}.parquet"
        empty_df.to_parquet(output_path)
        logger.warning(f"No valid data processed for {region}, created empty file")
        return output_path
    
    # Combine all stations
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Log some statistics
    logger.info(f"\nProcessed data summary for {region}:")
    logger.info(f"Total records: {len(combined_df)}")
    logger.info(f"Date range: {combined_df['year'].min()} to {combined_df['year'].max()}")
    logger.info(f"Stations: {len(combined_df['station_id'].unique())}")
    logger.info(f"\nMean flood days per year: {combined_df['flood_days'].mean():.2f}")
    
    # Additional statistics about data completeness
    complete_data = combined_df[combined_df['missing_days'] < 365]
    logger.info("\nData completeness:")
    logger.info(f"Records with complete data: {len(complete_data)} ({len(complete_data)/len(combined_df)*100:.1f}%)")
    logger.info(f"Mean flood days (complete data only): {complete_data['flood_days'].mean():.2f}")
    
    # Save to parquet
    output_path = output_dir / f"historical_htf_{region}.parquet"
    combined_df.to_parquet(output_path)
    logger.info(f"\nSaved {len(combined_df)} records to {output_path}")
    
    # Display first few rows and summary statistics
    logger.info("\nFirst few rows of processed data:")
    logger.info(combined_df.head().to_string())
    logger.info("\nSummary statistics:")
    logger.info(combined_df.describe().to_string())
    
    return output_path

def main():
    """Process raw flood data files."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Load region configuration
        regions_config = load_region_config()
        
        # Set up directories
        raw_data_dir = OUTPUT_DIR / "noaa" / "historical"
        output_dir = OUTPUT_DIR / "historical"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Process each region
        for region, region_def in regions_config.items():
            try:
                output_path = process_region(
                    region=region,
                    region_def=region_def,
                    raw_data_dir=raw_data_dir,
                    output_dir=output_dir
                )
                
                if output_path:
                    if output_path.stat().st_size > 0:
                        logger.info(f"Successfully processed {region}")
                    else:
                        logger.warning(f"No data available for {region}, empty file created")
                else:
                    logger.warning(f"Failed to process {region}")
                    
            except Exception as e:
                logger.error(f"Error processing region {region}: {str(e)}")
                continue
            
    except Exception as e:
        logger.error(f"Error processing flood data: {str(e)}")
        raise

if __name__ == "__main__":
    main() 