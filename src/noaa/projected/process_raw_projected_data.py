"""
Process raw projected HTF data files into regional parquet files.
Handles decadal projections with multiple sea level rise scenarios.
"""

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

def process_region_projections(
    region: str,
    region_def: Dict,
    raw_data_dir: Path,
    output_dir: Path
) -> Optional[Path]:
    """Process projected flood data for a specific region.
    
    Args:
        region: Name of the region
        region_def: Region definition from config
        raw_data_dir: Directory containing raw parquet files
        output_dir: Directory to save processed data
        
    Returns:
        Path to output file if successful, None otherwise
    """
    logger.info(f"\nProcessing projected data for region: {region}")
    
    # Get stations for this region
    try:
        region_stations = get_region_stations(region)
        logger.info(f"Found {len(region_stations)} stations for {region}")
        
        if not region_stations:
            # Create empty DataFrame with correct structure
            empty_df = pd.DataFrame(columns=[
                'station_id', 'station_name', 'decade', 'region',
                'low_scenario', 'intermediate_low_scenario', 
                'intermediate_scenario', 'intermediate_high_scenario',
                'high_scenario', 'scenario_range', 'median_scenario'
            ])
            output_path = output_dir / f"processed_projected_htf_{region}.parquet"
            empty_df.to_parquet(output_path)
            logger.warning(f"No stations found for {region}, created empty file")
            return output_path
            
    except Exception as e:
        logger.error(f"Error getting stations for {region}: {str(e)}")
        return None
    
    # Read the raw projected data file for this region
    input_file = raw_data_dir / f"projected_htf_{region}.parquet"
    if not input_file.exists():
        logger.warning(f"No projected data file found for {region}")
        return None
        
    try:
        df = pd.read_parquet(input_file)
    except Exception as e:
        logger.error(f"Error reading projected data for {region}: {str(e)}")
        return None
        
    # Filter to only include stations in our region mapping
    df = df[df['station'].isin(region_stations)].copy()
    
    if df.empty:
        logger.warning(f"No valid stations found in projected data for {region}")
        return None
        
    # Add region column
    df['region'] = region
    
    # Log some statistics
    logger.info(f"\nProcessed projected data summary for {region}:")
    logger.info(f"Total records: {len(df)}")
    logger.info(f"Decade range: {df['decade'].min()} to {df['decade'].max()}")
    logger.info(f"Stations: {len(df['station'].unique())}")
    
    # Calculate scenario statistics
    logger.info("\nScenario Statistics:")
    for scenario in ['low_scenario', 'intermediate_low_scenario', 
                    'intermediate_scenario', 'intermediate_high_scenario',
                    'high_scenario']:
        mean_days = df[scenario].mean()
        logger.info(f"{scenario}: mean {mean_days:.1f} days/year")
    
    # Save processed data
    output_path = output_dir / f"processed_projected_htf_{region}.parquet"
    df.to_parquet(output_path)
    logger.info(f"\nSaved {len(df)} records to {output_path}")
    
    # Display first few rows and summary statistics
    logger.info("\nFirst few rows of processed data:")
    logger.info(df.head().to_string())
    logger.info("\nSummary statistics:")
    logger.info(df.describe().to_string())
    
    return output_path

def main():
    """Process projected flood data files."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Load region configuration
        regions_config = load_region_config()
        
        # Set up directories
        raw_data_dir = OUTPUT_DIR / "projected"
        output_dir = OUTPUT_DIR / "processed_projected"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Process each region
        for region, region_def in regions_config.items():
            try:
                output_path = process_region_projections(
                    region=region,
                    region_def=region_def,
                    raw_data_dir=raw_data_dir,
                    output_dir=output_dir
                )
                
                if output_path:
                    if output_path.stat().st_size > 0:
                        logger.info(f"Successfully processed projections for {region}")
                    else:
                        logger.warning(f"No projected data available for {region}, empty file created")
                else:
                    logger.warning(f"Failed to process projections for {region}")
                    
            except Exception as e:
                logger.error(f"Error processing region {region}: {str(e)}")
                continue
            
    except Exception as e:
        logger.error(f"Error processing projected data: {str(e)}")
        raise

if __name__ == "__main__":
    main() 