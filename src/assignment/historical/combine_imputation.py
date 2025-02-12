"""
Script to combine regional imputation files into a single dataset.
"""

import pandas as pd
from pathlib import Path
import logging

from src.config import IMPUTATION_DIR

logger = logging.getLogger(__name__)

def combine_imputation_files(output_path: Path = None) -> Path:
    """Combine regional imputation files into a single dataset.
    
    Args:
        output_path: Optional custom output path
        
    Returns:
        Path to combined file
    """
    # Find all regional imputation files
    imputation_files = list(IMPUTATION_DIR.glob("imputation_structure_*_2*.parquet"))
    
    if not imputation_files:
        raise FileNotFoundError("No imputation files found")
    
    # Sort by timestamp to get most recent for each region
    imputation_files.sort()
    
    # Read and combine all files
    dfs = []
    for file in imputation_files:
        logger.info(f"Reading {file.name}")
        df = pd.read_parquet(file)
        dfs.append(df)
        
    # Combine all regions
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Set default output path if not provided
    if output_path is None:
        output_path = IMPUTATION_DIR / "imputation_structure_all_regions.parquet"
    
    # Save combined file
    combined_df.to_parquet(output_path, index=False)
    logger.info(f"Saved combined data to {output_path}")
    logger.info(f"Total records: {len(combined_df)}")
    
    return output_path

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    combine_imputation_files() 