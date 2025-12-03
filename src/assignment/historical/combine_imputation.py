"""
Script to combine regional imputation files into a single dataset.

This module should be run after the imputation pipeline completes to create
a unified structure file for the assignment pipeline.

Usage:
    uv run python -m src.assignment.historical.combine_imputation
"""

import pandas as pd
from pathlib import Path
import logging
from typing import Dict, Optional

from src.config import IMPUTATION_DATA_DIR, IMPUTATION_DIR

logger = logging.getLogger(__name__)


def get_latest_regional_files(data_dir: Path) -> Dict[str, Path]:
    """Find the most recent imputation file for each region.

    Args:
        data_dir: Directory containing imputation parquet files

    Returns:
        Dictionary mapping region name to file path
    """
    all_files = list(data_dir.glob("imputation_structure_*_2*.parquet"))

    if not all_files:
        raise FileNotFoundError(f"No imputation files found in {data_dir}")

    # Group by region and get most recent
    region_files = {}
    for f in all_files:
        # Extract region name from filename
        # Format: imputation_structure_<region>_<timestamp>.parquet
        parts = f.stem.split('_')
        region_parts = []
        for i, p in enumerate(parts):
            if i >= 2 and not p.startswith('2'):
                region_parts.append(p)
            elif p.startswith('2'):
                break
        region = '_'.join(region_parts)

        # Keep most recent file per region (lexicographic sort of timestamps)
        if region not in region_files or f.name > region_files[region].name:
            region_files[region] = f

    return region_files


def combine_imputation_files(
    input_dir: Optional[Path] = None,
    output_path: Optional[Path] = None
) -> Path:
    """Combine regional imputation files into a single dataset.

    Args:
        input_dir: Directory containing regional imputation files.
                   Defaults to IMPUTATION_DATA_DIR (output/imputation/data/).
        output_path: Output path for combined file.
                     Defaults to IMPUTATION_DIR/imputation_structure_all_regions.parquet

    Returns:
        Path to combined file
    """
    # Set default paths
    if input_dir is None:
        input_dir = IMPUTATION_DATA_DIR
    if output_path is None:
        output_path = IMPUTATION_DIR / "imputation_structure_all_regions.parquet"

    input_dir = Path(input_dir)
    output_path = Path(output_path)

    # Get latest file for each region
    region_files = get_latest_regional_files(input_dir)

    logger.info(f"Found {len(region_files)} regions to combine:")
    for region, filepath in sorted(region_files.items()):
        logger.info(f"  {region}: {filepath.name}")

    # Read and combine all files
    dfs = []
    for region, filepath in sorted(region_files.items()):
        df = pd.read_parquet(filepath)
        logger.info(f"  {region}: {len(df):,} records")
        dfs.append(df)

    # Combine all regions
    combined_df = pd.concat(dfs, ignore_index=True)

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save combined file
    combined_df.to_parquet(output_path, index=False)

    logger.info(f"\nCombined imputation structure:")
    logger.info(f"  Total records: {len(combined_df):,}")
    logger.info(f"  Unique counties: {combined_df['county_fips'].nunique()}")
    logger.info(f"  Unique stations: {combined_df['station_id'].nunique()}")
    logger.info(f"  Regions: {combined_df['region'].nunique()}")
    logger.info(f"  Output: {output_path}")

    return output_path


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    combine_imputation_files() 