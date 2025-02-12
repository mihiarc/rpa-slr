"""
Main entry point for historical HTF data assignment.
"""

from pathlib import Path
from typing import Optional
import logging

from src.assignment.historical.data_loader import HistoricalDataLoader
from src.assignment.historical.aggregator import HistoricalAggregator

logger = logging.getLogger(__name__)

def process_historical_htf(
    htf_data_path: Path,
    reference_points_path: Path,
    output_dir: Path,
    flood_data_path: Optional[Path] = None,
    config_dir: Optional[Path] = None
) -> None:
    """Process historical HTF data for all regions.
    
    Args:
        htf_data_path: Path to imputation structure data
        reference_points_path: Path to reference points data
        output_dir: Directory to save outputs
        flood_data_path: Optional path to HTF flood data
        config_dir: Optional custom config directory
    """
    # Initialize data loader
    data_loader = HistoricalDataLoader(config_dir)
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Process each region
    for region in data_loader.region_config.keys():
        try:
            logger.info(f"Processing region: {region}")
            
            # Load data
            htf_df, ref_points, stations = data_loader.load_regional_data(
                region,
                htf_data_path,
                reference_points_path,
                flood_data_path
            )
            
            # Create aggregator
            aggregator = HistoricalAggregator()
            
            # Aggregate to county level
            county_data = aggregator.aggregate_to_county(
                htf_df=htf_df,
                reference_points=ref_points,
                stations=stations
            )
            
            # Save results
            output_path = output_dir / f"historical_htf_{region}.parquet"
            county_data.to_parquet(output_path)
            logger.info(f"Saved results to {output_path}")
            
        except Exception as e:
            logger.error(f"Error processing region {region}: {str(e)}")
            continue 