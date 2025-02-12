"""
Command line interface for projected HTF data processing.

This module provides a CLI for processing projected high tide flooding data:
- Fetches data for specified regions and decades
- Validates and processes the data by scenario
- Outputs processed data to CSV/parquet files
"""

import argparse
import logging
from pathlib import Path
import sys
import yaml

from .projected_htf_fetcher import ProjectedHTFFetcher
from .projected_htf_processor import ProjectedHTFProcessor
from ..core import NOAACache

logger = logging.getLogger(__name__)

def setup_logging(verbose: bool = False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Process projected high tide flooding data'
    )
    
    parser.add_argument(
        '--region',
        required=True,
        help='Region to process (e.g., alaska, hawaii, pacific_islands)'
    )
    
    parser.add_argument(
        '--start-decade',
        type=int,
        required=True,
        help='Start decade (inclusive, e.g., 2020)'
    )
    
    parser.add_argument(
        '--end-decade',
        type=int,
        required=True,
        help='End decade (inclusive, e.g., 2100)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path('output/projected'),
        help='Output directory for processed data'
    )
    
    parser.add_argument(
        '--config-dir',
        type=Path,
        help='Custom config directory path'
    )
    
    parser.add_argument(
        '--format',
        choices=['csv', 'parquet'],
        default='parquet',
        help='Output file format'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser.parse_args()

def validate_region(region: str, config_dir: Path) -> bool:
    """Validate region against available configurations."""
    try:
        # Load region mappings
        with open(config_dir / "region_mappings.yaml") as f:
            region_config = yaml.safe_load(f)
            
        # Check if region exists
        return region.lower() in [r.lower() for r in region_config['regions'].keys()]
    except Exception as e:
        logger.error(f"Error validating region: {e}")
        return False

def main():
    """Main execution function."""
    args = parse_args()
    setup_logging(args.verbose)
    
    # Use default config dir if not specified
    config_dir = args.config_dir or Path(__file__).parent.parent.parent.parent / "config"
    
    # Validate region
    if not validate_region(args.region, config_dir):
        logger.error(f"Invalid region: {args.region}")
        sys.exit(1)
    
    try:
        # Initialize components
        cache = NOAACache(config_dir=config_dir)
        fetcher = ProjectedHTFFetcher(cache=cache, region=args.region)
        
        # Get dataset status
        status = fetcher.get_dataset_status()
        logger.info(f"\nDataset Status:")
        logger.info(f"Region: {status['region']}")
        logger.info(f"Station Count: {status['station_count']}")
        logger.info(f"Decade Range: {status['decade_range']}")
        logger.info(f"Completeness: {status['completeness']*100:.1f}%")
        
        # Log cache statistics
        cache_stats = status['cache_stats']
        total_requests = cache_stats['hits'] + cache_stats['misses']
        if total_requests > 0:
            hit_rate = (cache_stats['hits'] / total_requests) * 100
            logger.info(f"\nCache Statistics:")
            logger.info(f"Cache Hits: {cache_stats['hits']}")
            logger.info(f"Cache Misses: {cache_stats['misses']}")
            logger.info(f"Cache Errors: {cache_stats['errors']}")
            logger.info(f"Cache Hit Rate: {hit_rate:.1f}%")
        
        # Get regional dataset
        dataset = fetcher.get_regional_dataset(
            start_decade=args.start_decade,
            end_decade=args.end_decade
        )
        
        if not dataset:
            logger.warning("No data to output")
            sys.exit(0)
        
        # Create output directory
        args.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate and save dataset
        output_file = fetcher.generate_dataset(
            output_path=args.output_dir,
            stations=list(dataset.keys())
        )
            
        logger.info(f"\nOutput saved to: {output_file}")
        
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 