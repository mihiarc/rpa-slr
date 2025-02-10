"""
Command line interface for NOAA data quality analysis.

This module provides a CLI for analyzing the quality of NOAA high tide flooding data,
supporting both historical and projected datasets.
"""

import argparse
import logging
from pathlib import Path
import json
import sys
from typing import Optional
from datetime import datetime

from .data_quality import DataQualityAnalyzer
from ..noaa.core import NOAACache
from .. import config

logger = logging.getLogger(__name__)

def setup_logging(verbose: bool = False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    
    # Ensure log directory exists
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Set up file handler
    log_file = config.LOG_DIR / "data_quality_analysis.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    
    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(
        '%(levelname)s - %(message)s'
    ))
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        handlers=[file_handler, console_handler]
    )
    
    # Ensure all loggers are set to DEBUG when verbose is True
    if verbose:
        for name in logging.root.manager.loggerDict:
            logger = logging.getLogger(name)
            logger.setLevel(logging.DEBUG)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Analyze quality of NOAA high tide flooding data'
    )
    
    parser.add_argument(
        '--region',
        help='Region to analyze (e.g., alaska, hawaii, pacific_islands)'
    )
    
    parser.add_argument(
        '--station',
        help='Specific station ID to analyze'
    )
    
    parser.add_argument(
        '--start-year',
        type=int,
        help=f'Start year (inclusive, defaults to {config.HISTORICAL_SETTINGS["start_year"]} for historical)'
    )
    
    parser.add_argument(
        '--end-year',
        type=int,
        help=f'End year (inclusive, defaults to {config.HISTORICAL_SETTINGS["end_year"]} for historical)'
    )
    
    parser.add_argument(
        '--data-type',
        choices=['historical', 'projected'],
        default='historical',
        help='Type of data to analyze'
    )
    
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=config.OUTPUT_DIR / "analysis",
        help='Output directory for analysis results'
    )
    
    parser.add_argument(
        '--format',
        choices=['json', 'text', 'markdown'],
        default='markdown',
        help='Output format (markdown provides the most readable and detailed report)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser.parse_args()

def format_analysis_text(analysis: dict, indent: int = 0) -> str:
    """Format analysis results as human-readable text.
    
    Args:
        analysis: Analysis results dictionary
        indent: Number of spaces to indent
        
    Returns:
        Formatted text representation
    """
    prefix = ' ' * indent
    lines = []
    
    for key, value in sorted(analysis.items()):
        if isinstance(value, dict):
            lines.append(f"{prefix}{key}:")
            lines.append(format_analysis_text(value, indent + 2))
        elif isinstance(value, list):
            lines.append(f"{prefix}{key}:")
            for item in value:
                if isinstance(item, dict):
                    lines.append(format_analysis_text(item, indent + 2))
                else:
                    lines.append(f"{prefix}  - {item}")
        elif isinstance(value, float):
            lines.append(f"{prefix}{key}: {value:.3f}")
        else:
            lines.append(f"{prefix}{key}: {value}")
            
    return '\n'.join(lines)

def save_analysis_results(
    results: dict,
    output_path: Path,
    format: str = 'markdown'
) -> None:
    """Save analysis results to file.
    
    Args:
        results: Analysis results to save
        output_path: Path to save results to
        format: Output format ('json', 'text', or 'markdown')
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if format == 'json':
        class NumpyEncoder(json.JSONEncoder):
            def default(self, obj):
                import numpy as np
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                return super().default(obj)
                
        with open(output_path.with_suffix('.json'), 'w') as f:
            json.dump(results, f, indent=2, cls=NumpyEncoder)
    elif format == 'text':
        with open(output_path.with_suffix('.txt'), 'w') as f:
            f.write(format_analysis_text(results))
    else:  # markdown
        with open(output_path.with_suffix('.md'), 'w') as f:
            f.write(format_analysis_markdown(results))
            
    logger.info(f"Analysis results saved to: {output_path}")

def format_analysis_markdown(analysis: dict, level: int = 1) -> str:
    """Format analysis results as markdown.
    
    Args:
        analysis: Analysis results dictionary
        level: Current heading level
        
    Returns:
        Formatted markdown representation
    """
    lines = []
    
    for key, value in sorted(analysis.items()):
        # Convert key to title case for headings
        heading = key.replace('_', ' ').title()
        
        if isinstance(value, dict):
            lines.append(f"{'#' * level} {heading}")
            lines.append("")
            lines.append(format_analysis_markdown(value, level + 1))
        elif isinstance(value, list):
            lines.append(f"{'#' * level} {heading}")
            lines.append("")
            for item in value:
                if isinstance(item, dict):
                    lines.append(format_analysis_markdown(item, level + 1))
                else:
                    lines.append(f"- {item}")
            lines.append("")
        elif isinstance(value, float):
            lines.append(f"**{heading}**: {value:.3f}")
        else:
            lines.append(f"**{heading}**: {value}")
            
    return '\n'.join(lines)

def main():
    """Main execution function."""
    args = parse_args()
    setup_logging(args.verbose)
    
    try:
        # Initialize components
        cache = NOAACache(config_dir=config.CONFIG_DIR)
        analyzer = DataQualityAnalyzer(cache=cache)
        
        # Run analysis
        if args.station:
            logger.info(f"Analyzing station: {args.station}")
            results = analyzer.analyze_station_data(
                args.station,
                args.start_year,
                args.end_year,
                args.data_type
            )
        elif args.region:
            logger.info(f"Analyzing region: {args.region}")
            results = analyzer.analyze_regional_data(
                args.region,
                args.start_year,
                args.end_year,
                args.data_type
            )
        else:
            logger.error("Either --station or --region must be specified")
            sys.exit(1)
            
        # Save results
        if not results:
            logger.warning("No results to save")
            sys.exit(0)
            
        # Generate output filename
        target = args.station or args.region
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = args.output_dir / f"quality_analysis_{args.data_type}_{target}_{timestamp}"
        
        # Save results
        save_analysis_results(results, output_path, args.format)
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 