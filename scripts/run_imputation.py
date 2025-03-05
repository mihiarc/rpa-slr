#!/usr/bin/env python3
"""
Script to run the imputation process for a specific region.
Example: python scripts/run_imputation.py --region west_coast
"""

import argparse
import logging
import sys
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.imputation.main import ImputationManager
from src.config import IMPUTATION_DIR, IMPUTATION_LOGS_DIR

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run imputation process for a specific region")
    parser.add_argument(
        "--region", 
        type=str, 
        help="Region to process (e.g., west_coast, north_atlantic)"
    )
    parser.add_argument(
        "--output-dir", 
        type=str, 
        default=str(IMPUTATION_DIR / "data"),
        help="Directory to save output files"
    )
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create logs directory if it doesn't exist
    IMPUTATION_LOGS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Set up basic logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # Output to console
            logging.FileHandler(IMPUTATION_LOGS_DIR / "imputation.log")  # Output to file
        ]
    )
    
    # Initialize and run the imputation manager
    manager = ImputationManager(
        output_dir=output_dir,
        region=args.region
    )
    
    # Run the imputation process
    output_files = manager.run()
    
    # Print results
    if output_files:
        print(f"\nSuccessfully generated imputation structures for {len(output_files)} region(s):")
        for region, path in output_files.items():
            print(f"  - {region}: {path}")
        return 0
    else:
        print("\nNo imputation structures were generated.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 