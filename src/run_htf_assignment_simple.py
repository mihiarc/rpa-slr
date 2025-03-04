"""
Script to run the simplified HTF assignment process.
"""

from pathlib import Path
import logging
from assignment.assignment import process_htf_assignment

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Run the simplified HTF assignment process."""
    try:
        # Set up paths
        base_dir = Path(__file__).parent.parent
        
        # Input paths
        mapping_path = base_dir / "output" / "imputation" / "imputation_structure_all_regions.parquet"
        historical_path = base_dir / "output" / "historical"
        
        # Output directory
        output_dir = base_dir / "output" / "county_htf_simple"
        
        logger.info("Starting simplified HTF assignment process")
        logger.info(f"Using gauge-county mapping: {mapping_path}")
        logger.info(f"Using historical HTF data: {historical_path}")
        logger.info(f"Output directory: {output_dir}")
        
        # Run the assignment process
        process_htf_assignment(
            mapping_path=mapping_path,
            historical_path=historical_path,
            output_dir=output_dir
        )
        
        logger.info("HTF assignment process completed successfully")
        
    except Exception as e:
        logger.error(f"Error in HTF assignment process: {str(e)}")
        raise

if __name__ == "__main__":
    main() 