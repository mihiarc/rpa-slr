"""
Main pipeline for assigning HTF data from gauges to counties.
"""

import logging
from pathlib import Path
from typing import Optional, Dict
import pandas as pd
from datetime import datetime
import shapely.wkb

from .data_loader import (
    load_gauge_county_mapping,
    load_htf_data,
    validate_gauge_coverage
)
from .assignment import (
    calculate_historical_county_htf,
    calculate_projected_county_htf
)

logger = logging.getLogger(__name__)

def generate_data_dictionary(df: pd.DataFrame, name: str, description: str) -> str:
    """
    Generate a markdown data dictionary for the DataFrame.
    
    Args:
        df: DataFrame to document
        name: Name of the dataset
        description: High-level description of the dataset
        
    Returns:
        Markdown formatted data dictionary
    """
    # Get basic dataset info
    n_records = len(df)
    n_counties = df['county_fips'].nunique()
    year_range = f"{df['year'].min()}-{df['year'].max()}"
    
    # Column descriptions with enhanced NOAA documentation
    column_descriptions = {
        'county_fips': 'County FIPS code (unique identifier for US counties)',
        'county_name': 'Name of the county',
        'state_fips': 'State FIPS code',
        'year': 'Calendar year of observation',
        'total_flood_days': 'Total number of high tide flood days in the year (sum of minor, moderate, and major flood days)',
        'geometry': 'County geometry in Well-Known Text (WKT) format',
        'minor_flood_days': 'Number of days with minor flood threshold exceedance. Minor flooding typically results in minimal impacts to infrastructure.',
        'moderate_flood_days': 'Number of days with moderate flood threshold exceedance. Moderate flooding can result in significant impacts to coastal infrastructure.',
        'major_flood_days': 'Number of days with major flood threshold exceedance. Major flooding can result in severe impacts to coastal infrastructure.',
        # Projected data specific columns
        'low_scenario': 'Projected flood days under the low sea level rise scenario',
        'intermediate_low_scenario': 'Projected flood days under the intermediate-low sea level rise scenario',
        'intermediate_scenario': 'Projected flood days under the intermediate sea level rise scenario',
        'intermediate_high_scenario': 'Projected flood days under the intermediate-high sea level rise scenario',
        'high_scenario': 'Projected flood days under the high sea level rise scenario'
    }
    
    # Generate markdown
    lines = [
        f"# {name} Data Dictionary",
        "",
        "## Dataset Description",
        description,
        "",
        "## Dataset Information",
        f"- **Number of Records:** {n_records:,}",
        f"- **Number of Counties:** {n_counties:,}",
        f"- **Year Range:** {year_range}",
        f"- **Generation Date:** {datetime.now().strftime('%Y-%m-%d')}",
        "",
        "## Data Source",
        "This dataset is derived from NOAA's High Tide Flooding data products:",
        "- Historical data comes from NOAA's Annual Flood Count product, which provides observed flooding events at different severity thresholds",
        "- Projected data comes from NOAA's Decadal Projections product, which provides future flooding frequency estimates under different sea level rise scenarios",
        "",
        "## Column Descriptions and Statistics",
        "",
        "| Column Name | Description | Data Type | Example Values (Recent) | Summary Statistics |",
        "|------------|-------------|------------|----------------------|-------------------|"
    ]
    
    # Get most recent year for examples
    most_recent_year = df['year'].max()
    recent_data = df[df['year'] == most_recent_year]
    
    # Add each column's details with summary statistics
    for col in df.columns:
        dtype = str(df[col].dtype)
        
        # Get example values from most recent year
        example = str(recent_data[col].dropna().iloc[:3].tolist())[:50] + "..."
        
        # Generate summary statistics based on data type
        if pd.api.types.is_numeric_dtype(df[col].dtype) and col not in ['year', 'county_fips', 'state_fips']:
            stats = df[col].describe()
            summary = (f"Mean: {stats['mean']:.2f}, "
                      f"Std: {stats['std']:.2f}, "
                      f"Min: {stats['min']:.2f}, "
                      f"Max: {stats['max']:.2f}")
        elif col in ['year', 'county_fips', 'state_fips']:
            unique_count = df[col].nunique()
            summary = f"Unique values: {unique_count}"
        elif col == 'county_name':
            unique_count = df[col].nunique()
            summary = f"Unique counties: {unique_count}"
        else:
            summary = "N/A"
            
        desc = column_descriptions.get(col, "No description available")
        lines.append(f"| {col} | {desc} | {dtype} | {example} | {summary} |")
    
    # Add notes section with enhanced documentation
    lines.extend([
        "",
        "## Notes",
        "- Missing values are represented as empty cells in the CSV",
        "- Geometry data is stored as WKT strings for compatibility",
        "- All flood day counts are floating-point numbers due to weighted gauge aggregation",
        "- Historical data provides actual observed flooding events broken down by severity (minor, moderate, major)",
        "- Projected data provides estimates under different climate scenarios (low to high)",
        "",
        "## Processing Information",
        "This dataset was generated by aggregating gauge-level high tide flooding data to the county level using weighted relationships between gauges and counties. The weights are based on proximity and other relevant factors.",
        "",
        "### Data Transformation",
        "1. Original data is collected at NOAA tide gauge stations",
        "2. Each county is associated with up to three nearest tide gauges",
        "3. Gauge data is weighted based on proximity to county reference points",
        "4. County-level estimates are calculated using weighted averages of gauge measurements",
        "",
        "### Quality Notes",
        "- Some counties may have fewer than three associated gauges",
        "- Flood day counts are weighted averages and may include fractional days",
        "- Historical data represents actual observations while projections are model-based estimates",
        "",
        "For more information about the source data, refer to NOAA's documentation at:",
        "https://tidesandcurrents.noaa.gov/publications/HTF_Notice_of_Methodology_Update_2023.pdf"
    ])
    
    return "\n".join(lines)

def save_results(
    df: pd.DataFrame,
    base_path: Path,
    name: str,
    description: str
) -> None:
    """
    Save results in both parquet and CSV formats, along with a data dictionary.
    
    Args:
        df: DataFrame to save
        base_path: Base directory for output
        name: Base name for the output files
        description: Description of the dataset for the data dictionary
    """
    # Save parquet for internal use (efficient storage and full data types)
    parquet_dir = base_path / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = parquet_dir / f"{name}.parquet"
    df.to_parquet(parquet_path)
    
    # Save CSV for external sharing
    csv_dir = base_path / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_dir / f"{name}.csv"
    
    # For CSV output, we'll convert geometry to WKT string if present
    if 'geometry' in df.columns:
        df = df.copy()
        # Handle both bytes and shapely geometry objects
        df['geometry'] = df['geometry'].apply(
            lambda geom: (
                shapely.wkb.loads(geom).wkt if isinstance(geom, bytes)
                else geom.wkt if geom is not None
                else None
            )
        )
    
    df.to_csv(csv_path, index=False)
    
    # Generate and save data dictionary
    data_dict = generate_data_dictionary(df, name, description)
    dict_path = csv_dir / f"{name}_data_dictionary.md"
    dict_path.write_text(data_dict)
    
    logger.info(f"Saved {name} to:")
    logger.info(f"  - Parquet: {parquet_path}")
    logger.info(f"  - CSV: {csv_path}")
    logger.info(f"  - Data Dictionary: {dict_path}")

def generate_county_list(df: pd.DataFrame, output_path: Path) -> None:
    """
    Generate a markdown file listing all unique counties in the dataset.
    
    Args:
        df: DataFrame containing county information
        output_path: Path to save the markdown file
    """
    # Get unique counties with their information
    counties = df[['county_fips', 'county_name', 'state_fips']].drop_duplicates().sort_values('county_fips')
    
    # Generate markdown content
    lines = [
        "# Counties Included in HTF Analysis",
        "",
        f"Total number of counties: {len(counties)}",
        "",
        "| FIPS Code | County Name | State FIPS |",
        "|-----------|-------------|------------|"
    ]
    
    # Add each county
    for _, county in counties.iterrows():
        lines.append(f"| {county['county_fips']} | {county['county_name']} | {county['state_fips']} |")
    
    # Write to file
    output_path.write_text("\n".join(lines))
    logger.info(f"Generated county list at {output_path}")

def process_htf_dataset(
    mapping_path: str | Path,
    historical_htf_path: str | Path,
    projected_htf_path: str | Path,
    output_dir: str | Path
) -> None:
    """
    Process both historical and projected HTF datasets to create county-level data.
    Uses checkpointing to save intermediate results.
    
    Args:
        mapping_path: Path to the gauge-county mapping file
        historical_htf_path: Path to historical HTF data
        projected_htf_path: Path to projected HTF data
        output_dir: Directory to save output files
    """
    logger.info("Starting HTF data processing pipeline")
    output_dir = Path(output_dir)
    checkpoint_dir = output_dir / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    # Load mapping data
    mapping_df = load_gauge_county_mapping(mapping_path)
    
    # Generate county list from mapping data
    county_list_path = output_dir / "county_list.md"
    generate_county_list(mapping_df, county_list_path)
    
    # Load HTF data
    historical_df, projected_df = load_htf_data(historical_htf_path, projected_htf_path)
    
    # Validate gauge coverage
    validate_gauge_coverage(mapping_df, historical_df, 'historical')
    validate_gauge_coverage(mapping_df, projected_df, 'projected')
    
    # Process historical data with checkpoint
    historical_checkpoint = checkpoint_dir / "historical_county_htf.parquet"
    if historical_checkpoint.exists():
        logger.info("Loading historical data from checkpoint")
        historical_county_htf = pd.read_parquet(historical_checkpoint)
    else:
        logger.info("Processing historical HTF data")
        historical_county_htf = calculate_historical_county_htf(
            historical_df,
            mapping_df
        )
        # Save checkpoint
        logger.info("Saving historical data checkpoint")
        historical_county_htf.to_parquet(historical_checkpoint)
    
    # Process projected data with checkpoint
    projected_checkpoint = checkpoint_dir / "projected_county_htf.parquet"
    if projected_checkpoint.exists():
        logger.info("Loading projected data from checkpoint")
        projected_county_htf = pd.read_parquet(projected_checkpoint)
    else:
        logger.info("Processing projected HTF data")
        projected_county_htf = calculate_projected_county_htf(
            projected_df,
            mapping_df
        )
        # Save checkpoint
        logger.info("Saving projected data checkpoint")
        projected_county_htf.to_parquet(projected_checkpoint)
    
    # Create final output directory
    final_output_dir = output_dir / "final"
    final_output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Save final results in both formats with data dictionaries
        save_results(
            historical_county_htf,
            final_output_dir,
            "historical_county_htf",
            "Historical high tide flooding data aggregated to county level from NOAA tide gauge observations."
        )
        save_results(
            projected_county_htf,
            final_output_dir,
            "projected_county_htf",
            "Projected future high tide flooding data aggregated to county level from NOAA tide gauge projections."
        )
        
        logger.info(
            f"Processed {historical_county_htf['county_fips'].nunique()} counties "
            f"for historical data and {projected_county_htf['county_fips'].nunique()} "
            f"counties for projected data"
        )
        
        # If everything succeeded, we can optionally clean up checkpoints
        # Uncomment these lines if you want to automatically remove checkpoints after success
        # historical_checkpoint.unlink()
        # projected_checkpoint.unlink()
        # checkpoint_dir.rmdir()
        
    except Exception as e:
        logger.error(f"Error saving final results: {str(e)}")
        logger.info("Intermediate results are still available in the checkpoints directory")
        raise

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Example usage with corrected paths
    process_htf_dataset(
        mapping_path="data/processed/imputed_gauge_county_mapping.parquet",
        historical_htf_path="data/processed/historical_htf/historical_htf.parquet",
        projected_htf_path="data/processed/projected_htf/projected_htf.parquet",
        output_dir="data/processed/county_htf"
    ) 