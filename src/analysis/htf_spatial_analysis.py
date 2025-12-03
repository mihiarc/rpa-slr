"""
Script to analyze HTF data and generate a comprehensive markdown report.

This script focuses on spatial patterns and flood statistics,
complementing the temporal analysis in htf_temporal_analysis.py.
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from glob import glob
from . import htf_spatial_visualization as viz
from . import htf_temporal_analysis as temporal

# Note: Do not call logging.basicConfig here - let the application configure logging
logger = logging.getLogger(__name__)

def analyze_flood_data(historical_dir: Path) -> dict:
    """Analyze the HTF dataset focusing on flood patterns.
    
    Args:
        historical_dir: Directory containing historical HTF data files
    
    Returns:
        Dictionary containing analysis results
    """
    # Load all regional files
    region_files = list(historical_dir.glob("historical_htf_*.parquet"))
    if not region_files:
        raise FileNotFoundError(f"No historical HTF files found in {historical_dir}")
    
    # Combine regional data
    dfs = []
    for file in region_files:
        df = pd.read_parquet(file)
        dfs.append(df)
    
    df = pd.concat(dfs, ignore_index=True)
    
    # Calculate flood statistics
    stats = {
        "flood_totals": {
            "total": df['total_flood_days'].sum(),
            "major": df['major_flood_days'].sum(),
            "moderate": df['moderate_flood_days'].sum(),
            "minor": df['minor_flood_days'].sum()
        },
        "flood_averages": {
            "total": df['total_flood_days'].mean(),
            "major": df['major_flood_days'].mean(),
            "moderate": df['moderate_flood_days'].mean(),
            "minor": df['minor_flood_days'].mean()
        },
        "regional_patterns": []
    }
    
    # Analyze regional patterns
    for region in df['region'].unique():
        region_df = df[df['region'] == region]
        stats["regional_patterns"].append({
            "region": region,
            "avg_total_floods": region_df['total_flood_days'].mean(),
            "max_total_floods": region_df['total_flood_days'].max(),
            "major_flood_pct": (region_df['major_flood_days'].sum() / 
                              region_df['total_flood_days'].sum() * 100),
            "most_impacted_county": {
                "name": region_df.loc[region_df['total_flood_days'].idxmax(), 'county_name'],
                "total_floods": region_df['total_flood_days'].max()
            }
        })
    
    # Find most impacted counties
    top_counties = df.nlargest(10, 'total_flood_days')
    stats["most_impacted_counties"] = []
    for _, row in top_counties.iterrows():
        stats["most_impacted_counties"].append({
            "name": row['county_name'],
            "region": row['region_display'],
            "total_floods": row['total_flood_days'],
            "major_floods": row['major_flood_days']
        })
    
    return stats

def generate_flood_report(flood_stats: dict, output_dir: Path) -> None:
    """Generate a markdown report focusing on flood patterns.
    
    Args:
        flood_stats: Dictionary containing flood analysis results
        output_dir: Directory to save the report
    """
    report = f"""# High Tide Flooding (HTF) Analysis Report
Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Flood Statistics Overview

### Total Flood Days
- Total Flood Events: {flood_stats['flood_totals']['total']:,.0f}
- Major Flood Events: {flood_stats['flood_totals']['major']:,.0f}
- Moderate Flood Events: {flood_stats['flood_totals']['moderate']:,.0f}
- Minor Flood Events: {flood_stats['flood_totals']['minor']:,.0f}

### Average Flood Days per County
- Total: {flood_stats['flood_averages']['total']:.1f}
- Major: {flood_stats['flood_averages']['major']:.1f}
- Moderate: {flood_stats['flood_averages']['moderate']:.1f}
- Minor: {flood_stats['flood_averages']['minor']:.1f}

## Regional Analysis

"""
    
    # Add regional summaries
    for region in flood_stats['regional_patterns']:
        report += f"### {region['region'].replace('_', ' ').title()}\n"
        report += f"- Average Flood Days: {region['avg_total_floods']:.1f}\n"
        report += f"- Maximum Flood Days: {region['max_total_floods']:.1f}\n"
        report += f"- Major Flood Percentage: {region['major_flood_pct']:.1f}%\n"
        report += (f"- Most Impacted County: {region['most_impacted_county']['name']} "
                  f"({region['most_impacted_county']['total_floods']:.1f} flood days)\n\n")
    
    report += "## Most Impacted Counties\n\n"
    
    for county in flood_stats['most_impacted_counties']:
        report += (f"- {county['name']} ({county['region']}): "
                  f"{county['total_floods']:.1f} total flood days, "
                  f"{county['major_floods']:.1f} major flood days\n")
    
    report += """
## Spatial Patterns

### Regional Flood Distribution
![Regional Flood Distribution](flood_distribution.png)

### County-Level Flood Days
![County Flood Days](county_flood_days.png)

### Major Flood Hotspots
![Major Flood Hotspots](major_flood_hotspots.png)

For temporal analysis and trends over time, please refer to the temporal analysis report.
"""
    
    # Save the report
    report_file = output_dir / 'flood_analysis_report.md'
    report_file.write_text(report)
    logger.info(f"Report generated at {report_file}")

def main():
    try:
        # Setup output directory
        output_dir = viz.setup_output_dir()
        logger.info(f"Setting up output directory at {output_dir}")
        
        # Generate visualizations first
        logger.info("Generating visualizations...")
        viz.main()
        
        # Load and analyze data
        historical_dir = Path("output/historical")
        
        if historical_dir.exists():
            logger.info("Analyzing flood data...")
            flood_stats = analyze_flood_data(historical_dir)
            
            logger.info("Generating flood analysis report...")
            generate_flood_report(flood_stats, output_dir)
            
            logger.info("Analysis complete!")
        else:
            logger.error("Historical data directory not found")
            
    except Exception as e:
        logger.error(f"Error in analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main() 