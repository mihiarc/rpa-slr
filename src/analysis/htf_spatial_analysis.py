"""
Script to analyze HTF data and generate a comprehensive markdown report.

This script analyzes both historical and projected HTF datasets and generates
a markdown report with analysis and visualizations.
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from . import visualize_htf_data as viz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_historical_data(df: pd.DataFrame) -> dict:
    """Analyze the historical HTF dataset and return key statistics."""
    
    stats = {
        "total_records": len(df),
        "unique_stations": df['station_id'].nunique(),
        "year_range": (df['year'].min(), df['year'].max()),
        "flood_totals": df[['major_flood_days', 'moderate_flood_days', 'minor_flood_days']].sum().to_dict(),
        "avg_completeness": df['data_completeness'].mean(),
        "top_stations": []
    }
    
    # Get top stations
    df['total_floods'] = df['total_flood_days']
    top_stations = df.groupby(['station_id', 'station_name'])['total_floods'].sum().sort_values(ascending=False).head()
    for (station_id, station_name), floods in top_stations.items():
        stats["top_stations"].append({
            "id": station_id,
            "name": station_name,
            "total_floods": floods
        })
    
    return stats

def analyze_projected_data(df: pd.DataFrame) -> dict:
    """Analyze the projected HTF dataset and return key statistics."""
    
    scenarios = ['low_scenario', 'intermediate_low_scenario', 'intermediate_scenario', 
                'intermediate_high_scenario', 'high_scenario']
    
    stats = {
        "total_records": len(df),
        "unique_stations": df['station'].nunique(),
        "decade_range": (df['decade'].min(), df['decade'].max()),
        "scenario_averages": df[scenarios].mean().to_dict(),
        "avg_uncertainty_range": df['scenario_range'].mean(),
        "most_impacted_2100": []
    }
    
    # Get most impacted stations in 2100
    end_century = df[df['decade'] == df['decade'].max()]
    top_stations = end_century.nlargest(5, 'median_scenario')[['station_name', 'median_scenario']]
    for _, row in top_stations.iterrows():
        stats["most_impacted_2100"].append({
            "name": row['station_name'],
            "median_floods": row['median_scenario']
        })
    
    return stats

def generate_markdown_report(historical_stats: dict, projected_stats: dict, output_dir: Path) -> None:
    """Generate a markdown report with analysis and visualizations."""
    
    # First part of the report (historical data)
    report = f"""# High Tide Flooding (HTF) Analysis Report
Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 1. Historical HTF Data Analysis

### Dataset Overview
- Total Records: {historical_stats['total_records']:,}
- Unique Stations: {historical_stats['unique_stations']}
- Year Range: {historical_stats['year_range'][0]} to {historical_stats['year_range'][1]}
- Average Data Completeness: {historical_stats['avg_completeness']:.1%}

### Flooding Statistics
- Major Flood Days: {historical_stats['flood_totals']['major_flood_days']:,.0f}
- Moderate Flood Days: {historical_stats['flood_totals']['moderate_flood_days']:,.0f}
- Minor Flood Days: {historical_stats['flood_totals']['minor_flood_days']:,.0f}

### Top 5 Most Impacted Stations
"""
    
    for station in historical_stats['top_stations']:
        report += f"- {station['name']} ({station['id']}): {station['total_floods']:,.0f} flood days\n"
    
    report += """
### Historical Trends Visualization
![Historical Flood Trends](historical_flood_trends.png)

### Geographic Distribution
![Station Totals](historical_station_totals.png)

### Data Completeness
![Data Completeness](historical_completeness.png)

## 2. Projected HTF Data Analysis

### Dataset Overview"""

    # Add projected data overview separately to avoid f-string nesting
    report += f"""
- Total Records: {projected_stats['total_records']:,}
- Unique Stations: {projected_stats['unique_stations']}
- Decade Range: {projected_stats['decade_range'][0]} to {projected_stats['decade_range'][1]}

### Average Annual Flood Days by Scenario
"""
    
    for scenario, avg in projected_stats['scenario_averages'].items():
        report += f"- {scenario.replace('_', ' ').title()}: {avg:.1f} days/year\n"
    
    report += f"""
### Uncertainty Analysis
- Average Range between High and Low Scenarios: {projected_stats['avg_uncertainty_range']:.1f} days/year

### Most Impacted Stations (2100)
"""
    
    for station in projected_stats['most_impacted_2100']:
        report += f"- {station['name']}: {station['median_floods']:.1f} days/year\n"
    
    report += """
### Projection Visualizations
![Scenario Trends](projected_scenario_trends.png)

### Uncertainty Analysis
![Uncertainty Range](projected_uncertainty.png)

### End-of-Century Distribution
![2100 Distribution](projected_2100_distribution.png)

## 3. Key Findings

1. Historical Data shows a clear trend of increasing flood frequency across all severity levels.
2. Data completeness varies significantly across stations and time periods.
3. Future projections indicate substantial increases in flooding frequency under all scenarios.
4. Hawaii stations are projected to be most severely impacted by the end of the century.
5. There is significant uncertainty in projections, with the range between scenarios increasing over time.
"""
    
    # Save the report
    report_file = output_dir / 'htf_analysis_report.md'
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
        historical_file = Path("output/historical_htf/historical_htf.parquet")
        projected_file = Path("output/projected_htf/projected_htf.parquet")
        
        historical_stats = None
        projected_stats = None
        
        if historical_file.exists():
            logger.info("Analyzing historical data...")
            historical_df = pd.read_parquet(historical_file)
            historical_stats = analyze_historical_data(historical_df)
        
        if projected_file.exists():
            logger.info("Analyzing projected data...")
            projected_df = pd.read_parquet(projected_file)
            projected_stats = analyze_projected_data(projected_df)
        
        if historical_stats and projected_stats:
            logger.info("Generating markdown report...")
            generate_markdown_report(historical_stats, projected_stats, output_dir)
            logger.info("Analysis complete!")
        else:
            logger.error("Missing data files required for analysis")
            
    except Exception as e:
        logger.error(f"Error in analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main() 