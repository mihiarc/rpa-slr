"""
Script to analyze temporal patterns in high tide flooding data.

This script focuses on time series analysis of HTF data, including:
- Annual trends in flood days
- Seasonal patterns
- Year-over-year changes
- Regional temporal variations
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

def load_regional_data(historical_dir: Path) -> pd.DataFrame:
    """Load and combine all regional HTF data.
    
    Args:
        historical_dir: Directory containing historical HTF data files
        
    Returns:
        Combined DataFrame with all regional data
    """
    region_files = list(historical_dir.glob("historical_htf_*.parquet"))
    if not region_files:
        raise FileNotFoundError(f"No historical HTF files found in {historical_dir}")
    
    dfs = []
    for file in region_files:
        df = pd.read_parquet(file)
        dfs.append(df)
    
    return pd.concat(dfs, ignore_index=True)

def analyze_temporal_trends(df: pd.DataFrame) -> Dict:
    """Analyze temporal trends in HTF data.
    
    Args:
        df: DataFrame containing HTF data
        
    Returns:
        Dictionary containing temporal analysis results
    """
    # Calculate annual statistics
    annual_stats = df.groupby('year').agg({
        'total_flood_days': ['mean', 'std', 'min', 'max'],
        'major_flood_days': 'mean',
        'moderate_flood_days': 'mean',
        'minor_flood_days': 'mean'
    }).round(2)
    
    # Calculate year-over-year changes
    yoy_changes = annual_stats['total_flood_days']['mean'].pct_change() * 100
    
    # Calculate regional trends
    regional_trends = df.groupby(['region', 'year'])['total_flood_days'].mean().unstack()
    trend_slopes = {
        region: np.polyfit(regional_trends.columns, row.values, 1)[0]
        for region, row in regional_trends.iterrows()
    }
    
    return {
        "annual_stats": annual_stats,
        "yoy_changes": yoy_changes,
        "trend_slopes": trend_slopes,
        "regional_trends": regional_trends
    }

def plot_temporal_trends(
    df: pd.DataFrame,
    trends: Dict,
    output_dir: Path
) -> None:
    """Create visualizations of temporal trends.
    
    Args:
        df: HTF data DataFrame
        trends: Dictionary of trend analysis results
        output_dir: Directory to save plots
    """
    # 1. Overall annual trend with uncertainty
    plt.figure(figsize=(12, 6))
    annual_stats = trends['annual_stats']
    mean_line = annual_stats['total_flood_days']['mean']
    std_line = annual_stats['total_flood_days']['std']
    
    plt.plot(mean_line.index, mean_line.values, 'b-', label='Mean Flood Days')
    plt.fill_between(
        mean_line.index,
        mean_line - std_line,
        mean_line + std_line,
        alpha=0.2,
        color='b',
        label='±1 Standard Deviation'
    )
    
    plt.title('Annual High Tide Flooding Trend')
    plt.xlabel('Year')
    plt.ylabel('Average Flood Days')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig(output_dir / 'annual_trend.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. Flood severity composition over time
    severity_data = trends['annual_stats'][['major_flood_days', 'moderate_flood_days', 'minor_flood_days']]
    severity_data.plot(kind='area', stacked=True, figsize=(12, 6))
    plt.title('Flood Severity Composition Over Time')
    plt.xlabel('Year')
    plt.ylabel('Average Flood Days')
    plt.legend(title='Severity')
    plt.grid(True, alpha=0.3)
    plt.savefig(output_dir / 'severity_composition.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 3. Regional trends comparison
    plt.figure(figsize=(12, 6))
    for region in trends['regional_trends'].index:
        trend_data = trends['regional_trends'].loc[region]
        plt.plot(trend_data.index, trend_data.values, label=region, marker='o', markersize=4)
    
    plt.title('Regional High Tide Flooding Trends')
    plt.xlabel('Year')
    plt.ylabel('Average Flood Days')
    plt.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / 'regional_trends.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 4. Year-over-year changes
    plt.figure(figsize=(12, 6))
    yoy_changes = trends['yoy_changes']
    plt.bar(yoy_changes.index, yoy_changes.values)
    plt.axhline(y=0, color='r', linestyle='-', alpha=0.3)
    plt.title('Year-over-Year Changes in Flood Frequency')
    plt.xlabel('Year')
    plt.ylabel('Percent Change')
    plt.grid(True, alpha=0.3)
    plt.savefig(output_dir / 'yoy_changes.png', dpi=300, bbox_inches='tight')
    plt.close()

def generate_trend_report(trends: Dict, output_dir: Path) -> None:
    """Generate a markdown report of temporal trends.
    
    Args:
        trends: Dictionary of trend analysis results
        output_dir: Directory to save the report
    """
    annual_stats = trends['annual_stats']
    trend_slopes = trends['trend_slopes']
    
    report = f"""# High Tide Flooding Temporal Analysis
Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Overall Trends

### Annual Statistics
- First Year: {annual_stats.index.min()}
- Last Year: {annual_stats.index.max()}
- Average Flood Days: {annual_stats['total_flood_days']['mean'].mean():.1f}
- Maximum in a Year: {annual_stats['total_flood_days']['max'].max():.1f}

### Trend Analysis
"""
    
    # Add regional trend information
    report += "\n### Regional Trends (Average Annual Change in Flood Days)\n"
    for region, slope in trend_slopes.items():
        report += f"- {region.replace('_', ' ').title()}: {slope:.2f} days/year\n"
    
    # Add visualization references
    report += """
## Visualizations

### Annual Trend
![Annual Trend](annual_trend.png)
Shows the overall trend in flood days with uncertainty bands.

### Severity Composition
![Severity Composition](severity_composition.png)
Shows how the composition of flood severity has changed over time.

### Regional Comparison
![Regional Trends](regional_trends.png)
Compares flooding trends across different regions.

### Year-over-Year Changes
![YoY Changes](yoy_changes.png)
Shows the percentage change in flood days from year to year.
"""
    
    # Save the report
    report_path = output_dir / 'temporal_analysis.md'
    report_path.write_text(report)
    logger.info(f"Saved temporal analysis report to {report_path}")

def main():
    """Run temporal analysis of HTF data."""
    try:
        # Setup
        output_dir = Path("output/analysis/temporal")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load data
        historical_dir = Path("output/historical")
        df = load_regional_data(historical_dir)
        
        # Analyze trends
        trends = analyze_temporal_trends(df)
        
        # Generate visualizations
        plot_temporal_trends(df, trends, output_dir)
        
        # Generate report
        generate_trend_report(trends, output_dir)
        
        logger.info("Temporal analysis complete!")
        
    except Exception as e:
        logger.error(f"Error in temporal analysis: {str(e)}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main() 