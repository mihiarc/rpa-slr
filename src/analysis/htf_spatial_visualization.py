"""
Script to create visualizations for historical and projected HTF datasets.

This script generates various plots to visualize trends and patterns in the HTF data,
saving them to an output directory.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set style for all plots
plt.style.use('seaborn-v0_8')  # Use the updated seaborn style name
sns.set_palette("husl")

def setup_output_dir(base_dir: str = "output/visualizations") -> Path:
    """Create and return output directory for plots."""
    output_dir = Path(base_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir

def plot_historical_trends(df: pd.DataFrame, output_dir: Path) -> None:
    """Generate plots for historical HTF data."""
    
    # 1. Annual flood days by severity over time
    plt.figure(figsize=(12, 6))
    yearly_floods = df.groupby('year')[['major_flood_days', 'moderate_flood_days', 'minor_flood_days']].mean()
    yearly_floods.plot(kind='line', marker='o', markersize=4)
    plt.title('Average Annual Flood Days by Severity (1920-2024)')
    plt.xlabel('Year')
    plt.ylabel('Average Flood Days')
    plt.legend(title='Flood Severity')
    plt.grid(True, alpha=0.3)
    plt.savefig(output_dir / 'historical_flood_trends.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. Geographic distribution of total floods
    plt.figure(figsize=(12, 6))
    station_totals = df.groupby('station_name')['total_flood_days'].sum().sort_values(ascending=True)
    top_20_stations = station_totals.tail(20)
    sns.barplot(x=top_20_stations.values, y=top_20_stations.index)
    plt.title('Top 20 Stations by Total Flood Days')
    plt.xlabel('Total Flood Days')
    plt.tight_layout()
    plt.savefig(output_dir / 'historical_station_totals.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 3. Data completeness heatmap
    plt.figure(figsize=(15, 8))
    pivot_data = df.pivot_table(
        values='data_completeness',
        index='station_name',
        columns='year',
        aggfunc='first'
    )
    sns.heatmap(pivot_data, cmap='YlOrRd', cbar_kws={'label': 'Data Completeness'})
    plt.title('Data Completeness by Station and Year')
    plt.tight_layout()
    plt.savefig(output_dir / 'historical_completeness.png', dpi=300, bbox_inches='tight')
    plt.close()

def plot_projected_trends(df: pd.DataFrame, output_dir: Path) -> None:
    """Generate plots for projected HTF data."""
    
    # 1. Scenario comparison over time
    plt.figure(figsize=(12, 6))
    scenarios = ['low_scenario', 'intermediate_low_scenario', 'intermediate_scenario', 
                'intermediate_high_scenario', 'high_scenario']
    
    decade_means = df.groupby('decade')[scenarios].mean()
    decade_means.plot(kind='line', marker='o', markersize=4)
    plt.title('Average Projected Flood Days by Scenario (2020-2100)')
    plt.xlabel('Decade')
    plt.ylabel('Average Annual Flood Days')
    plt.legend(title='Scenario', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_dir / 'projected_scenario_trends.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. Uncertainty range by decade
    plt.figure(figsize=(12, 6))
    decade_uncertainty = df.groupby('decade')['scenario_range'].mean()
    plt.bar(decade_uncertainty.index, decade_uncertainty.values)
    plt.title('Average Scenario Range by Decade')
    plt.xlabel('Decade')
    plt.ylabel('Range (High - Low Scenario)')
    plt.grid(True, alpha=0.3)
    plt.savefig(output_dir / 'projected_uncertainty.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 3. End-of-century impact distribution
    plt.figure(figsize=(12, 6))
    end_century = df[df['decade'] == df['decade'].max()]
    sns.boxplot(data=end_century[scenarios])
    plt.title('Distribution of Projected Flood Days by Scenario (2100)')
    plt.xticks(rotation=45)
    plt.ylabel('Annual Flood Days')
    plt.tight_layout()
    plt.savefig(output_dir / 'projected_2100_distribution.png', dpi=300, bbox_inches='tight')
    plt.close()

def main():
    try:
        # Setup output directory
        output_dir = setup_output_dir()
        logger.info(f"Saving visualizations to {output_dir}")
        
        # Load and visualize historical data
        historical_file = Path("output/historical_htf/historical_htf.parquet")
        if historical_file.exists():
            logger.info("Generating historical data visualizations...")
            historical_df = pd.read_parquet(historical_file)
            plot_historical_trends(historical_df, output_dir)
        
        # Load and visualize projected data
        projected_file = Path("output/projected_htf/projected_htf.parquet")
        if projected_file.exists():
            logger.info("Generating projected data visualizations...")
            projected_df = pd.read_parquet(projected_file)
            plot_projected_trends(projected_df, output_dir)
            
        logger.info("Visualization generation complete!")
        
    except Exception as e:
        logger.error(f"Error generating visualizations: {str(e)}")
        raise

if __name__ == "__main__":
    main() 