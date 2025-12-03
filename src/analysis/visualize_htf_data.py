"""
Script to visualize county-level high tide flooding data.
Creates time series of regional trends and county-level statistics.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import glob
import logging

# Note: Do not call logging.basicConfig here - let the application configure logging
logger = logging.getLogger(__name__)

def load_county_data(data_dir: Path) -> pd.DataFrame:
    """Load and combine all county HTF data files."""
    all_files = glob.glob(str(data_dir / 'county_htf_values_*.parquet'))
    dfs = []
    for file in all_files:
        df = pd.read_parquet(file)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def clean_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Clean duplicate entries by keeping the non-NaN values."""
    # Sort by flood_days being non-NaN (True values first) and missing_days (ascending)
    df['is_valid'] = ~df['flood_days'].isna()
    df = df.sort_values(['county_fips', 'year', 'is_valid', 'missing_days'], 
                       ascending=[True, True, False, True])
    
    # Keep first occurrence (valid data if exists, otherwise least missing days)
    df = df.drop_duplicates(subset=['county_fips', 'year'], keep='first')
    df = df.drop(columns=['is_valid'])
    return df

def calculate_recent_averages(df: pd.DataFrame, start_year: int = 2015) -> pd.DataFrame:
    """Calculate average flood days for recent years by county."""
    # Filter for recent years and valid data
    recent_df = df[df['year'] >= start_year].copy()
    recent_df = recent_df[recent_df['missing_days'] < 180]  # Filter out years with too much missing data
    
    # Calculate averages
    county_avgs = recent_df.groupby('county_fips').agg({
        'flood_days': 'mean',
        'region': 'first'  # Keep the region
    }).reset_index()
    
    return county_avgs

def plot_regional_trends(df: pd.DataFrame, output_dir: Path):
    """Generate time series plots of regional flooding trends."""
    # Filter for years >= 1970 and valid data
    df_filtered = df[df['year'] >= 1970]
    annual_regional = df_filtered[df_filtered['missing_days'] < 180].groupby(['year', 'region'])['flood_days'].mean().reset_index()
    
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=annual_regional, x='year', y='flood_days', hue='region')
    plt.title('Average Annual Flood Days by Region (1970-Present)')
    plt.xlabel('Year')
    plt.ylabel('Average Flood Days')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_dir / 'regional_trends.png')
    plt.close()

def plot_county_trends(df: pd.DataFrame, output_dir: Path):
    """Create a heatmap of flood days for top 20 counties."""
    # Filter for years >= 1970
    df_filtered = df[df['year'] >= 1970]
    
    # Identify top 20 counties by average flood days in recent years
    recent_avgs = calculate_recent_averages(df_filtered)
    top_counties = recent_avgs.nlargest(20, 'flood_days')['county_fips'].tolist()
    
    # Filter data for these counties and create pivot table
    county_data = df_filtered[df_filtered['county_fips'].isin(top_counties)].copy()
    county_data = county_data[county_data['missing_days'] < 180]  # Filter out years with too much missing data
    
    pivot_data = county_data.pivot(index='county_fips', columns='year', values='flood_days')
    
    plt.figure(figsize=(15, 8))
    sns.heatmap(pivot_data, cmap='YlOrRd', cbar_kws={'label': 'Flood Days'})
    plt.title('Annual Flood Days for Top 20 Most Impacted Counties (1970-Present)')
    plt.xlabel('Year')
    plt.ylabel('County FIPS')
    plt.tight_layout()
    plt.savefig(output_dir / 'county_trends_heatmap.png')
    plt.close()

def generate_summary_stats(df: pd.DataFrame, output_dir: Path):
    """Generate summary statistics and save to a text file."""
    # Filter for years >= 1970
    df_filtered = df[df['year'] >= 1970]
    
    with open(output_dir / 'summary_stats.txt', 'w') as f:
        # Overall statistics
        f.write("Overall Statistics (1970-Present):\n")
        f.write(f"Total number of counties: {df_filtered['county_fips'].nunique()}\n")
        f.write(f"Year range: {df_filtered['year'].min()} to {df_filtered['year'].max()}\n")
        f.write(f"Number of regions: {df_filtered['region'].nunique()}\n\n")
        
        # Recent trends (2015-2019)
        recent_avgs = calculate_recent_averages(df_filtered)
        f.write("Top 10 Counties by Recent Flooding (2015-2019):\n")
        top_10 = recent_avgs.nlargest(10, 'flood_days')
        for _, row in top_10.iterrows():
            f.write(f"County {row['county_fips']}: {row['flood_days']:.2f} average flood days (Region: {row['region']})\n")

def export_to_csv(df: pd.DataFrame, output_dir: Path):
    """Export cleaned data to CSV file."""
    # Filter for years >= 1970
    df_filtered = df[df['year'] >= 1970].copy()
    
    # Sort by county_fips and year for easier reading
    df_filtered = df_filtered.sort_values(['county_fips', 'year'])
    
    # Save to CSV
    output_file = output_dir / 'county_htf_data_1970_present.csv'
    df_filtered.to_csv(output_file, index=False)
    logger.info(f"Data exported to {output_file}")

def main():
    """Run the visualization pipeline."""
    try:
        # Setup paths
        base_dir = Path('.')
        data_dir = base_dir / 'output' / 'county_htf_historical'
        output_dir = base_dir / 'output' / 'analysis'
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load data
        logger.info("Loading county HTF data...")
        df = load_county_data(data_dir)
        
        logger.info("Cleaning duplicates...")
        df = clean_duplicates(df)
        
        # Export cleaned data to CSV
        logger.info("Exporting cleaned data to CSV...")
        export_to_csv(df, output_dir)
        
        # Create visualizations
        logger.info("Generating regional trends plot...")
        plot_regional_trends(df, output_dir)
        
        logger.info("Generating county trends heatmap...")
        plot_county_trends(df, output_dir)
        
        logger.info("Generating summary statistics...")
        generate_summary_stats(df, output_dir)
        
        logger.info("Visualization complete!")
        
    except Exception as e:
        logger.error(f"Error in visualization: {str(e)}")
        raise

if __name__ == "__main__":
    main() 