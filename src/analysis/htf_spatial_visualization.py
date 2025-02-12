"""
Script to generate visualizations for HTF flood data analysis.

This script creates maps and plots focusing on flood patterns and severity.
"""

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from pathlib import Path
import logging
from typing import Tuple, List
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_region_projection(region: str) -> ccrs.Projection:
    """Get the appropriate cartopy projection for a region."""
    projections = {
        'alaska': ccrs.AlbersEqualArea(central_longitude=-160),
        'hawaii': ccrs.AlbersEqualArea(central_longitude=-157),
        'pacific_islands': ccrs.AlbersEqualArea(central_longitude=145),
        'puerto_rico': ccrs.AlbersEqualArea(central_longitude=-66),
        'virgin_islands': ccrs.AlbersEqualArea(central_longitude=-64.5)
    }
    return projections.get(region, ccrs.AlbersEqualArea(central_longitude=-96))

def plot_regional_flood_map(region: str, htf_df: pd.DataFrame, 
                          counties: gpd.GeoDataFrame, output_dir: Path) -> None:
    """Create a choropleth map of flood days for a region."""
    region_df = htf_df[htf_df['region'] == region].copy()
    region_counties = counties[counties['region'] == region].copy()
    
    # Merge flood data with county geometries
    merged = region_counties.merge(region_df, on='county_fips')
    
    # Create figure with appropriate projection
    proj = get_region_projection(region)
    fig, ax = plt.subplots(figsize=(12, 8), 
                          subplot_kw={'projection': proj})
    
    # Add map features
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.STATES)
    
    # Plot flood days
    merged.plot(column='total_flood_days', 
               ax=ax,
               legend=True,
               legend_kwds={'label': 'Total Flood Days'},
               cmap='YlOrRd')
    
    # Adjust title and layout
    plt.title(f'Total Flood Days - {region.replace("_", " ").title()}')
    plt.tight_layout()
    
    # Save figure
    output_file = output_dir / f'flood_days_{region}.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()

def plot_flood_severity_distribution(htf_df: pd.DataFrame, 
                                   output_dir: Path) -> None:
    """Create plots showing the distribution of flood severity."""
    # Calculate flood severity percentages
    severity_data = pd.DataFrame({
        'Minor': htf_df['minor_flood_days'].sum(),
        'Moderate': htf_df['moderate_flood_days'].sum(),
        'Major': htf_df['major_flood_days'].sum()
    }, index=['Flood Days'])
    
    # Create stacked bar plot
    ax = severity_data.T.plot(kind='bar', figsize=(10, 6))
    plt.title('Distribution of Flood Severity')
    plt.xlabel('Severity Level')
    plt.ylabel('Number of Flood Days')
    
    # Add percentage labels
    total = severity_data.sum().sum()
    for i, v in enumerate(severity_data.iloc[0]):
        percentage = (v / total) * 100
        ax.text(i, v, f'{percentage:.1f}%', 
                ha='center', va='bottom')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'flood_severity_distribution.png', 
                dpi=300, bbox_inches='tight')
    plt.close()

def plot_regional_flood_comparison(htf_df: pd.DataFrame, 
                                 output_dir: Path) -> None:
    """Create plots comparing flood patterns across regions."""
    # Calculate regional averages
    regional_stats = htf_df.groupby('region_display').agg({
        'total_flood_days': 'mean',
        'major_flood_days': 'mean',
        'moderate_flood_days': 'mean',
        'minor_flood_days': 'mean'
    }).round(1)
    
    # Create multi-bar plot
    ax = regional_stats.plot(kind='bar', figsize=(12, 6))
    plt.title('Average Flood Days by Region and Severity')
    plt.xlabel('Region')
    plt.ylabel('Average Number of Flood Days')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Flood Type')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'regional_flood_comparison.png', 
                dpi=300, bbox_inches='tight')
    plt.close()

def plot_major_flood_hotspots(htf_df: pd.DataFrame, 
                            counties: gpd.GeoDataFrame,
                            output_dir: Path) -> None:
    """Create a map highlighting areas with high major flood days."""
    # Calculate major flood percentages
    htf_df['major_flood_pct'] = (htf_df['major_flood_days'] / 
                                htf_df['total_flood_days'] * 100)
    
    # Merge with county geometries
    merged = counties.merge(htf_df, on='county_fips')
    
    # Create figure
    fig, ax = plt.subplots(figsize=(15, 10), 
                          subplot_kw={'projection': ccrs.AlbersEqualArea(
                              central_longitude=-96)})
    
    # Add map features
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.STATES)
    
    # Plot major flood percentage
    merged.plot(column='major_flood_pct',
               ax=ax,
               legend=True,
               legend_kwds={'label': 'Major Flood Days (%)'},
               cmap='RdPu')
    
    plt.title('Major Flood Day Percentage by County')
    plt.tight_layout()
    
    # Save figure
    plt.savefig(output_dir / 'major_flood_hotspots.png', 
                dpi=300, bbox_inches='tight')
    plt.close()

def setup_output_dir() -> Path:
    """Create and return the output directory for visualizations."""
    output_dir = Path('output/analysis')
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir

def main():
    try:
        # Setup output directory
        output_dir = setup_output_dir()
        logger.info(f"Output directory: {output_dir}")
        
        # Load historical HTF data
        historical_dir = Path("output/historical")
        if not historical_dir.exists():
            raise FileNotFoundError("Historical data directory not found")
        
        # Load and combine regional data
        dfs = []
        for file in historical_dir.glob("historical_htf_*.parquet"):
            df = pd.read_parquet(file)
            dfs.append(df)
        
        htf_df = pd.concat(dfs, ignore_index=True)
        
        # Load county geometries
        counties = gpd.read_file("data/processed/county_geometries.geojson")
        
        # Generate visualizations
        logger.info("Generating flood severity distribution plot...")
        plot_flood_severity_distribution(htf_df, output_dir)
        
        logger.info("Generating regional flood comparison plot...")
        plot_regional_flood_comparison(htf_df, output_dir)
        
        logger.info("Generating major flood hotspots map...")
        plot_major_flood_hotspots(htf_df, counties, output_dir)
        
        logger.info("Generating regional flood maps...")
        for region in htf_df['region'].unique():
            logger.info(f"Processing {region}...")
            plot_regional_flood_map(region, htf_df, counties, output_dir)
        
        logger.info("Visualization generation complete!")
        
    except Exception as e:
        logger.error(f"Error in visualization: {str(e)}")
        raise

if __name__ == "__main__":
    main() 