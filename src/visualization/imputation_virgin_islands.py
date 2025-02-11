"""
Script to visualize Virgin Islands imputation coverage using choropleth maps.
"""

import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import yaml
from pathlib import Path
import logging
import seaborn as sns

from src.config import (
    CONFIG_DIR,
    PROCESSED_DIR,
    IMPUTATION_MAPS_DIR,
    IMPUTATION_DIR,
    COUNTY_FILE
)

def calculate_county_coverage(imputation_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate coverage metrics for each county.
    
    Args:
        imputation_df: DataFrame containing imputation structure
        
    Returns:
        DataFrame with county-level coverage metrics
    """
    # Group by county and calculate metrics
    county_metrics = []
    
    for county_fips in imputation_df['county_fips'].unique():
        county_data = imputation_df[imputation_df['county_fips'] == county_fips]
        
        # Calculate metrics
        n_stations = len(county_data['station_id'].unique())
        max_weight = county_data['weight'].max()
        mean_weight = county_data['weight'].mean()
        
        county_metrics.append({
            'county_fips': county_fips,
            'n_stations': n_stations,
            'max_weight': max_weight,
            'mean_weight': mean_weight,
            'coverage_score': mean_weight * n_stations  # Combined metric
        })
    
    return pd.DataFrame(county_metrics)

def plot_virgin_islands_coverage(
    imputation_file: Path,
    output_path: Path):
    """Create a choropleth map of Virgin Islands imputation coverage.
    
    Args:
        imputation_file: Path to imputation structure file
        output_path: Path to save the map
    """
    # Load configurations
    with open(CONFIG_DIR / "region_mappings.yaml") as f:
        region_config = yaml.safe_load(f)['regions']['virgin_islands']
    
    with open(CONFIG_DIR / "tide_stations" / "virgin_islands_tide_stations.yaml") as f:
        station_config = yaml.safe_load(f)
    
    # Load data
    print("Loading data...")
    
    # Load Virgin Islands imputation data
    print("\nLoading Virgin Islands imputation data...")
    imputation_df = pd.read_parquet(imputation_file)
    print(f"Virgin Islands imputation records: {len(imputation_df)}")
    
    # Load counties from main county file and filter for Virgin Islands
    print("\nLoading county geometries...")
    counties = gpd.read_parquet(COUNTY_FILE)
    
    # Get Virgin Islands state FIPS code
    virgin_islands_fips = '78'  # Virgin Islands territory FIPS code
    
    # Filter counties to Virgin Islands
    region_counties = counties[
        (counties['STATEFP'] == virgin_islands_fips) &
        (counties['GEOID'].isin(imputation_df['county_fips']))
    ].copy()
    
    print("\nRegion counts:")
    print(f"Virgin Islands: {len(region_counties)} counties")
    
    # Calculate county coverage metrics
    coverage_metrics = calculate_county_coverage(imputation_df)
    
    # Print coverage metrics summary
    print("\nCoverage metrics summary:")
    print(f"Number of counties with metrics: {len(coverage_metrics)}")
    
    # Merge metrics with counties
    region_counties = region_counties.merge(
        coverage_metrics,
        left_on='GEOID',
        right_on='county_fips',
        how='left'
    )
    
    # Print merge results
    print(f"\nNumber of counties after merge: {len(region_counties)}")
    print("Counties without coverage metrics:")
    missing_metrics = region_counties[region_counties['coverage_score'].isna()]
    if len(missing_metrics) > 0:
        print(missing_metrics[['GEOID', 'NAME', 'STATEFP']].to_string())
    
    # Set up Virgin Islands-specific projection
    projection = ccrs.AlbersEqualArea(
        central_longitude=-64.8,  # Centered on Virgin Islands
        central_latitude=18.3,    # Centered on Virgin Islands
        standard_parallels=(17.5, 19.0)  # Adjusted for Virgin Islands' latitude
    )
    
    # Set figure style
    plt.style.use('seaborn-v0_8')
    
    # Create figure with wider aspect ratio for Virgin Islands
    fig = plt.figure(figsize=(15, 8))  # Adjusted for the archipelago's layout
    ax = fig.add_subplot(1, 1, 1, projection=projection)
    
    # Add context features with improved styling
    ax.add_feature(cfeature.LAND.with_scale('10m'), facecolor='#E6E6E6', alpha=0.3)
    ax.add_feature(cfeature.OCEAN.with_scale('10m'), facecolor='#FFFFFF', alpha=0.3)
    ax.coastlines(resolution='10m', color='#666666', linewidth=0.8)
    
    # Project counties
    region_counties = region_counties.to_crs(projection.proj4_init)
    
    # Create continuous color map for coverage scores
    norm = plt.Normalize(
        vmin=region_counties['coverage_score'].min(),
        vmax=region_counties['coverage_score'].max()
    )
    cmap = plt.cm.Blues
    
    # Plot counties with continuous color scale
    region_counties.plot(
        ax=ax,
        column='coverage_score',
        cmap=cmap,
        norm=norm,
        edgecolor='#666666',
        linewidth=0.5,
        legend=True,
        legend_kwds={
            'label': 'Coverage Score',
            'orientation': 'horizontal',
            'shrink': 0.6,
            'aspect': 30,
            'pad': 0.01
        }
    )
    
    # Add station locations with better styling
    station_points = []
    station_names = []
    for station_id, station_info in station_config['stations'].items():
        # Add station marker
        point = ax.plot(
            station_info['location']['lon'],
            station_info['location']['lat'],
            marker='o',
            color='#800000',  # Darker red
            markeredgecolor='white',
            markeredgewidth=1,
            markersize=8,
            transform=ccrs.PlateCarree(),
            zorder=5,
            label='Tide Station' if len(station_points) == 0 else ''  # Label only first station
        )
        station_points.extend(point)
        
        # Add station name with better text styling
        # Adjust text placement based on location to avoid overlap
        lon_offset = 0.05
        lat_offset = 0  # Initialize vertical offset
        
        # Adjust text placement based on station location
        if station_info['location']['lon'] > -64.8:  # Eastern stations
            lon_offset = 0.1
            ha = 'left'
        else:  # Western stations
            lon_offset = -0.1
            ha = 'right'
            
        # Alternate vertical offsets for nearby stations
        if len(station_names) % 2 == 0:
            lat_offset = 0.05
        else:
            lat_offset = -0.05
            
        ax.text(
            station_info['location']['lon'] + lon_offset,
            station_info['location']['lat'] + lat_offset,
            station_info['name'],
            color='black',
            fontsize=9,
            fontweight='bold',
            transform=ccrs.PlateCarree(),
            ha=ha,
            va='center',
            bbox=dict(
                facecolor='white',
                edgecolor='none',
                alpha=0.7,
                pad=1
            )
        )
        station_names.append(station_info['name'])
    
    # Add legend for tide stations
    station_legend = ax.legend(
        handles=[station_points[0]],
        labels=['Tide Station'],
        title='Stations',
        loc='upper right',
        fontsize=10,
        title_fontsize=12,
        frameon=True,
        framealpha=0.9,
        edgecolor='#666666'
    )
    station_legend.get_frame().set_facecolor('white')
    
    # Set map extent with padding - Use region config bounds
    bounds = region_config['bounds']
    lon_pad = (bounds['max_lon'] - bounds['min_lon']) * 0.1
    lat_pad = (bounds['max_lat'] - bounds['min_lat']) * 0.1
    
    ax.set_extent([
        bounds['min_lon'] - lon_pad,
        bounds['max_lon'] + lon_pad,
        bounds['min_lat'] - lat_pad,
        bounds['max_lat'] + lat_pad
    ], crs=ccrs.PlateCarree())
    
    # Add gridlines with better styling
    gl = ax.gridlines(
        draw_labels=True,
        x_inline=False,
        y_inline=False,
        linewidth=0.5,
        color='gray',
        alpha=0.3,
        linestyle=':'
    )
    gl.top_labels = False
    gl.right_labels = False
    gl.xlabel_style = {'size': 9}
    gl.ylabel_style = {'size': 9}
    
    # Add title and caption with better styling
    n_counties = len(region_counties)
    n_stations = len(station_config['stations'])
    mean_coverage = coverage_metrics['coverage_score'].mean()
    
    title = (
        f"U.S. Virgin Islands Tide Gauge Coverage\n"
        f"{n_counties} Counties, {n_stations} Stations\n"
        f"Mean Coverage Score: {mean_coverage:.3f}"
    )
    ax.set_title(title, pad=10, size=14, weight='bold')
    
    # Add caption using figure suptitle for automatic placement
    caption = (
        "Tide gauge coverage assessment for U.S. Virgin Islands counties. "
        "Coverage scores (CS) combine the number of tide stations (n) and their mean weight (w̄) as CS = n × w̄, "
        "where weights decrease with distance from each station. "
        "Counties are categorized as high (CS > 1.5), medium (1.0 < CS ≤ 1.5), or low coverage (CS ≤ 1.0)."
    )
    
    plt.figtext(0.5, 0.02, caption, ha='center', va='bottom', fontsize=11, 
                wrap=True, family='serif')
    
    # Adjust layout to prevent caption overlap
    plt.tight_layout()
    
    # Save the map
    plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.1)
    plt.close()
    
    print(f"\nSaved map to {output_path}")
    
    # Print detailed metrics
    print("\nCounty Coverage Metrics:")
    coverage_metrics = coverage_metrics.merge(
        region_counties[['GEOID', 'NAME', 'STATEFP']],
        left_on='county_fips',
        right_on='GEOID'
    )
    print(coverage_metrics.sort_values('coverage_score', ascending=False).to_string())

def main():
    """Generate Virgin Islands imputation coverage visualization."""
    # Find most recent Virgin Islands imputation file
    imputation_files = list(IMPUTATION_DIR.glob("imputation_structure_virgin_islands_*.parquet"))
    if not imputation_files:
        print("No Virgin Islands imputation structure files found")
        return
        
    imputation_file = sorted(imputation_files)[-1]  # Get most recent
    
    # Set up output path
    output_dir = IMPUTATION_MAPS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "virgin_islands_coverage.png"
    
    plot_virgin_islands_coverage(
        imputation_file=imputation_file,
        output_path=output_path
    )

if __name__ == '__main__':
    main() 