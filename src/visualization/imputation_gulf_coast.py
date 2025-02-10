"""
Script to visualize Gulf Coast imputation coverage using choropleth maps.
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
    PROCESSED_DATA_DIR,
    OUTPUT_DIR,
    MAPS_OUTPUT_DIR,
    IMPUTATION_OUTPUT_DIR,
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

def plot_gulf_coast_coverage(
    imputation_file: Path,
    output_path: Path):
    """Create a choropleth map of Gulf Coast imputation coverage.
    
    Args:
        imputation_file: Path to imputation structure file
        output_path: Path to save the map
    """
    # Load configurations
    with open(CONFIG_DIR / "region_mappings.yaml") as f:
        region_config = yaml.safe_load(f)['regions']['gulf_coast']
    
    with open(CONFIG_DIR / "tide_stations" / "gulf_coast_tide_stations.yaml") as f:
        station_config = yaml.safe_load(f)
    
    # Load data
    print("Loading data...")
    
    # Load Gulf Coast imputation data
    print("\nLoading Gulf Coast imputation data...")
    imputation_df = pd.read_parquet(imputation_file)
    print(f"Gulf Coast imputation records: {len(imputation_df)}")
    
    # Load counties from main county file and filter for Gulf Coast states
    print("\nLoading county geometries...")
    counties = gpd.read_parquet(PROCESSED_DATA_DIR / "county.parquet")
    
    # Get Gulf Coast state FIPS codes
    gulf_states = {'01': 'AL', '12': 'FL', '22': 'LA', '28': 'MS', '48': 'TX'}
    
    # Filter counties to Gulf Coast states and coastal counties
    region_counties = counties[
        counties['STATEFP'].isin(gulf_states.keys()) &
        counties['GEOID'].isin(imputation_df['county_fips'])
    ].copy()
    
    print("\nRegion counts by state:")
    state_counts = region_counties['STATEFP'].value_counts()
    for state_fips, count in state_counts.items():
        print(f"{gulf_states[state_fips]}: {count} counties")
    
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
    
    # Set up Gulf Coast-specific projection
    projection = ccrs.AlbersEqualArea(
        central_longitude=-90.0,  # Centered on Gulf Coast
        central_latitude=28.0,
        standard_parallels=(26.0, 30.0)
    )
    
    # Set figure style
    plt.style.use('seaborn-v0_8')
    
    # Create figure
    fig = plt.figure(figsize=(15, 12))
    ax = fig.add_subplot(1, 1, 1, projection=projection)
    
    # Add context features with improved styling
    ax.add_feature(cfeature.LAND.with_scale('50m'), facecolor='#E6E6E6', alpha=0.3)
    ax.add_feature(cfeature.OCEAN.with_scale('50m'), facecolor='#FFFFFF', alpha=0.3)
    ax.coastlines(resolution='50m', color='#666666', linewidth=0.8)
    
    # Project counties
    region_counties = region_counties.to_crs(projection.proj4_init)
    
    # Create bins for coverage scores
    scores = region_counties['coverage_score'].dropna()
    bins = [scores.min() - 0.1, scores.quantile(0.33), scores.quantile(0.66), scores.max()]
    labels = ['Low Coverage', 'Medium Coverage', 'High Coverage']
    
    # Add coverage category
    region_counties['coverage_category'] = pd.cut(
        region_counties['coverage_score'],
        bins=bins,
        labels=labels,
        include_lowest=True
    )
    
    # Get color palette - using a more muted palette
    palette = sns.color_palette('Blues', n_colors=4)[1:]  # Skip the lightest color
    color_dict = dict(zip(labels, palette))
    
    # Plot counties colored by coverage category
    for category in labels:
        category_counties = region_counties[region_counties['coverage_category'] == category]
        category_counties.plot(
            ax=ax,
            color=color_dict[category],
            label=category,
            edgecolor='#666666',
            linewidth=0.5
        )
    
    # Add legend for coverage categories with nicer styling
    coverage_legend_elements = [
        plt.Rectangle(
            (0,0), 1, 1, 
            facecolor=color_dict[label],
            edgecolor='#666666',
            linewidth=0.5,
            label=label
        )
        for label in labels
    ]
    
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
            label='Tide Station'
        )
        station_points.extend(point)
        
        # Add station name with better text styling
        ax.text(
            station_info['location']['lon'] + 0.05,
            station_info['location']['lat'],
            station_info['name'],
            color='black',
            fontsize=9,
            fontweight='bold',
            transform=ccrs.PlateCarree(),
            ha='left',
            va='center',
            bbox=dict(
                facecolor='white',
                edgecolor='none',
                alpha=0.7,
                pad=1
            )
        )
        station_names.append(station_info['name'])
    
    # Create combined legend with better styling
    legend = ax.legend(
        handles=coverage_legend_elements + [station_points[0]],
        labels=labels + ['Tide Station'],
        title='Coverage Categories',
        loc='upper right',
        fontsize=10,
        title_fontsize=12,
        frameon=True,
        framealpha=0.9,
        edgecolor='#666666'
    )
    legend.get_frame().set_facecolor('white')
    
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
        f"Gulf Coast Tide Gauge Coverage\n"
        f"{n_counties} Counties, {n_stations} Stations\n"
        f"Mean Coverage Score: {mean_coverage:.3f}"
    )
    ax.set_title(title, pad=10, size=14, weight='bold')
    
    # Add caption using figure suptitle for automatic placement
    caption = (
        "Tide gauge coverage assessment for Gulf Coast counties. "
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
    """Generate Gulf Coast imputation coverage visualization."""
    # Find most recent Gulf Coast imputation file
    imputation_files = list(IMPUTATION_OUTPUT_DIR.glob("imputation_structure_gulf_coast_*.parquet"))
    if not imputation_files:
        print("No Gulf Coast imputation structure files found")
        return
        
    imputation_file = sorted(imputation_files)[-1]  # Get most recent
    
    # Set up output path
    output_dir = MAPS_OUTPUT_DIR / "imputation"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "gulf_coast_coverage.png"
    
    plot_gulf_coast_coverage(
        imputation_file=imputation_file,
        output_path=output_path
    )

if __name__ == '__main__':
    main() 