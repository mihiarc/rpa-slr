"""
Script to visualize Hawaii imputation coverage using choropleth maps.
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
    COASTAL_COUNTIES_FILE,
    REFERENCE_POINTS_FILE
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

def plot_hawaii_coverage(
    imputation_file: Path,
    counties_file: Path,
    output_path: Path):
    """Create a choropleth map of Hawaii imputation coverage.
    
    Args:
        imputation_file: Path to imputation structure file
        counties_file: Path to coastal counties file
        output_path: Path to save the map
    """
    # Load configurations
    with open(CONFIG_DIR / "region_mappings.yaml") as f:
        region_config = yaml.safe_load(f)['regions']['hawaii']
    
    with open(CONFIG_DIR / "tide_stations" / "hawaii_tide_stations.yaml") as f:
        station_config = yaml.safe_load(f)
    
    # Load data
    print("Loading data...")
    imputation_df = pd.read_parquet(imputation_file)
    counties = gpd.read_parquet(PROCESSED_DIR / "county.parquet")
    
    # Filter for Hawaii
    region_counties = counties[counties['region'] == 'hawaii'].copy()
    region_imputation = imputation_df[imputation_df['region'] == 'hawaii'].copy()
    
    # Calculate county coverage metrics
    coverage_metrics = calculate_county_coverage(region_imputation)
    
    # Merge metrics with counties
    region_counties = region_counties.merge(
        coverage_metrics,
        left_on='GEOID',
        right_on='county_fips',
        how='left'
    )
    
    # Set up Hawaii-specific projection
    projection = ccrs.AlbersEqualArea(
        central_longitude=-157.0,
        central_latitude=20.0,
        standard_parallels=(19.0, 21.0)
    )
    
    # Set figure style
    plt.style.use('seaborn-v0_8')
    
    # Create figure with gridspec for layout control
    fig = plt.figure(figsize=(15, 14))  # Made slightly taller to accommodate caption
    gs = fig.add_gridspec(2, 1, height_ratios=[0.9, 0.1], hspace=0.05)
    
    # Create map axis
    ax = fig.add_subplot(gs[0], projection=projection)
    
    # Add minimal context features
    ax.coastlines(resolution='10m', color='#666666', linewidth=0.8)
    
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
            marker='o',  # Changed to circle
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
    
    # Set map extent with padding
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
    
    # Add title and statistics with better styling
    n_counties = len(region_counties)
    n_stations = len(station_config['stations'])
    mean_coverage = coverage_metrics['coverage_score'].mean()
    
    title = (
        f"Hawaii Tide Gauge Coverage\n"
        f"{n_counties} Counties, {n_stations} Stations\n"
        f"Mean Coverage Score: {mean_coverage:.3f}"
    )
    ax.set_title(title, pad=10, size=14, weight='bold')
    
    # Add caption in its own axis
    caption_ax = fig.add_subplot(gs[1])
    caption_ax.axis('off')
    
    caption = (
        "Tide gauge coverage assessment for Hawaii counties. "
        "Coverage scores (CS) combine the number of tide stations (n) and their mean weight (w̄) as CS = n × w̄, "
        "where weights decrease with distance from each station. "
        "Counties are categorized as high (CS > 1.5), medium (1.0 < CS ≤ 1.5), or low coverage (CS ≤ 1.0)."
    )
    
    caption_ax.text(
        0.5, 0.7,  # Moved up slightly from center
        caption,
        ha='center',
        va='center',
        fontsize=11,
        color='black',
        transform=caption_ax.transAxes,
        wrap=True,
        family='serif'  # More academic font
    )
    
    # Save the map (remove the previous adjustments since we're using gridspec)
    plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.1)
    plt.close()
    
    print(f"\nSaved map to {output_path}")
    
    # Print detailed metrics
    print("\nCounty Coverage Metrics:")
    coverage_metrics = coverage_metrics.merge(
        region_counties[['GEOID', 'NAME']],
        left_on='county_fips',
        right_on='GEOID'
    )
    print(coverage_metrics.sort_values('coverage_score', ascending=False).to_string())

def main():
    """Generate Hawaii imputation coverage visualization."""
    # Find most recent Hawaii imputation file
    imputation_files = list(IMPUTATION_DIR.glob("imputation_structure_hawaii_*.parquet"))
    if not imputation_files:
        print("No Hawaii imputation structure files found")
        return
        
    imputation_file = sorted(imputation_files)[-1]  # Get most recent
    
    # Set up output path
    output_dir = IMPUTATION_MAPS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "hawaii_coverage.png"
    
    plot_hawaii_coverage(
        imputation_file=imputation_file,
        counties_file=COASTAL_COUNTIES_FILE,
        output_path=output_path
    )

if __name__ == '__main__':
    main() 