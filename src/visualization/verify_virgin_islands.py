"""
Script to verify US Virgin Islands shoreline data with tide stations.
"""

import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import yaml

def plot_virgin_islands_shoreline(shapefile_path: Path, stations_path: Path, output_path: Path):
    """Create a focused map of US Virgin Islands shoreline data from shapefile.
    
    Args:
        shapefile_path: Path to shoreline shapefile
        stations_path: Path to tide stations config
        output_path: Path to save the map
    """
    # Read the shapefile and filter for Virgin Islands
    print("Reading shoreline shapefile...")
    shoreline = gpd.read_file(shapefile_path)
    vi_shoreline = shoreline[shoreline['FIPS_ALPHA'] == 'VI'].copy()
    print(f"Total Virgin Islands features: {len(vi_shoreline)}")
    
    # Read tide stations configuration
    print("Reading tide stations configuration...")
    with open(stations_path) as f:
        stations_config = yaml.safe_load(f)
    
    # Get the actual bounds of the data
    bounds = vi_shoreline.total_bounds
    print(f"Data bounds: {bounds}")
    
    # Set up Virgin Islands-specific projection
    projection = ccrs.PlateCarree()
    
    # Create figure and axis
    fig, ax = plt.subplots(
        figsize=(12, 8),
        subplot_kw={'projection': projection}
    )
    
    # Add context features
    ax.add_feature(cfeature.LAND.with_scale('10m'), facecolor='#E6E6E6', alpha=0.3)
    ax.add_feature(cfeature.OCEAN.with_scale('10m'), facecolor='#FFFFFF', alpha=0.3)
    ax.add_feature(cfeature.COASTLINE.with_scale('10m'), edgecolor='gray', linewidth=0.5)
    
    # Project and plot the data
    vi_shoreline = vi_shoreline.to_crs(projection.proj4_init)
    
    # Plot shoreline features
    vi_shoreline.plot(
        ax=ax,
        color='#FF3366',
        linewidth=2.0,
        alpha=1.0
    )
    
    # Add tide gauge stations with different colors by region
    region_colors = {
        'St. Thomas': '#1f77b4',
        'St. John': '#2ca02c',
        'St. Croix': '#d62728'
    }
    
    for station_id, station in stations_config['stations'].items():
        lon = station['location']['lon']
        lat = station['location']['lat']
        region = station['region']
        color = region_colors[region]
        
        # Plot marker
        ax.plot(lon, lat, 
                marker='o',
                color=color,
                markersize=8,
                transform=ccrs.PlateCarree(),
                zorder=5)
        
        # Add label with station name
        ax.text(lon + 0.01, lat,
                station['name'],
                color=color,
                fontsize=8,
                transform=ccrs.PlateCarree(),
                ha='left',
                va='center')
    
    # Set map extent to show all of Virgin Islands
    ax.set_extent([-65.1, -64.5, 17.6, 18.5], crs=ccrs.PlateCarree())
    
    # Add gridlines
    gl = ax.gridlines(
        draw_labels=True,
        x_inline=False,
        y_inline=False,
        linewidth=0.5,
        color='gray',
        alpha=0.3
    )
    gl.top_labels = False
    gl.right_labels = False
    
    # Add title
    plt.title('US Virgin Islands Shoreline Coverage Analysis', pad=10)
    
    # Add legend for regions
    legend_elements = [plt.Line2D([0], [0], marker='o', color='w',
                                markerfacecolor=color, label=region, markersize=8)
                      for region, color in region_colors.items()]
    ax.legend(handles=legend_elements, loc='upper right', 
             title='Tide Station Regions', fontsize=8)
    
    # Save the map
    plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.1)
    plt.close()
    
    print(f"\nSaved map to {output_path}")

def main():
    shapefile_path = Path('data/raw/shapefile_shoreline/Southeast_Caribbean/Southeast_Caribbean.shp')
    stations_path = Path('config/virgin_islands_tide_stations.yaml')
    output_path = Path('output/maps/verification/virgin_islands_verification.png')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    plot_virgin_islands_shoreline(shapefile_path, stations_path, output_path)

if __name__ == '__main__':
    main() 