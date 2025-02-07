"""
Script to verify Mid-Atlantic shoreline data with counties and tide stations.
"""

import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import yaml

def plot_mid_atlantic_shoreline(shapefile_path: Path, county_path: Path, stations_path: Path, output_path: Path):
    """Create a focused map of Mid-Atlantic shoreline data from shapefile.
    
    Args:
        shapefile_path: Path to shoreline shapefile
        county_path: Path to county shapefile
        stations_path: Path to tide stations config
        output_path: Path to save the map
    """
    # Read the shapefile and filter for Mid-Atlantic states
    print("Reading shoreline shapefile...")
    shoreline = gpd.read_file(shapefile_path)
    ma_states = ['NJ', 'DE', 'MD', 'VA']  # Mid-Atlantic states
    ma_shoreline = shoreline[shoreline['FIPS_ALPHA'].isin(ma_states)].copy()
    print(f"Total Mid-Atlantic features: {len(ma_shoreline)}")
    
    # Read and filter county shapefile for Mid-Atlantic states
    print("Reading county shapefile...")
    counties = gpd.read_file(county_path)
    state_fips = ['34', '10', '24', '51']  # FIPS codes for NJ, DE, MD, VA
    ma_counties = counties[counties['STATEFP'].isin(state_fips)].copy()
    print(f"Mid-Atlantic counties: {len(ma_counties)}")
    
    # Read tide stations configuration
    print("Reading tide stations configuration...")
    with open(stations_path) as f:
        stations_config = yaml.safe_load(f)
    
    # Get the actual bounds of the data
    bounds = ma_shoreline.total_bounds
    print(f"Data bounds: {bounds}")
    
    # Set up Mid-Atlantic-specific projection
    projection = ccrs.AlbersEqualArea(
        central_longitude=-76.0,  # Centered on Chesapeake Bay
        central_latitude=38.5,
        standard_parallels=(37.0, 40.0)
    )
    
    # Create figure and axis
    fig, ax = plt.subplots(
        figsize=(12, 15),  # Taller figure to show full Chesapeake Bay
        subplot_kw={'projection': projection}
    )
    
    # Add context features
    ax.add_feature(cfeature.LAND.with_scale('10m'), facecolor='#E6E6E6', alpha=0.3)
    ax.add_feature(cfeature.OCEAN.with_scale('10m'), facecolor='#FFFFFF', alpha=0.3)
    ax.add_feature(cfeature.COASTLINE.with_scale('10m'), edgecolor='gray', linewidth=0.5)
    
    # Project and plot the data
    ma_shoreline = ma_shoreline.to_crs(projection.proj4_init)
    ma_counties = ma_counties.to_crs(projection.proj4_init)
    
    # Plot county boundaries
    ma_counties.boundary.plot(
        ax=ax,
        color='#404040',
        linewidth=1.5,
        linestyle='--',
        alpha=0.7
    )
    
    # Plot shoreline features
    ma_shoreline.plot(
        ax=ax,
        color='#FF3366',
        linewidth=2.0,
        alpha=1.0
    )
    
    # Add tide gauge stations with different colors by region
    region_colors = {
        'New Jersey Coast': '#1f77b4',
        'Delaware Bay': '#2ca02c',
        'Maryland Coast': '#9467bd',
        'Upper Chesapeake': '#ff7f0e',
        'Lower Chesapeake': '#d62728',
        'Virginia Coast': '#8c564b'
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
        ax.text(lon + 0.05, lat,
                station['name'],
                color=color,
                fontsize=8,
                transform=ccrs.PlateCarree(),
                ha='left',
                va='center')
    
    # Set map extent to show all of Mid-Atlantic region
    ax.set_extent([-76.5, -73.5, 36.5, 40.5], crs=ccrs.PlateCarree())
    
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
    plt.title('Mid-Atlantic Shoreline Coverage Analysis', pad=10)
    
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
    shapefile_path = Path('data/raw/shapefile_shoreline/North_Atlantic/North_Atlantic.shp')
    county_path = Path('data/raw/shapefile_county/tl_2024_us_county.shp')
    stations_path = Path('config/mid_atlantic_tide_stations.yaml')
    output_path = Path('output/maps/verification/mid_atlantic_counties_verification.png')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    plot_mid_atlantic_shoreline(shapefile_path, county_path, stations_path, output_path)

if __name__ == '__main__':
    main() 