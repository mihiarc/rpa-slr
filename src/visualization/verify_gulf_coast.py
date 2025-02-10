"""
Script to verify Gulf Coast shoreline data with counties and tide stations.
"""

import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import yaml

def plot_gulf_coast_shoreline(shapefile_path: Path, county_path: Path, stations_path: Path, output_path: Path):
    """Create a focused map of Gulf Coast shoreline data from shapefile.
    
    Args:
        shapefile_path: Path to shoreline shapefile
        county_path: Path to county shapefile
        stations_path: Path to tide stations config
        output_path: Path to save the map
    """
    # Read the shapefile and filter for Gulf Coast states
    print("Reading shoreline shapefile...")
    shoreline = gpd.read_file(shapefile_path)
    gulf_states = ['FL', 'AL', 'MS', 'LA', 'TX']  # Gulf Coast states
    gulf_shoreline = shoreline[shoreline['FIPS_ALPHA'].isin(gulf_states)].copy()
    print(f"Total Gulf Coast features: {len(gulf_shoreline)}")
    
    # Read and filter county shapefile for Gulf Coast states
    print("Reading county shapefile...")
    counties = gpd.read_file(county_path)
    state_fips = ['12', '01', '28', '22', '48']  # FIPS codes for FL, AL, MS, LA, TX
    gulf_counties = counties[counties['STATEFP'].isin(state_fips)].copy()
    print(f"Gulf Coast counties: {len(gulf_counties)}")
    
    # Read tide stations configuration
    print("Reading tide stations configuration...")
    with open(stations_path) as f:
        stations_config = yaml.safe_load(f)
    
    # Get the actual bounds of the data
    bounds = gulf_shoreline.total_bounds
    print(f"Data bounds: {bounds}")
    
    # Set up Gulf Coast-specific projection
    projection = ccrs.AlbersEqualArea(
        central_longitude=-90.0,  # Centered on Gulf Coast
        central_latitude=27.0,    # Moved center south to better capture Key West
        standard_parallels=(24.0, 30.0)  # Adjusted parallels to cover full range
    )
    
    # Create figure and axis
    fig, ax = plt.subplots(
        figsize=(15, 10),  # Increased height to accommodate southern extent
        subplot_kw={'projection': projection}
    )
    
    # Add context features
    ax.add_feature(cfeature.LAND.with_scale('10m'), facecolor='#E6E6E6', alpha=0.3)
    ax.add_feature(cfeature.OCEAN.with_scale('10m'), facecolor='#FFFFFF', alpha=0.3)
    ax.add_feature(cfeature.COASTLINE.with_scale('10m'), edgecolor='gray', linewidth=0.5)
    
    # Project and plot the data
    gulf_shoreline = gulf_shoreline.to_crs(projection.proj4_init)
    gulf_counties = gulf_counties.to_crs(projection.proj4_init)
    
    # Plot county boundaries
    gulf_counties.boundary.plot(
        ax=ax,
        color='#404040',
        linewidth=1.5,
        linestyle='--',
        alpha=0.7
    )
    
    # Plot shoreline features
    gulf_shoreline.plot(
        ax=ax,
        color='#FF3366',
        linewidth=2.0,
        alpha=1.0
    )
    
    # Add tide gauge stations
    for station_id, station in stations_config['stations'].items():
        lon = station['location']['lon']
        lat = station['location']['lat']
        
        # Plot marker
        ax.plot(lon, lat, 
                marker='o',
                color='#0066CC',
                markersize=8,
                transform=ccrs.PlateCarree(),
                zorder=5)
        
        # Add label with station name
        ax.text(lon + 0.05, lat,
                station['name'],
                color='#0066CC',
                fontsize=8,
                transform=ccrs.PlateCarree(),
                ha='left',
                va='center')
    
    # Set map extent to show all of Gulf Coast region including Key West
    ax.set_extent([-98.0, -80.0, 24.0, 31.0], crs=ccrs.PlateCarree())
    
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
    plt.title('Gulf Coast Shoreline Coverage Analysis', pad=10)
    
    # Save the map
    plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.1)
    plt.close()
    
    print(f"\nSaved map to {output_path}")

def main():
    shapefile_path = Path('data/raw/shapefile_shoreline/Gulf_Of_Mexico/Gulf_Of_Mexico.shp')
    county_path = Path('data/raw/shapefile_county/tl_2024_us_county.shp')
    stations_path = Path('config/gulf_coast_tide_stations.yaml')
    output_path = Path('output/maps/verification/gulf_coast_counties_verification.png')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    plot_gulf_coast_shoreline(shapefile_path, county_path, stations_path, output_path)

if __name__ == '__main__':
    main() 