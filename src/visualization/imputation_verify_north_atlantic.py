"""
Script to verify North Atlantic shoreline data with counties and tide stations.
"""

import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import yaml

def plot_north_atlantic_shoreline(shapefile_path: Path, county_path: Path, stations_path: Path, output_path: Path):
    """Create a focused map of North Atlantic shoreline data from shapefile.
    
    Args:
        shapefile_path: Path to shoreline shapefile
        county_path: Path to county shapefile
        stations_path: Path to tide stations config
        output_path: Path to save the map
    """
    # Read the shapefile and filter for North Atlantic states
    print("Reading shoreline shapefile...")
    shoreline = gpd.read_file(shapefile_path)
    na_states = ['ME', 'NH', 'MA', 'RI', 'CT']  # North Atlantic states
    na_shoreline = shoreline[shoreline['FIPS_ALPHA'].isin(na_states)].copy()
    print(f"Total North Atlantic features: {len(na_shoreline)}")
    
    # Read and filter county shapefile for North Atlantic states
    print("Reading county shapefile...")
    counties = gpd.read_file(county_path)
    state_fips = ['23', '33', '25', '44', '09']  # FIPS codes for ME, NH, MA, RI, CT
    na_counties = counties[counties['STATEFP'].isin(state_fips)].copy()
    print(f"North Atlantic counties: {len(na_counties)}")
    
    # Read tide stations configuration
    print("Reading tide stations configuration...")
    with open(stations_path) as f:
        stations_config = yaml.safe_load(f)
    
    # Get the actual bounds of the data
    bounds = na_shoreline.total_bounds
    print(f"Data bounds: {bounds}")
    
    # Set up North Atlantic-specific projection
    projection = ccrs.AlbersEqualArea(
        central_longitude=-69.5,  # Centered on region
        central_latitude=43.0,
        standard_parallels=(41.0, 45.0)
    )
    
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
    na_shoreline = na_shoreline.to_crs(projection.proj4_init)
    na_counties = na_counties.to_crs(projection.proj4_init)
    
    # Plot county boundaries
    na_counties.boundary.plot(
        ax=ax,
        color='#404040',
        linewidth=1.5,
        linestyle='--',
        alpha=0.7
    )
    
    # Plot shoreline features
    na_shoreline.plot(
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
    
    # Set map extent to show all of North Atlantic region
    ax.set_extent([-73.5, -66.5, 41.0, 45.5], crs=ccrs.PlateCarree())
    
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
    plt.title('North Atlantic Shoreline Coverage Analysis', pad=10)
    
    # Save the map
    plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.1)
    plt.close()
    
    print(f"\nSaved map to {output_path}")

def main():
    shapefile_path = Path('data/raw/shapefile_shoreline/North_Atlantic/North_Atlantic.shp')
    county_path = Path('data/raw/shapefile_county/tl_2024_us_county.shp')
    stations_path = Path('config/north_atlantic_tide_stations.yaml')
    output_path = Path('output/maps/verification/north_atlantic_counties_verification.png')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    plot_north_atlantic_shoreline(shapefile_path, county_path, stations_path, output_path)

if __name__ == '__main__':
    main() 