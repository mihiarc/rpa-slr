"""
Script to verify Hawaii shoreline data directly from shapefile.
"""

import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import yaml

def plot_hawaii_shoreline(shapefile_path: Path, county_path: Path, stations_path: Path, output_path: Path):
    """Create a focused map of Hawaiian shoreline data from shapefile.
    
    Args:
        shapefile_path: Path to Pacific Islands shapefile
        county_path: Path to county shapefile
        stations_path: Path to Hawaii tide stations config
        output_path: Path to save the map
    """
    # Read the shapefile and filter for Hawaii
    print("Reading shoreline shapefile...")
    shoreline = gpd.read_file(shapefile_path)
    hawaii_shoreline = shoreline[shoreline['FIPS_ALPHA'] == 'HI'].copy()
    print(f"Total Hawaii features: {len(hawaii_shoreline)}")
    
    # Read and filter county shapefile for Hawaii
    print("Reading county shapefile...")
    counties = gpd.read_file(county_path)
    hawaii_counties = counties[counties['STATEFP'] == '15'].copy()
    print(f"Hawaii counties: {len(hawaii_counties)}")
    
    # Read tide stations configuration
    print("Reading tide stations configuration...")
    with open(stations_path) as f:
        stations_config = yaml.safe_load(f)
    
    # Get the actual bounds of the data
    bounds = hawaii_shoreline.total_bounds
    print(f"Data bounds: {bounds}")
    
    # Set up Hawaii-specific projection
    projection = ccrs.Mercator(
        central_longitude=-157.0,
        min_latitude=18.5,
        max_latitude=23.0
    )
    
    # Create figure and axis with simplified dimensions
    fig, ax = plt.subplots(
        figsize=(12, 8),
        subplot_kw={'projection': projection}
    )
    
    # Add context features
    ax.add_feature(cfeature.LAND.with_scale('10m'), facecolor='#E6E6E6', alpha=0.3)
    ax.add_feature(cfeature.OCEAN.with_scale('10m'), facecolor='#FFFFFF', alpha=0.3)
    ax.add_feature(cfeature.COASTLINE.with_scale('10m'), linewidth=0.5, color='gray')
    
    # Project the data
    hawaii_shoreline = hawaii_shoreline.to_crs(projection.proj4_init)
    hawaii_counties = hawaii_counties.to_crs(projection.proj4_init)
    
    # Plot county boundaries
    hawaii_counties.boundary.plot(
        ax=ax,
        color='#404040',
        linewidth=2.0,
        linestyle='--',
        alpha=0.7
    )
    
    # Add county labels
    for idx, row in hawaii_counties.iterrows():
        if row['NAME'] != 'Honolulu':  # Skip Honolulu label
            centroid = row.geometry.centroid
            ax.text(
                centroid.x, centroid.y,
                row['NAME'],
                color='#404040',
                fontsize=10,
                fontweight='bold',
                ha='center',
                va='center',
                transform=projection
            )
    
    # Plot shoreline features
    hawaii_shoreline.plot(
        ax=ax,
        color='#FF3366',
        linewidth=3.0,
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
        # Add label with station name and ID
        ax.text(lon + 0.05, lat,
                f"{station['name']} ({station_id})",
                color='#0066CC',
                fontsize=8,
                transform=ccrs.PlateCarree(),
                ha='left',
                va='center')
    
    # Set map extent to show all Hawaiian islands
    ax.set_extent([-161, -154.5, 18.5, 22.5], crs=ccrs.PlateCarree())
    
    # Add gridlines with simplified labels
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
    plt.title('Hawaiian Islands Shoreline, Counties, and Tide Gauges', pad=10)
    
    # Save the map with minimal padding
    plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.1)
    plt.close()
    
    print(f"\nSaved map to {output_path}")

def main():
    shapefile_path = Path('data/raw/shapefile_shoreline/Pacific_Islands/Pacific_Islands.shp')
    county_path = Path('data/raw/shapefile_county/tl_2024_us_county.shp')
    stations_path = Path('config/hawaii_tide_stations.yaml')
    output_path = Path('output/maps/verification/hawaii_counties_verification.png')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    plot_hawaii_shoreline(shapefile_path, county_path, stations_path, output_path)

if __name__ == '__main__':
    main() 