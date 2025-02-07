"""
Script to verify Alaska tide stations and counties.
"""

import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import yaml

def plot_alaska_coverage(county_path: Path, stations_path: Path, output_path: Path):
    """Create a map of Alaska counties and tide stations.
    
    Args:
        county_path: Path to county shapefile
        stations_path: Path to tide stations config
        output_path: Path to save the map
    """
    # Read and filter county shapefile for Alaska
    print("Reading county shapefile...")
    counties = gpd.read_file(county_path)
    ak_counties = counties[counties['STATEFP'] == '02'].copy()  # Alaska FIPS code is 02
    print(f"Alaska counties: {len(ak_counties)}")
    
    # Read tide stations configuration
    print("Reading tide stations configuration...")
    with open(stations_path) as f:
        stations_config = yaml.safe_load(f)
    
    # Set up Alaska-specific projection
    projection = ccrs.AlbersEqualArea(
        central_longitude=-150.0,
        central_latitude=60.0,
        standard_parallels=(55.0, 65.0)
    )
    
    # Create figure and axis
    fig, ax = plt.subplots(
        figsize=(15, 10),
        subplot_kw={'projection': projection}
    )
    
    # Add context features
    ax.add_feature(cfeature.LAND.with_scale('50m'), facecolor='#E6E6E6', alpha=0.3)
    ax.add_feature(cfeature.OCEAN.with_scale('50m'), facecolor='#FFFFFF', alpha=0.3)
    ax.add_feature(cfeature.COASTLINE.with_scale('50m'), edgecolor='gray', linewidth=0.5)
    
    # Project and plot county boundaries
    ak_counties = ak_counties.to_crs(projection.proj4_init)
    ak_counties.boundary.plot(
        ax=ax,
        color='#404040',
        linewidth=1.5,
        linestyle='--',
        alpha=0.7
    )
    
    # Add tide gauge stations with different colors by region
    region_colors = {
        'Southeast Alaska': '#1f77b4',
        'South Central Alaska': '#2ca02c',
        'Western Alaska': '#d62728',
        'Northern Alaska': '#9467bd'
    }
    
    # Plot stations
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
        ax.text(lon + 0.5, lat,
                station['name'],
                color=color,
                fontsize=8,
                transform=ccrs.PlateCarree(),
                ha='left',
                va='center')
    
    # Set map extent to show all of Alaska
    ax.set_extent([-180, -130, 51, 72], crs=ccrs.PlateCarree())
    
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
    plt.title('Alaska Counties and Tide Stations', pad=10)
    
    # Add legend for regions
    legend_elements = [plt.Line2D([0], [0], marker='o', color='w',
                                markerfacecolor=color, label=region, markersize=8)
                      for region, color in region_colors.items()]
    ax.legend(handles=legend_elements, loc='lower right', 
             title='Tide Station Regions', fontsize=8)
    
    # Save the map
    plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.1)
    plt.close()
    
    print(f"\nSaved map to {output_path}")

def main():
    county_path = Path('data/raw/shapefile_county/tl_2024_us_county.shp')
    stations_path = Path('config/alaska_tide_stations.yaml')
    output_path = Path('output/maps/verification/alaska_verification.png')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    plot_alaska_coverage(county_path, stations_path, output_path)

if __name__ == '__main__':
    main() 