"""
Script to verify Pacific Islands shoreline data with tide stations.
"""

import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import yaml

def plot_pacific_islands_shoreline(shapefile_path: Path, stations_path: Path, output_path: Path):
    """Create a focused map of Pacific Islands shoreline data from shapefile.
    
    Args:
        shapefile_path: Path to shoreline shapefile
        stations_path: Path to tide stations config
        output_path: Path to save the map
    """
    # Read the shapefile and filter for Pacific Islands territories
    print("Reading shoreline shapefile...")
    shoreline = gpd.read_file(shapefile_path)
    # Filter for Guam (GU), Northern Mariana Islands (MP), American Samoa (AS)
    pi_states = ['GU', 'MP', 'AS']
    pi_shoreline = shoreline[shoreline['FIPS_ALPHA'].isin(pi_states)].copy()
    print(f"Total Pacific Islands features: {len(pi_shoreline)}")
    
    # Read tide stations configuration
    print("Reading tide stations configuration...")
    with open(stations_path) as f:
        stations_config = yaml.safe_load(f)
    
    # Get the actual bounds of the data
    bounds = pi_shoreline.total_bounds
    print(f"Data bounds: {bounds}")
    
    # Create three subplots for different island groups
    fig = plt.figure(figsize=(15, 12))
    
    # Define regions and their extents
    regions = {
        'Marianas': {
            'extent': [144.5, 146.0, 13.2, 15.5],
            'position': [0.1, 0.35, 0.35, 0.55],
            'title': 'Guam and Northern Mariana Islands'
        },
        'American_Samoa': {
            'extent': [-171.0, -170.0, -14.5, -14.0],
            'position': [0.55, 0.35, 0.35, 0.55],
            'title': 'American Samoa'
        }
    }
    
    # Add tide gauge stations with different colors by region
    region_colors = {
        'Guam': '#1f77b4',
        'Northern Mariana Islands': '#2ca02c',
        'American Samoa': '#d62728'
    }
    
    # Create a subplot for each region
    for region_name, region_info in regions.items():
        ax = fig.add_axes(region_info['position'], projection=ccrs.PlateCarree())
        
        # Add context features
        ax.add_feature(cfeature.LAND.with_scale('10m'), facecolor='#E6E6E6', alpha=0.3)
        ax.add_feature(cfeature.OCEAN.with_scale('10m'), facecolor='#FFFFFF', alpha=0.3)
        ax.add_feature(cfeature.COASTLINE.with_scale('10m'), edgecolor='gray', linewidth=0.5)
        
        # Plot shoreline features
        pi_shoreline.plot(
            ax=ax,
            color='#FF3366',
            linewidth=2.0,
            alpha=1.0
        )
        
        # Plot stations for this region
        for station_id, station in stations_config['stations'].items():
            lon = station['location']['lon']
            lat = station['location']['lat']
            region = station['region']
            
            # Check if station is within current map extent
            if (lon >= region_info['extent'][0] and lon <= region_info['extent'][1] and
                lat >= region_info['extent'][2] and lat <= region_info['extent'][3]):
                
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
        
        # Set map extent
        ax.set_extent(region_info['extent'], crs=ccrs.PlateCarree())
        
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
        
        # Add title for this region
        ax.set_title(region_info['title'])
    
    # Add main title
    fig.suptitle('Pacific Islands Shoreline Coverage Analysis', y=0.95, fontsize=14)
    
    # Add legend for regions
    legend_elements = [plt.Line2D([0], [0], marker='o', color='w',
                                markerfacecolor=color, label=region, markersize=8)
                      for region, color in region_colors.items()]
    fig.legend(handles=legend_elements,
              loc='upper center',
              bbox_to_anchor=(0.5, 0.25),
              title='Tide Station Regions',
              fontsize=8)
    
    # Save the map
    plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.1)
    plt.close()
    
    print(f"\nSaved map to {output_path}")

def main():
    shapefile_path = Path('data/raw/shapefile_shoreline/Pacific_Islands/Pacific_Islands.shp')
    stations_path = Path('config/pacific_islands_tide_stations.yaml')
    output_path = Path('output/maps/verification/pacific_islands_verification.png')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    plot_pacific_islands_shoreline(shapefile_path, stations_path, output_path)

if __name__ == '__main__':
    main() 