"""
Script to generate visualizations for the imputation structure report.
Adapts verification scripts to show imputation weights and coverage.
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
from typing import Dict, List, Tuple
import seaborn as sns
import os

from src.config import (
    CONFIG_DIR,
    PROCESSED_DIR,
    IMPUTATION_MAPS_DIR,
    IMPUTATION_DIR,
    COUNTY_FILE,
    COASTAL_COUNTIES_FILE,
    REFERENCE_POINTS_FILE
)

logger = logging.getLogger(__name__)

def load_region_mappings() -> Dict:
    """Load region mappings from configuration."""
    with open(CONFIG_DIR / "region_mappings.yaml") as f:
        return yaml.safe_load(f)['regions']

def load_tide_stations(region: str) -> Dict:
    """Load tide station configuration for a region."""
    station_file = CONFIG_DIR / "tide_stations" / f"{region}_tide_stations.yaml"
    with open(station_file) as f:
        return yaml.safe_load(f)

def get_region_projection(region: str, region_config: Dict) -> ccrs.AlbersEqualArea:
    """Get cartopy projection for a region."""
    bounds = region_config.get('bounds', {})
    if not bounds:
        return None
        
    center_lon = (bounds['min_lon'] + bounds['max_lon']) / 2
    center_lat = (bounds['min_lat'] + bounds['max_lat']) / 2
    lat_span = bounds['max_lat'] - bounds['min_lat']
    
    return ccrs.AlbersEqualArea(
        central_longitude=center_lon,
        central_latitude=center_lat,
        standard_parallels=(
            center_lat - lat_span/4,
            center_lat + lat_span/4
        )
    )

def get_region_bounds(region: str, region_config: Dict) -> Tuple[float, float, float, float]:
    """Get map bounds for a region."""
    bounds = region_config.get('bounds', {})
    if not bounds:
        return None
        
    # Add some padding to the bounds
    lon_pad = (bounds['max_lon'] - bounds['min_lon']) * 0.1
    lat_pad = (bounds['max_lat'] - bounds['min_lat']) * 0.1
    
    return (
        bounds['min_lon'] - lon_pad,
        bounds['max_lon'] + lon_pad,
        bounds['min_lat'] - lat_pad,
        bounds['max_lat'] + lat_pad
    )

def get_representative_points(
    reference_points: gpd.GeoDataFrame,
    imputation_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Get representative points for each county.
    
    Args:
        reference_points: GeoDataFrame of all reference points
        imputation_df: DataFrame containing imputation structure
        
    Returns:
        GeoDataFrame with one representative point per county
    """
    # Group by county and get point with highest weight
    county_points = []
    
    for county_fips in reference_points['county_fips'].unique():
        # Get points for this county
        county_ref_points = reference_points[
            reference_points['county_fips'] == county_fips
        ]
        
        # Get weights for these points
        county_weights = imputation_df[
            imputation_df['reference_point_id'].isin(county_ref_points.index)
        ]
        
        if len(county_weights) > 0:
            # Find point with highest weight
            max_weight_point = county_weights.loc[
                county_weights.groupby('reference_point_id')['weight'].idxmax()
            ]
            
            # Get the reference point with highest weight
            ref_point = county_ref_points.loc[
                max_weight_point['reference_point_id'].iloc[0]
            ]
            
            # Add dominant station info
            dominant_station = max_weight_point.loc[
                max_weight_point['weight'].idxmax()
            ]
            
            county_points.append({
                'county_fips': county_fips,
                'county_name': ref_point['county_name'],
                'state_fips': ref_point['state_fips'],
                'region': ref_point['region'],
                'geometry': ref_point.geometry,
                'dominant_station': dominant_station['station_id'],
                'station_name': dominant_station['station_name'],
                'weight': dominant_station['weight']
            })
    
    return gpd.GeoDataFrame(county_points, crs=reference_points.crs)

def plot_imputation_structure(
    region: str,
    imputation_df: pd.DataFrame,
    reference_points: gpd.GeoDataFrame,
    counties: gpd.GeoDataFrame,
    output_dir: Path) -> Path:
    """Create a map showing imputation structure for a region.
    
    Args:
        region: Region identifier
        imputation_df: DataFrame containing imputation structure
        reference_points: GeoDataFrame of reference points
        counties: GeoDataFrame of coastal counties
        output_dir: Directory to save output
        
    Returns:
        Path to saved map
    """
    logger.info(f"\nGenerating map for region: {region}")
    
    # Load region configuration
    region_mappings = load_region_mappings()
    region_config = region_mappings.get(region)
    
    if not region_config:
        logger.warning(f"No configuration found for region: {region}")
        return None
    
    # Load tide station configuration
    try:
        station_config = load_tide_stations(region)
    except Exception as e:
        logger.warning(f"Could not load tide station config for {region}: {e}")
        return None
    
    # Get region-specific projection and bounds
    projection = get_region_projection(region, region_config)
    bounds = get_region_bounds(region, region_config)
    
    if not projection or not bounds:
        logger.warning(f"No projection or bounds defined for region: {region}")
        return None
    
    # Create figure and axis
    fig, ax = plt.subplots(
        figsize=(15, 12),
        subplot_kw={'projection': projection}
    )
    
    # Add context features
    ax.add_feature(cfeature.LAND.with_scale('10m'), facecolor='#E6E6E6', alpha=0.3)
    ax.add_feature(cfeature.OCEAN.with_scale('10m'), facecolor='#FFFFFF', alpha=0.3)
    ax.add_feature(cfeature.COASTLINE.with_scale('10m'), edgecolor='gray', linewidth=0.5)
    
    # Filter data for this region
    region_points = reference_points[reference_points['region'] == region].copy()
    region_counties = counties[counties['region'] == region].copy()
    region_imputation = imputation_df[imputation_df['region'] == region].copy()
    
    # Get representative points
    rep_points = get_representative_points(region_points, region_imputation)
    
    # Project data
    region_counties = region_counties.to_crs(projection.proj4_init)
    rep_points = rep_points.to_crs(projection.proj4_init)
    
    # Plot county boundaries
    region_counties.boundary.plot(
        ax=ax,
        color='#404040',
        linewidth=1.5,
        linestyle='--',
        alpha=0.7
    )
    
    # Get unique stations and assign colors
    stations = region_imputation.drop_duplicates('station_id')
    
    # Add station locations from config
    stations_with_loc = []
    for _, station in stations.iterrows():
        station_info = station_config['stations'].get(station['station_id'], {})
        if station_info and 'location' in station_info:
            station_data = station.to_dict()
            station_data.update({
                'station_lon': float(station_info['location']['lon']),
                'station_lat': float(station_info['location']['lat']),
                'station_name': station_info.get('name', station_data['station_name'])  # Use config name if available
            })
            stations_with_loc.append(station_data)
    
    if not stations_with_loc:
        logger.error(f"No station locations found for region {region}")
        return None
        
    stations = pd.DataFrame(stations_with_loc)
    n_stations = len(stations)
    colors = plt.cm.Set3(np.linspace(0, 1, n_stations))
    station_colors = dict(zip(stations['station_id'], colors))
    
    # Plot tide stations with labels
    legend_elements = []
    for _, station in stations.iterrows():
        color = station_colors[station['station_id']]
        
        # Plot station marker
        ax.plot(
            station['station_lon'],
            station['station_lat'],
            marker='s',
            color=color,
            markersize=10,
            transform=ccrs.PlateCarree(),
            zorder=5,
            label=station['station_name']
        )
        
        # Add label
        ax.text(
            station['station_lon'] + 0.05,
            station['station_lat'],
            station['station_name'],
            color=color,
            fontsize=8,
            transform=ccrs.PlateCarree(),
            ha='left',
            va='center'
        )
        
        # Add to legend
        legend_elements.append(
            plt.Line2D([0], [0], marker='o', color='w',
                      markerfacecolor=color,
                      label=f"{station['station_name']}",
                      markersize=8)
        )
    
    # Plot representative points colored by dominant station
    for _, point in rep_points.iterrows():
        color = station_colors[point['dominant_station']]
        
        ax.plot(
            point.geometry.x,
            point.geometry.y,
            marker='o',
            color=color,
            markersize=8,
            alpha=0.7,
            transform=ccrs.PlateCarree()
        )
    
    # Set map extent
    ax.set_extent(bounds, crs=ccrs.PlateCarree())
    
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
    
    # Add legend
    ax.legend(
        handles=legend_elements,
        title='Tide Stations',
        loc='upper right',
        fontsize=8,
        title_fontsize=10
    )
    
    # Add title and statistics
    n_counties = len(region_counties)
    n_stations = len(stations)
    avg_weight = region_imputation['weight'].mean()
    
    title = (
        f"{region.replace('_', ' ').title()} Imputation Structure\n"
        f"{n_counties} Counties, {n_stations} Stations\n"
        f"Average Station Weight: {avg_weight:.3f}"
    )
    plt.title(title, pad=10)
    
    # Save the map
    output_path = output_dir / f"{region}_imputation_structure.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.1)
    plt.close()
    
    logger.info(f"Saved map to {output_path}")
    return output_path

def generate_report(
    imputation_dir: Path = IMPUTATION_DIR,
    output_dir: Path = IMPUTATION_MAPS_DIR,
    region: str = None
):
    """Generate visualization report for imputation structure.
    
    Args:
        imputation_dir: Directory containing imputation structure files
        output_dir: Directory to save output maps
        region: Specific region to generate report for
    """
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load reference data
    logger.info("Loading reference data...")
    reference_points = gpd.read_parquet(REFERENCE_POINTS_FILE)
    counties = gpd.read_parquet(COASTAL_COUNTIES_FILE)
    
    # Find most recent imputation file for each region
    imputation_files = list(imputation_dir.glob("imputation_structure_*.parquet"))
    if not imputation_files:
        logger.error("No imputation structure files found")
        return
    
    # Process each region's imputation structure
    for imp_file in imputation_files:
        try:
            # Extract full region name from filename
            # Format is imputation_structure_REGION_TIMESTAMP.parquet
            parts = imp_file.stem.split('_')
            timestamp_idx = next(i for i, part in enumerate(parts) if part.startswith('202'))
            region = '_'.join(parts[2:timestamp_idx])  # Join all parts between 'structure' and timestamp
            
            # Load imputation structure
            logger.info(f"Processing {region}...")
            imputation_df = pd.read_parquet(imp_file)
            
            # Generate map
            plot_imputation_structure(
                region=region,
                imputation_df=imputation_df,
                reference_points=reference_points,
                counties=counties,
                output_dir=output_dir
            )
            
        except Exception as e:
            logger.error(f"Error processing {imp_file.name}: {str(e)}")
            continue

def main():
    """Generate imputation structure visualization report."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        generate_report()
        logger.info("Completed imputation structure report generation")
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        raise

if __name__ == "__main__":
    main() 