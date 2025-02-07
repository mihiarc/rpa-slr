"""
Module for generating regional maps showing gauge-county relationships and coverage density.
Helps visualize how dense or sparse the gauge coverage is for each region.
"""

import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
from pathlib import Path
import logging
from typing import Dict, List, Tuple
import yaml
import pandas as pd
from shapely.geometry import Point, box
import pyproj

from src.config import OUTPUT_DIR, CONFIG_DIR

logger = logging.getLogger(__name__)

class RegionMapper:
    """Generates regional maps showing gauge-county relationships."""
    
    def __init__(self, 
                 output_dir: Path = OUTPUT_DIR / "imputation" / "maps",
                 fips_config: Path = CONFIG_DIR / "fips_mappings.yaml"):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load FIPS mappings and region definitions
        with open(fips_config) as f:
            config = yaml.safe_load(f)
            self.regions = config['regions']
            self.state_names = config['state_names']
            self.field_mappings = config['field_mappings']
        
        # Initialize coordinate transformers
        self.transformer = pyproj.Transformer.from_crs(
            "EPSG:4326",  # WGS84 (lat/lon)
            "EPSG:3857",  # Web Mercator
            always_xy=True
        )
    
    def _transform_bounds(self, bounds: Dict[str, float]) -> Tuple[float, float, float, float]:
        """
        Transform bounds from WGS84 (lat/lon) to Web Mercator.
        
        Args:
            bounds: Dictionary with minx, miny, maxx, maxy in WGS84
            
        Returns:
            Tuple of (minx, miny, maxx, maxy) in Web Mercator
        """
        minx, miny = self.transformer.transform(bounds['minx'], bounds['miny'])
        maxx, maxy = self.transformer.transform(bounds['maxx'], bounds['maxy'])
        return minx, miny, maxx, maxy
    
    def _create_fips_codes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create FIPS codes based on field mappings with proper zero-padding.
        
        State FIPS codes are 2 digits, County FIPS codes are 5 digits (2 state + 3 county).
        """
        df = df.copy()
        
        # Create county FIPS (5 digits: 2 state + 3 county)
        county_fields = self.field_mappings['county_fips']
        df['county_fips'] = (
            df[county_fields[0]].astype(str).str.zfill(2) +  # State part (2 digits)
            df[county_fields[1]].astype(str).str.zfill(3)    # County part (3 digits)
        )
        
        # Create state FIPS (2 digits)
        state_field = self.field_mappings['state_fips']
        df['state_fips'] = df[state_field].astype(str).str.zfill(2)
        
        return df
    
    def _plot_county_gauge_connection(self,
                                   ax: plt.Axes,
                                   county: gpd.GeoSeries,
                                   gauge_stations: gpd.GeoDataFrame,
                                   gauge_id: str) -> None:
        """
        Plot connection between a county and its assigned gauge station.
        
        Args:
            ax: Matplotlib axes to plot on
            county: GeoSeries containing county data
            gauge_stations: GeoDataFrame of gauge stations
            gauge_id: ID of the gauge station to connect to
        """
        if pd.notnull(gauge_id):
            gauge_match = gauge_stations[gauge_stations['station_id'] == gauge_id]
            if not gauge_match.empty:
                gauge = gauge_match.iloc[0]
                ax.plot(
                    [county.geometry.centroid.x, gauge.geometry.x],
                    [county.geometry.centroid.y, gauge.geometry.y],
                    'b-',
                    alpha=0.2,
                    linewidth=1,
                    zorder=1  # Ensure lines are drawn below points
                )
            else:
                logger.warning(
                    f"Gauge station {gauge_id} not found for county {county['county_fips']} "
                    f"in {county['NAME']} County"
                )
    
    def _create_region_map(self,
                          region: str,
                          counties: gpd.GeoDataFrame,
                          gauge_stations: gpd.GeoDataFrame,
                          imputation_data: gpd.GeoDataFrame) -> Path:
        """
        Create map for a specific region showing gauge-county relationships.
        
        Args:
            region: Name of the region
            counties: GeoDataFrame of county geometries
            gauge_stations: GeoDataFrame of gauge station locations
            imputation_data: GeoDataFrame with imputation structure
            
        Returns:
            Path to saved map file
        """
        # Set up the figure
        fig, ax = plt.subplots(figsize=(15, 10))
        
        # Filter data for region
        region_info = self.regions[region]
        region_counties = counties[counties['state_fips'].isin(region_info['states'])]
        
        if region_counties.empty:
            logger.warning(f"No counties found in region {region}")
            return None
        
        # Project to Web Mercator for basemap compatibility
        try:
            region_counties = region_counties.to_crs(epsg=3857)
            region_gauges = gauge_stations.to_crs(epsg=3857)
        except Exception as e:
            logger.error(f"Error projecting data for region {region}: {str(e)}")
            return None
        
        # Plot counties colored by gauge coverage
        try:
            region_counties.plot(
                ax=ax,
                column='total_weight',  # Use total weight as a measure of coverage
                cmap='YlOrRd',
                legend=True,
                legend_kwds={'label': 'Gauge Coverage Weight'},
                alpha=0.6
            )
        except Exception as e:
            logger.error(f"Error plotting counties for region {region}: {str(e)}")
            plt.close(fig)
            return None
        
        # Plot gauge stations
        try:
            region_gauges.plot(
                ax=ax,
                color='blue',
                markersize=50,
                marker='*',
                label='Tide Gauges'
            )
        except Exception as e:
            logger.error(f"Error plotting gauge stations for region {region}: {str(e)}")
            plt.close(fig)
            return None
        
        # Add connections between counties and their assigned gauges
        for _, county in region_counties.iterrows():
            try:
                self._plot_county_gauge_connection(ax, county, region_gauges, county['gauge_id_1'])
                self._plot_county_gauge_connection(ax, county, region_gauges, county['gauge_id_2'])
            except Exception as e:
                logger.error(
                    f"Error plotting connections for county {county['county_fips']} "
                    f"in {county['NAME']} County: {str(e)}"
                )
                continue
        
        # Transform and set bounds
        try:
            bounds = region_info['bounds']
            minx, miny, maxx, maxy = self._transform_bounds(bounds)
            ax.set_xlim(minx, maxx)
            ax.set_ylim(miny, maxy)
        except Exception as e:
            logger.error(f"Error setting map bounds for region {region}: {str(e)}")
            plt.close(fig)
            return None
        
        # Add basemap
        try:
            ctx.add_basemap(
                ax,
                source=ctx.providers.CartoDB.Positron,
                zoom=6
            )
        except Exception as e:
            logger.error(f"Error adding basemap for region {region}: {str(e)}")
            # Continue without basemap, as this is not critical
        
        # Customize the plot
        ax.set_title(f"{region} Region - Gauge Coverage Analysis")
        ax.legend()
        
        # Save the map
        try:
            output_file = self.output_dir / f"{region.lower().replace(' ', '_')}_coverage.png"
            fig.savefig(
                output_file,
                dpi=300,                # High resolution
                bbox_inches='tight',     # Trim whitespace
                pad_inches=0.2,         # Add small padding
                facecolor='white',      # White background
                edgecolor='none',       # No edge color
                transparent=False       # Ensure white background is saved
            )
            plt.close(fig)
            return output_file
        except Exception as e:
            logger.error(f"Error saving map for region {region}: {str(e)}")
            plt.close(fig)
            return None
    
    def generate_maps(self,
                     counties_file: Path,
                     gauge_stations_file: Path,
                     imputation_file: Path) -> Dict[str, Path]:
        """
        Generate maps for all regions.
        
        Args:
            counties_file: Path to county geometries file
            gauge_stations_file: Path to gauge stations YAML file
            imputation_file: Path to imputation structure file
            
        Returns:
            Dictionary mapping region names to output file paths
        """
        logger.info("Loading input data...")
        counties = gpd.read_parquet(counties_file)
        imputation_data = gpd.read_parquet(imputation_file)
        
        # Create FIPS codes using field mappings
        counties = self._create_fips_codes(counties)
        
        # Load and process gauge stations from YAML
        with open(gauge_stations_file) as f:
            yaml_data = yaml.safe_load(f)
        
        # Convert stations to GeoDataFrame
        stations_list = []
        for station_id, data in yaml_data['stations'].items():
            stations_list.append({
                'station_id': station_id,
                'name': data['name'],
                'geometry': Point(data['location']['lon'], data['location']['lat'])
            })
        gauge_stations = gpd.GeoDataFrame(stations_list, crs='EPSG:4326')
        
        # Merge imputation data with counties
        counties = counties.merge(
            imputation_data[['county_fips', 'gauge_id_1', 'gauge_id_2', 'total_weight']],
            on='county_fips',
            how='left'
        )
        
        # Generate maps for each region
        output_files = {}
        for region in self.regions:
            logger.info(f"Generating map for {region}...")
            output_file = self._create_region_map(
                region,
                counties,
                gauge_stations,
                imputation_data
            )
            output_files[region] = output_file
            logger.info(f"Saved map to {output_file}")
        
        return output_files

def main():
    """Generate regional coverage maps."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize mapper
    mapper = RegionMapper()
    
    # Generate maps
    output_files = mapper.generate_maps(
        counties_file=Path("data/processed/coastal_counties.parquet"),
        gauge_stations_file=Path("config/tide-stations-list.yaml"),
        imputation_file=OUTPUT_DIR / "imputation" / "imputation_structure.parquet"
    )
    
    logger.info("Generated maps for all regions:")
    for region, file_path in output_files.items():
        logger.info(f"- {region}: {file_path}")

if __name__ == "__main__":
    main() 