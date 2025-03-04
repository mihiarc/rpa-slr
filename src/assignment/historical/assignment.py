"""
Historical HTF data assignment module.

Handles assignment of historical HTF data to counties using regional processing.
"""

import pandas as pd
import geopandas as gpd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging
from pathlib import Path
import yaml
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from tqdm.auto import tqdm

from ..common.weights import WeightCalculator
from src.assignment.historical.data_loader import HistoricalDataLoader
from src.assignment.historical.aggregator import HistoricalAggregator

logger = logging.getLogger(__name__)

class HistoricalAssignment:
    """Assigns historical HTF data to counties using regional approach."""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize historical assignment processor.
        
        Args:
            config_dir: Optional custom config directory
        """
        self.config_dir = config_dir or (Path(__file__).parent.parent.parent.parent / "config")
        
        # Load configurations
        with open(self.config_dir / "region_mappings.yaml") as f:
            self.region_config = yaml.safe_load(f)
            
        # Initialize weight calculator
        self.weight_calculator = WeightCalculator(config_dir=self.config_dir)
        
    def process_region(
        self,
        region: str,
        htf_df: pd.DataFrame,
        reference_points: gpd.GeoDataFrame,
        stations: gpd.GeoDataFrame,
        n_processes: int = None
    ) -> pd.DataFrame:
        """Process historical HTF data for a specific region.
        
        Args:
            region: Name of the region
            htf_df: Historical HTF data for the region
            reference_points: Coastal reference points for the region
            stations: Tide stations for the region
            n_processes: Number of processes for parallel processing
            
        Returns:
            DataFrame with county-level historical HTF values
        """
        logger.info(f"Processing historical HTF assignment for region: {region}")
        
        if n_processes is None:
            n_processes = max(1, multiprocessing.cpu_count() - 1)
            
        # Calculate weights for the region
        weights_df = self.weight_calculator.calculate_weights(
            reference_points=reference_points,
            stations=stations,
            region=region
        )
        
        # Prepare HTF data
        prepared_htf = self._prepare_htf_data(htf_df, weights_df)
        
        # Group reference points by county
        county_points = reference_points.groupby('county_fips')
        
        # Process each county with regional context
        county_data = [
            (
                county_fips,
                group,
                weights_df[weights_df['reference_point_id'].isin(group.index)],
                prepared_htf
            )
            for county_fips, group in county_points
        ]
        
        # Process counties in parallel
        logger.info(f"Processing {len(county_data)} counties using {n_processes} processes")
        
        with ProcessPoolExecutor(max_workers=n_processes) as executor:
            results = list(tqdm(
                executor.map(self._process_county, county_data),
                total=len(county_data),
                desc=f"Processing {region} counties"
            ))
            
        # Combine results
        result_df = pd.concat(results, ignore_index=True)
        
        # Add region information
        result_df['region'] = region
        result_df['region_name'] = self.region_config['regions'][region].get(
            'display_name', region.replace('_', ' ').title()
        )
        
        return result_df
    
    def _prepare_htf_data(
        self,
        htf_df: pd.DataFrame,
        weights_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Prepare historical HTF data for processing.
        
        Args:
            htf_df: Raw HTF data
            weights_df: Station weights
            
        Returns:
            Prepared HTF data
        """
        # Get unique years and stations
        years = htf_df['year'].unique()
        stations = weights_df['station_id'].unique()
        
        # Create complete index
        index = pd.MultiIndex.from_product(
            [years, stations],
            names=['year', 'station_id']
        )
        
        # Reindex HTF data
        complete_df = htf_df.set_index(['year', 'station_id']).reindex(index)
        
        # Fill missing values with 0 (assume no floods when data is missing)
        flood_columns = ['total_flood_days', 'major_flood_days', 
                        'moderate_flood_days', 'minor_flood_days']
        for col in flood_columns:
            if col in complete_df.columns:
                complete_df[col] = complete_df[col].fillna(0)
                
        return complete_df.reset_index()
    
    def _process_county(
        self,
        county_data: Tuple[str, gpd.GeoDataFrame, pd.DataFrame, pd.DataFrame]
    ) -> pd.DataFrame:
        """Process historical HTF data for a single county.
        
        Args:
            county_data: Tuple of (county_fips, reference_points, weights, htf_data)
            
        Returns:
            DataFrame with county results
        """
        county_fips, ref_points, weights, htf_data = county_data
        
        # Get unique years
        years = htf_data['year'].unique()
        
        # Initialize results
        results = []
        
        # Process each year
        for year in years:
            year_data = htf_data[htf_data['year'] == year]
            
            # Calculate weighted averages for each flood category
            flood_categories = ['total_flood_days', 'major_flood_days', 
                              'moderate_flood_days', 'minor_flood_days']
            
            flood_values = {}
            for category in flood_categories:
                if category in year_data.columns:
                    # Merge HTF data with weights
                    weighted_data = pd.merge(
                        weights,
                        year_data[['station_id', category]],
                        on='station_id',
                        how='left'
                    )
                    
                    # Calculate weighted average
                    weighted_sum = (weighted_data[category] * weighted_data['weight']).sum()
                    total_weight = weighted_data['weight'].sum()
                    
                    flood_values[category] = weighted_sum / total_weight if total_weight > 0 else np.nan
            
            # Create result record
            result = {
                'year': year,
                'county_fips': county_fips,
                'county_name': ref_points['county_name'].iloc[0],
                'state_fips': ref_points['state_fips'].iloc[0],
                'geometry': ref_points.unary_union  # Combine all reference point geometries
            }
            result.update(flood_values)
            
            results.append(result)
            
        return pd.DataFrame(results)

def process_historical_htf(
    htf_data_path: Path,
    reference_points_path: Path,
    output_dir: Path,
    config_dir: Optional[Path] = None
) -> None:
    """Process historical HTF data for all regions.
    
    Args:
        htf_data_path: Path to imputation structure data
        reference_points_path: Path to reference points data
        output_dir: Directory to save outputs
        config_dir: Optional custom config directory
    """
    # Initialize data loader
    data_loader = HistoricalDataLoader(config_dir)
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Process each region
    for region in data_loader.region_config.keys():
        try:
            logger.info(f"Processing region: {region}")
            
            # Load data
            htf_df, ref_points, stations = data_loader.load_regional_data(
                region,
                htf_data_path,
                reference_points_path
            )
            
            # Create aggregator
            aggregator = HistoricalAggregator()
            
            # Aggregate to county level
            county_data = aggregator.aggregate_to_county(
                htf_df=htf_df,
                reference_points=ref_points,
                stations=stations
            )
            
            # Save results
            output_path = output_dir / f"historical_htf_{region}.parquet"
            county_data.to_parquet(output_path)
            logger.info(f"Saved results to {output_path}")
            
        except Exception as e:
            logger.error(f"Error processing region {region}: {str(e)}")
            continue 