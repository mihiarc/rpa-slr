"""
Historical HTF data assignment processor.
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor
import numpy as np

from ..common.weights import WeightCalculator
from .data_loader import HistoricalDataLoader

logger = logging.getLogger(__name__)

class HistoricalProcessor:
    """Processes historical HTF data assignment to counties using regional processing."""
    
    def __init__(
        self,
        config_dir: Optional[Path] = None,
        max_distance_km: float = 100.0,
        min_weight: float = 0.1,
        idw_power: float = 2.0,
        n_processes: int = 4
    ):
        """Initialize processor.
        
        Args:
            config_dir: Optional custom config directory path
            max_distance_km: Maximum distance in km for station influence
            min_weight: Minimum weight to consider a station's influence
            idw_power: Power parameter for inverse distance weighting
            n_processes: Number of processes for parallel processing
        """
        self.config_dir = config_dir
        self.max_distance_km = max_distance_km
        self.min_weight = min_weight
        self.idw_power = idw_power
        self.n_processes = n_processes
        
        # Initialize components
        self.data_loader = HistoricalDataLoader(config_dir)
        self.weight_calculator = WeightCalculator(
            config_dir=config_dir,
            max_distance_km=max_distance_km,
            min_weight=min_weight,
            idw_power=idw_power
        )
    
    def process_region(
        self,
        region: str,
        htf_data_path: Path,
        reference_points_path: Path
    ) -> pd.DataFrame:
        """Process historical HTF data assignment for a region.
        
        Args:
            region: Name of the region to process
            htf_data_path: Path to imputation structure data
            reference_points_path: Path to reference points data
            
        Returns:
            DataFrame with reference point HTF data
        """
        logger.info(f"Processing historical HTF assignment for region: {region}")
        
        # Load regional data
        htf_df, ref_points, stations = self.data_loader.load_regional_data(
            region, htf_data_path, reference_points_path
        )
        
        # Process each reference point in parallel
        with ProcessPoolExecutor(max_workers=self.n_processes) as executor:
            futures = []
            
            # Group data by reference point
            ref_point_groups = htf_df.groupby('reference_point_id')
            
            for ref_id, group in ref_point_groups:
                ref_point = ref_points[ref_points['reference_id'] == ref_id].iloc[0]
                
                futures.append(
                    executor.submit(
                        self._process_reference_point,
                        ref_point,
                        group
                    )
                )
            
            # Collect results
            results = []
            for future in futures:
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as e:
                    logger.error(f"Error processing reference point: {e}")
        
        if not results:
            logger.error(f"No valid results for region {region}")
            return pd.DataFrame()
        
        # Combine results
        result_df = pd.concat(results, ignore_index=True)
        
        # Add region column
        result_df['region'] = region
        
        return result_df
    
    def _process_reference_point(
        self,
        ref_point: pd.Series,
        point_data: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        """Process HTF data for a single reference point.
        
        Args:
            ref_point: Reference point data
            point_data: DataFrame with station weights for this point
            
        Returns:
            DataFrame with processed HTF data for the reference point
        """
        try:
            # Create result record with reference point metadata
            result = {
                'reference_id': ref_point['reference_id'],
                'county_fips': ref_point['county_fips'],
                'county_name': ref_point['county_name'],
                'state': ref_point['state'],
                'region': ref_point['region'],
                'total_weight': point_data['weight'].sum()
            }
            
            return pd.DataFrame([result])
            
        except Exception as e:
            logger.error(
                f"Error processing reference point {ref_point['reference_id']}: {e}"
            )
            return None 