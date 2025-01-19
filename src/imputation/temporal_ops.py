"""
Temporal operations for water level imputation at coastal reference points.
Handles the integration of gauge readings over time and their imputation to reference points.

This module is responsible for:
1. Loading and processing gauge water level time series
2. Handling missing data and different temporal resolutions
3. Applying spatial weights to compute water levels at reference points
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Union
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class WaterLevelProcessor:
    """Processes and imputes water level data from gauge stations to reference points."""
    
    def __init__(self,
                 imputation_structure_file: Path,
                 output_dir: Path,
                 min_weight_threshold: float = 0.01,
                 min_gauges_required: int = 1):
        """
        Initialize water level processor.
        
        Args:
            imputation_structure_file: Path to imputation structure from spatial phase
            output_dir: Directory for output files
            min_weight_threshold: Minimum weight to consider a gauge's influence
            min_gauges_required: Minimum gauges needed for valid imputation
        """
        self.imputation_structure_file = imputation_structure_file
        self.output_dir = output_dir
        self.min_weight_threshold = min_weight_threshold
        self.min_gauges_required = min_gauges_required
        
        # Load imputation structure
        self.imputation_df = pd.read_parquet(imputation_structure_file)
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_gauge_data(self,
                       gauge_data_dir: Path,
                       start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None,
                       resample_freq: str = '1H') -> pd.DataFrame:
        """
        Load and preprocess gauge water level data.
        
        Args:
            gauge_data_dir: Directory containing gauge data files
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
            resample_freq: Frequency to resample data to (e.g. '1H' for hourly)
            
        Returns:
            DataFrame with gauge water levels indexed by time
        """
        # Implementation will depend on gauge data format
        raise NotImplementedError
    
    def _validate_gauge_coverage(self,
                               point_data: pd.Series,
                               available_gauges: set) -> bool:
        """
        Check if a reference point has sufficient gauge coverage.
        
        Args:
            point_data: Series containing point's gauge associations
            available_gauges: Set of gauge IDs with available data
            
        Returns:
            Boolean indicating if point has sufficient coverage
        """
        valid_gauges = 0
        total_weight = 0
        
        # Check each associated gauge
        for i in range(1, 4):  # Up to 3 gauges
            gauge_id = point_data.get(f'gauge_id_{i}')
            weight = point_data.get(f'weight_{i}', 0)
            
            if (gauge_id in available_gauges and 
                weight >= self.min_weight_threshold):
                valid_gauges += 1
                total_weight += weight
        
        return (valid_gauges >= self.min_gauges_required and 
                total_weight > 0.5)  # At least 50% weight coverage
    
    def _impute_point_water_level(self,
                                 point_data: pd.Series,
                                 gauge_levels: pd.DataFrame,
                                 time_index: pd.DatetimeIndex) -> pd.Series:
        """
        Impute water levels for a single reference point.
        
        Args:
            point_data: Series containing point's gauge associations
            gauge_levels: DataFrame of gauge water levels
            time_index: DatetimeIndex for output
            
        Returns:
            Series of imputed water levels indexed by time
        """
        # Initialize arrays
        weights = []
        levels = []
        
        # Collect valid gauge data
        for i in range(1, 4):  # Up to 3 gauges
            gauge_id = point_data.get(f'gauge_id_{i}')
            weight = point_data.get(f'weight_{i}', 0)
            
            if (gauge_id in gauge_levels.columns and 
                weight >= self.min_weight_threshold):
                weights.append(weight)
                levels.append(gauge_levels[gauge_id])
        
        if not weights:
            return pd.Series(index=time_index, dtype=float)
        
        # Normalize weights
        weights = np.array(weights) / sum(weights)
        
        # Calculate weighted average
        imputed_levels = sum(w * l for w, l in zip(weights, levels))
        
        return pd.Series(imputed_levels, index=time_index)
    
    def process_water_levels(self,
                           gauge_data_dir: Path,
                           output_file: Path,
                           start_date: Optional[datetime] = None,
                           end_date: Optional[datetime] = None,
                           resample_freq: str = '1H') -> Path:
        """
        Process and impute water levels for all reference points.
        
        Args:
            gauge_data_dir: Directory containing gauge data files
            output_file: Path to save results
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
            resample_freq: Frequency to resample data to
            
        Returns:
            Path to output file
        """
        logger.info("Loading gauge data...")
        gauge_levels = self.load_gauge_data(
            gauge_data_dir,
            start_date,
            end_date,
            resample_freq
        )
        
        available_gauges = set(gauge_levels.columns)
        time_index = gauge_levels.index
        
        logger.info("Imputing water levels for reference points...")
        results = []
        
        for idx, point in self.imputation_df.iterrows():
            if self._validate_gauge_coverage(point, available_gauges):
                water_levels = self._impute_point_water_level(
                    point,
                    gauge_levels,
                    time_index
                )
                
                results.append({
                    'county_fips': point['county_fips'],
                    'county_name': point['county_name'],
                    'state_fips': point['state_fips'],
                    'geometry': point['geometry'],
                    'water_levels': water_levels
                })
        
        # Create output DataFrame
        result_df = pd.DataFrame(results)
        
        # Save results
        result_df.to_parquet(output_file, compression='snappy')
        logger.info(f"Saved water levels to {output_file}")
        
        return output_file 