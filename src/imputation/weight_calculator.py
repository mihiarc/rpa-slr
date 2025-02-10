"""
Weight calculation methods for gauge station influence on reference points.
Implements various distance-based weighting schemes.


This module calculates weights that determine how much influence each tide gauge station has on a reference point.
It is needed because:

1. Spatial Interpolation:
   - Water levels need to be interpolated between gauge stations
   - Each reference point is influenced by multiple nearby gauges
   - The influence should decrease with distance

2. Multiple Weighting Methods:
   - Different weighting schemes (inverse distance, Gaussian, etc.) 
   - Each method has different distance-decay characteristics
   - Allows selecting optimal method for different scenarios

3. Robust Imputation:
   - Handles missing data by redistributing weights
   - Close stations get higher weights than distant ones
   - Prevents any single station from dominating

The module is critical for:
- Converting raw gauge measurements into interpolated water levels at the coastline reference points
- Providing smooth transitions between gauge influences
- Supporting the overall imputation of water levels at reference points
"""

import numpy as np
from typing import List, Dict, Literal
import logging

logger = logging.getLogger(__name__)

WeightMethod = Literal['idw', 'gaussian', 'linear', 'hybrid']

class WeightCalculator:
    """Calculates weights for gauge stations based on distance."""
    
    def __init__(self):
        pass
    
    def calculate_for_points(self, point_data: List[Dict], available_gauges: set) -> List[Dict]:
        """
        Calculate inverse distance weights for each point's nearest available gauges.
        Selects up to 2 nearest gauges that have HTF data available.
        Preserves all points, marking those without HTF data coverage.
        
        Args:
            point_data: List of dictionaries containing point and gauge information
            available_gauges: Set of gauge IDs that have HTF data
            
        Returns:
            List of dictionaries with weights and coverage information added
        """
        weighted_points = []
        
        for point in point_data:
            # Find the two nearest gauges that have HTF data
            valid_gauges = []
            valid_distances = []
            
            for gauge_id, distance in zip(point['backup_gauge_ids'], point['backup_distances']):
                if gauge_id in available_gauges:
                    valid_gauges.append(gauge_id)
                    valid_distances.append(distance)
                    if len(valid_gauges) == 2:  # We have enough gauges
                        break
            
            # Create base point data that we'll keep regardless of gauge coverage
            point_data_out = {
                'county_fips': point['county_fips'],
                'county_name': point['county_name'],
                'state_fips': point['state_fips'],
                'geometry': point['geometry'],
                'n_gauges': len(valid_gauges),
                'total_weight': 0.0,  # Will be updated if we have valid gauges
                'has_htf_data': len(valid_gauges) > 0,
                'nearest_gauge_id': point['backup_gauge_ids'][0],  # Keep track of nearest gauge even if no HTF data
                'nearest_gauge_distance': point['backup_distances'][0],
                'nearest_gauge_has_htf': point['backup_gauge_ids'][0] in available_gauges
            }
            
            # If we have valid gauges, calculate weights
            if valid_gauges:
                weights = [1 / (d ** 2) for d in valid_distances]
                total_weight = sum(weights)
                weights = [w / total_weight for w in weights]
                point_data_out['total_weight'] = 1.0
                
                # Add gauge information
                for i, (gauge_id, distance, weight) in enumerate(zip(valid_gauges, valid_distances, weights), 1):
                    point_data_out.update({
                        f'gauge_id_{i}': gauge_id,
                        f'distance_{i}': distance,
                        f'weight_{i}': weight
                    })
            
            # Always set gauge_id_2 fields, even if None
            if len(valid_gauges) < 2:
                point_data_out.update({
                    'gauge_id_2': None,
                    'distance_2': None,
                    'weight_2': 0.0
                })
            
            weighted_points.append(point_data_out)
        
        return weighted_points 