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
    
    def __init__(self,
                 method: WeightMethod = 'hybrid',
                 close_threshold: float = 1000,
                 max_distance: float = 50000):
        """
        Initialize weight calculator.
        
        Args:
            method: Weighting method to use
            close_threshold: Distance threshold for 'very close' gauges in meters
            max_distance: Maximum distance to consider for weighting in meters
        """
        self.method = method
        self.close_threshold = close_threshold
        self.max_distance = max_distance
        
        # Map method names to calculation functions
        self._weight_functions = {
            'idw': self._inverse_distance_weights,
            'gaussian': self._gaussian_weights,
            'linear': self._linear_weights,
            'hybrid': self._hybrid_weights
        }
    
    def _inverse_distance_weights(self, distances: np.ndarray) -> np.ndarray:
        """Calculate inverse distance squared weights."""
        return 1 / (distances ** 2)
    
    def _gaussian_weights(self, distances: np.ndarray) -> np.ndarray:
        """Calculate Gaussian weights."""
        sigma = self.max_distance / 3  # Scale parameter
        return np.exp(-(distances ** 2) / (2 * sigma ** 2))
    
    def _linear_weights(self, distances: np.ndarray) -> np.ndarray:
        """Calculate linear decay weights."""
        return 1 - (distances / self.max_distance)
    
    def _hybrid_weights(self, distances: np.ndarray) -> np.ndarray:
        """Calculate hybrid weights (constant for close points, IDW for far points)."""
        return np.where(
            distances <= self.close_threshold,
            1.0,
            1 / (distances ** 2)
        )
    
    def calculate(self, distances: np.ndarray) -> np.ndarray:
        """
        Calculate weights for given distances using specified method.
        
        Args:
            distances: Array of distances in meters
            
        Returns:
            Array of normalized weights that sum to 1
        """
        # Cap distances at max_distance
        distances = np.minimum(distances, self.max_distance)
        
        # Get weight function for method
        weight_func = self._weight_functions.get(self.method)
        if weight_func is None:
            raise ValueError(f"Unknown weighting method: {self.method}")
        
        # Calculate weights
        weights = weight_func(distances)
        
        # Normalize weights to sum to 1
        return weights / np.sum(weights)
    
    def calculate_for_points(self, point_data: List[Dict]) -> List[Dict]:
        """
        Calculate weights for a list of point data dictionaries.
        
        Args:
            point_data: List of dictionaries containing point and gauge information
                       Each dict should have distance_1, distance_2, etc. keys
                       
        Returns:
            List of dictionaries with added weight_1, weight_2, etc. keys
        """
        for point in point_data:
            # Get valid distances
            distances = []
            i = 1
            while f'distance_{i}' in point:
                if point[f'distance_{i}'] is not None:
                    distances.append(point[f'distance_{i}'])
                i += 1
            
            if distances:
                # Calculate weights for valid distances
                weights = self.calculate(np.array(distances))
                
                # Add weights to point data
                for i, weight in enumerate(weights, 1):
                    point[f'weight_{i}'] = weight
                
                # Fill remaining weights with 0
                while f'distance_{i}' in point:
                    point[f'weight_{i}'] = 0.0
                    i += 1
            
        return point_data 