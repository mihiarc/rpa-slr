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
from tqdm import tqdm

logger = logging.getLogger(__name__)

WeightMethod = Literal['idw', 'gaussian', 'linear', 'hybrid']

class WeightCalculator:
    """Calculate weights for tide gauge stations."""
    
    def __init__(self, 
                 max_distance_meters: float = 100000,  # 100km max distance
                 power: float = 2,  # inverse distance power
                 min_weight: float = 0.1):
        """
        Initialize weight calculator.
        
        Args:
            max_distance_meters: Maximum distance to consider for weights
            power: Power parameter for inverse distance weighting
            min_weight: Minimum weight to assign
        """
        self.max_distance = max_distance_meters
        self.power = power
        self.min_weight = min_weight
        
        logger.info(f"Initialized WeightCalculator")
        logger.info(f"Max distance: {self.max_distance/1000:.1f}km")
        logger.info(f"IDW power: {self.power}")
        
    def _calculate_single_mapping_weights(self, mapping: Dict) -> Dict:
        """
        Calculate weights for a single reference point mapping.
        
        Args:
            mapping: Dictionary containing reference point to gauge mappings
            
        Returns:
            Updated mapping dictionary with calculated weights
        """
        distances = np.array([m['distance_meters'] for m in mapping['mappings']])
        
        # Filter by max distance
        valid_mask = distances <= self.max_distance
        if not any(valid_mask):
            # If no stations within max distance, use all stations
            valid_mask = np.ones_like(distances, dtype=bool)
            
        valid_distances = distances[valid_mask]
        
        # Calculate inverse distance weights
        weights = 1 / (valid_distances ** self.power)
        
        # Normalize weights to sum to 1
        weights = weights / np.sum(weights)
        
        # Apply minimum weight threshold
        weights = np.maximum(weights, self.min_weight)
        weights = weights / np.sum(weights)  # Renormalize
        
        # Update mapping with weights
        valid_mappings = [m for i, m in enumerate(mapping['mappings']) if valid_mask[i]]
        for m, w in zip(valid_mappings, weights):
            m['weight'] = float(w)
            
        mapping['mappings'] = valid_mappings
        return mapping
    
    def calculate_weights(self, mappings: List[Dict]) -> List[Dict]:
        """
        Calculate weights for all reference point mappings.
        
        Args:
            mappings: List of mapping dictionaries
            
        Returns:
            Updated mappings with calculated weights
        """
        if not mappings:
            return []
            
        logger.info(f"Processing {len(mappings)} mappings")
        
        # Process all mappings sequentially with progress bar
        weighted_mappings = []
        for mapping in tqdm(mappings, desc="Calculating weights"):
            try:
                weighted_mapping = self._calculate_single_mapping_weights(mapping)
                weighted_mappings.append(weighted_mapping)
            except Exception as e:
                logger.error(f"Error processing mapping: {str(e)}")
                continue
        
        logger.info(f"Completed weight calculation for {len(weighted_mappings)} mappings")
        return weighted_mappings 