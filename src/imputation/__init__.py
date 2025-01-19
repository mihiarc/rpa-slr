"""
Imputation module for coastal water level analysis.
Handles the spatial association between reference points and tide gauge stations.

This module provides functionality for:
1. Loading and validating input data
2. Finding nearest tide gauges for reference points
3. Calculating spatial weights for imputation
4. Generating analysis reports
"""

from .main import ImputationManager
from .data_loader import DataLoader
from .spatial_ops import NearestGaugeFinder
from .weight_calculator import WeightCalculator

__version__ = "0.1.0"
__author__ = "RPA SLR Team"

# Expose main interface
__all__ = [
    "ImputationManager",
    "DataLoader",
    "NearestGaugeFinder",
    "WeightCalculator",
]

# Module metadata
metadata = {
    "phase": "spatial_association",
    "input_files": [
        "coastal_reference_points.parquet",
        "tide-stations-list.json"
    ],
    "output_files": [
        "imputation_structure.parquet",
        "imputation_report.html"
    ],
    "dependencies": [
        "numpy",
        "pandas",
        "geopandas",
        "scipy",
        "matplotlib",
        "seaborn",
        "jinja2"
    ]
}