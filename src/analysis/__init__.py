"""Analysis module for HTF data.

This module provides tools for analyzing HTF data, including:
- Spatial analysis of flood patterns
- Temporal trend analysis
- Report generation
"""

from .htf_spatial_analysis import analyze_flood_data
from .htf_temporal_analysis import analyze_temporal_trends

__all__ = [
    'analyze_flood_data',
    'analyze_temporal_trends'
] 