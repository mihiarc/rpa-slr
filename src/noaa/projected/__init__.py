"""
Projected HTF data handling.

This module handles the retrieval and processing of projected high tide flooding data:
- Fetching HTF projections from NOAA API
- Processing projection data by region
- Command line interface for data retrieval
"""

from .projected_htf_fetcher import ProjectedHTFFetcher
from .projected_htf_processor import ProjectedHTFProcessor

__all__ = ['ProjectedHTFFetcher', 'ProjectedHTFProcessor']
