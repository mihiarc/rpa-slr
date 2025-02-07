"""
NOAA High Tide Flooding (HTF) Data Access.

This package provides tools for accessing and processing NOAA's HTF data:

Core Infrastructure:
- API client with authentication and error handling
- Rate limiting for API requests
- Caching system for efficient data retrieval

Historical Data:
- Fetching historical minor flood counts
- Processing historical data by region
- Command line tools for historical data

Projected Data:
- Fetching HTF projections
- Processing projection data by region
- Command line tools for projections
"""

from .core import NOAAClient, NOAAApiError, NOAACache
from .historical import HistoricalHTFFetcher, HistoricalHTFProcessor
from .projected import ProjectedHTFFetcher, ProjectedHTFProcessor

__all__ = [
    # Core
    'NOAAClient',
    'NOAAApiError',
    'NOAACache',
    # Historical
    'HistoricalHTFFetcher',
    'HistoricalHTFProcessor',
    # Projected
    'ProjectedHTFFetcher',
    'ProjectedHTFProcessor',
] 