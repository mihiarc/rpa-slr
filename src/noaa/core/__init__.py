"""
Core NOAA API infrastructure.

This module provides the foundational components for interacting with the NOAA API:
- API client with authentication and error handling
- Rate limiting for API requests
- Caching system for efficient data retrieval
"""

from .noaa_client import NOAAClient, NOAAApiError
from .rate_limiter import RateLimiter
from .cache_manager import NOAACache

__all__ = ['NOAAClient', 'NOAAApiError', 'RateLimiter', 'NOAACache']
