"""
Historical HTF data handling.

This module handles the retrieval and processing of historical high tide flooding data:
- Fetching historical minor flood counts from NOAA API
- Processing historical data by region
- Command line interface for data retrieval
"""

from .historical_htf_fetcher import HistoricalHTFFetcher
from .historical_htf_processor import HistoricalHTFProcessor

__all__ = ['HistoricalHTFFetcher', 'HistoricalHTFProcessor']
