"""
NOAA API Client for accessing high tide flooding data.
"""

from typing import Dict, List, Optional
import requests
import logging
from pathlib import Path
import json
from datetime import datetime, timedelta

from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

class NOAAApiError(Exception):
    """Exception raised when NOAA API request fails."""
    def __init__(self, message: str, response: Optional[requests.Response] = None):
        """Initialize the error.
        
        Args:
            message: Error message
            response: Optional response object that caused the error
        """
        self.message = message
        self.response = response
        super().__init__(self.message)

class NOAAClient:
    """Client for interacting with NOAA Tides & Currents API."""

    def __init__(self, api_base_url: str = "https://api.tidesandcurrents.noaa.gov/dpapi/prod/webapi", requests_per_second: float = 2.0):
        """Initialize the NOAA API client.

        Args:
            api_base_url: Base URL for the NOAA API
            requests_per_second: Maximum number of requests per second. Defaults to 2.0.
        """
        self.api_base_url = api_base_url.rstrip('/')
        self.rate_limiter = RateLimiter(requests_per_second)
        # Use session for connection pooling and improved performance
        self._session = requests.Session()

    def fetch_annual_flood_counts(
        self,
        station: Optional[str] = None,
        year: Optional[int] = None,
        range: Optional[int] = None
    ) -> List[Dict]:
        """
        Fetch annual high tide flood data for a station.
        
        Args:
            station: 7-digit NOAA station identifier (if None, returns all stations)
            year: Year to fetch data for (if None, returns all available years)
            range: Number of years to fetch (if None, defaults to 0)
            
        Returns:
            List of annual records, each containing:
            - stnId: Station ID
            - stnName: Station name
            - year: Year of data
            - majCount: Major flood days
            - modCount: Moderate flood days
            - minCount: Minor flood days
            - nanCount: Missing data days
            
        Raises:
            NOAAApiError: If the API request fails
        """
        if not station:
            raise NOAAApiError("Station ID is required")
            
        endpoint = "/htf/htf_annual.json"
        params = {'station': station}
        
        if year is not None:
            params['year'] = year
        if range is not None:
            params['range'] = range

        url = f"{self.api_base_url}{endpoint}"
        logger.debug(f"Making API request to URL: {url}")
        logger.debug(f"Request parameters: {params}")
        
        try:
            self.rate_limiter.wait()
            logger.debug("Rate limiter check passed, making request")
            response = self._session.get(url, params=params)
            logger.debug(f"API response status code: {response.status_code}")
            logger.debug(f"API response headers: {dict(response.headers)}")
            logger.debug(f"API response content: {response.text}")
            
            response.raise_for_status()
            data = response.json()
            
            logger.debug(f"Response data keys: {list(data.keys())}")
            
            if "AnnualFloodCount" not in data:
                logger.error(f"Missing AnnualFloodCount in response. Response keys: {list(data.keys())}")
                raise NOAAApiError("No flood count data in response", response=response)
                
            logger.debug(f"Successfully parsed response with {len(data['AnnualFloodCount'])} records")
            return data['AnnualFloodCount']
            
        except requests.exceptions.RequestException as e:
            logger.error(f"NOAA API request failed for station {station}: {str(e)}")
            if hasattr(e, 'response'):
                logger.error(f"Error response content: {e.response.text if e.response else 'No response content'}")
            raise NOAAApiError(f"Failed to fetch flood count data: {str(e)}", response=e.response if hasattr(e, 'response') else None)
        except (ValueError, KeyError) as e:
            logger.error(f"Failed to parse NOAA API response for station {station}: {str(e)}")
            raise NOAAApiError(f"Invalid response format: {str(e)}", response=response if 'response' in locals() else None)

    def fetch_decadal_projections(
        self,
        station: Optional[str] = None,
        decade: Optional[int] = None,
        range: Optional[int] = None
    ) -> List[Dict]:
        """Fetch decadal high tide flood projections for a station.
        
        Args:
            station: Station ID. If None, returns data for all stations.
            decade: Target decade (e.g., 2050). If None, returns all decades.
            range: Number of decades to fetch. If None, defaults to 0.
            
        Returns:
            List of decadal projection records, each containing:
            - stnId: Station ID
            - stnName: Station name
            - decade: Decade of projection
            - source: Data source
            - low: Low scenario flood days per year
            - intLow: Intermediate-Low scenario flood days per year
            - intermediate: Intermediate scenario flood days per year
            - intHigh: Intermediate-High scenario flood days per year
            - high: High scenario flood days per year
            
        Raises:
            NOAAApiError: If the API request fails or response is invalid.
        """
        if not station:
            raise NOAAApiError("Station ID is required")
            
        endpoint = "/htf/htf_projection_decadal.json"
        params = {'station': station}
        
        if decade is not None:
            params['decade'] = decade
        if range is not None:
            params['range'] = range
            
        url = f"{self.api_base_url}{endpoint}"
        logger.debug(f"Making API request to URL: {url}")
        logger.debug(f"Request parameters: {params}")
        
        try:
            self.rate_limiter.wait()
            logger.debug("Rate limiter check passed, making request")
            response = self._session.get(url, params=params)
            logger.debug(f"API response status code: {response.status_code}")
            logger.debug(f"API response headers: {dict(response.headers)}")
            logger.debug(f"API response content: {response.text}")
            
            response.raise_for_status()
            data = response.json()
            
            logger.debug(f"Response data keys: {list(data.keys())}")
            
            if "DecadalProjection" not in data:
                logger.error(f"Missing DecadalProjection in response. Response keys: {list(data.keys())}")
                raise NOAAApiError("No projection data in response", response=response)
                
            logger.debug(f"Successfully parsed response with {len(data['DecadalProjection'])} records")
            return data['DecadalProjection']
            
        except requests.exceptions.RequestException as e:
            logger.error(f"NOAA API request failed for station {station}: {str(e)}")
            if hasattr(e, 'response'):
                logger.error(f"Error response content: {e.response.text if e.response else 'No response content'}")
            raise NOAAApiError(f"Failed to fetch projection data: {str(e)}", response=e.response if hasattr(e, 'response') else None)
        except (ValueError, KeyError) as e:
            logger.error(f"Failed to parse NOAA API response for station {station}: {str(e)}")
            raise NOAAApiError(f"Invalid response format: {str(e)}", response=response if 'response' in locals() else None)
