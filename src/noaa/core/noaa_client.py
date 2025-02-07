"""
NOAA API Client for accessing high tide flooding data.
"""

from typing import Dict, List, Optional
import requests
import logging
from pathlib import Path
import json

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
            api_base_url: Base URL for the NOAA API. Defaults to production URL.
            requests_per_second: Maximum number of requests per second. Defaults to 2.0.
        """
        self.api_base_url = api_base_url.rstrip('/')
        self.rate_limiter = RateLimiter(requests_per_second)

    def fetch_annual_flood_counts(
        self,
        station: Optional[str] = None,
    ) -> List[Dict]:
        """
        Fetch annual high tide flood data for a station.
        
        Args:
            station: 7-digit NOAA station identifier (if None, returns all stations)
            
        Returns:
            List of annual records, each containing:
            - stnId: Station identifier
            - stnName: Station name
            - year: 4-digit calendar year
            - majCount: The number of flood days that exceed the major flood threshold
            - modCount: The number of flood days that exceed the moderate flood threshold
            - minCount: The number of flood days that exceed the minor flood threshold
            - nanCount: The number of days that the flood data is missing
            
        Raises:
            NOAAApiError: If the API request fails
        """
        endpoint = f"{self.api_base_url}/htf/htf_annual.json"
        
        params = {}
        if station is not None:
            params["station"] = station

        try:
            self.rate_limiter.wait()
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            
            if "AnnualFloodCount" not in data:
                raise NOAAApiError("No flood count data in response", response=response)
                
            return data["AnnualFloodCount"]
            
        except requests.exceptions.RequestException as e:
            logger.error(f"NOAA API request failed for station {station}: {str(e)}")
            raise NOAAApiError(f"Failed to fetch flood count data: {str(e)}", response=e.response if hasattr(e, 'response') else None)
        except (ValueError, KeyError) as e:
            logger.error(f"Failed to parse NOAA API response for station {station}: {str(e)}")
            raise NOAAApiError(f"Invalid response format: {str(e)}", response=response if 'response' in locals() else None)

    def fetch_decadal_projections(
        self,
        station: Optional[str] = None,
    ) -> Dict:
        """Fetch decadal high tide flood projections for a station.
        
        Args:
            station: Station ID. If None, returns data for all stations.
            
        Returns:
            Dict containing the decadal projections data.
            
        Raises:
            NOAAApiError: If the API request fails or response is invalid.
        """
        params = {}
        if station is not None:
            params['station'] = station
            
        try:
            response = requests.get(
                f"{self.api_base_url}/htf/htf_projection_decadal.json",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            if not data:
                raise NOAAApiError(f"No data returned for station {station}")
                
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"NOAA API request failed for station {station}: {str(e)}")
            raise NOAAApiError(f"API request failed: {str(e)}")
            
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            logger.error(f"Invalid response from NOAA API for station {station}: {str(e)}")
            raise NOAAApiError(f"Invalid API response: {str(e)}")
 