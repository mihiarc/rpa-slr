"""
Rate limiter for NOAA API requests.
Prevents exceeding API rate limits and maintains good API citizenship.
"""

import time
from typing import Optional
import logging
from threading import Lock

logger = logging.getLogger(__name__)

class RateLimiter:
    """Rate limiter for NOAA API requests."""
    
    def __init__(self, requests_per_second: float = 2.0):
        """Initialize the rate limiter.
        
        Args:
            requests_per_second (float): Maximum number of requests per second
        """
        self._requests_per_second = requests_per_second
        self._min_interval = 1.0 / requests_per_second
        self._last_request_time: Optional[float] = None
        self._lock = Lock()
    
    @property
    def requests_per_second(self) -> float:
        """Get the configured requests per second limit."""
        return self._requests_per_second
    
    def wait(self) -> None:
        """Wait if necessary to maintain the rate limit."""
        with self._lock:
            current_time = time.time()
            
            if self._last_request_time is not None:
                elapsed = current_time - self._last_request_time
                if elapsed < self._min_interval:
                    sleep_time = self._min_interval - elapsed
                    logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
            
            self._last_request_time = time.time() 