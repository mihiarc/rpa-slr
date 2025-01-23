"""
Cache manager for NOAA data.
"""

from typing import Dict, List, Optional
from pathlib import Path
import logging
import json
import yaml

logger = logging.getLogger(__name__)

class NOAACache:
    """Cache manager for NOAA data."""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize the cache manager.
        
        Args:
            config_dir: Optional custom config directory
        """
        # Use project root config directory by default
        self.config_dir = config_dir or (Path(__file__).parent.parent.parent / "config")
        
        # Load NOAA settings from project root config directory
        settings_file = self.config_dir / "noaa_settings.yaml"
        with open(settings_file) as f:
            self.settings = yaml.safe_load(f)
        
        # Set up paths based on config
        self.stations_file = self.config_dir / "tide-stations-list.yaml"
        self.cache_dir = Path(__file__).parent / self.settings['cache']['directory']
        self._stations = None
        
        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
    def _get_cache_path(self, station_id: str, data_type: str) -> Path:
        """Get the cache file path for a station.
        
        Args:
            station_id: Station identifier
            data_type: Type of data ('historical' or 'projected')
            
        Returns:
            Path to the cache file
        """
        return self.cache_dir / data_type / f"{station_id}.json"
        
    def get_stations(self) -> List[Dict]:
        """Get the list of NOAA stations.
        
        Returns:
            List of station records with id, name, and location
        """
        if self._stations is None:
            try:
                with open(self.stations_file) as f:
                    data = yaml.safe_load(f)
                    # Convert YAML structure to list format for compatibility
                    self._stations = [
                        {
                            'id': station_id,
                            'name': station_data['name'],
                            'location': station_data['location']
                        }
                        for station_id, station_data in data['stations'].items()
                    ]
            except Exception as e:
                logger.error(f"Error reading stations list: {e}")
                self._stations = []
                
        return self._stations
    
    def validate_station_id(self, station_id: str) -> bool:
        """Validate that a station ID exists.
        
        Args:
            station_id: Station identifier to validate
            
        Returns:
            True if valid, False otherwise
        """
        stations = self.get_stations()
        return any(s['id'] == station_id for s in stations)
    
    def get_annual_data(
        self,
        station_id: str,
        year: int,
        data_type: str
    ) -> Optional[Dict]:
        """Get cached annual data for a station.
        
        Args:
            station_id: Station identifier
            year: Year of data
            data_type: Type of data ('historical' or 'projected')
            
        Returns:
            Cached data if available, None otherwise
        """
        cache_path = self._get_cache_path(station_id, data_type)
        
        if not cache_path.exists():
            return None
            
        try:
            with open(cache_path) as f:
                cached_data = json.load(f)
                
            # Find the record for the specific year
            for record in cached_data:
                record_year = record.get('year') if data_type == 'historical' else record.get('decade')
                if record_year == year:
                    return record
                    
            return None
            
        except Exception as e:
            logger.error(f"Error reading cache for station {station_id}: {e}")
            return None
    
    def cache_annual_data(
        self,
        station_id: str,
        year: int,
        data: Dict,
        data_type: str
    ) -> None:
        """Cache annual data for a station.
        
        Args:
            station_id: Station identifier
            year: Year of data
            data: Data to cache
            data_type: Type of data ('historical' or 'projected')
        """
        if data_type not in self.settings['cache']['data_types']:
            raise ValueError(f"Invalid data type: {data_type}")
            
        cache_path = self._get_cache_path(station_id, data_type)
        
        # Ensure parent directory exists
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            # Read existing cache if any
            cached_data = []
            if cache_path.exists():
                with open(cache_path) as f:
                    cached_data = json.load(f)
                    
            # Remove any existing record for this year
            year_field = 'year' if data_type == 'historical' else 'decade'
            cached_data = [r for r in cached_data if r.get(year_field) != year]
            
            # Add new record
            cached_data.append(data)
            
            # Sort by year/decade
            cached_data.sort(key=lambda x: x.get(year_field))
            
            # Write back to cache
            with open(cache_path, 'w') as f:
                json.dump(cached_data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error caching data for station {station_id}: {e}")
    
    def save_annual_data(
        self,
        station_id: str,
        year: int,
        data: Dict,
        data_type: str
    ) -> None:
        """Alias for cache_annual_data.
        
        Args:
            station_id: Station identifier
            year: Year of data
            data: Data to cache
            data_type: Type of data ('historical' or 'projected')
        """
        return self.cache_annual_data(station_id, year, data, data_type) 