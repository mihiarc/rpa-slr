"""
Cache manager for NOAA data.
Handles caching of NOAA API responses for both historical and projected data.
"""

from typing import Dict, List, Optional
from pathlib import Path
import logging
import json
import yaml
from datetime import datetime, timedelta
import shutil

logger = logging.getLogger(__name__)

class NOAACache:
    """Cache manager for NOAA data."""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize the cache manager.
        
        Args:
            config_dir: Optional custom config directory. If None, uses project root config.
        """
        # Use project root config directory by default
        self.config_dir = config_dir or (Path(__file__).parent.parent.parent.parent / "config")
        
        # Load NOAA settings
        settings_file = self.config_dir / "noaa_api_settings.yaml"
        with open(settings_file) as f:
            self.settings = yaml.safe_load(f)
        
        # Setup cache directory
        self.cache_dir = self.config_dir.parent / self.settings['cache']['directory']
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories for each data type
        for data_type in self.settings['cache']['data_types']:
            (self.cache_dir / data_type).mkdir(exist_ok=True)
            
        # Load station list
        self.stations = self._load_stations()
        
        # Load cache settings
        self._load_cache_settings()
        
        # Perform initial cleanup
        self._cleanup_old_cache()
        
    def _load_stations(self) -> List[Dict]:
        """Load stations with flexible format support."""
        stations = []
        stations_dir = self.config_dir / "tide_stations"
        
        for config_file in stations_dir.glob('*.yaml'):
            try:
                with open(config_file) as f:
                    region_config = yaml.safe_load(f)
                    
                for station_id, data in region_config['stations'].items():
                    # Handle both old and new formats
                    station = {
                        'id': station_id,
                        'name': data['name'],
                        'region': data.get('region', '').lower()
                    }
                    
                    # Handle both location formats
                    if 'location' in data:
                        station.update({
                            'latitude': str(data['location']['lat']),
                            'longitude': str(data['location']['lon'])
                        })
                    else:
                        station.update({
                            'latitude': str(data.get('latitude')),
                            'longitude': str(data.get('longitude'))
                        })
                        
                    stations.append(station)
            except Exception as e:
                logger.error(f"Error loading stations from {config_file}: {e}")
                continue
        
        return stations
    
    def get_stations(self, region: Optional[str] = None) -> List[Dict]:
        """Get the list of tide stations, optionally filtered by region.
        
        Args:
            region: Optional region name to filter stations
            
        Returns:
            List of station dictionaries
        """
        if region:
            return [s for s in self.stations if s['region'].lower() == region.lower()]
        return self.stations
    
    def validate_station_id(self, station_id: str) -> bool:
        """Validate a station ID against the known stations list."""
        return any(s['id'] == station_id for s in self.stations)
    
    def _get_cache_path(self, station_id: str, data_type: str) -> Path:
        """Get the cache file path for a station and data type."""
        return self.cache_dir / data_type / f"{station_id}.json"

    # Historical Data Methods
    def get_historical_data(self, station_id: str, year: Optional[int] = None) -> Optional[Dict]:
        """Get cached historical data for a station.
        
        Args:
            station_id: NOAA station identifier
            year: Specific year to retrieve (None for all years)
            
        Returns:
            Historical flood count data if available
        """
        cache_file = self._get_cache_path(station_id, 'historical')
        
        if not cache_file.exists():
            return None
            
        try:
            with open(cache_file) as f:
                data = json.load(f)
                
            if year is not None:
                return next((record for record in data if record.get('year') == year), None)
            
            return data
        except Exception as e:
            logger.error(f"Error reading historical cache file {cache_file}: {e}")
            return None
    
    def save_historical_data(self, station_id: str, year: int, data: Dict):
        """Save historical data to cache.
        
        Args:
            station_id: NOAA station identifier
            year: Year of the data
            data: Historical flood count data to cache
        """
        cache_file = self._get_cache_path(station_id, 'historical')
        
        try:
            # Load existing data if any
            if cache_file.exists():
                with open(cache_file) as f:
                    cached_data = json.load(f)
                    
                # Remove existing record for this year if present
                cached_data = [record for record in cached_data if record.get('year') != year]
                cached_data.append(data)
            else:
                cached_data = [data]
                
            # Write updated data
            with open(cache_file, 'w') as f:
                json.dump(cached_data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving historical data to cache file {cache_file}: {e}")

    # Projected Data Methods
    def get_projected_data(self, station_id: str, decade: Optional[int] = None) -> Optional[Dict]:
        """Get cached projected data for a station.
        
        Args:
            station_id: NOAA station identifier
            decade: Specific decade to retrieve (None for all decades)
            
        Returns:
            Projected flood count data if available
        """
        cache_file = self._get_cache_path(station_id, 'projected')
        
        if not cache_file.exists():
            return None
            
        try:
            with open(cache_file) as f:
                data = json.load(f)
                
            if decade is not None:
                return next((record for record in data if record.get('decade') == decade), None)
            
            return data
        except Exception as e:
            logger.error(f"Error reading projected cache file {cache_file}: {e}")
            return None
    
    def save_projected_data(self, station_id: str, decade: int, data: Dict):
        """Save projected data to cache.
        
        Args:
            station_id: NOAA station identifier
            decade: Decade of the projection
            data: Projected flood count data to cache
        """
        cache_file = self._get_cache_path(station_id, 'projected')
        
        try:
            # Load existing data if any
            if cache_file.exists():
                with open(cache_file) as f:
                    cached_data = json.load(f)
                    
                # Remove existing record for this decade if present
                cached_data = [record for record in cached_data if record.get('decade') != decade]
                cached_data.append(data)
            else:
                cached_data = [data]
                
            # Write updated data
            with open(cache_file, 'w') as f:
                json.dump(cached_data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving projected data to cache file {cache_file}: {e}")
    
    def _load_cache_settings(self):
        """Load cache settings from config file."""
        cache_settings = self.settings.get('cache', {})
        self.cache_settings = {
            'retention': cache_settings.get('retention', {
                'historical': 30,  # days
                'projected': 90,   # days
                'metadata': 7      # days
            }),
            'update_frequency': cache_settings.get('update_frequency', {
                'historical': 24,  # hours
                'projected': 168,  # hours (1 week)
                'metadata': 12     # hours
            })
        }
            
    def _cleanup_old_cache(self):
        """Clean up expired cache files based on retention settings."""
        now = datetime.now()
        
        for data_type in ['historical', 'projected', 'metadata']:
            retention_days = self.cache_settings['retention'][data_type]
            cache_dir = self.cache_dir / data_type
            
            if not cache_dir.exists():
                continue
                
            for cache_file in cache_dir.glob("*.json"):
                # Get file modification time
                mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
                age = now - mtime
                
                # Remove if older than retention period
                if age > timedelta(days=retention_days):
                    try:
                        cache_file.unlink()
                        logger.info(f"Removed expired cache file: {cache_file}")
                    except Exception as e:
                        logger.error(f"Error removing cache file {cache_file}: {e}")
                        
    def needs_update(self, station_id: str, data_type: str) -> bool:
        """Check if cache needs update based on update frequency.
        
        Args:
            station_id: Station identifier
            data_type: Type of data ('historical' or 'projected')
            
        Returns:
            True if cache needs update, False otherwise
        """
        cache_path = self._get_cache_path(station_id, data_type)
        if not cache_path.exists():
            return True
            
        now = datetime.now()
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        age = now - mtime
        
        update_hours = self.cache_settings['update_frequency'][data_type]
        return age > timedelta(hours=update_hours) 