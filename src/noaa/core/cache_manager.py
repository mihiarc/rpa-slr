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
        
        # Load or initialize cache stats
        self.stats_file = self.cache_dir / "cache_stats.json"
        self._stats_write_interval = 100  # Write stats every N operations
        self._stats_pending_writes = 0
        self._load_cache_stats()

        # Perform initial cleanup
        self._cleanup_old_cache()
        
    def _load_cache_stats(self):
        """Load or initialize cache statistics."""
        try:
            if self.stats_file.exists():
                with open(self.stats_file) as f:
                    self.stats = json.load(f)
            else:
                self.stats = {
                    'hits': 0,
                    'misses': 0,
                    'errors': 0,
                    'last_reset': datetime.now().isoformat()
                }
                self._save_cache_stats()
        except Exception as e:
            logger.error(f"Error loading cache stats: {e}")
            self.stats = {
                'hits': 0,
                'misses': 0,
                'errors': 0,
                'last_reset': datetime.now().isoformat()
            }
            
    def _save_cache_stats(self):
        """Save cache statistics to file."""
        try:
            with open(self.stats_file, 'w') as f:
                json.dump(self.stats, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving cache stats: {e}")
            
    def _update_stats(self, stat_type: str):
        """Update cache statistics.

        Stats are batched and written to disk periodically to reduce I/O.

        Args:
            stat_type: Type of stat to update ('hits', 'misses', or 'errors')
        """
        self.stats[stat_type] += 1
        self._stats_pending_writes += 1

        # Only write to disk periodically to reduce I/O
        if self._stats_pending_writes >= self._stats_write_interval:
            self._save_cache_stats()
            self._stats_pending_writes = 0

    def flush_stats(self):
        """Force write of pending cache statistics to disk."""
        if self._stats_pending_writes > 0:
            self._save_cache_stats()
            self._stats_pending_writes = 0

    def __del__(self):
        """Ensure stats are saved when cache manager is destroyed."""
        try:
            self.flush_stats()
        except Exception:
            pass  # Ignore errors during cleanup
        
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

    def _validate_cache_data(self, data: Dict) -> bool:
        """Validate cache data structure.
        
        Args:
            data: Data to validate
            
        Returns:
            True if valid, False otherwise
        """
        if data is None:
            return False
            
        if isinstance(data, list):
            return all(self._validate_single_record(record) for record in data)
        
        return self._validate_single_record(data)
        
    def _validate_single_record(self, record: Dict) -> bool:
        """Validate a single cache record.
        
        Args:
            record: Record to validate
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['decade']
        scenario_fields = ['low', 'intLow', 'intermediate', 'intHigh', 'high']
        
        # Check required fields
        if not all(field in record for field in required_fields):
            return False
            
        # Check scenario fields (at least one should be present)
        if not any(field in record for field in scenario_fields):
            return False
            
        return True

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
            logger.debug(f"Cache miss: No cache file for station {station_id}")
            self._update_stats('misses')
            return None
            
        try:
            with open(cache_file) as f:
                data = json.load(f)
                
            if not self._validate_cache_data(data):
                logger.warning(f"Invalid cache data format for station {station_id}")
                self._update_stats('errors')
                return None
                
            if decade is not None:
                if isinstance(data, list):
                    result = next((record for record in data if record.get('decade') == decade), None)
                else:
                    result = data if data.get('decade') == decade else None
                    
                if result:
                    logger.debug(f"Cache hit: Found data for station {station_id}, decade {decade}")
                    self._update_stats('hits')
                else:
                    logger.debug(f"Cache miss: No data for station {station_id}, decade {decade}")
                    self._update_stats('misses')
                return result
            
            logger.debug(f"Cache hit: Found all data for station {station_id}")
            self._update_stats('hits')
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding cache file {cache_file}: {e}")
            self._update_stats('errors')
            # Remove corrupted cache file
            cache_file.unlink(missing_ok=True)
            return None
        except Exception as e:
            logger.error(f"Error reading projected cache file {cache_file}: {e}")
            self._update_stats('errors')
            return None
    
    def save_projected_data(self, station_id: str, decade: int, data: Dict):
        """Save projected data to cache.
        
        Args:
            station_id: NOAA station identifier
            decade: Decade of the projection
            data: Projected flood count data to cache
        """
        if not self._validate_cache_data(data):
            logger.error(f"Invalid data format for station {station_id}")
            self._update_stats('errors')
            return
            
        cache_file = self._get_cache_path(station_id, 'projected')
        
        try:
            # Load existing data if any
            if cache_file.exists():
                with open(cache_file) as f:
                    try:
                        cached_data = json.load(f)
                        if not isinstance(cached_data, list):
                            cached_data = [cached_data] if cached_data else []
                    except json.JSONDecodeError:
                        logger.warning(f"Corrupted cache file for {station_id}, resetting")
                        cached_data = []
            else:
                cached_data = []
                
            # Remove existing record for this decade if present
            cached_data = [record for record in cached_data if record.get('decade') != decade]
            
            # Add new record
            if isinstance(data, list):
                cached_data.extend(data)
            else:
                cached_data.append(data)
                
            # Write updated data
            with open(cache_file, 'w') as f:
                json.dump(cached_data, f, indent=2)
                
            logger.debug(f"Cached data for station {station_id}, decade {decade}")
            
        except Exception as e:
            logger.error(f"Error saving projected data to cache file {cache_file}: {e}")
            self._update_stats('errors')
    
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
        cleaned = 0
        
        for data_type in ['historical', 'projected', 'metadata']:
            retention_days = self.cache_settings['retention'][data_type]
            cache_dir = self.cache_dir / data_type
            
            if not cache_dir.exists():
                continue
                
            for cache_file in cache_dir.glob("*.json"):
                try:
                    # Get file modification time
                    mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
                    age = now - mtime
                    
                    # Remove if older than retention period
                    if age > timedelta(days=retention_days):
                        cache_file.unlink()
                        cleaned += 1
                        logger.debug(f"Removed expired cache file: {cache_file}")
                except Exception as e:
                    logger.error(f"Error cleaning cache file {cache_file}: {e}")
                    
        if cleaned > 0:
            logger.info(f"Cleaned {cleaned} expired cache files")
                        
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
        
    def get_stats(self) -> Dict:
        """Get cache statistics.
        
        Returns:
            Dict containing hit/miss/error counts and last reset time
        """
        return self.stats.copy() 