"""
Historical HTF data processor.

Processes historical high tide flooding data by region, handling:
- Regional data validation
- Quality control checks
- Data aggregation

IMPORTANT - Flood Severity Levels:
    The NOAA API provides three severity levels of flood days:
    - majCount: Major flood days (highest severity, significant damage)
    - modCount: Moderate flood days (medium severity)
    - minCount: Minor flood days (lowest severity, nuisance flooding)

    This processor ONLY uses minCount (minor flood days) for analysis.
    Major and moderate flood counts are intentionally excluded because:
    1. Minor flooding is the most frequent and consistent metric
    2. It provides the best signal for detecting sea level rise trends
    3. Major/moderate events are rare and statistically noisy

    The output column 'flood_days' represents MINOR flood days only.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import yaml

from ..core import NOAACache
from .historical_htf_fetcher import HistoricalHTFFetcher

logger = logging.getLogger(__name__)

class HistoricalHTFProcessor:
    """Processes historical HTF data by region."""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize the processor.
        
        Args:
            config_dir: Optional custom config directory
        """
        self.config_dir = config_dir or (Path(__file__).parent.parent.parent.parent / "config")
        logger.debug(f"Using config directory: {self.config_dir}")
        
        self.cache = NOAACache(config_dir=self.config_dir)
        self.fetcher = HistoricalHTFFetcher(self.cache)
        
        # Load region mappings
        region_file = self.config_dir / "region_mappings.yaml"
        logger.debug(f"Loading region mappings from: {region_file}")
        with open(region_file) as f:
            self.region_config = yaml.safe_load(f)
            
    def process_region(self, region: str, start_year: int, end_year: int) -> pd.DataFrame:
        """Process historical HTF data for a specific region.
        
        Args:
            region: Name of the region to process
            start_year: Start year (inclusive)
            end_year: End year (inclusive)
            
        Returns:
            DataFrame containing processed historical HTF data for the region
            
        Raises:
            ValueError: If region is not found or data is invalid
        """
        logger.info(f"Processing region: {region} for years {start_year}-{end_year}")
        
        # Validate region
        if region not in self.region_config['regions']:
            logger.error(f"Region '{region}' not found in region config. Available regions: {list(self.region_config['regions'].keys())}")
            raise ValueError(f"Invalid region: {region}")
            
        # Get states in region
        states = self.region_config['regions'][region]['state_codes']
        logger.debug(f"States in region {region}: {states}")
        
        # Get stations in region
        stations = self._get_region_stations(region)
        logger.info(f"Found {len(stations)} stations in region {region}")
        if stations:
            logger.debug(f"First few stations: {stations[:3]}")
        
        # Process each station
        data = []
        for station in stations:
            logger.debug(f"Processing station: {station['id']} ({station['name']})")
            station_data = self._process_station(
                station['id'],
                start_year,
                end_year
            )
            if station_data:
                logger.debug(f"Got {len(station_data)} records for station {station['id']}")
                data.extend(station_data)
            else:
                logger.warning(f"No data returned for station {station['id']}")
                
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        if df.empty:
            logger.warning(f"No data found for region {region}")
            return df
            
        # Add region column
        df['region'] = region
        logger.info(f"Processed {len(df)} total records for region {region}")
        
        return df
        
    def _get_region_stations(self, region: str) -> List[Dict]:
        """Get list of stations in a region.
        
        Args:
            region: Name of the region
            
        Returns:
            List of station records
        """
        # Convert region name to filename format (e.g., "Gulf Coast" -> "gulf_coast")
        region_file = region.lower().replace(' ', '_')
        
        # Load regional tide station config
        config_file = self.config_dir / "tide_stations" / f"{region_file}_tide_stations.yaml"
        logger.debug(f"Loading tide stations from: {config_file}")
        
        try:
            with open(config_file) as f:
                config = yaml.safe_load(f)
                
            stations = [
                {
                    'id': station_id,
                    'name': station_data['name'],
                    'location': station_data['location']
                }
                for station_id, station_data in config['stations'].items()
            ]
            logger.debug(f"Loaded {len(stations)} stations from config")
            return stations
            
        except FileNotFoundError:
            logger.error(f"Tide station config file not found: {config_file}")
            return []
        except Exception as e:
            logger.error(f"Error loading tide station config: {e}")
            return []
        
    def _process_station(
        self,
        station_id: str,
        start_year: int,
        end_year: int
    ) -> List[Dict]:
        """Process historical data for a single station.
        
        Args:
            station_id: Station identifier
            start_year: Start year (inclusive)
            end_year: End year (inclusive)
            
        Returns:
            List of processed records
        """
        logger.debug(f"Processing station {station_id} for years {start_year}-{end_year}")
        data = []
        
        try:
            # Fetch data from NOAA API
            station_data = self.fetcher.get_station_data(station=station_id)
            
            if station_data:
                # Filter by year range
                for record in station_data:
                    year = record.get('year')
                    if year and start_year <= year <= end_year:
                        processed = {
                            'station_id': station_id,
                            'year': year,
                            'flood_days': record.get('minCount', 0),  # Only using minor flood counts
                            'missing_days': record.get('nanCount', 0)
                        }
                        data.append(processed)
                        logger.debug(f"Processed record: {processed}")
            else:
                logger.debug(f"No data returned from API for station {station_id}")
                
        except Exception as e:
            logger.error(f"Error processing station {station_id}: {e}")
            
        logger.debug(f"Processed {len(data)} records for station {station_id}")
        return data
        
    def _validate_record(self, record: Dict) -> bool:
        """Validate a historical HTF record.
        
        Args:
            record: Record to validate
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['minCount', 'nanCount']
        if not all(field in record for field in required_fields):
            logger.warning(f"Missing required fields in record: {record}")
            return False
            
        # Validate numeric fields
        try:
            min_count = int(record['minCount'])
            nan_count = int(record['nanCount'])
            
            # Basic range checks
            if min_count < 0 or nan_count < 0:
                logger.warning(f"Invalid negative counts: min_count={min_count}, nan_count={nan_count}")
                return False
            if min_count + nan_count > 366:  # Account for leap years
                logger.warning(f"Total days exceeds year length: min_count={min_count}, nan_count={nan_count}")
                return False
                
            return True
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Error validating record: {e}")
            return False
