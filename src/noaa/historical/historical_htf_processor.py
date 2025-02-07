"""
Historical HTF data processor.

Processes historical high tide flooding data by region, handling:
- Regional data validation
- Quality control checks
- Data aggregation
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import yaml

from ..core import NOAACache

logger = logging.getLogger(__name__)

class HistoricalHTFProcessor:
    """Processes historical HTF data by region."""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize the processor.
        
        Args:
            config_dir: Optional custom config directory
        """
        self.config_dir = config_dir or (Path(__file__).parent.parent.parent.parent / "config")
        self.cache = NOAACache(config_dir=self.config_dir)
        
        # Load FIPS mappings for region definitions
        with open(self.config_dir / "fips_mappings.yaml") as f:
            self.fips_config = yaml.safe_load(f)
            
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
        # Validate region
        if region not in self.fips_config['regions']:
            raise ValueError(f"Invalid region: {region}")
            
        # Get states in region
        states = self.fips_config['regions'][region]['states']
        
        # Get stations in region
        stations = self._get_region_stations(region)
        
        # Process each station
        data = []
        for station in stations:
            station_data = self._process_station(
                station['id'],
                start_year,
                end_year
            )
            if station_data:
                data.extend(station_data)
                
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        if df.empty:
            logger.warning(f"No data found for region {region}")
            return df
            
        # Add region column
        df['region'] = region
        
        return df
        
    def _get_region_stations(self, region: str) -> List[Dict]:
        """Get list of stations in a region.
        
        Args:
            region: Name of the region
            
        Returns:
            List of station records
        """
        # Load regional tide station config
        config_file = self.config_dir / f"{region.lower()}_tide_stations.yaml"
        with open(config_file) as f:
            config = yaml.safe_load(f)
            
        return [
            {
                'id': station_id,
                'name': station_data['name'],
                'location': station_data['location']
            }
            for station_id, station_data in config['stations'].items()
        ]
        
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
        data = []
        for year in range(start_year, end_year + 1):
            record = self.cache.get_annual_data(station_id, year, 'historical')
            if record and self._validate_record(record):
                processed = {
                    'station_id': station_id,
                    'year': year,
                    'flood_days': record['minCount'],  # Only using minor flood counts
                    'missing_days': record['nanCount']
                }
                data.append(processed)
                
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
            return False
            
        # Validate numeric fields
        try:
            min_count = int(record['minCount'])
            nan_count = int(record['nanCount'])
            
            # Basic range checks
            if min_count < 0 or nan_count < 0:
                return False
            if min_count + nan_count > 366:  # Account for leap years
                return False
                
            return True
            
        except (ValueError, TypeError):
            return False
