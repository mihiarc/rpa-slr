"""
Projected HTF data processor.

Processes projected high tide flooding data by region, handling:
- Regional data validation
- Scenario-based processing
- Data aggregation
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import yaml

from ..core import NOAACache

logger = logging.getLogger(__name__)

class ProjectedHTFProcessor:
    """Processes projected HTF data by region."""
    
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
            
        # Load NOAA settings for scenario information
        with open(self.config_dir / "noaa_settings.yaml") as f:
            self.noaa_settings = yaml.safe_load(f)
            
    def process_region(self, region: str, start_decade: int, end_decade: int) -> pd.DataFrame:
        """Process projected HTF data for a specific region.
        
        Args:
            region: Name of the region to process
            start_decade: Start decade (inclusive)
            end_decade: End decade (inclusive)
            
        Returns:
            DataFrame containing processed projected HTF data for the region
            
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
                start_decade,
                end_decade
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
        start_decade: int,
        end_decade: int
    ) -> List[Dict]:
        """Process projected data for a single station.
        
        Args:
            station_id: Station identifier
            start_decade: Start decade (inclusive)
            end_decade: End decade (inclusive)
            
        Returns:
            List of processed records
        """
        data = []
        for decade in range(start_decade, end_decade + 10, 10):
            record = self.cache.get_annual_data(station_id, decade, 'projected')
            if record and self._validate_record(record):
                # Create a record for each scenario
                for scenario in self.noaa_settings['data']['projected']['response_fields'][4:]:  # Skip metadata fields
                    processed = {
                        'station_id': station_id,
                        'decade': decade,
                        'scenario': scenario,
                        'flood_days': record[scenario]
                    }
                    data.append(processed)
                    
        return data
        
    def _validate_record(self, record: Dict) -> bool:
        """Validate a projected HTF record.
        
        Args:
            record: Record to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Check for required scenario fields
        scenario_fields = self.noaa_settings['data']['projected']['response_fields'][4:]  # Skip metadata fields
        if not all(field in record for field in scenario_fields):
            return False
            
        # Validate numeric fields
        try:
            for field in scenario_fields:
                value = float(record[field])
                
                # Basic range checks
                if value < 0 or value > 366:  # Max possible days per year
                    return False
                    
            return True
            
        except (ValueError, TypeError):
            return False
