"""
Projected High Tide Flooding Data Service.

This module handles retrieval and processing of projected high tide flooding data from NOAA.
Each record contains projected flood days for different sea level rise scenarios:
- Low
- Intermediate-Low
- Intermediate
- Intermediate-High
- High
"""

from typing import Dict, List, Optional
from datetime import datetime
import logging
from pathlib import Path
import pandas as pd
import numpy as np
import yaml

from ..core.noaa_client import NOAAClient, NOAAApiError
from ..core.cache_manager import NOAACache

logger = logging.getLogger(__name__)

class ProjectedHTFFetcher:
    """Service for managing projected high tide flooding data."""
    
    def __init__(self, cache: NOAACache, region: str):
        """Initialize the projected HTF service.
        
        Args:
            cache: NOAACache instance for data caching
            region: Region identifier (e.g., 'gulf_coast', 'hawaii')
        """
        logger.debug(f"Initializing ProjectedHTFFetcher for region: {region}")
        self.client = NOAAClient()
        self.cache = cache
        self.region = region.lower()
        
        # Load NOAA settings for validation
        self.settings = self.cache.settings['data']['projected']
        
        # Load region configuration
        self.config_dir = Path(cache.config_dir)
        self._load_region_config()
        
    def _load_region_config(self):
        """Load region-specific configuration."""
        # Load region mappings
        with open(self.config_dir / "region_mappings.yaml") as f:
            region_config = yaml.safe_load(f)
            
        if self.region not in region_config['regions']:
            raise ValueError(f"Invalid region: {self.region}")
            
        self.region_info = region_config['regions'][self.region]
        
        # Load regional tide stations
        station_file = self.config_dir / "tide_stations" / f"{self.region}_tide_stations.yaml"
        with open(station_file) as f:
            self.station_config = yaml.safe_load(f)
    
    def get_regional_stations(self) -> List[str]:
        """Get list of station IDs for the region.
        
        Returns:
            List of station IDs
        """
        return list(self.station_config['stations'].keys())
    
    def _validate_station_id(self, station_id: str) -> bool:
        """Validate if a station ID belongs to the current region.
        
        Args:
            station_id: NOAA station identifier
            
        Returns:
            True if valid, False otherwise
        """
        return station_id in self.station_config['stations']
    
    def get_station_data(self, station_id: str, decade: Optional[int] = None) -> List[Dict]:
        """Get projected HTF data for a station.
        
        Args:
            station_id: NOAA station identifier
            decade: Optional specific decade to retrieve
            
        Returns:
            List of projected HTF records
            
        Raises:
            ValueError: If station ID is invalid
            NOAAApiError: If API request fails
        """
        if not self._validate_station_id(station_id):
            raise ValueError(f"Invalid station ID: {station_id}")
        
        # Check cache first
        cached_data = self.cache.get_projected_data(station_id, decade)
        if cached_data is not None:
            if isinstance(cached_data, list):
                return cached_data
            return [cached_data]
        
        # Check if cache needs update
        if not self.cache.needs_update(station_id, 'projected'):
            return []
        
        try:
            # Fetch from API
            data = self.client.fetch_decadal_projections(station_id)
            
            # Cache all decades
            for record in data:
                self.cache.save_projected_data(station_id, record['decade'], record)
            
            # Return requested decade if specified
            if decade is not None:
                return [record for record in data if record['decade'] == decade]
            
            return data
        
        except Exception as e:
            logger.error(f"Error fetching projected data for station {station_id}: {e}")
            raise
    
    def get_regional_dataset(
        self,
        start_decade: Optional[int] = None,
        end_decade: Optional[int] = None
    ) -> Dict[str, List[Dict]]:
        """Get the complete projected HTF dataset for the region.
        
        Args:
            start_decade: Start decade (inclusive). If None, uses settings default.
            end_decade: End decade (inclusive). If None, uses settings default.
            
        Returns:
            Dict mapping station IDs to their projected flood count records
        """
        start_decade = start_decade or self.settings['start_decade']
        end_decade = end_decade or self.settings['end_decade']
        
        # Get stations
        stations = self.get_regional_stations()
        logger.info(f"Fetching data for {len(stations)} stations in {self.region}")
        
        dataset = {}
        for station_id in stations:
            try:
                station_data = []
                for decade in range(start_decade, end_decade + 10, 10):
                    data = self.get_station_data(station_id, decade)
                    if data:
                        station_data.extend(data)
                        
                if station_data:
                    dataset[station_id] = station_data
                    
            except Exception as e:
                logger.error(f"Error fetching data for station {station_id}: {e}")
                continue
                
        return dataset
    
    def get_dataset_status(self) -> Dict:
        """Get status information about the regional projected dataset.
        
        Returns:
            Dict containing:
            - region: Region identifier
            - station_count: Number of stations with projections
            - decade_range: Min and max decades in dataset
            - completeness: Percentage of expected data points present
            - cache_stats: Cache hit/miss statistics
        """
        # Get all data for region
        dataset = self.get_regional_dataset()
        
        # Initialize status
        status = {
            "region": self.region,
            "station_count": len(dataset),
            "decade_range": {"min": None, "max": None},
            "completeness": 0.0,
            "cache_stats": self.cache.get_stats()
        }
        
        if not dataset:
            return status
            
        # Track decades and completeness
        all_decades = set()
        total_datapoints = 0
        complete_datapoints = 0
        
        for station_data in dataset.values():
            for record in station_data:
                decade = record["decade"]
                all_decades.add(decade)
                
                # Count completeness (all 5 scenarios present)
                total_datapoints += 5  # 5 scenarios per decade
                for scenario in ['low', 'intLow', 'intermediate', 'intHigh', 'high']:
                    if record.get(scenario) is not None:
                        complete_datapoints += 1
        
        # Update status
        if all_decades:
            status["decade_range"]["min"] = min(all_decades)
            status["decade_range"]["max"] = max(all_decades)
        
        if total_datapoints > 0:
            status["completeness"] = complete_datapoints / total_datapoints
            
        return status
    
    def generate_dataset(
        self,
        output_path: Path,
        stations: Optional[List[str]] = None
    ) -> Path:
        """Generate and save the projected HTF dataset in a structured format.
        
        Args:
            output_path: Directory to save the dataset
            stations: Optional list of station IDs to include. If None, uses all regional stations.
            
        Returns:
            Path to the generated dataset file
        """
        # Get the raw dataset
        stations = stations or self.get_regional_stations()
        raw_data = self.get_regional_dataset()
        
        # Filter to requested stations if specified
        if stations:
            raw_data = {k: v for k, v in raw_data.items() if k in stations}
        
        # Transform into a flat structure for efficient storage
        records = []
        for station, station_data in raw_data.items():
            for decadal_record in station_data:
                # Get projection values with default of 0 for None
                low = decadal_record.get('low', 0) or 0
                int_low = decadal_record.get('intLow', 0) or 0
                intermediate = decadal_record.get('intermediate', 0) or 0
                int_high = decadal_record.get('intHigh', 0) or 0
                high = decadal_record.get('high', 0) or 0
                
                records.append({
                    'station': decadal_record['stnId'],
                    'station_name': decadal_record['stnName'],
                    'decade': decadal_record['decade'],
                    'source': decadal_record.get('source', 'NOAA'),
                    # Scenario projections (days per year)
                    'low_scenario': low,
                    'intermediate_low_scenario': int_low,
                    'intermediate_scenario': intermediate,
                    'intermediate_high_scenario': int_high,
                    'high_scenario': high,
                    # Add derived fields
                    'scenario_range': high - low,
                    'median_scenario': intermediate
                })
        
        # Convert to DataFrame for efficient storage and querying
        df = pd.DataFrame.from_records(records)
        
        # Ensure output directory exists
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save as parquet for efficient storage and reading
        output_file = output_path / f'projected_htf_{self.region}.parquet'
        df.to_parquet(output_file, index=False)
        
        logger.info(f"Generated projected HTF dataset at {output_file}")
        logger.info(f"Dataset contains {len(df)} records from {len(raw_data)} stations")
        
        return output_file 