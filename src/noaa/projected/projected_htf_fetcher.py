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

from ..core.noaa_client import NOAAClient, NOAAApiError
from ..core.cache_manager import NOAACache

logger = logging.getLogger(__name__)

class ProjectedHTFFetcher:
    """Service for managing projected high tide flooding data."""
    
    def __init__(self, cache: NOAACache):
        """Initialize the projected HTF service.
        
        Args:
            cache: NOAACache instance for data caching
        """
        logger.debug("Initializing ProjectedHTFFetcher")
        self.client = NOAAClient()
        self.cache = cache
        
        # Load NOAA settings for validation
        self.settings = self.cache.settings['data']['projected']
    
    def get_station_data(
        self,
        station: Optional[str] = None,
        decade: Optional[int] = None
    ) -> List[Dict]:
        """Get projected HTF data for a station.
        
        Args:
            station: NOAA station identifier. If None, returns data for all stations.
            decade: Specific decade to retrieve. If None, returns all available decades.
            
        Returns:
            List of decadal flood count records containing:
            - Station ID and name
            - Decade
            - Projected flood days for each scenario
            
        Raises:
            ValueError: If station ID is invalid or decade is out of range
            NOAAApiError: If there is an error fetching data from the API
        """
        logger.info(f"Fetching projected data for station: {station}, decade: {decade}")
        
        if station and not self.cache.validate_station_id(station):
            logger.error(f"Invalid station ID: {station}")
            raise ValueError(f"Invalid station ID: {station}")
            
        # Validate decade if provided
        if decade is not None:
            if not (self.settings['start_decade'] <= decade <= self.settings['end_decade']):
                msg = f"Decade {decade} out of valid range ({self.settings['start_decade']}-{self.settings['end_decade']})"
                logger.error(msg)
                raise ValueError(msg)
            
        try:
            # Check cache first
            if station:
                logger.debug(f"Checking cache for station {station}")
                cached_data = self.cache.get_projected_data(station, decade)
                if cached_data:
                    logger.debug(f"Found cached data for station {station}")
                    return cached_data
            
            # Fetch from API if not in cache
            logger.debug(f"Fetching data from NOAA API for station {station}")
            data = self.client.fetch_decadal_projections(station=station, decade=decade)
            
            # Cache the data
            logger.debug(f"Caching {len(data)} records for station {station}")
            for record in data:
                self.cache.save_projected_data(
                    station_id=record["stnId"],
                    decade=record["decade"],
                    data=record
                )
            
            return data
                
        except NOAAApiError as e:
            logger.error(f"Error fetching projected data for station {station}: {str(e)}")
            raise NOAAApiError(f"Failed to fetch projected data: {str(e)}")
    
    def get_complete_dataset(
        self,
        stations: Optional[List[str]] = None,
    ) -> Dict[str, List[Dict]]:
        """Get the complete projected HTF dataset.
        
        Args:
            stations: List of station IDs. If None, fetches data for all stations.
            
        Returns:
            Dict mapping station IDs to their projected flood count records
        """
        # Get stations list if not provided
        stations = stations or [s['id'] for s in self.cache.get_stations()]
        
        dataset = {}
        for station_id in stations:
            try:
                station_data = self.get_station_data(station=station_id)
                if station_data:
                    dataset[station_id] = station_data
                    
            except Exception as e:
                logger.error(f"Error fetching data for station {station_id}: {e}")
                continue
                
        return dataset
    
    def get_dataset_status(self) -> Dict:
        """Get status information about the projected dataset.
        
        Returns:
            Dict containing:
            - station_count: Number of stations with projections
            - decade_range: Min and max decades in dataset
            - completeness: Percentage of expected data points present
        """
        # Get all data
        dataset = self.get_complete_dataset()
        
        # Initialize status
        status = {
            "station_count": len(dataset),
            "decade_range": {"min": None, "max": None},
            "completeness": 0.0
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
        stations: Optional[List[str]] = None,
    ) -> Path:
        """Generate and save the projected HTF dataset in a structured format.
        
        Args:
            output_path: Directory to save the dataset
            stations: List of station IDs. If None, includes all available stations.
            
        Returns:
            Path to the generated dataset file
        """
        # Get the raw dataset
        raw_data = self.get_complete_dataset(stations=stations)
        
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
                    'source': decadal_record['source'],
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
        output_file = output_path / 'projected_htf.parquet'
        df.to_parquet(output_file, index=False)
        
        logger.info(f"Generated projected HTF dataset at {output_file}")
        logger.info(f"Dataset contains {len(df)} records from {len(raw_data)} stations")
        
        return output_file 