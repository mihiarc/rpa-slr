"""
Historical High Tide Flooding Data Service.

This module handles retrieval and processing of historical high tide flooding data from NOAA.
Each record contains actual observed flooding events broken down by severity levels:
- Major flood days
- Moderate flood days
- Minor flood days
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

class HistoricalHTFFetcher:
    """Service for managing historical high tide flooding data."""
    
    def __init__(self, cache: NOAACache):
        """Initialize the historical HTF service.
        
        Args:
            cache: NOAACache instance for data caching
        """
        logger.debug("Initializing HistoricalHTFFetcher")
        self.client = NOAAClient()
        self.cache = cache
        
        # Load NOAA settings for validation
        self.settings = self.cache.settings['data']['historical']
    
    def get_station_data(
        self,
        station: Optional[str] = None,
        year: Optional[int] = None
    ) -> List[Dict]:
        """Get historical HTF data for a station.
        
        Args:
            station: NOAA station identifier. If None, returns data for all stations.
            year: Specific year to retrieve. If None, returns all available years.
            
        Returns:
            List of annual flood count records containing:
            - Station ID and name
            - Year
            - Major, moderate, minor flood counts
            - Missing data count
            
        Raises:
            ValueError: If station ID is invalid or year is out of range
            NOAAApiError: If there is an error fetching data from the API
        """
        logger.info(f"Fetching historical data for station: {station}, year: {year}")
        
        if station and not self.cache.validate_station_id(station):
            logger.error(f"Invalid station ID: {station}")
            raise ValueError(f"Invalid station ID: {station}")
            
        # Validate year if provided
        if year is not None:
            if not (self.settings['start_year'] <= year <= self.settings['end_year']):
                msg = f"Year {year} out of valid range ({self.settings['start_year']}-{self.settings['end_year']})"
                logger.error(msg)
                raise ValueError(msg)
            
        try:
            # Check cache first
            if station:
                logger.debug(f"Checking cache for station {station}")
                cached_data = self.cache.get_historical_data(station, year)
                if cached_data:
                    logger.debug(f"Found cached data for station {station}")
                    return cached_data
            
            # Fetch from API if not in cache
            logger.debug(f"Fetching data from NOAA API for station {station}")
            data = self.client.fetch_annual_flood_counts(station=station, year=year)
            
            # Cache the data
            logger.debug(f"Caching {len(data)} records for station {station}")
            for record in data:
                self.cache.save_historical_data(
                    station_id=record["stnId"],
                    year=record["year"],
                    data=record
                )
            
            return data
                
        except NOAAApiError as e:
            logger.error(f"Error fetching historical data for station {station}: {str(e)}")
            raise NOAAApiError(f"Failed to fetch historical data: {str(e)}")
    
    def get_complete_dataset(
        self,
        stations: Optional[List[str]] = None,
    ) -> Dict[str, List[Dict]]:
        """Get the complete historical HTF dataset.
        
        Args:
            stations: List of station IDs. If None, fetches data for all stations.
            
        Returns:
            Dict mapping station IDs to their historical flood count records
        """
        logger.info(f"Fetching complete dataset for {len(stations) if stations else 'all'} stations")
        
        # Get stations list if not provided
        stations = stations or [s['id'] for s in self.cache.get_stations()]
        logger.debug(f"Processing {len(stations)} stations")
        
        dataset = {}
        for station_id in stations:
            try:
                logger.debug(f"Fetching data for station {station_id}")
                station_data = self.get_station_data(station=station_id)
                if station_data:
                    logger.debug(f"Got {len(station_data)} records for station {station_id}")
                    dataset[station_id] = station_data
                else:
                    logger.warning(f"No data returned for station {station_id}")
                    
            except Exception as e:
                logger.error(f"Error fetching data for station {station_id}: {e}")
                continue
                
        logger.info(f"Completed dataset fetch. Got data for {len(dataset)} stations")
        return dataset
    
    def get_dataset_status(self) -> Dict:
        """Get status information about the historical dataset.
        
        Returns:
            Dict containing:
            - station_count: Number of stations with data
            - year_range: Min and max years in dataset
            - completeness: Percentage of expected data points present
        """
        logger.info("Getting dataset status")
        stations = self.cache.get_stations()
        dataset = self.get_complete_dataset()
        
        # Initialize status
        status = {
            "station_count": len(stations),
            "year_range": {"min": None, "max": None},
            "completeness": 0.0
        }
        
        if not dataset:
            logger.warning("No data in dataset")
            return status
            
        # Track years and completeness
        all_years = set()
        total_records = 0
        complete_records = 0
        
        for station_data in dataset.values():
            for record in station_data:
                year = record["year"]
                all_years.add(year)
                
                # Count record completeness
                total_records += 1
                if record["nanCount"] == 0:
                    complete_records += 1
        
        # Update status
        if all_years:
            status["year_range"]["min"] = min(all_years)
            status["year_range"]["max"] = max(all_years)
        
        if total_records > 0:
            status["completeness"] = complete_records / total_records
            
        logger.info(f"Dataset status: {status}")
        return status
    
    def generate_dataset(
        self,
        output_path: Path,
        stations: Optional[List[str]] = None,
    ) -> Path:
        """Generate and save the historical HTF dataset in a structured format.
        
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
        for station_id, station_data in raw_data.items():
            for annual_record in station_data:
                # Get count values with default of 0 for None
                maj_count = annual_record.get('majCount', 0) or 0
                mod_count = annual_record.get('modCount', 0) or 0
                min_count = annual_record.get('minCount', 0) or 0
                nan_count = annual_record.get('nanCount', 0) or 0
                
                records.append({
                    'station_id': annual_record['stnId'],
                    'station_name': annual_record['stnName'],
                    'year': annual_record['year'],
                    'major_flood_days': maj_count,
                    'moderate_flood_days': mod_count,
                    'minor_flood_days': min_count,
                    'missing_days': nan_count,
                    # Add derived fields
                    'total_flood_days': maj_count + mod_count + min_count,
                    'data_completeness': (365 - nan_count) / 365  # Simplified, doesn't account for leap years
                })
        
        # Convert to DataFrame for efficient storage and querying
        df = pd.DataFrame.from_records(records)
        
        # Ensure output directory exists
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save as parquet for efficient storage and reading
        output_file = output_path / 'historical_htf.parquet'
        df.to_parquet(output_file, index=False)
        
        logger.info(f"Generated historical HTF dataset at {output_file}")
        logger.info(f"Dataset contains {len(df)} records from {len(raw_data)} stations")
        
        return output_file 