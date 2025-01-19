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

from .client import NOAAClient, NOAAApiError
from .cache import NOAACache

logger = logging.getLogger(__name__)

class HistoricalHTFService:
    """Service for managing historical high tide flooding data."""
    
    def __init__(self, cache_dir: Optional[Path] = None):
        """Initialize the historical HTF service.
        
        Args:
            cache_dir: Optional custom cache directory. Defaults to Path("data/noaa_cache").
        """
        self.client = NOAAClient()
        self.cache = NOAACache(config_dir=cache_dir)
        
    def get_station_data(
        self,
        station: Optional[str] = None,
    ) -> List[Dict]:
        """Get historical HTF data for a station.
        
        Args:
            station: NOAA station identifier. If None, returns data for all stations.
            
        Returns:
            List of annual flood count records containing:
            - Station ID and name
            - Year
            - Major, moderate, minor flood counts
            - Missing data count
        """
        if station and not self.cache.validate_station_id(station):
            raise ValueError(f"Invalid station ID: {station}")
            
        try:
            # Fetch all records for the station
            data = self.client.fetch_annual_flood_counts(station=station)
            
            # Cache the data
            for record in data:
                self.cache.save_annual_data(station, record["year"], record, "historical")
            
            return data
                
        except NOAAApiError as e:
            logger.error(f"Error fetching data for station {station}: {str(e)}")
            raise
    
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
        """Get status information about the historical dataset.
        
        Returns:
            Dict containing:
            - station_count: Number of stations with data
            - year_range: Min and max years in dataset
            - completeness: Percentage of expected data points present
        """
        stations = self.cache.get_stations()
        dataset = self.get_complete_dataset()
        
        # Initialize status
        status = {
            "station_count": len(stations),
            "year_range": {"min": None, "max": None},
            "completeness": 0.0
        }
        
        if not dataset:
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