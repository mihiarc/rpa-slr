"""
Projected High Tide Flooding Data Service.

This module handles retrieval and processing of projected high tide flooding data from NOAA.
Each record contains projected flooding frequencies under different climate scenarios:
- Low
- Intermediate-Low
- Intermediate
- Intermediate-High
- High
"""

from typing import Dict, List, Optional
import logging
from pathlib import Path
import pandas as pd
import numpy as np

from .client import NOAAClient, NOAAApiError
from .cache import NOAACache

logger = logging.getLogger(__name__)

class ProjectedHTFService:
    """Service for managing projected high tide flooding data."""
    
    def __init__(self, cache_dir: Optional[Path] = None):
        """Initialize the projected HTF service.
        
        Args:
            cache_dir: default to data/noaa_cache
        """
        self.client = NOAAClient()
        self.cache = NOAACache(config_dir=cache_dir)
        
    def get_station_data(self, station: Optional[str] = None) -> Dict:
        """Get projected HTF data for a station.
        
        Args:
            station: Station ID. If None, returns data for all stations.
            
        Returns:
            Dict mapping station IDs to their projected data.
        """
        try:
            data = self.client.fetch_decadal_projections(station=station)
            if not data:
                logger.warning(f"No data returned for station {station}")
                return {}
                
            if 'DecadalProjection' not in data:
                logger.warning(f"No projection data in response for station {station}")
                return {}
                
            return {station: data['DecadalProjection']}
            
        except NOAAApiError as e:
            logger.error(f"Error fetching data for station {station}: {str(e)}")
            return {}
    
    def get_complete_dataset(
        self,
        stations: Optional[List[str]] = None,
    ) -> Dict:
        """Get the complete projected HTF dataset.
        
        Args:
            stations: List of station IDs. If None, fetches data for all stations.
            
        Returns:
            Dict mapping station IDs to their projected data.
        """
        if stations is None:
            stations = [s['id'] for s in self.cache.get_stations()]
            
        dataset = {}
        for station in stations:
            try:
                station_data = self.get_station_data(station=station)
                if station_data:
                    dataset.update(station_data)
            except Exception as e:
                logger.error(f"Error processing station {station}: {str(e)}")
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