"""
NOAA Data Quality Analysis.

This module provides comprehensive data quality analysis for NOAA high tide flooding data,
including validation, completeness checks, and anomaly detection for both historical
and projected datasets.
"""

from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from pathlib import Path

from ..noaa.core.noaa_client import NOAAClient
from ..noaa.core.cache_manager import NOAACache
from .. import config

logger = logging.getLogger(__name__)

class DataQualityAnalyzer:
    """Analyzer for NOAA data quality."""
    
    def __init__(self, cache: Optional[NOAACache] = None):
        """Initialize the data quality analyzer.
        
        Args:
            cache: Optional NOAACache instance. If None, creates a new one.
        """
        self.client = NOAAClient()
        self.cache = cache or NOAACache()
        
        # Load NOAA settings
        self.historical_settings = config.HISTORICAL_SETTINGS
        self.projected_settings = config.PROJECTED_SETTINGS
        
        # Set up output directory
        self.output_dir = config.OUTPUT_DIR / "analysis"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def analyze_station_data(
        self,
        data: pd.DataFrame,
        station_id: str,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
    ) -> Dict:
        """Analyze data quality for a specific station.
        
        Args:
            data: DataFrame containing HTF data
            station_id: NOAA station identifier
            start_year: Optional start year for analysis
            end_year: Optional end year for analysis
            
        Returns:
            Dict containing quality metrics:
            - temporal_coverage: Percentage of time period covered
            - completeness: Percentage of non-missing values
            - data_quality_issues: List of identified issues
            - anomalies: List of potential anomalies
            - summary_stats: Basic statistical summaries
        """
        logger.info(f"Analyzing data for station {station_id}")
        
        try:
            # Filter data for station
            station_data = data[data['station_id'] == station_id].copy()
            if station_data.empty:
                return self._empty_analysis_result()
            
            # Filter by year range if specified
            if start_year:
                station_data = station_data[station_data['year'] >= start_year]
            if end_year:
                station_data = station_data[station_data['year'] <= end_year]
                
            if station_data.empty:
                return self._empty_analysis_result()
            
            # Perform analysis
            return {
                'temporal_coverage': self._analyze_temporal_coverage(station_data),
                'completeness': self._analyze_completeness(station_data),
                'data_quality_issues': self._identify_quality_issues(station_data),
                'anomalies': self._detect_anomalies(station_data),
                'summary_stats': self._calculate_summary_stats(station_data)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing station {station_id}: {e}")
            return self._empty_analysis_result()
            
    def analyze_regional_data(
        self,
        data: pd.DataFrame,
        region: str,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
    ) -> Dict:
        """Analyze data quality for all stations in a region.
        
        Args:
            data: DataFrame containing HTF data
            region: Name of the region
            start_year: Optional start year for analysis
            end_year: Optional end year for analysis
            
        Returns:
            Dict containing:
            - station_analyses: Dict mapping station IDs to their analysis results
            - regional_summary: Overall quality metrics for the region
            - cross_station_issues: Issues identified across stations
        """
        logger.info(f"Analyzing data for region {region}")
        
        # Filter data for region
        regional_data = data[data['region'] == region].copy()
        if regional_data.empty:
            logger.warning(f"No data found for region {region}")
            return {}
            
        # Get unique stations
        stations = regional_data['station_id'].unique()
        
        # Analyze each station
        station_analyses = {}
        for station_id in stations:
            analysis = self.analyze_station_data(
                data=regional_data,
                station_id=station_id,
                start_year=start_year,
                end_year=end_year
            )
            station_analyses[station_id] = analysis
            
        # Generate regional summary
        return {
            'station_analyses': station_analyses,
            'regional_summary': self._summarize_regional_analysis(station_analyses),
            'cross_station_issues': self._identify_cross_station_issues(station_analyses)
        }
        
    def _empty_analysis_result(self) -> Dict:
        """Generate an empty analysis result structure."""
        return {
            'temporal_coverage': 0.0,
            'completeness': 0.0,
            'data_quality_issues': [],
            'anomalies': [],
            'summary_stats': {}
        }
        
    def _analyze_temporal_coverage(self, data: pd.DataFrame) -> float:
        """Analyze temporal coverage of the data.
        
        Args:
            data: DataFrame containing station data
            
        Returns:
            Coverage percentage (0.0 to 1.0)
        """
        if data.empty:
            return 0.0
            
        years = sorted(data['year'].unique())
        if not years:
            return 0.0
            
        # Calculate coverage
        actual_range = years[-1] - years[0] + 1
        expected_range = len(years)
        
        return expected_range / actual_range if actual_range > 0 else 0.0
        
    def _analyze_completeness(self, data: pd.DataFrame) -> float:
        """Analyze data completeness.
        
        Args:
            data: DataFrame containing station data
            
        Returns:
            Completeness percentage (0.0 to 1.0)
        """
        if data.empty:
            return 0.0
            
        total_records = len(data)
        missing_records = len(data[data['missing_days'] > 0])
            
        return 1.0 - (missing_records / total_records if total_records > 0 else 0.0)
        
    def _identify_quality_issues(self, data: pd.DataFrame) -> List[Dict]:
        """Identify data quality issues.
        
        Args:
            data: DataFrame containing station data
            
        Returns:
            List of identified issues
        """
        issues = []
        
        # Check for years with high missing days
        high_missing = data[data['missing_days'] > 180]
        for _, row in high_missing.iterrows():
            issues.append({
                'type': 'high_missing_days',
                'year': row['year'],
                'missing_days': row['missing_days'],
                'description': f"More than 180 missing days in {row['year']}"
            })
            
        # Check for unusually high flood days
        mean_floods = data['flood_days'].mean()
        std_floods = data['flood_days'].std()
        if not pd.isna(std_floods) and std_floods > 0:
            high_floods = data[data['flood_days'] > mean_floods + 3*std_floods]
            for _, row in high_floods.iterrows():
                issues.append({
                    'type': 'high_flood_days',
                    'year': row['year'],
                    'flood_days': row['flood_days'],
                    'description': f"Unusually high flood days in {row['year']}"
                })
                
        # Check for gaps in years
        years = sorted(data['year'].unique())
        for i in range(len(years)-1):
            if years[i+1] - years[i] > 1:
                issues.append({
                    'type': 'year_gap',
                    'start_year': years[i],
                    'end_year': years[i+1],
                    'description': f"Gap in data between {years[i]} and {years[i+1]}"
                })
                
        return issues
        
    def _detect_anomalies(self, data: pd.DataFrame) -> List[Dict]:
        """Detect anomalies in the data.
        
        Args:
            data: DataFrame containing station data
            
        Returns:
            List of detected anomalies
        """
        anomalies = []
        
        if data.empty or len(data) < 2:
            return anomalies
            
        # Calculate year-over-year changes
        data = data.sort_values('year')
        data['flood_days_change'] = data['flood_days'].diff()
        
        # Detect sudden changes
        mean_change = data['flood_days_change'].mean()
        std_change = data['flood_days_change'].std()
        
        if not pd.isna(std_change) and std_change > 0:
            threshold = 3 * std_change
            sudden_changes = data[abs(data['flood_days_change']) > threshold]
            
            for _, row in sudden_changes.iterrows():
                anomalies.append({
                    'type': 'sudden_change',
                    'year': row['year'],
                    'change': row['flood_days_change'],
                    'description': f"Sudden change in flood days in {row['year']}"
                })
                
        return anomalies
        
    def _calculate_summary_stats(self, data: pd.DataFrame) -> Dict:
        """Calculate summary statistics.
        
        Args:
            data: DataFrame containing station data
            
        Returns:
            Dict of summary statistics
        """
        if data.empty:
            return {}
            
        return {
            'years_covered': len(data['year'].unique()),
            'start_year': int(data['year'].min()),
            'end_year': int(data['year'].max()),
            'total_records': len(data),
            'flood_days_stats': {
                'mean': float(data['flood_days'].mean()),
                'std': float(data['flood_days'].std()),
                'min': float(data['flood_days'].min()),
                'max': float(data['flood_days'].max()),
                'median': float(data['flood_days'].median())
            },
            'missing_days_stats': {
                'mean': float(data['missing_days'].mean()),
                'total': int(data['missing_days'].sum()),
                'years_with_missing': len(data[data['missing_days'] > 0])
            }
        }
        
    def _summarize_regional_analysis(self, station_analyses: Dict[str, Dict]) -> Dict:
        """Summarize analyses across all stations in a region.
        
        Args:
            station_analyses: Dict mapping station IDs to their analysis results
            
        Returns:
            Dict containing regional summary metrics
        """
        if not station_analyses:
            return {}
            
        # Calculate average metrics
        completeness_values = [a['completeness'] for a in station_analyses.values()]
        
        return {
            'stations_analyzed': len(station_analyses),
            'mean_completeness': np.mean(completeness_values),
            'stations_with_issues': sum(
                1 for a in station_analyses.values()
                if a['data_quality_issues']
            ),
            'stations_with_anomalies': sum(
                1 for a in station_analyses.values()
                if a['anomalies']
            )
        }
        
    def _identify_cross_station_issues(self, station_analyses: Dict[str, Dict]) -> List[Dict]:
        """Identify issues that appear across multiple stations.
        
        Args:
            station_analyses: Dict mapping station IDs to their analysis results
            
        Returns:
            List of cross-station issues
        """
        if not station_analyses:
            return []
            
        issues = []
        
        # Collect years with issues across stations
        problem_years = {}
        for station_id, analysis in station_analyses.items():
            for issue in analysis['data_quality_issues']:
                year = issue.get('year')
                if year:
                    if year not in problem_years:
                        problem_years[year] = []
                    problem_years[year].append({
                        'station_id': station_id,
                        'issue_type': issue['type']
                    })
                    
        # Identify years with issues across multiple stations
        for year, stations in problem_years.items():
            if len(stations) > 1:
                issues.append({
                    'type': 'multi_station_issue',
                    'year': year,
                    'affected_stations': len(stations),
                    'description': f"Multiple stations had issues in {year}",
                    'details': stations
                })
                
        return issues
        
    def _get_region_stations(self, region: str) -> List[Dict]:
        """Get list of stations in a region.
        
        Args:
            region: Name of the region
            
        Returns:
            List of station records
        """
        # Convert region name to filename format
        region_file = region.lower().replace(' ', '_')
        config_file = self.cache.config_dir / "tide_stations" / f"{region_file}_tide_stations.yaml"
        
        try:
            with open(config_file) as f:
                import yaml
                config = yaml.safe_load(f)
                return [
                    {
                        'id': station_id,
                        'name': station_data['name'],
                        'location': station_data['location']
                    }
                    for station_id, station_data in config['stations'].items()
                ]
        except Exception as e:
            logger.error(f"Error loading stations for region {region}: {e}")
            return [] 