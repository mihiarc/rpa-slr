"""Tests for the Historical High Tide Flooding Service."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from pathlib import Path
import json

from src.noaa.htf_fetcher import HistoricalHTFService
from src.noaa.client import NOAAApiError

# Test data fixtures
SAMPLE_STATIONS = [
    {
        "id": "8638610",
        "name": "Sewells Point, VA",
        "lat": 36.9467,
        "lon": -76.3300
    },
    {
        "id": "8658120",
        "name": "Wilmington, NC",
        "lat": 34.2267,
        "lon": -77.9533
    }
]

SAMPLE_FLOOD_DATA = [
    {
        "stnId": "8638610",
        "stnName": "Sewells Point, VA",
        "year": 2010,
        "majCount": 0,
        "modCount": 1,
        "minCount": 6,
        "nanCount": 0
    },
    {
        "stnId": "8638610",
        "stnName": "Sewells Point, VA",
        "year": 2011,
        "majCount": 2,
        "modCount": 2,
        "minCount": 6,
        "nanCount": 0
    }
]

@pytest.fixture
def mock_client():
    """Create a mock NOAA client."""
    with patch('src.noaa.htf_fetcher.NOAAClient') as mock:
        client = mock.return_value
        client.fetch_annual_flood_counts.return_value = SAMPLE_FLOOD_DATA
        yield client

@pytest.fixture
def mock_cache():
    """Create a mock NOAA cache."""
    with patch('src.noaa.htf_fetcher.NOAACache') as mock:
        cache = mock.return_value
        cache.get_stations.return_value = SAMPLE_STATIONS
        cache.validate_station_id.return_value = True
        yield cache

@pytest.fixture
def service(mock_client, mock_cache, tmp_path):
    """Create a HistoricalHTFService instance with mocked dependencies."""
    return HistoricalHTFService(cache_dir=tmp_path)

class TestHistoricalHTFService:
    """Test suite for HistoricalHTFService."""
    
    def test_init(self, service):
        """Test service initialization."""
        assert service.client is not None
        assert service.cache is not None
    
    def test_get_station_data_success(self, service, mock_client, mock_cache):
        """Test successful retrieval of station data."""
        # Test with specific station
        data = service.get_station_data(station="8638610")
        assert len(data) == 2
        assert data[0]["stnId"] == "8638610"
        assert data[0]["year"] == 2010
        
        # Verify client call
        mock_client.fetch_annual_flood_counts.assert_called_with(station="8638610")
        
        # Verify cache calls
        mock_cache.validate_station_id.assert_called_with("8638610")
        assert mock_cache.save_annual_data.call_count == 2  # Two records saved
    
    def test_get_station_data_invalid_station(self, service, mock_cache):
        """Test handling of invalid station ID."""
        mock_cache.validate_station_id.return_value = False
        
        with pytest.raises(ValueError, match="Invalid station ID"):
            service.get_station_data(station="invalid")
    
    def test_get_station_data_api_error(self, service, mock_client):
        """Test handling of API errors."""
        mock_client.fetch_annual_flood_counts.side_effect = NOAAApiError("API Error")
        
        with pytest.raises(NOAAApiError):
            service.get_station_data(station="8638610")
    
    def test_get_complete_dataset(self, service, mock_client):
        """Test retrieval of complete dataset."""
        # Setup different responses for different stations
        def mock_fetch(station=None):
            if station == "8638610":
                return SAMPLE_FLOOD_DATA
            return []
        mock_client.fetch_annual_flood_counts.side_effect = mock_fetch
        
        dataset = service.get_complete_dataset(stations=["8638610", "8658120"])
        
        assert "8638610" in dataset
        assert len(dataset["8638610"]) == 2
        assert "8658120" not in dataset  # No data returned for this station
    
    def test_generate_dataset(self, service, mock_client, tmp_path):
        """Test dataset generation and file output."""
        output_path = tmp_path / "output"
        output_file = service.generate_dataset(output_path, stations=["8638610"])
        
        # Verify file creation
        assert output_file.exists()
        assert output_file.suffix == ".parquet"
        
        # Read and verify data
        df = pd.read_parquet(output_file)
        assert len(df) == 2  # Two records
        assert "total_flood_days" in df.columns
        assert "data_completeness" in df.columns
        
        # Verify derived fields
        first_row = df.iloc[0]
        assert first_row["total_flood_days"] == 7  # 0 + 1 + 6
        assert first_row["data_completeness"] == 1.0  # No missing days
    
    def test_get_dataset_status_empty(self, service, mock_client):
        """Test dataset status with no data."""
        mock_client.fetch_annual_flood_counts.return_value = []
        
        status = service.get_dataset_status()
        assert status["station_count"] == len(SAMPLE_STATIONS)
        assert status["year_range"]["min"] is None
        assert status["year_range"]["max"] is None
        assert status["completeness"] == 0.0
    
    def test_get_dataset_status_with_data(self, service, mock_client):
        """Test dataset status with sample data."""
        # Setup data with varying completeness
        mixed_data = [
            {
                "stnId": "8638610",
                "stnName": "Sewells Point, VA",
                "year": 2010,
                "majCount": 0,
                "modCount": 1,
                "minCount": 6,
                "nanCount": 0  # Complete record
            },
            {
                "stnId": "8638610",
                "stnName": "Sewells Point, VA",
                "year": 2011,
                "majCount": 2,
                "modCount": 2,
                "minCount": 6,
                "nanCount": 30  # Incomplete record
            }
        ]
        mock_client.fetch_annual_flood_counts.return_value = mixed_data
        
        status = service.get_dataset_status()
        
        # Verify station count
        assert status["station_count"] == len(SAMPLE_STATIONS)
        
        # Verify year range
        assert status["year_range"]["min"] == 2010
        assert status["year_range"]["max"] == 2011
        
        # Verify completeness (1 complete record out of 2)
        assert status["completeness"] == 0.5
    
    def test_error_handling_in_complete_dataset(self, service, mock_client):
        """Test error handling when fetching complete dataset."""
        def mock_fetch(station=None):
            if station == "8638610":
                return SAMPLE_FLOOD_DATA
            raise NOAAApiError("API Error")
            
        mock_client.fetch_annual_flood_counts.side_effect = mock_fetch
        
        # Should continue despite errors
        dataset = service.get_complete_dataset(stations=["8638610", "8658120"])
        assert "8638610" in dataset  # Successful fetch included
        assert "8658120" not in dataset  # Failed fetch excluded
    
    def test_data_transformation(self, service, mock_client, tmp_path):
        """Test data transformation logic in dataset generation."""
        output_path = tmp_path / "output"
        output_file = service.generate_dataset(output_path, stations=["8638610"])
        
        df = pd.read_parquet(output_file)
        
        # Verify all required columns
        required_columns = {
            'station_id', 'station_name', 'year',
            'major_flood_days', 'moderate_flood_days', 'minor_flood_days',
            'missing_days', 'total_flood_days', 'data_completeness'
        }
        assert all(col in df.columns for col in required_columns)
        
        # Verify calculations
        assert df['total_flood_days'].equals(
            df['major_flood_days'] + df['moderate_flood_days'] + df['minor_flood_days']
        )
        assert all(df['data_completeness'] <= 1.0)
        assert all(df['data_completeness'] >= 0.0)

def test_historical_service_init():
    service = HistoricalHTFService()
    assert service is not None
    
def test_get_station_data():
    # Mock setup
    with patch('src.noaa.htf_fetcher.NOAAClient') as mock:
        mock.return_value.fetch_annual_flood_counts.return_value = []
        service = HistoricalHTFService()
        data = service.get_station_data("8638610")
        assert data == []

def test_cache_integration():
    # Mock setup
    with patch('src.noaa.htf_fetcher.NOAACache') as mock:
        mock.return_value.validate_station_id.return_value = True
        service = HistoricalHTFService()
        assert service.cache is not None 