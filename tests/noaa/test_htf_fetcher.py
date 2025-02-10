"""Tests for the Historical HTF Fetcher."""
import os
from pathlib import Path
import pytest
import yaml
from unittest.mock import Mock, patch

from src.noaa.core.noaa_client import NOAAApiError
from src.noaa.core.cache_manager import NOAACache
from src.noaa.historical.historical_htf_fetcher import HistoricalHTFFetcher

# Sample data for testing
SAMPLE_NOAA_SETTINGS = {
    'api': {
        'base_url': 'https://api.tidesandcurrents.noaa.gov/dpapi/prod/webapi',
        'rate_limit': 10
    },
    'cache': {
        'directory': 'data/cache',
        'data_types': ['historical', 'projected']
    }
}

SAMPLE_STATIONS = {
    '8638610': {
        'name': 'Sewells Point, VA',
        'latitude': '36.9467',
        'longitude': '-76.3300',
        'region': 'mid_atlantic'
    },
    '8658120': {
        'name': 'Wilmington, NC',
        'latitude': '34.2267',
        'longitude': '-77.9533',
        'region': 'mid_atlantic'
    }
}

@pytest.fixture
def setup_config_files(tmp_path):
    """Create temporary config files for testing."""
    # Create config directory
    config_dir = tmp_path
    config_dir.mkdir(exist_ok=True)

    # Write NOAA settings
    settings_file = config_dir / "noaa_api_settings.yaml"
    with open(settings_file, 'w') as f:
        yaml.dump(SAMPLE_NOAA_SETTINGS, f)

    # Create cache directory structure
    cache_dir = tmp_path / "data" / "cache"
    for data_type in SAMPLE_NOAA_SETTINGS['cache']['data_types']:
        (cache_dir / data_type).mkdir(parents=True, exist_ok=True)

    return config_dir

@pytest.fixture
def mock_client():
    """Create a mock NOAA client."""
    with patch('src.noaa.core.noaa_client.NOAAClient') as mock:
        mock.return_value.fetch_annual_flood_counts.return_value = {
            'AnnualFloodCount': [
                {'year': '2020', 'count': 10},
                {'year': '2021', 'count': 15}
            ]
        }
        yield mock

@pytest.fixture
def mock_cache():
    """Create a mock NOAA cache."""
    with patch('src.noaa.core.cache_manager.NOAACache') as mock:
        mock.return_value.validate_station_id.return_value = True
        mock.return_value.get_stations.return_value = SAMPLE_STATIONS
        yield mock

@pytest.fixture
def service(setup_config_files):
    """Create a HistoricalHTFFetcher instance with real cache."""
    cache = NOAACache(config_dir=setup_config_files)
    return HistoricalHTFFetcher(cache=cache)

class TestHistoricalHTFFetcher:
    """Test cases for the Historical HTF Fetcher."""

    def test_init(self, service):
        """Test fetcher initialization."""
        assert isinstance(service, HistoricalHTFFetcher)
        assert isinstance(service.cache, NOAACache)

    def test_get_station_data_success(self, service, mock_client):
        """Test successful retrieval of station data."""
        data = service.get_station_data('8638610', 2020, 2021)
        assert len(data) == 2
        assert data[0]['year'] == '2020'
        assert data[0]['count'] == 10

    def test_get_station_data_invalid_station(self, service):
        """Test handling of invalid station ID."""
        with pytest.raises(ValueError):
            service.get_station_data('invalid_id', 2020, 2021)

    def test_get_station_data_api_error(self, service, mock_client):
        """Test handling of API errors."""
        mock_client.return_value.fetch_annual_flood_counts.side_effect = NOAAApiError("API Error")
        with pytest.raises(NOAAApiError):
            service.get_station_data('8638610', 2020, 2021)

def test_historical_service_init():
    """Test service initialization with mock cache."""
    mock_cache = Mock(spec=NOAACache)
    service = HistoricalHTFFetcher(cache=mock_cache)
    assert isinstance(service, HistoricalHTFFetcher)
    assert service.cache == mock_cache

def test_get_station_data():
    """Test get_station_data with mock cache and client."""
    mock_cache = Mock(spec=NOAACache)
    mock_cache.validate_station_id.return_value = True
    service = HistoricalHTFFetcher(cache=mock_cache)
    
    with patch('src.noaa.core.noaa_client.NOAAClient') as mock_client:
        mock_client.return_value.fetch_annual_flood_counts.return_value = {
            'AnnualFloodCount': [
                {'year': '2020', 'count': 10}
            ]
        }
        data = service.get_station_data('8638610', 2020, 2020)
        assert len(data) == 1
        assert data[0]['year'] == '2020'
        assert data[0]['count'] == 10

def test_cache_integration():
    """Test cache integration with mock cache."""
    mock_cache = Mock(spec=NOAACache)
    mock_cache.validate_station_id.return_value = True
    service = HistoricalHTFFetcher(cache=mock_cache)
    
    with patch('src.noaa.core.noaa_client.NOAAClient') as mock_client:
        mock_client.return_value.fetch_annual_flood_counts.return_value = {
            'AnnualFloodCount': [
                {'year': '2020', 'count': 10}
            ]
        }
        service.get_station_data('8638610', 2020, 2020)
        mock_cache.validate_station_id.assert_called_once_with('8638610') 