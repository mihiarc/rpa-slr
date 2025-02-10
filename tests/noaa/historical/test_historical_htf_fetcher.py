"""Tests for the Historical HTF Fetcher."""

import pytest
import json
from pathlib import Path
from unittest.mock import patch, Mock
import yaml

from src.noaa.core.noaa_client import NOAAClient, NOAAApiError
from src.noaa.core.cache_manager import NOAACache
from src.noaa.historical.historical_htf_fetcher import HistoricalHTFFetcher

# Sample data for testing
SAMPLE_HISTORICAL_RESPONSE = {
    "metadata": {
        "id": "8638610",
        "name": "Sewells Point, VA",
        "lat": "36.9467",
        "lon": "-76.3300"
    },
    "AnnualFloodCount": [
        {
            "stnId": "8638610",
            "stnName": "Sewells Point, VA",
            "year": 2020,
            "majCount": 0,
            "modCount": 1,
            "minCount": 6,
            "nanCount": 0
        },
        {
            "stnId": "8638610",
            "stnName": "Sewells Point, VA",
            "year": 2021,
            "majCount": 2,
            "modCount": 2,
            "minCount": 6,
            "nanCount": 0
        }
    ]
}

SAMPLE_STATIONS = {
    '8638610': {
        'name': 'Sewells Point, VA',
        'latitude': '36.9467',
        'longitude': '-76.3300',
        'region': 'mid_atlantic'
    }
}

@pytest.fixture
def setup_config_files(tmp_path):
    """Create temporary config files for testing."""
    config_dir = tmp_path
    config_dir.mkdir(exist_ok=True)

    # Write NOAA settings
    settings_file = config_dir / "noaa_api_settings.yaml"
    settings = {
        'api': {
            'base_url': 'https://api.tidesandcurrents.noaa.gov/dpapi/prod/webapi',
            'requests_per_second': 2.0,
            'endpoints': {
                'historical': '/htf/htf_annual.json'
            }
        },
        'cache': {
            'directory': 'data/cache',
            'data_types': ['historical'],
            'retention': {
                'historical': 30
            },
            'update_frequency': {
                'historical': 24
            }
        },
        'stations': {
            'config_dir': 'tide_stations'
        }
    }
    with open(settings_file, 'w') as f:
        yaml.dump(settings, f)

    # Create tide stations directory and config
    stations_dir = config_dir / "tide_stations"
    stations_dir.mkdir(exist_ok=True)
    
    region_file = stations_dir / "mid_atlantic_tide_stations.yaml"
    region_config = {
        'metadata': {
            'region': 'Mid-Atlantic',
            'description': 'Test stations'
        },
        'stations': SAMPLE_STATIONS
    }
    with open(region_file, 'w') as f:
        yaml.dump(region_config, f)

    # Create cache directory structure
    cache_dir = config_dir.parent / "data" / "cache"
    (cache_dir / 'historical').mkdir(parents=True, exist_ok=True)

    return config_dir

class TestHistoricalHTFFetcher:
    """Test suite for HistoricalHTFFetcher."""

    def test_init(self, setup_config_files):
        """Test fetcher initialization."""
        cache = NOAACache(config_dir=setup_config_files)
        fetcher = HistoricalHTFFetcher(cache=cache)
        assert isinstance(fetcher, HistoricalHTFFetcher)
        assert isinstance(fetcher.cache, NOAACache)

    def test_get_station_data_success(self, setup_config_files):
        """Test successful retrieval of station data."""
        cache = NOAACache(config_dir=setup_config_files)
        fetcher = HistoricalHTFFetcher(cache=cache)

        with patch('src.noaa.core.noaa_client.NOAAClient.fetch_annual_flood_counts') as mock_fetch:
            mock_fetch.return_value = SAMPLE_HISTORICAL_RESPONSE['AnnualFloodCount']
            data = fetcher.get_station_data('8638610')
            
            assert len(data) == 2
            assert data[0]['year'] == 2020
            assert data[0]['minCount'] == 6
            assert data[1]['year'] == 2021
            assert data[1]['majCount'] == 2

    def test_get_station_data_invalid_station(self, setup_config_files):
        """Test handling of invalid station ID."""
        cache = NOAACache(config_dir=setup_config_files)
        fetcher = HistoricalHTFFetcher(cache=cache)

        with pytest.raises(ValueError, match="Invalid station ID"):
            fetcher.get_station_data('invalid_id')

    def test_get_station_data_api_error(self, setup_config_files):
        """Test handling of API errors."""
        cache = NOAACache(config_dir=setup_config_files)
        fetcher = HistoricalHTFFetcher(cache=cache)

        with patch('src.noaa.core.noaa_client.NOAAClient.fetch_annual_flood_counts') as mock_fetch:
            mock_fetch.side_effect = NOAAApiError("API Error")
            with pytest.raises(NOAAApiError):
                fetcher.get_station_data('8638610')

    def test_get_complete_dataset(self, setup_config_files):
        """Test retrieval of complete dataset."""
        cache = NOAACache(config_dir=setup_config_files)
        fetcher = HistoricalHTFFetcher(cache=cache)

        with patch('src.noaa.core.noaa_client.NOAAClient.fetch_annual_flood_counts') as mock_fetch:
            mock_fetch.return_value = SAMPLE_HISTORICAL_RESPONSE['AnnualFloodCount']
            dataset = fetcher.get_complete_dataset(['8638610'])
            
            assert len(dataset) == 1
            assert '8638610' in dataset
            assert len(dataset['8638610']) == 2

    def test_get_dataset_status(self, setup_config_files):
        """Test dataset status retrieval."""
        cache = NOAACache(config_dir=setup_config_files)
        fetcher = HistoricalHTFFetcher(cache=cache)

        with patch('src.noaa.core.noaa_client.NOAAClient.fetch_annual_flood_counts') as mock_fetch:
            mock_fetch.return_value = SAMPLE_HISTORICAL_RESPONSE['AnnualFloodCount']
            fetcher.get_station_data('8638610')  # Populate cache
            
            status = fetcher.get_dataset_status()
            assert status['year_range']['min'] == 2020
            assert status['year_range']['max'] == 2021
            assert status['completeness'] == 1.0  # No missing data

    def test_generate_dataset(self, setup_config_files):
        """Test dataset generation."""
        cache = NOAACache(config_dir=setup_config_files)
        fetcher = HistoricalHTFFetcher(cache=cache)
        output_dir = setup_config_files.parent / "output"

        with patch('src.noaa.core.noaa_client.NOAAClient.fetch_annual_flood_counts') as mock_fetch:
            mock_fetch.return_value = SAMPLE_HISTORICAL_RESPONSE['AnnualFloodCount']
            output_file = fetcher.generate_dataset(output_dir, ['8638610'])
            
            assert output_file.exists()
            assert output_file.name == 'historical_htf.parquet' 