"""Tests for the NOAA Cache Manager."""

import pytest
import json
import yaml
from pathlib import Path
from unittest.mock import patch, mock_open
import os

from src.noaa.core.cache_manager import NOAACache

# Sample data for testing
SAMPLE_NOAA_SETTINGS = {
    'api': {
        'base_url': 'https://api.tidesandcurrents.noaa.gov/dpapi/prod/webapi',
        'requests_per_second': 2.0,
        'endpoints': {
            'historical': '/htf/htf_annual.json',
            'projected': '/htf/htf_projection_decadal.json'
        }
    },
    'cache': {
        'directory': 'data/cache',
        'data_types': ['historical', 'projected']
    },
    'stations': {
        'config_dir': 'tide_stations'
    }
}

SAMPLE_STATIONS = {
    '8638610': {
        'name': 'Sewells Point, VA',
        'location': {
            'lat': 36.9467,
            'lon': -76.3300
        },
        'region': 'mid_atlantic'
    },
    '8658120': {
        'name': 'Wilmington, NC',
        'location': {
            'lat': 34.2267,
            'lon': -77.9533
        },
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
    with open(settings_file, 'w') as f:
        yaml.dump(SAMPLE_NOAA_SETTINGS, f)

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
    for data_type in SAMPLE_NOAA_SETTINGS['cache']['data_types']:
        (cache_dir / data_type).mkdir(parents=True, exist_ok=True)

    return config_dir

class TestNOAACache:
    """Test suite for NOAACache."""

    def test_init_default(self, setup_config_files, monkeypatch):
        """Test cache initialization with default config directory."""
        monkeypatch.setattr(Path, "parent", lambda _: setup_config_files)
        cache = NOAACache()
        assert cache.config_dir == setup_config_files
        assert cache.cache_dir == setup_config_files.parent / "data" / "cache"

    def test_init_custom(self, setup_config_files):
        """Test cache initialization with custom config directory."""
        cache = NOAACache(config_dir=setup_config_files)
        stations = cache.get_stations()
        assert len(stations) == 2
        assert stations[0]['id'] == '8638610'
        assert stations[0]['region'] == 'mid_atlantic'

    def test_validate_station_id_valid(self, setup_config_files):
        """Test validation of valid station ID."""
        cache = NOAACache(config_dir=setup_config_files)
        assert cache.validate_station_id('8638610') is True

    def test_validate_station_id_invalid(self, setup_config_files):
        """Test validation of invalid station ID."""
        cache = NOAACache(config_dir=setup_config_files)
        assert cache.validate_station_id('invalid_id') is False

    def test_get_stations(self, setup_config_files):
        """Test retrieval of all stations."""
        cache = NOAACache(config_dir=setup_config_files)
        stations = cache.get_stations()
        assert len(stations) == 2
        assert stations[0]['id'] == '8638610'
        assert stations[0]['region'] == 'mid_atlantic'

    def test_get_stations_by_region(self, setup_config_files):
        """Test retrieval of stations by region."""
        cache = NOAACache(config_dir=setup_config_files)
        stations = cache.get_stations(region='mid_atlantic')
        assert len(stations) == 2
        assert all(s['region'] == 'mid_atlantic' for s in stations)

    def test_get_stations_invalid_region(self, setup_config_files):
        """Test retrieval of stations with invalid region."""
        cache = NOAACache(config_dir=setup_config_files)
        stations = cache.get_stations(region='invalid')
        assert len(stations) == 0

    def test_historical_data_caching(self, setup_config_files):
        """Test caching of historical data."""
        cache = NOAACache(config_dir=setup_config_files)
        data = {'year': 2020, 'count': 10}
        cache.save_historical_data('8638610', 2020, data)
        assert cache.get_historical_data('8638610', 2020) == data

    def test_projected_data_caching(self, setup_config_files):
        """Test caching of projected data."""
        cache = NOAACache(config_dir=setup_config_files)
        data = {'decade': 2050, 'count': 20}
        cache.save_projected_data('8638610', 2050, data)
        assert cache.get_projected_data('8638610', 2050) == data 