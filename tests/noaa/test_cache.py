"""Tests for the NOAA cache manager."""

import pytest
import json
from pathlib import Path
from src.noaa.cache import NOAACache

# Test data fixtures
SAMPLE_STATION = {
    "id": "8638610",
    "name": "Sewells Point, VA",
    "lat": 36.9467,
    "lon": -76.3300
}

SAMPLE_HISTORICAL_DATA = {
    "stnId": "8638610",
    "stnName": "Sewells Point, VA",
    "year": 2010,
    "majCount": 0,
    "modCount": 1,
    "minCount": 6,
    "nanCount": 0
}

SAMPLE_PROJECTED_DATA = {
    "stnId": "8638610",
    "stnName": "Sewells Point, VA",
    "decade": 2050,
    "source": "test_source",
    "low": 85,
    "intLow": 100,
    "intermediate": 125,
    "intHigh": 150,
    "high": 185
}

@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for testing."""
    # Create necessary subdirectories
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    
    # Create a sample stations file
    stations_file = data_dir / "tide-stations-list.json"
    stations_file.write_text(json.dumps([SAMPLE_STATION]))
    
    return tmp_path

@pytest.fixture
def cache(temp_dir):
    """Create a NOAACache instance for testing."""
    return NOAACache(config_dir=temp_dir)

class TestNOAACache:
    """Test suite for NOAACache class."""
    
    def test_init_default(self):
        """Test cache initialization with default config directory."""
        cache = NOAACache()
        expected_dir = Path(__file__).parent.parent.parent / "src" / "noaa"
        assert cache.config_dir.resolve() == expected_dir.resolve()
        assert cache.cache_dir.exists()
    
    def test_init_custom(self, temp_dir):
        """Test cache initialization with custom config directory."""
        cache = NOAACache(config_dir=temp_dir)
        assert cache.config_dir == temp_dir
        assert cache.cache_dir.exists()
    
    def test_get_stations(self, cache):
        """Test retrieving station list."""
        stations = cache.get_stations()
        assert len(stations) == 1
        assert stations[0]["id"] == SAMPLE_STATION["id"]
        assert stations[0]["name"] == SAMPLE_STATION["name"]
    
    def test_validate_station_id(self, cache):
        """Test station ID validation."""
        assert cache.validate_station_id("8638610") is True
        assert cache.validate_station_id("invalid") is False
    
    def test_cache_path_generation(self, cache):
        """Test cache file path generation."""
        historical_path = cache._get_cache_path("8638610", "historical")
        projected_path = cache._get_cache_path("8638610", "projected")
        
        assert historical_path.parent.name == "historical"
        assert projected_path.parent.name == "projected"
        assert historical_path.name == "8638610.json"
        assert projected_path.name == "8638610.json"
    
    def test_cache_historical_data(self, cache):
        """Test caching historical data."""
        station_id = "8638610"
        year = 2010
        
        # Cache should be empty initially
        assert cache.get_annual_data(station_id, year, "historical") is None
        
        # Cache the data
        cache.cache_annual_data(station_id, year, SAMPLE_HISTORICAL_DATA, "historical")
        
        # Verify cached data
        cached = cache.get_annual_data(station_id, year, "historical")
        assert cached is not None
        assert cached["year"] == year
        assert cached["majCount"] == SAMPLE_HISTORICAL_DATA["majCount"]
    
    def test_cache_projected_data(self, cache):
        """Test caching projected data."""
        station_id = "8638610"
        decade = 2050
        
        # Cache should be empty initially
        assert cache.get_annual_data(station_id, decade, "projected") is None
        
        # Cache the data
        cache.cache_annual_data(station_id, decade, SAMPLE_PROJECTED_DATA, "projected")
        
        # Verify cached data
        cached = cache.get_annual_data(station_id, decade, "projected")
        assert cached is not None
        assert cached["decade"] == decade
        assert cached["low"] == SAMPLE_PROJECTED_DATA["low"]
    
    def test_update_existing_cache(self, cache):
        """Test updating existing cached data."""
        station_id = "8638610"
        year = 2010
        
        # Cache initial data
        cache.cache_annual_data(station_id, year, SAMPLE_HISTORICAL_DATA, "historical")
        
        # Update with modified data
        modified_data = SAMPLE_HISTORICAL_DATA.copy()
        modified_data["majCount"] = 99
        cache.cache_annual_data(station_id, year, modified_data, "historical")
        
        # Verify updated data
        cached = cache.get_annual_data(station_id, year, "historical")
        assert cached["majCount"] == 99
    
    def test_save_annual_data_alias(self, cache):
        """Test save_annual_data alias method."""
        station_id = "8638610"
        year = 2010
        
        cache.save_annual_data(station_id, year, SAMPLE_HISTORICAL_DATA, "historical")
        cached = cache.get_annual_data(station_id, year, "historical")
        assert cached is not None
        assert cached["year"] == year 