# Cache System Alignment Guide

## 1. Cache System and Regional Config Alignment

### Current State
- Cache system expects a single station list
- Regional configs are now properly organized in `config/tide_stations/` directory
- NOAA API settings in `noaa_api_settings.yaml`
- Missing cache retention and update frequency settings

### Required Changes

#### A. NOAA API Settings Update
1. Update `noaa_api_settings.yaml` to include cache settings:
```yaml
# Current
cache:
  directory: "data/cache"
  data_types:
    - historical
    - projected

# Required additions
cache:
  directory: "data/cache"
  data_types:
    - historical
    - projected
  retention:
    historical: 30  # days
    projected: 90   # days
    metadata: 7     # days
  update_frequency:
    historical: 24  # hours
    projected: 168  # hours (1 week)
    metadata: 12    # hours
```

2. Update stations configuration in `noaa_api_settings.yaml`:
```yaml
# Current
stations:
  list_file: "data/tide-stations-list.yaml"
  metadata:
    description: "Configuration for NOAA tide gauge stations"
    source: "NOAA Tides and Currents"
    last_updated: "2024-01-01"
    data_url: "https://tidesandcurrents.noaa.gov/api/datagetter"
    documentation_url: "https://tidesandcurrents.noaa.gov/api/"

# Required changes
stations:
  config_dir: "tide_stations"  # Directory containing regional configs
  metadata:
    description: "Configuration for NOAA tide gauge stations"
    source: "NOAA Tides and Currents"
    last_updated: "2024-01-01"
    data_url: "https://tidesandcurrents.noaa.gov/api/datagetter"
    documentation_url: "https://tidesandcurrents.noaa.gov/api/"
```

#### B. Regional Config Standardization
1. Standardize station data format across all files in `config/tide_stations/`:
```yaml
# Current format in some files
stations:
  '8638610':
    name: 'Sewells Point, VA'
    location:
      lat: 36.9467
      lon: -76.3300
    region: 'Mid-Atlantic'

# Required format for all files
stations:
  '8638610':
    name: 'Sewells Point, VA'
    latitude: '36.9467'
    longitude: '-76.3300'
    region: 'mid_atlantic'
```

#### C. Cache Directory Structure
1. Update cache directory resolution in `cache_manager.py`:
```python
# Current
self.cache_dir = Path(self.settings['cache']['directory'])

# Updated
self.cache_dir = self.config_dir.parent / self.settings['cache']['directory']
```

2. Ensure cache directory structure:
```
data/
└── cache/
    ├── historical/
    ├── projected/
    └── metadata/
```

#### D. Regional Config Integration
1. Update `_load_stations()` method to use new directory structure:
```python
def _load_stations(self) -> List[Dict]:
    stations = []
    stations_dir = self.config_dir / self.settings['stations']['config_dir']
    
    for config_file in stations_dir.glob('*.yaml'):
        with open(config_file) as f:
            region_config = yaml.safe_load(f)
            
        for station_id, data in region_config['stations'].items():
            station = {
                'id': station_id,
                'name': data['name'],
                'latitude': str(data.get('latitude') or data['location']['lat']),
                'longitude': str(data.get('longitude') or data['location']['lon']),
                'region': data.get('region', '').lower()
            }
            stations.append(station)
    return stations
```

## 2. Test Alignment Requirements

### A. Test Fixtures Update

1. Update `SAMPLE_NOAA_SETTINGS`:
```python
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
        'data_types': ['historical', 'projected'],
        'retention': {
            'historical': 30,
            'projected': 90,
            'metadata': 7
        },
        'update_frequency': {
            'historical': 24,
            'projected': 168,
            'metadata': 12
        }
    },
    'stations': {
        'config_dir': 'tide_stations',
        'metadata': {
            'description': 'Test configuration'
        }
    }
}
```

2. Update `setup_config_files` fixture:
```python
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
```

### B. Test Case Updates

1. Update station validation tests:
```python
def test_validate_station_id_valid(self, setup_config_files):
    """Test validation of valid station ID."""
    cache = NOAACache(config_dir=setup_config_files)
    assert cache.validate_station_id('8638610') is True
    
def test_validate_station_id_invalid(self, setup_config_files):
    """Test validation of invalid station ID."""
    cache = NOAACache(config_dir=setup_config_files)
    assert cache.validate_station_id('invalid_id') is False
```

2. Update station retrieval tests:
```python
def test_get_stations(self, setup_config_files):
    """Test retrieval of all stations."""
    cache = NOAACache(config_dir=setup_config_files)
    stations = cache.get_stations()
    assert len(stations) == 2
    assert stations[0]['id'] == '8638610'
    assert stations[0]['region'] == 'mid_atlantic'
```

### C. Integration Test Updates

1. Add cache persistence tests:
```python
def test_cache_persistence(self, setup_config_files):
    """Test that cached data persists between instances."""
    cache1 = NOAACache(config_dir=setup_config_files)
    data = {'test': 'data'}
    cache1.save_historical_data('8638610', 2020, data)
    
    cache2 = NOAACache(config_dir=setup_config_files)
    assert cache2.get_historical_data('8638610', 2020) == data
```

## Implementation Steps

1. Update NOAA API settings file with new cache configuration
2. Create data migration script for regional config files
3. Update cache manager implementation to use new directory structure
4. Update test fixtures and test cases
5. Add new integration tests
6. Update documentation

## Migration Notes

1. Existing cache data may need to be migrated
2. Regional configs are now properly organized in `tide_stations/`
3. Tests should be updated before implementation
4. Cache manager should maintain backward compatibility during transition
5. Consider adding validation for config file formats
6. Add logging for cache operations and data migrations 