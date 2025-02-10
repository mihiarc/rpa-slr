# Cache System Documentation

## Overview

The cache system manages NOAA high tide flooding (HTF) data at two distinct levels:
1. Historical flood count data
2. Projected (decadal) flood data

Each data type has its own specific structure and requirements based on the NOAA data products.

## Data Organization

### Historical Data
- Annual flood counts by station
- Structure matches NOAA's Annual Flood Count product:
```json
{
  "stnId": "8638610",
  "stnName": "Sewells Point, VA",
  "year": 2020,
  "majCount": 0,
  "modCount": 1,
  "minCount": 6,
  "nanCount": 0
}
```

### Projected Data
- Decadal projections by station
- Structure matches NOAA's Decadal Projections product:
```json
{
  "stnId": "8638610",
  "stnName": "Sewells Point, VA",
  "decade": 2050,
  "source": "...",
  "low": 85,
  "intLow": 100,
  "intermediate": 125,
  "intHigh": 150,
  "high": 185
}
```

## Directory Structure

```
data/
└── cache/
    ├── historical/           # Historical flood count data
    │   └── {station_id}.json
    └── projected/           # Projected flood data
        └── {station_id}.json
```

## Regional Configuration

Stations are organized by region in the `config/tide_stations/` directory:
```
config/
└── tide_stations/
    ├── alaska_tide_stations.yaml
    ├── hawaii_tide_stations.yaml
    ├── mid_atlantic_tide_stations.yaml
    └── ...
```

### Station Configuration Format

The system supports both legacy and current station formats:

```yaml
# Current Format
stations:
  '8638610':
    name: 'Sewells Point, VA'
    latitude: '36.9467'
    longitude: '-76.3300'
    region: 'mid_atlantic'

# Legacy Format (also supported)
stations:
  '8638610':
    name: 'Sewells Point, VA'
    location:
      lat: 36.9467
      lon: -76.3300
    region: 'Mid-Atlantic'
```

## Cache Operations

### Reading Data

```python
# Historical Data
cache = NOAACache()
data = cache.get_historical_data('8638610', year=2020)

# Projected Data
data = cache.get_projected_data('8638610', decade=2050)
```

### Writing Data

```python
# Historical Data
cache.save_historical_data('8638610', 2020, {
    'year': 2020,
    'majCount': 0,
    'modCount': 1,
    'minCount': 6,
    'nanCount': 0
})

# Projected Data
cache.save_projected_data('8638610', 2050, {
    'decade': 2050,
    'low': 85,
    'intLow': 100,
    'intermediate': 125,
    'intHigh': 150,
    'high': 185
})
```

### Station Operations

```python
# Get all stations
stations = cache.get_stations()

# Get stations by region
stations = cache.get_stations(region='mid_atlantic')

# Validate station ID
is_valid = cache.validate_station_id('8638610')
```

## Error Handling

The cache system includes robust error handling:
1. Graceful handling of missing files
2. Logging of file read/write errors
3. Support for both data formats
4. Region-specific error isolation

## Usage Notes

1. Historical and projected data are kept separate to reflect their different structures
2. Region-based organization allows for regional processing
3. Flexible station format support maintains compatibility
4. Error handling prevents cascade failures
5. Each station's data is cached independently 