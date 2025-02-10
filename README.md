# County-Level Tidal Flooding Data Processing

A Python package for retrieving and processing NOAA high tide flooding (HTF) data at the county level. This package provides tools for both historical data analysis and future projections.

## Features

- **Historical Data Processing**
  - Fetch historical high tide flooding data from NOAA
  - Process data by region
    - regions define in `config/region_mappings.yaml`
    - each region has its own tide stations defined in `config/{region}_tide_stations.yaml`
  - Imputation module matches tide stations to reference points along the coast of each county
  - Assignment module assigns annual flood days to each county based on imputation module results
  - Outputs annual flood days by county in CSV

- **Projected Data Processing**
  - Fetch projected high tide flooding data from NOAA
  - Process projections by region and scenario
    - regions define in `config/region_mappings.yaml`
    - each region has its own tide stations defined in `config/{region}_tide_stations.yaml`
    - scenarios define in `config/scenario_mappings.yaml`
  - Imputation module matches tide stations to reference points along the coast of each county
  - Assignment module assigns projected flood days to each county based on imputation module results
  - Outputs projected flood days by county in CSV

## Installation

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install the package:
```bash
# For users
pip install .

# For developers
pip install -e .[dev]
```

## Requirements

- Python 3.8 or higher
- Core dependencies:
  - requests >= 2.31.0
  - pandas >= 2.0.0
  - numpy >= 1.24.0
  - pyarrow >= 14.0.1
  - pyyaml >= 6.0.0
  - scipy >= 1.10.0
  - matplotlib >= 3.7.0

Development dependencies (installed with `.[dev]`):
  - pytest >= 7.0.0
  - black >= 23.0.0
  - flake8 >= 6.0.0
  - mypy >= 1.0.0

## Configuration

The package uses YAML configuration files located in the `config` directory:

- `noaa_api_settings.yaml`: NOAA API settings and data type configurations
- `region_mappings.yaml`: Region mappings
- `{region}_tide_stations.yaml`: Region-specific tide station configurations

### Regional Configurations

Each region has its own configuration file that defines:
- Tide station locations (latitude/longitude)
- Station names and identifiers
- Regional groupings/classifications
- Coverage areas

Supported Regions:
- Alaska (`alaska_tide_stations.yaml`)
- Hawaii (`hawaii_tide_stations.yaml`)
- Pacific Islands (`pacific_islands_tide_stations.yaml`)
- Virgin Islands (`virgin_islands_tide_stations.yaml`)
- Puerto Rico (`puerto_rico_tide_stations.yaml`)
- Mid-Atlantic (`mid_atlantic_tide_stations.yaml`)
- North Atlantic (`north_atlantic_tide_stations.yaml`)
- South Atlantic (`south_atlantic_tide_stations.yaml`)
- Gulf Coast (`gulf_coast_tide_stations.yaml`)
- West Coast (`west_coast_tide_stations.yaml`)

## Usage

### Historical Data Processing

Process historical HTF data for a specific region:

```bash
python -m noaa.historical.historical_htf_cli \
    --region gulf_coast \
    --start-year 1920 \
    --end-year 2024 \
    --output-dir output/historical \
    --format parquet
```

### Projected Data Processing

Process projected HTF data for a specific region:

```bash
python -m noaa.projected.projected_htf_cli \
    --region hawaii \
    --start-decade 2020 \
    --end-decade 2100 \
    --output-dir output/projected \
    --format parquet
```

### Data Quality Analysis

Analyze data quality for a specific region or station:

```bash
# Analyze a region
python -m src.analysis.generate_report \
    --region gulf_coast \
    --data-type historical \
    --format markdown \
    --verbose

The data quality analysis provides:
- Temporal coverage metrics
- Data completeness assessment
- Summary statistics

### Common Arguments

Both tools support the following arguments:

- `--region`: Region to process (required)
- `--output-dir`: Output directory for processed data
- `--format`: Output format (csv/parquet for data, markdown for analysis)
- `--verbose`: Enable verbose logging

## Final Output Format

### Historical Data

The processed historical data includes:
- County
- Year
- Number of flood days
- Region identifier

### Projected Data

The projected data includes:
- County
- Decade
- Scenario (e.g., intermediate, intermediate-high)
- Projected flood days
- Region identifier

## Development

### Project Structure

```
county_level_tidal_flooding/
├── config/                           # Configuration files
│   ├── region_mappings.yaml         # Region and county mappings
│   ├── noaa_api_settings.yaml       # NOAA API settings
│   └── tide_stations/
│       ├── alaska_tide_stations.yaml
│       ├── hawaii_tide_stations.yaml
│       ├── pacific_islands_tide_stations.yaml
│       ├── virgin_islands_tide_stations.yaml
│       ├── puerto_rico_tide_stations.yaml
│       ├── mid_atlantic_tide_stations.yaml
│       └── north_atlantic_tide_stations.yaml
│       └── south_atlantic_tide_stations.yaml
│       └── gulf_coast_tide_stations.yaml
│       └── west_coast_tide_stations.yaml
├── output/
│   ├── historical/
│   ├── projected/
│   ├── analysis/
│  
├── src/
│   └── noaa/
│       ├── core/
│       │   ├── __init__.py
│       │   ├── noaa_client.py
│       │   ├── rate_limiter.py
│       │   └── cache_manager.py
│       ├── historical/
│       │   ├── __init__.py
│       │   ├── historical_htf_cli.py
│       │   ├── historical_htf_fetcher.py
│       │   └── historical_htf_processor.py
│       └── projected/
│           ├── __init__.py
│           ├── projected_htf_cli.py
│           ├── projected_htf_fetcher.py
│           └── projected_htf_processor.py
├── README.md
└── requirements.txt
```

## Analysis Pipeline

### Historical HTF Analysis
1. Processing historical HTF observations by region
   - Data structure: Annual counts of minor flooding events
   - Time range: 1920-2024
   - Source: NOAA Annual Flood Count Product (minor flooding only)
2. Generating county-level historical estimates
   - Output: Annual minor flooding frequency by county

### Projected HTF Analysis
1. Processing projected HTF data by region
   - Data structure: Decadal flooding frequency projections
   - Time range: 2020-2100
   - Source: NOAA Decadal Projections Product
   - Multiple sea level rise scenarios
3. Generating county-level projections
   - Output: Projected flooding frequency by county
   - Separate outputs for each sea level rise scenario

The separation into historical and projected analyses is necessary due to:
- Different data structures and temporal resolutions
- Distinct quality control requirements

### Imputation Pipeline

1. Create station to county mapping
   - Generate reference points along the coast of each county
   - Match stations to the nearest reference point with weights based on distance
   - Output: station to county mapping

### Assignment Pipeline

1. Assign flood days to each county based on the station to county mapping
   - aggregate flood days by county and year
   - Output: county-level flood days in csv

### Imputation Coverage Visualization

The package includes visualization tools for analyzing tide gauge coverage across different coastal regions:

- **Coverage Metrics**: 
  - Combines number of tide stations (n) and their mean weights (w̄) as CS = n × w̄
  - Weights decrease with distance from each station
  - Higher scores indicate better coverage

- **Regional Visualizations**:
  - Mid Atlantic (`imputation_mid_atlantic.py`)
  - North Atlantic (`imputation_north_atlantic.py`)
  - South Atlantic (`imputation_south_atlantic.py`)
  - Gulf Coast (`imputation_gulf_coast.py`)
  - Puerto Rico (`imputation_puerto_rico.py`)
  - Virgin Islands (`imputation_virgin_islands.py`)
  - Hawaii (`imputation_hawaii.py`)

Each visualization includes:
- Choropleth maps showing coverage scores
- Tide station locations and names
- Coverage statistics and metrics
- Region-specific projections and styling

Output maps are saved to `output/maps/imputation/` directory.

