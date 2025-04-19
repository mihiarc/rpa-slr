# County-Level Climate Projection Pipeline

This project processes climate projection data from Google Earth Engine to generate county-level climate indicators for the United States.

## Setup

### Prerequisites

- Python 3.8+
- Google Earth Engine account with authentication set up
- Access to the NASA GDDP-CMIP6 dataset in Earth Engine

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/county-level-tidal-flooding.git
   cd county-level-tidal-flooding
   ```

2. Create a virtual environment using `uv`:
   ```bash
   pip install uv
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   uv pip install -r requirements.txt
   ```

## Configuration

Create a `config/config.yml` file with the following structure:

```yaml
earth_engine:
  project_id: your-gee-project-id
  model: ACCESS-CM2
  image_collection: NASA/GDDP-CMIP6
  variable: tas

climate:
  scenario: ssp585
  variables:
    - tas
    - pr

data:
  years:
    start: 2040
    end: 2060
  dir: data

output:
  dir: output

processing:
  chunk_size: 10000
  max_concurrent_tasks: 3000
```

## Usage

### Running the Pipeline

To run the complete pipeline:

```bash
python -m src.pipeline_orchestrator
```

This will:
1. Authenticate with Google Earth Engine
2. Download climate projection data
3. Process the data to generate county-level indicators
4. Save outputs to the configured output directory

### Download-Only Mode

To only download the data without processing:

```bash
python -m src.pipeline_orchestrator --download-only
```

### Custom Configuration

To use a custom configuration file:

```bash
python -m src.pipeline_orchestrator --config path/to/your/config.yml
```

## Pipeline Components

- **Authentication**: Handles Google Earth Engine authentication
- **Data Download**: Downloads climate projection data for specified models and scenarios
- **Processing**: Calculates climate indicators from raw projection data
- **County Aggregation**: Aggregates indicators to county level
- **Output Generation**: Saves results as CSV files

## Output Files

The pipeline generates the following outputs in the configured output directory:

- `county_temperature_indicators.csv`: County-level temperature indicators
- `county_precipitation_indicators.csv`: County-level precipitation indicators

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Project Structure

```
county-level-climate/
├── data/             # Directory for downloaded climate data
├── output/           # Directory for output CSV files
├── src/              # Source code
│   ├── gee_climate_projections.py    # Google Earth Engine functions
│   ├── climate_indicators.py         # Climate indicator calculations
│   └── county_projections.py         # Main script
└── README.md         # This file
```

## Climate Indicators

This project calculates various climate indicators at the county level:

### Temperature Indicators

- Annual mean temperature
- Number of hot days (days with temp > 30°C)
- Number of frost days (min temp < 0°C)
- Growing degree days

### Precipitation Indicators

- Annual total precipitation
- Number of heavy precipitation days (> 10mm)
- Maximum consecutive dry days
- Precipitation intensity (simple daily intensity index)

## References

- [Google Earth Engine](https://earthengine.google.com/)
- [NASA GDDP-CMIP6 Dataset](https://developers.google.com/earth-engine/datasets/catalog/NASA_GDDP-CMIP6)
- [xclim GitHub Repository](https://github.com/Ouranosinc/xclim)
- [TIGER/2018/Counties](https://developers.google.com/earth-engine/datasets/catalog/TIGER_2018_Counties)

## Testing

This project includes comprehensive unit tests and integration tests for the pipeline orchestrator.

### Running Tests

To run the tests, use the included test script:

```bash
# Run all tests with coverage report
./run_tests.sh

# Run only unit tests
./run_tests.sh unit

# Run only integration tests
./run_tests.sh integration
```

### Test Structure

- **Unit Tests**: Tests for individual components and functions in isolation
- **Integration Tests**: Tests for the interaction between components and end-to-end functionality

The tests use pytest with fixtures to mock external dependencies like Google Earth Engine, allowing tests to run without requiring actual API access.

### Test Coverage

The tests aim to cover:

- Pipeline initialization and configuration
- Earth Engine authentication
- Data download and processing
- Error handling and edge cases
- Command-line interface functionality 