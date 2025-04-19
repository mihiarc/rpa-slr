# Test Suite for County-Level Climate Projections

This directory contains the test suite for the county-level climate projections code.

## Overview

The tests are organized by module, with each module having a corresponding test file:

- `test_county_projections.py`: Tests for the `county_projections.py` module
- `test_climate_indicators.py`: Tests for the `climate_indicators.py` module
- `test_gee_climate_projections.py`: Tests for the `gee_climate_projections.py` module
- `test_config_manager.py`: Tests for the `config_manager.py` module
- `test_gee_auth.py`: Tests for the `gee_auth.py` module

## Running the Tests

### Using the run_tests.sh script

The easiest way to run the tests is to use the provided script:

```bash
# Make sure the script is executable
chmod +x run_tests.sh

# Run the script
./run_tests.sh
```

This will activate the virtual environment, install dependencies, and run the tests with coverage reporting.

### Running tests manually

You can also run the tests manually:

```bash
# Activate the virtual environment
source .venv/bin/activate

# Run all tests with coverage
python -m pytest tests/ -v --cov=src

# Run a specific test file
python -m pytest tests/test_county_projections.py -v --cov=src.county_projections

# Generate HTML coverage report
python -m pytest tests/ -v --cov=src --cov-report=html
```

## Testing Approach

### Mocking External Dependencies

Many of the modules depend on external services like Google Earth Engine. To test these modules without making actual API calls, we use mocking:

1. For `county_projections.py`, we use pytest-mock to patch the external dependencies at import time
2. Tests use parametrization where possible to improve test coverage
3. Each test focuses on a single function with clear setup, execution, and assertion phases

### Coverage Requirements

The test suite aims for at least 80% code coverage for all modules. Current coverage:

- `county_projections.py`: 96%

## Adding New Tests

When adding new tests:

1. Follow the existing structure for test files
2. Use descriptive test function names (e.g., `test_download_climate_data`)
3. Use fixtures for common setup
4. Mock external dependencies to avoid making actual API calls
5. Verify coverage using `--cov` flag

## Test Dependencies

The test suite requires the following packages:

- pytest
- pytest-mock
- pytest-cov
- All dependencies of the main code

These are installed automatically by the `run_tests.sh` script. 