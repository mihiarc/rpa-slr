import os
import sys
import pytest
import yaml
from unittest.mock import MagicMock, patch

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@pytest.fixture
def mock_config():
    """Fixture that returns a mock configuration dictionary."""
    return {
        'earth_engine': {
            'project_id': 'test-project',
            'model': 'TEST-MODEL',
            'image_collection': 'TEST/COLLECTION',
            'variable': 'tas'
        },
        'climate': {
            'scenario': 'ssp585',
            'variables': ['tas', 'pr']
        },
        'data': {
            'years': {
                'start': 2040,
                'end': 2060
            },
            'dir': 'test_data'
        },
        'output': {
            'dir': 'test_output'
        },
        'processing': {
            'chunk_size': 100,
            'max_concurrent_tasks': 10
        }
    }

@pytest.fixture
def mock_ee():
    """Fixture that mocks the Earth Engine API."""
    with patch('ee.Initialize'), patch('ee.FeatureCollection') as mock_fc:
        # Create a mock FeatureCollection that can be used in tests
        mock_feature = MagicMock()
        mock_feature.getInfo.return_value = {'features': [
            {'properties': {'GEOID': '01001'}, 'geometry': {'type': 'Polygon'}}
        ]}
        mock_fc.return_value = mock_feature
        yield

@pytest.fixture
def mock_load_config(mock_config):
    """Fixture that mocks the load_config function."""
    with patch('src.config_manager.load_config', return_value=mock_config):
        yield mock_config

@pytest.fixture
def mock_setup_earth_engine():
    """Fixture that mocks the setup_earth_engine function."""
    with patch('src.config_manager.setup_earth_engine', return_value=True):
        yield

@pytest.fixture
def mock_ensure_directories():
    """Fixture that mocks the ensure_directories function."""
    with patch('src.config_manager.ensure_directories'):
        yield

@pytest.fixture
def mock_download_climate_data():
    """Fixture that mocks the download_climate_data function."""
    with patch('src.county_projections.download_climate_data') as mock_download:
        # Return different file paths for different variables
        mock_download.side_effect = lambda model, scenario, variable, start_year, end_year, output_dir: \
            f"test_data/{variable}_data.nc"
        yield mock_download

@pytest.fixture
def mock_process_counties_with_indicators():
    """Fixture that mocks the process_counties_with_indicators function."""
    with patch('src.county_projections.process_counties_with_indicators') as mock_process:
        # Return mock data frame for each call
        mock_process.return_value = {'indicator1': [1, 2, 3], 'indicator2': [4, 5, 6]}
        yield mock_process

@pytest.fixture
def test_directories():
    """Fixture that creates test directories and cleans them up."""
    # Create test directories
    os.makedirs('test_data', exist_ok=True)
    os.makedirs('test_output', exist_ok=True)
    
    yield
    
    # Clean up temporary test directories
    # Comment these out if you want to inspect the files after the test
    # import shutil
    # shutil.rmtree('test_data', ignore_errors=True)
    # shutil.rmtree('test_output', ignore_errors=True) 