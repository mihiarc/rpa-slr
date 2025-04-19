import os
import pytest
import tempfile
import pandas as pd
import numpy as np
import xarray as xr
import yaml
from unittest.mock import patch, MagicMock

# Import modules
from src.gee_auth import GEEAuthenticator, authenticate_ee
from src.config_manager import load_config, setup_earth_engine, ensure_directories
from src.gee_climate_projections import (
    get_us_counties,
    get_cmip6_collection,
    calculate_county_climate_stats,
    export_counties_to_csv
)
from src.climate_indicators import (
    convert_ee_to_xarray,
    calculate_temperature_indicators,
    calculate_precipitation_indicators
)

# Create a fixture for a temporary config file
@pytest.fixture
def test_config_file():
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create config and output directories
        config_dir = os.path.join(temp_dir, "config")
        output_dir = os.path.join(temp_dir, "output")
        os.makedirs(config_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        
        # Create a test config file
        config_data = {
            'earth_engine': {
                'project_id': 'test-project',
                'model': 'ACCESS-CM2',
                'variable': 'tas'
            },
            'climate': {
                'scenario': 'ssp585'
            },
            'data': {
                'years': {
                    'start': 2040,
                    'end': 2045  # Smaller range for testing
                }
            },
            'output': {
                'dir': output_dir
            }
        }
        
        config_path = os.path.join(config_dir, "test_config.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)
            
        yield config_path
        
        # Cleanup is handled automatically by the tempfile context manager

# Mock for Earth Engine initialization
@pytest.fixture
def mock_ee():
    with patch('src.gee_auth.ee') as mock_ee:
        # Configure the mock to return appropriate values
        mock_ee.Number().getInfo.return_value = 1
        mock_ee.Initialize.return_value = None
        mock_ee.ServiceAccountCredentials.return_value = MagicMock()
        
        # Create a mock FeatureCollection for counties
        mock_counties = MagicMock()
        mock_counties.getInfo.return_value = {'features': [
            {'properties': {'NAME': 'Test County 1', 'STATEFP': '01', 'COUNTYFP': '001', 'tas': 290.5}},
            {'properties': {'NAME': 'Test County 2', 'STATEFP': '01', 'COUNTYFP': '002', 'tas': 291.3}}
        ]}
        mock_ee.FeatureCollection.return_value = mock_counties
        
        # Create a mock ImageCollection for climate data
        mock_collection = MagicMock()
        mock_filtered = MagicMock()
        mock_collection.filter.return_value = mock_filtered
        mock_filtered.filterDate.return_value = mock_filtered
        mock_filtered.mean.return_value = MagicMock()
        mock_ee.ImageCollection.return_value = mock_collection
        
        # Configure the mock Date class
        mock_ee.Date.fromYMD.return_value = MagicMock()
        
        # Configure the Filter class
        mock_ee.Filter.And.return_value = MagicMock()
        mock_ee.Filter.equals.return_value = MagicMock()
        
        yield mock_ee

@pytest.mark.integration
def test_end_to_end_workflow(test_config_file, mock_ee):
    """Test the end-to-end workflow from authentication to climate indicators."""
    
    # 1. Load configuration
    config = load_config(test_config_file)
    assert config is not None
    assert 'earth_engine' in config
    
    # 2. Test Earth Engine authentication
    authenticator = GEEAuthenticator(project=config['earth_engine']['project_id'])
    with patch('src.gee_auth.ee', mock_ee):
        auth_result = authenticator.authenticate()
        assert auth_result is True
    
    # 3. Test setup_earth_engine
    with patch('src.gee_auth.ee', mock_ee):
        setup_result = setup_earth_engine(config)
        assert setup_result is True
    
    # 4. Ensure directories exist
    ensure_directories(config)
    assert os.path.exists(config['output']['dir'])
    
    # 5. Test fetching counties
    with patch('src.gee_climate_projections.ee', mock_ee):
        counties = get_us_counties()
        assert counties is not None
    
    # 6. Test getting climate collection
    with patch('src.gee_climate_projections.ee', mock_ee):
        model = config['earth_engine']['model']
        scenario = config['climate']['scenario']
        variable = config['earth_engine']['variable']
        collection = get_cmip6_collection(model, scenario, variable)
        assert collection is not None
    
    # 7. Test calculating county statistics
    with patch('src.gee_climate_projections.ee', mock_ee):
        start_year = config['data']['years']['start']
        end_year = config['data']['years']['end']
        stats = calculate_county_climate_stats(counties, collection, start_year, end_year)
        assert stats is not None
    
    # 8. Test export to CSV
    output_file = os.path.join(config['output']['dir'], "test_counties.csv")
    with patch('pandas.DataFrame.to_csv') as mock_to_csv:
        export_counties_to_csv(stats, output_file)
        mock_to_csv.assert_called_once()
    
    # 9. Test conversion to xarray
    # Create sample data for testing
    lats = np.linspace(25, 49, 10)
    lons = np.linspace(-125, -66, 15)
    times = pd.date_range('2040-01-01', periods=12, freq='MS')
    data = np.random.rand(12, 10, 15) * 25 + 273.15  # Random temperatures in Kelvin
    
    ds = convert_ee_to_xarray(data, 'tas', lats, lons, times)
    assert isinstance(ds, xr.Dataset)
    assert 'tas' in ds
    assert ds.tas.attrs['units'] == 'K'
    
    # 10. Test calculating temperature indicators
    with patch('xclim.indices.tg_mean') as mock_tg_mean, \
         patch('xclim.indices.tx_days_above') as mock_tx_days, \
         patch('xclim.indices.tn_days_below') as mock_tn_days, \
         patch('xclim.indices.growing_degree_days') as mock_gdd:
        
        # Configure mocks
        mock_tg_mean.return_value = xr.DataArray(np.array([15.0]))
        mock_tx_days.return_value = xr.DataArray(np.array([45]))
        mock_tn_days.return_value = xr.DataArray(np.array([10]))
        mock_gdd.return_value = xr.DataArray(np.array([3000]))
        
        temp_indicators = calculate_temperature_indicators(ds)
        assert 'annual_mean_temp' in temp_indicators
        assert 'hot_days' in temp_indicators
        assert 'frost_days' in temp_indicators
        assert 'growing_degree_days' in temp_indicators
    
    # 11. Test calculating precipitation indicators
    # Create precipitation dataset
    pr_data = np.random.rand(12, 10, 15) * 0.0001  # Random precip in kg m-2 s-1
    pr_ds = convert_ee_to_xarray(pr_data, 'pr', lats, lons, times)
    
    with patch('xclim.indices.prcptot') as mock_prcptot, \
         patch('xclim.indices.days_over_precip_thresh') as mock_days_over, \
         patch('xclim.indices.maximum_consecutive_dry_days') as mock_consec_dry, \
         patch('xclim.indices.daily_pr_intensity') as mock_intensity:
        
        # Configure mocks
        mock_prcptot.return_value = xr.DataArray(np.array([900.0]))
        mock_days_over.return_value = xr.DataArray(np.array([25]))
        mock_consec_dry.return_value = xr.DataArray(np.array([15]))
        mock_intensity.return_value = xr.DataArray(np.array([12.5]))
        
        precip_indicators = calculate_precipitation_indicators(pr_ds)
        assert 'annual_total_precip' in precip_indicators
        assert 'heavy_precip_days' in precip_indicators
        assert 'max_consec_dry_days' in precip_indicators
        assert 'precip_intensity' in precip_indicators

    print("Integration test completed successfully!") 