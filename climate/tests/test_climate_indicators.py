import os
import sys
import pytest
import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime
from unittest.mock import patch, MagicMock

# Add the src directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from climate_indicators import (
    convert_ee_to_xarray,
    calculate_temperature_indicators,
    calculate_precipitation_indicators,
    aggregate_by_county
)

# Fixtures for testing
@pytest.fixture
def mock_ee_array():
    """Create a mock Earth Engine array."""
    # 3D array with shape (time, lat, lon)
    return np.random.random((10, 5, 5))

@pytest.fixture
def mock_coords():
    """Create mock coordinates for the xarray dataset."""
    times = pd.date_range(start='2020-01-01', periods=10, freq='D')
    lats = np.linspace(30, 34, 5)
    lons = np.linspace(-120, -116, 5)
    return times, lats, lons

@pytest.fixture
def mock_temp_dataset(mock_coords):
    """Create a mock temperature dataset."""
    times, lats, lons = mock_coords
    # Random temperature data in Kelvin
    data = 273.15 + 10 * np.random.random((len(times), len(lats), len(lons)))
    
    da = xr.DataArray(
        data=data,
        dims=["time", "lat", "lon"],
        coords={
            "time": times,
            "lat": lats,
            "lon": lons,
        },
        name="tas"
    )
    
    ds = da.to_dataset()
    ds.tas.attrs = {
        'standard_name': 'air_temperature',
        'long_name': 'Near-Surface Air Temperature',
        'units': 'K'
    }
    
    return ds

@pytest.fixture
def mock_precip_dataset(mock_coords):
    """Create a mock precipitation dataset."""
    times, lats, lons = mock_coords
    # Random precipitation data in kg m-2 s-1
    data = 0.0001 * np.random.random((len(times), len(lats), len(lons)))
    
    da = xr.DataArray(
        data=data,
        dims=["time", "lat", "lon"],
        coords={
            "time": times,
            "lat": lats,
            "lon": lons,
        },
        name="pr"
    )
    
    ds = da.to_dataset()
    ds.pr.attrs = {
        'standard_name': 'precipitation_flux',
        'long_name': 'Precipitation',
        'units': 'kg m-2 s-1'
    }
    
    return ds

@pytest.fixture
def mock_county_geometries():
    """Create mock county geometries."""
    # In a real test, you might use actual GeoJSON or shapely geometries
    # For this mock, we'll create a simple object that can be used with the rio.clip method
    
    class MockGeometry:
        def __init__(self, county_id):
            self.county_id = county_id
    
    counties = {
        'county1': MockGeometry('county1'),
        'county2': MockGeometry('county2')
    }
    
    return counties

@pytest.fixture
def mock_indicators():
    """Create mock indicators for testing county aggregation."""
    # Create a simpler mock approach that doesn't try to extend DataArray directly
    
    # Create mock values that will be returned when county aggregation happens
    county1_values = np.random.random(2)  # One value for each indicator
    county2_values = np.random.random(2)
    
    # Create a function to generate a mock DataArray with clip method
    def create_mock_indicator(idx):
        mock_indicator = MagicMock()
        
        # Set up the rio.clip() chain
        mock_rio = MagicMock()
        mock_clip_result = MagicMock()
        mock_mean_result = MagicMock()
        
        # Configure return values based on which county is being processed
        def clip_side_effect(geometries):
            if hasattr(geometries[0], 'county_id') and geometries[0].county_id == 'county1':
                mock_mean_result.values = county1_values[idx]
            else:
                mock_mean_result.values = county2_values[idx]
            return mock_clip_result
            
        mock_clip_result.mean.return_value = mock_mean_result
        mock_rio.clip.side_effect = clip_side_effect
        mock_indicator.rio = mock_rio
        
        return mock_indicator
    
    return {
        'indicator1': create_mock_indicator(0),
        'indicator2': create_mock_indicator(1)
    }


# Tests for convert_ee_to_xarray
def test_convert_ee_to_xarray_tas(mock_ee_array, mock_coords):
    """Test converting Earth Engine array to xarray for temperature data."""
    times, lats, lons = mock_coords
    
    # Call the function
    ds = convert_ee_to_xarray(mock_ee_array, 'tas', lats, lons, times)
    
    # Assertions
    assert isinstance(ds, xr.Dataset)
    assert 'tas' in ds
    assert ds.tas.attrs['standard_name'] == 'air_temperature'
    assert ds.tas.attrs['units'] == 'K'
    assert ds.tas.shape == mock_ee_array.shape
    np.testing.assert_array_equal(ds.tas.values, mock_ee_array)


def test_convert_ee_to_xarray_pr(mock_ee_array, mock_coords):
    """Test converting Earth Engine array to xarray for precipitation data."""
    times, lats, lons = mock_coords
    
    # Call the function
    ds = convert_ee_to_xarray(mock_ee_array, 'pr', lats, lons, times)
    
    # Assertions
    assert isinstance(ds, xr.Dataset)
    assert 'pr' in ds
    assert ds.pr.attrs['standard_name'] == 'precipitation_flux'
    assert ds.pr.attrs['units'] == 'kg m-2 s-1'
    assert ds.pr.shape == mock_ee_array.shape
    np.testing.assert_array_equal(ds.pr.values, mock_ee_array)


# Tests for calculate_temperature_indicators
def test_calculate_temperature_indicators(mock_temp_dataset, tmp_path):
    """Test calculating temperature indicators."""
    # Prepare test output file
    output_file = tmp_path / "temp_indicators.csv"
    
    # Mock the xclim indicators
    with patch('climate_indicators.xc.indices.tg_mean') as mock_tg_mean, \
         patch('climate_indicators.xc.indices.tx_days_above') as mock_tx_days_above, \
         patch('climate_indicators.xc.indices.tn_days_below') as mock_tn_days_below, \
         patch('climate_indicators.xc.indices.growing_degree_days') as mock_growing_degree_days:
        
        # Set returns for the mocks
        mock_tg_mean.return_value = xr.DataArray([15.0], dims=["time"])
        mock_tx_days_above.return_value = xr.DataArray([5], dims=["time"])
        mock_tn_days_below.return_value = xr.DataArray([3], dims=["time"])
        mock_growing_degree_days.return_value = xr.DataArray([200], dims=["time"])
        
        # Call the function
        indicators = calculate_temperature_indicators(mock_temp_dataset, str(output_file))
        
        # Assertions
        assert 'annual_mean_temp' in indicators
        assert 'hot_days' in indicators
        assert 'frost_days' in indicators
        assert 'growing_degree_days' in indicators
        
        # Check that the file was created
        assert output_file.exists()
        
        # Verify unit conversion
        assert mock_temp_dataset.tas.attrs['units'] == '°C'


def test_calculate_temperature_indicators_no_output(mock_temp_dataset):
    """Test calculating temperature indicators without output file."""
    # Mock the xclim indicators
    with patch('climate_indicators.xc.indices.tg_mean') as mock_tg_mean, \
         patch('climate_indicators.xc.indices.tx_days_above') as mock_tx_days_above, \
         patch('climate_indicators.xc.indices.tn_days_below') as mock_tn_days_below, \
         patch('climate_indicators.xc.indices.growing_degree_days') as mock_growing_degree_days:
        
        # Set returns for the mocks
        mock_tg_mean.return_value = xr.DataArray([15.0], dims=["time"])
        mock_tx_days_above.return_value = xr.DataArray([5], dims=["time"])
        mock_tn_days_below.return_value = xr.DataArray([3], dims=["time"])
        mock_growing_degree_days.return_value = xr.DataArray([200], dims=["time"])
        
        # Call the function without output_file
        indicators = calculate_temperature_indicators(mock_temp_dataset)
        
        # Assertions
        assert 'annual_mean_temp' in indicators
        assert 'hot_days' in indicators
        assert 'frost_days' in indicators
        assert 'growing_degree_days' in indicators


# Tests for calculate_precipitation_indicators
def test_calculate_precipitation_indicators(mock_precip_dataset, tmp_path):
    """Test calculating precipitation indicators."""
    # Prepare test output file
    output_file = tmp_path / "precip_indicators.csv"
    
    # Mock the xclim indicators
    with patch('climate_indicators.xc.indices.prcptot') as mock_prcptot, \
         patch('climate_indicators.xc.indices.days_over_precip_thresh') as mock_days_over_precip_thresh, \
         patch('climate_indicators.xc.indices.maximum_consecutive_dry_days') as mock_max_consec_dry_days, \
         patch('climate_indicators.xc.indices.daily_pr_intensity') as mock_daily_pr_intensity:
        
        # Set returns for the mocks
        mock_prcptot.return_value = xr.DataArray([500.0], dims=["time"])
        mock_days_over_precip_thresh.return_value = xr.DataArray([10], dims=["time"])
        mock_max_consec_dry_days.return_value = xr.DataArray([5], dims=["time"])
        mock_daily_pr_intensity.return_value = xr.DataArray([8.5], dims=["time"])
        
        # Call the function
        indicators = calculate_precipitation_indicators(mock_precip_dataset, str(output_file))
        
        # Assertions
        assert 'annual_total_precip' in indicators
        assert 'heavy_precip_days' in indicators
        assert 'max_consec_dry_days' in indicators
        assert 'precip_intensity' in indicators
        
        # Check that the file was created
        assert output_file.exists()
        
        # Verify unit conversion
        assert mock_precip_dataset.pr.attrs['units'] == 'mm/day'


def test_calculate_precipitation_indicators_no_output(mock_precip_dataset):
    """Test calculating precipitation indicators without output file."""
    # Mock the xclim indicators
    with patch('climate_indicators.xc.indices.prcptot') as mock_prcptot, \
         patch('climate_indicators.xc.indices.days_over_precip_thresh') as mock_days_over_precip_thresh, \
         patch('climate_indicators.xc.indices.maximum_consecutive_dry_days') as mock_max_consec_dry_days, \
         patch('climate_indicators.xc.indices.daily_pr_intensity') as mock_daily_pr_intensity:
        
        # Set returns for the mocks
        mock_prcptot.return_value = xr.DataArray([500.0], dims=["time"])
        mock_days_over_precip_thresh.return_value = xr.DataArray([10], dims=["time"])
        mock_max_consec_dry_days.return_value = xr.DataArray([5], dims=["time"])
        mock_daily_pr_intensity.return_value = xr.DataArray([8.5], dims=["time"])
        
        # Call the function without output_file
        indicators = calculate_precipitation_indicators(mock_precip_dataset)
        
        # Assertions
        assert 'annual_total_precip' in indicators
        assert 'heavy_precip_days' in indicators
        assert 'max_consec_dry_days' in indicators
        assert 'precip_intensity' in indicators


# Tests for aggregate_by_county
def test_aggregate_by_county(mock_indicators, mock_county_geometries, tmp_path):
    """Test aggregating indicators by county."""
    # Prepare test output file
    output_file = tmp_path / "county_indicators.csv"
    
    # Call the function
    result = aggregate_by_county(mock_indicators, mock_county_geometries, str(output_file))
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(mock_county_geometries)
    assert 'county_id' in result.columns
    assert 'indicator1' in result.columns
    assert 'indicator2' in result.columns
    
    # Check that the file was created
    assert output_file.exists()


def test_aggregate_by_county_no_output(mock_indicators, mock_county_geometries):
    """Test aggregating indicators by county without output file."""
    # Call the function without output_file
    result = aggregate_by_county(mock_indicators, mock_county_geometries)
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert len(result) == len(mock_county_geometries)
    assert 'county_id' in result.columns
    assert 'indicator1' in result.columns
    assert 'indicator2' in result.columns 