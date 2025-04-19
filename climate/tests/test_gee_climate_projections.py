"""
Tests for the gee_climate_projections.py module.
"""
import os
import sys
import pytest
from unittest.mock import patch, MagicMock

# Add the src directory to the path
sys.path.append('src')

from gee_climate_projections import (
    get_us_counties, get_cmip6_collection, calculate_county_climate_stats,
    export_counties_to_csv, main
)


class TestGEEClimateProjections:
    """Test suite for GEE climate projections module."""

    @patch('gee_climate_projections.ee')
    def test_get_us_counties(self, mock_ee):
        """Test get_us_counties function."""
        # Set up mock return values
        mock_feature_collection = MagicMock()
        mock_ee.FeatureCollection.return_value = mock_feature_collection
        
        # Call the function
        result = get_us_counties()
        
        # Assert the function called ee.FeatureCollection with the right dataset
        mock_ee.FeatureCollection.assert_called_once_with("TIGER/2018/Counties")
        # Assert the function returned the expected object
        assert result == mock_feature_collection

    @patch('gee_climate_projections.ee')
    def test_get_cmip6_collection_default_params(self, mock_ee):
        """Test get_cmip6_collection with default parameters."""
        # Set up mocks
        mock_collection = MagicMock()
        mock_filtered = MagicMock()
        mock_ee.ImageCollection.return_value = mock_collection
        mock_collection.filter.return_value = mock_filtered
        
        # Mock ee.Filter.And and related filters
        mock_and_filter = MagicMock()
        mock_ee.Filter.And.return_value = mock_and_filter
        
        mock_equals_filter1 = MagicMock(name="model_filter")
        mock_equals_filter2 = MagicMock(name="scenario_filter")
        mock_equals_filter3 = MagicMock(name="variable_filter")
        
        mock_ee.Filter.equals.side_effect = [
            mock_equals_filter1, mock_equals_filter2, mock_equals_filter3
        ]
        
        # Call the function with default parameters
        result = get_cmip6_collection()
        
        # Verify the collection was created correctly
        mock_ee.ImageCollection.assert_called_once_with("NASA/GDDP-CMIP6")
        
        # Verify filters were applied correctly
        mock_ee.Filter.equals.assert_any_call('model', 'ACCESS-CM2')
        mock_ee.Filter.equals.assert_any_call('scenario', 'ssp585')
        mock_ee.Filter.equals.assert_any_call('variable', 'tas')
        
        mock_ee.Filter.And.assert_called_once_with(
            mock_equals_filter1, mock_equals_filter2, mock_equals_filter3
        )
        
        mock_collection.filter.assert_called_once_with(mock_and_filter)
        
        # Verify return value
        assert result == mock_filtered

    @patch('gee_climate_projections.ee')
    def test_get_cmip6_collection_custom_params(self, mock_ee):
        """Test get_cmip6_collection with custom parameters."""
        # Set up mocks
        mock_collection = MagicMock()
        mock_filtered = MagicMock()
        mock_ee.ImageCollection.return_value = mock_collection
        mock_collection.filter.return_value = mock_filtered
        
        # Mock ee.Filter.And and related filters
        mock_and_filter = MagicMock()
        mock_ee.Filter.And.return_value = mock_and_filter
        
        mock_equals_filter1 = MagicMock(name="model_filter")
        mock_equals_filter2 = MagicMock(name="scenario_filter")
        mock_equals_filter3 = MagicMock(name="variable_filter")
        
        mock_ee.Filter.equals.side_effect = [
            mock_equals_filter1, mock_equals_filter2, mock_equals_filter3
        ]
        
        # Call the function with custom parameters
        custom_model = 'CESM2'
        custom_scenario = 'ssp245'
        custom_variable = 'pr'
        result = get_cmip6_collection(custom_model, custom_scenario, custom_variable)
        
        # Verify the filters were created with the custom parameters
        mock_ee.Filter.equals.assert_any_call('model', custom_model)
        mock_ee.Filter.equals.assert_any_call('scenario', custom_scenario)
        mock_ee.Filter.equals.assert_any_call('variable', custom_variable)

    @patch('gee_climate_projections.ee')
    def test_calculate_county_climate_stats(self, mock_ee):
        """Test calculate_county_climate_stats function."""
        # Mock counties and climate collection
        mock_counties = MagicMock()
        mock_climate_collection = MagicMock()
        mock_filtered_collection = MagicMock()
        mock_climate_collection.filterDate.return_value = mock_filtered_collection
        
        # Mock Date class
        mock_start_date = MagicMock()
        mock_end_date = MagicMock()
        mock_ee.Date.fromYMD.side_effect = [mock_start_date, mock_end_date]
        
        # Mock the map functionality
        mock_counties_with_stats = MagicMock()
        mock_counties.map.return_value = mock_counties_with_stats
        
        # Test parameters
        start_year = 2040
        end_year = 2060
        
        # Call the function
        result = calculate_county_climate_stats(mock_counties, mock_climate_collection, start_year, end_year)
        
        # Verify date filtering
        mock_ee.Date.fromYMD.assert_any_call(start_year, 1, 1)
        mock_ee.Date.fromYMD.assert_any_call(end_year, 12, 31)
        mock_climate_collection.filterDate.assert_called_once_with(mock_start_date, mock_end_date)
        
        # Verify mapping was called
        mock_counties.map.assert_called_once()
        
        # Check result
        assert result == mock_counties_with_stats

    def test_export_counties_to_csv(self, tmp_path):
        """Test export_counties_to_csv function."""
        # Create a mock feature collection with some test data
        mock_counties_with_stats = MagicMock()
        
        # Mock the getInfo method to return a dictionary with features
        test_features = {
            'features': [
                {'properties': {'GEOID': '01001', 'NAME': 'Test County 1', 'tas': 22.5}},
                {'properties': {'GEOID': '01002', 'NAME': 'Test County 2', 'tas': 23.1}}
            ]
        }
        mock_counties_with_stats.getInfo.return_value = test_features
        
        # Create a temporary output file path
        output_file = os.path.join(tmp_path, "test_output.csv")
        
        # Call the function
        with patch('gee_climate_projections.logger') as mock_logger:
            export_counties_to_csv(mock_counties_with_stats, output_file)
        
        # Verify the CSV file was created
        assert os.path.exists(output_file)
        
        # Verify the logger.info message
        mock_logger.info.assert_called_once_with(f"Saved county climate statistics to {output_file}")
        
        # Read the CSV file to verify its contents
        import pandas as pd
        df = pd.read_csv(output_file)
        
        # Check that the dataframe has the expected rows
        assert len(df) == 2
        assert 'GEOID' in df.columns
        assert 'NAME' in df.columns
        assert 'tas' in df.columns

    @patch('gee_climate_projections.load_config')
    @patch('gee_climate_projections.setup_earth_engine')
    @patch('gee_climate_projections.get_us_counties')
    @patch('gee_climate_projections.get_cmip6_collection')
    @patch('gee_climate_projections.calculate_county_climate_stats')
    @patch('gee_climate_projections.export_counties_to_csv')
    @patch('gee_climate_projections.os.path.join')
    @patch('gee_climate_projections.logger')
    def test_main(self, mock_logger, mock_join, mock_export, mock_calculate, 
                 mock_get_cmip6, mock_get_counties, mock_setup_earth_engine, mock_load_config):
        """Test the main function."""
        # Set up mocks
        mock_counties = MagicMock()
        mock_climate_collection = MagicMock()
        mock_counties_with_stats = MagicMock()
        mock_output_path = "mock_output_path.csv"
        
        # Configure returns
        mock_setup_earth_engine.return_value = True
        mock_config = {
            'earth_engine': {'model': 'ACCESS-CM2', 'variable': 'tas'},
            'climate': {'scenario': 'ssp585'},
            'data': {'years': {'start': 2040, 'end': 2060}},
            'output': {'dir': 'output'}
        }
        mock_load_config.return_value = mock_config
        mock_get_counties.return_value = mock_counties
        mock_get_cmip6.return_value = mock_climate_collection
        mock_calculate.return_value = mock_counties_with_stats
        mock_join.return_value = mock_output_path
        
        # Call the main function
        main()
        
        # Verify the correct sequence of function calls
        mock_load_config.assert_called_once()
        mock_setup_earth_engine.assert_called_once_with(mock_config)
        mock_get_counties.assert_called_once()
        
        # Check the correct parameters were passed to get_cmip6_collection
        mock_get_cmip6.assert_called_once_with('ACCESS-CM2', 'ssp585', 'tas')
        
        # Check the calculate_county_climate_stats function was called with correct params
        mock_calculate.assert_called_once_with(mock_counties, mock_climate_collection, 2040, 2060)
        
        # Check the export function was called
        mock_export.assert_called_once_with(mock_counties_with_stats, mock_output_path) 