import os
import sys
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock, mock_open

# Add the src directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Tests that use pytest-mock to patch functions directly
@pytest.mark.parametrize(
    "model,scenario,variable,start_year,end_year,output_dir,expected_output",
    [
        (
            "TEST-MODEL", "ssp585", "tas", 2040, 2060, "test_data",
            "test_data/tas_TEST-MODEL_ssp585_2040_2060.nc"
        ),
        (
            "ACCESS-CM2", "ssp245", "pr", 2030, 2050, "output",
            "output/pr_ACCESS-CM2_ssp245_2030_2050.nc"
        ),
    ]
)
def test_download_climate_data(mocker, model, scenario, variable, start_year, end_year, output_dir, expected_output):
    """Test the download_climate_data function."""
    # Mock all required functions and objects
    mock_ee = mocker.patch("src.county_projections.ee")
    mock_get_collection = mocker.patch("src.county_projections.get_cmip6_collection")
    mock_logger = mocker.patch("src.county_projections.logger")
    
    # Setup mock collection
    mock_collection = MagicMock()
    mock_filtered = MagicMock()
    mock_task = MagicMock()
    mock_task.id = "test_task_id"
    
    # Return values for method chains
    mock_collection.filterDate.return_value = mock_filtered
    mock_get_collection.return_value = mock_collection
    mock_ee.batch.Export.image.toDrive.return_value = mock_task
    
    # Import the function directly
    from src.county_projections import download_climate_data
    
    # Call the function
    result = download_climate_data(model, scenario, variable, start_year, end_year, output_dir)
    
    # Assert the function was called correctly
    mock_get_collection.assert_called_once_with(model, scenario, variable)
    mock_collection.filterDate.assert_called_once()
    mock_ee.Date.fromYMD.assert_any_call(start_year, 1, 1)
    mock_ee.Date.fromYMD.assert_any_call(end_year, 12, 31)
    mock_ee.Geometry.Rectangle.assert_called_once_with([-125, 24, -66, 50])
    
    # Check the result
    assert result == expected_output
    
    # Verify log message was generated
    mock_logger.info.assert_any_call(
        f"Downloading {variable} data for {model} {scenario} from {start_year} to {end_year}..."
    )


@pytest.mark.parametrize(
    "variable,expected_indicators_func",
    [
        ("tas", "calculate_temperature_indicators"),
        ("pr", "calculate_precipitation_indicators"),
    ]
)
def test_process_counties_with_indicators(mocker, variable, expected_indicators_func):
    """Test the process_counties_with_indicators function."""
    # Mock all required dependencies
    mock_xr = mocker.patch("src.county_projections.xr")
    mock_indicators_funcs = {
        "calculate_temperature_indicators": mocker.patch("src.county_projections.calculate_temperature_indicators"),
        "calculate_precipitation_indicators": mocker.patch("src.county_projections.calculate_precipitation_indicators")
    }
    mock_aggregate = mocker.patch("src.county_projections.aggregate_by_county")
    mock_convert = mocker.patch("src.county_projections.convert_counties_to_geometries")
    
    # Setup mock returns
    mock_ds = MagicMock()
    mock_xr.open_dataset.return_value = mock_ds
    mock_indicators = MagicMock()
    mock_indicators_funcs[expected_indicators_func].return_value = mock_indicators
    mock_geometries = {"01001": {}, "01002": {}}
    mock_convert.return_value = mock_geometries
    mock_result_df = pd.DataFrame({"county_id": ["01001", "01002"]})
    mock_aggregate.return_value = mock_result_df
    
    # Prepare arguments
    climate_data_file = f"test_data/{variable}_data.nc"
    counties = MagicMock()
    output_dir = "test_output"
    
    # Import the function directly
    from src.county_projections import process_counties_with_indicators
    
    # Call the function
    result = process_counties_with_indicators(climate_data_file, variable, counties, output_dir)
    
    # Assert
    mock_xr.open_dataset.assert_called_once_with(climate_data_file)
    mock_indicators_funcs[expected_indicators_func].assert_called_once_with(mock_ds)
    mock_convert.assert_called_once_with(counties)
    
    # Check the expected output file path
    expected_output_file = os.path.join(output_dir, f"county_{variable}_indicators.csv")
    if variable == "tas":
        expected_output_file = os.path.join(output_dir, "county_temperature_indicators.csv")
    elif variable == "pr":
        expected_output_file = os.path.join(output_dir, "county_precipitation_indicators.csv")
    
    mock_aggregate.assert_called_once_with(mock_indicators, mock_geometries, output_file=expected_output_file)
    assert result.equals(mock_result_df)


def test_process_counties_with_indicators_invalid_variable(mocker):
    """Test that process_counties_with_indicators raises an error for invalid variables."""
    # Mock minimal dependencies
    mock_xr = mocker.patch("src.county_projections.xr")
    
    # Import the function directly
    from src.county_projections import process_counties_with_indicators
    
    # Call the function with an invalid variable
    with pytest.raises(ValueError, match="Unsupported variable: invalid"):
        process_counties_with_indicators(
            "test_data/invalid_data.nc", "invalid", MagicMock(), "test_output"
        )


def test_convert_counties_to_geometries(mocker):
    """Test the convert_counties_to_geometries function."""
    # Create mock counties
    mock_counties = MagicMock()
    
    # Mock the getInfo method to return test data
    mock_features = {
        'features': [
            {
                'properties': {'GEOID': '01001'},
                'geometry': {'type': 'Polygon', 'coordinates': [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}
            },
            {
                'properties': {'GEOID': '01002'},
                'geometry': {'type': 'Polygon', 'coordinates': [[[1, 1], [1, 2], [2, 2], [2, 1], [1, 1]]]}
            }
        ]
    }
    mock_counties.getInfo.return_value = mock_features
    
    # Import the function directly
    from src.county_projections import convert_counties_to_geometries
    
    # Call the function
    result = convert_counties_to_geometries(mock_counties)
    
    # Assert
    assert isinstance(result, dict)
    assert len(result) == 2
    assert '01001' in result
    assert '01002' in result
    assert result['01001'] == mock_features['features'][0]['geometry']
    assert result['01002'] == mock_features['features'][1]['geometry']


def test_main_function(mocker):
    """Test the main function."""
    # Mock all the dependencies
    mock_load_config = mocker.patch("src.county_projections.load_config")
    mock_ensure_dirs = mocker.patch("src.county_projections.ensure_directories")
    mock_setup_ee = mocker.patch("src.county_projections.setup_earth_engine")
    mock_get_counties = mocker.patch("src.county_projections.get_us_counties")
    mock_download = mocker.patch("src.county_projections.download_climate_data")
    mock_logger = mocker.patch("src.county_projections.logger")
    
    # Setup mock returns
    mock_config = {
        'earth_engine': {'model': 'TEST-MODEL'},
        'climate': {'scenario': 'ssp585', 'variables': ['tas', 'pr']},
        'data': {'years': {'start': 2040, 'end': 2060}, 'dir': 'test_data'},
        'output': {'dir': 'test_output'}
    }
    mock_load_config.return_value = mock_config
    mock_setup_ee.return_value = True
    mock_counties = MagicMock()
    mock_get_counties.return_value = mock_counties
    mock_download.return_value = 'test_data/downloaded_file.nc'
    
    # Import the function directly
    from src.county_projections import main
    
    # Call the function
    main()
    
    # Assert
    mock_load_config.assert_called_once()
    mock_ensure_dirs.assert_called_once_with(mock_config)
    mock_setup_ee.assert_called_once_with(mock_config)
    mock_get_counties.assert_called_once()
    
    # Should be called twice, once for each variable (tas and pr)
    assert mock_download.call_count == 2
    
    # Check first call (temperature)
    mock_download.assert_any_call(
        'TEST-MODEL', 'ssp585', 'tas', 2040, 2060, 'test_data'
    )
    
    # Check second call (precipitation)
    mock_download.assert_any_call(
        'TEST-MODEL', 'ssp585', 'pr', 2040, 2060, 'test_data'
    )
    
    # Verify that the final log messages were generated
    mock_logger.info.assert_any_call("County-level climate projections have been calculated.")
    mock_logger.info.assert_any_call(f"Please check the {mock_config['output']['dir']} directory for results.")


def test_main_function_earth_engine_setup_failure(mocker):
    """Test the main function when Earth Engine setup fails."""
    # Mock dependencies
    mock_load_config = mocker.patch("src.county_projections.load_config")
    mock_ensure_dirs = mocker.patch("src.county_projections.ensure_directories")
    mock_setup_ee = mocker.patch("src.county_projections.setup_earth_engine")
    mock_logger = mocker.patch("src.county_projections.logger")
    
    # Setup mock returns - EE setup fails
    mock_config = {}
    mock_load_config.return_value = mock_config
    mock_setup_ee.return_value = False
    
    # Import the function directly
    from src.county_projections import main
    
    # Call the function
    main()
    
    # Assert
    mock_load_config.assert_called_once()
    mock_ensure_dirs.assert_called_once_with(mock_config)
    mock_setup_ee.assert_called_once_with(mock_config)
    mock_logger.error.assert_called_once_with("Failed to set up Earth Engine. Exiting.") 