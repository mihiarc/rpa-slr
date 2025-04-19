"""
Tests for the config_manager.py module.
"""
import os
import sys
import pytest
from unittest.mock import patch, MagicMock, mock_open
import yaml

# Add the src directory to the path
sys.path.append('src')

from config_manager import (
    load_config, setup_earth_engine, ensure_directories, DEFAULT_CONFIG_PATH
)


class TestConfigManager:
    """Test suite for the config manager module."""

    @patch('config_manager.yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open, read_data='earth_engine:\n  project_id: test-project')
    def test_load_config_success(self, mock_file, mock_safe_load):
        """Test loading configuration successfully."""
        # Setup mock return value
        test_config = {'earth_engine': {'project_id': 'test-project'}}
        mock_safe_load.return_value = test_config
        
        # Call the function
        result = load_config()
        
        # Verify the file was opened correctly
        mock_file.assert_called_once_with(DEFAULT_CONFIG_PATH, 'r')
        # Verify yaml.safe_load was called
        mock_safe_load.assert_called_once()
        # Verify the return value
        assert result == test_config

    @patch('config_manager.yaml.safe_load')
    @patch('builtins.open', side_effect=FileNotFoundError("File not found"))
    def test_load_config_file_not_found(self, mock_file, mock_safe_load):
        """Test loading configuration when file is not found."""
        # Call the function
        result = load_config()
        
        # Verify open was called with the right path
        mock_file.assert_called_once_with(DEFAULT_CONFIG_PATH, 'r')
        # Verify yaml.safe_load was not called
        mock_safe_load.assert_not_called()
        
        # Verify the return value is the default config
        assert 'earth_engine' in result
        assert 'data' in result
        assert 'processing' in result
        assert result['earth_engine']['project_id'] == 'ee-chrismihiar'

    @patch('config_manager.yaml.safe_load', side_effect=yaml.YAMLError("Invalid YAML"))
    @patch('builtins.open', new_callable=mock_open, read_data='invalid: yaml: content')
    def test_load_config_invalid_yaml(self, mock_file, mock_safe_load):
        """Test loading configuration with invalid YAML."""
        # Call the function
        result = load_config()
        
        # Verify open was called with the right path
        mock_file.assert_called_once_with(DEFAULT_CONFIG_PATH, 'r')
        # Verify yaml.safe_load was called
        mock_safe_load.assert_called_once()
        
        # Verify the return value is the default config
        assert 'earth_engine' in result
        assert 'data' in result
        assert 'processing' in result

    @patch('config_manager.load_config')
    @patch('config_manager.authenticate_ee')
    def test_setup_earth_engine_with_config(self, mock_authenticate, mock_load_config):
        """Test setting up Earth Engine with provided config."""
        # Setup mock return values
        mock_authenticate.return_value = True
        test_config = {'earth_engine': {'project_id': 'test-project'}}
        
        # Call the function
        result = setup_earth_engine(test_config)
        
        # Verify load_config was not called
        mock_load_config.assert_not_called()
        # Verify authenticate_ee was called with the right project
        mock_authenticate.assert_called_once_with(project='test-project')
        # Verify the return value
        assert result is True

    @patch('config_manager.load_config')
    @patch('config_manager.authenticate_ee')
    def test_setup_earth_engine_without_config(self, mock_authenticate, mock_load_config):
        """Test setting up Earth Engine without provided config."""
        # Setup mock return values
        mock_authenticate.return_value = True
        test_config = {'earth_engine': {'project_id': 'test-project'}}
        mock_load_config.return_value = test_config
        
        # Call the function
        result = setup_earth_engine()
        
        # Verify load_config was called
        mock_load_config.assert_called_once()
        # Verify authenticate_ee was called with the right project
        mock_authenticate.assert_called_once_with(project='test-project')
        # Verify the return value
        assert result is True

    @patch('config_manager.authenticate_ee', return_value=False)
    def test_setup_earth_engine_auth_failure(self, mock_authenticate):
        """Test setting up Earth Engine with authentication failure."""
        # Setup test config
        test_config = {'earth_engine': {'project_id': 'test-project'}}
        
        # Call the function
        result = setup_earth_engine(test_config)
        
        # Verify authenticate_ee was called
        mock_authenticate.assert_called_once()
        # Verify the return value
        assert result is False

    @patch('config_manager.os.makedirs')
    def test_ensure_directories_with_config(self, mock_makedirs):
        """Test ensuring directories exist with provided config."""
        # Setup test config
        test_config = {'output': {'dir': 'test-output'}}
        
        # Call the function
        ensure_directories(test_config)
        
        # Verify makedirs was called for both directories
        mock_makedirs.assert_any_call('test-output', exist_ok=True)
        mock_makedirs.assert_any_call('data', exist_ok=True)
        assert mock_makedirs.call_count == 2

    @patch('config_manager.load_config')
    @patch('config_manager.os.makedirs')
    def test_ensure_directories_without_config(self, mock_makedirs, mock_load_config):
        """Test ensuring directories exist without provided config."""
        # Setup mock return value
        test_config = {'output': {'dir': 'test-output'}}
        mock_load_config.return_value = test_config
        
        # Call the function
        ensure_directories()
        
        # Verify load_config was called
        mock_load_config.assert_called_once()
        # Verify makedirs was called for both directories
        mock_makedirs.assert_any_call('test-output', exist_ok=True)
        mock_makedirs.assert_any_call('data', exist_ok=True)
        assert mock_makedirs.call_count == 2 