import os
import sys
import pytest
from unittest.mock import patch, MagicMock

# Import the module to test
from src.pipeline_orchestrator import PipelineOrchestrator, parse_arguments

class TestPipelineOrchestrator:
    """Test cases for the PipelineOrchestrator class."""
    
    def test_init(self, mock_load_config, mock_ensure_directories, mock_config):
        """Test initialization of PipelineOrchestrator."""
        # Arrange & Act
        orchestrator = PipelineOrchestrator()
        
        # Assert
        assert orchestrator.config == mock_config
        assert orchestrator.model == mock_config['earth_engine']['model']
        assert orchestrator.scenario == mock_config['climate']['scenario']
        assert orchestrator.start_year == mock_config['data']['years']['start']
        assert orchestrator.end_year == mock_config['data']['years']['end']
        assert orchestrator.data_dir == mock_config['data']['dir']
        assert orchestrator.output_dir == mock_config['output']['dir']
        assert orchestrator.variables == mock_config['climate']['variables']
        assert orchestrator.ee_initialized is False
    
    def test_init_with_config_path(self, mock_load_config, mock_ensure_directories):
        """Test initialization with a specific config path."""
        # Arrange
        config_path = 'custom_config.yml'
        
        # Act
        with patch('src.pipeline_orchestrator.load_config') as mock_load:
            mock_load.return_value = mock_load_config
            orchestrator = PipelineOrchestrator(config_path=config_path)
        
        # Assert
        mock_load.assert_called_once_with(config_path)
    
    def test_initialize_earth_engine(self, mock_load_config, mock_ensure_directories, mock_setup_earth_engine):
        """Test Earth Engine initialization."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        
        # Act
        result = orchestrator.initialize_earth_engine()
        
        # Assert
        assert result is True
        assert orchestrator.ee_initialized is True
    
    def test_initialize_earth_engine_failure(self, mock_load_config, mock_ensure_directories):
        """Test Earth Engine initialization failure."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        
        # Act
        with patch('src.pipeline_orchestrator.setup_earth_engine', return_value=False):
            result = orchestrator.initialize_earth_engine()
        
        # Assert
        assert result is False
        assert orchestrator.ee_initialized is False
    
    def test_get_county_boundaries(self, mock_load_config, mock_ensure_directories, mock_ee):
        """Test getting county boundaries."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        
        # Act
        with patch('src.pipeline_orchestrator.get_us_counties') as mock_get_counties:
            mock_counties = MagicMock()
            mock_get_counties.return_value = mock_counties
            result = orchestrator.get_county_boundaries()
        
        # Assert
        assert result == mock_counties
        mock_get_counties.assert_called_once()
    
    def test_download_climate_projections(self, mock_load_config, mock_ensure_directories, 
                                          mock_setup_earth_engine, mock_download_climate_data):
        """Test downloading climate projections."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        orchestrator.ee_initialized = True
        
        # Act
        result = orchestrator.download_climate_projections()
        
        # Assert
        assert len(result) == 2  # Two variables: tas and pr
        assert result['tas'] == "test_data/tas_data.nc"
        assert result['pr'] == "test_data/pr_data.nc"
        assert mock_download_climate_data.call_count == 2
    
    def test_download_climate_projections_not_initialized(self, mock_load_config, mock_ensure_directories,
                                                         mock_setup_earth_engine, mock_download_climate_data):
        """Test downloading climate projections when EE is not initialized."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        orchestrator.ee_initialized = False
        
        # Act
        result = orchestrator.download_climate_projections()
        
        # Assert
        assert len(result) == 2
        assert mock_download_climate_data.call_count == 2
    
    def test_download_climate_projections_initialization_failure(self, mock_load_config, mock_ensure_directories):
        """Test downloading climate projections when EE initialization fails."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        orchestrator.ee_initialized = False
        
        # Act & Assert
        with patch('src.pipeline_orchestrator.setup_earth_engine', return_value=False):
            with pytest.raises(RuntimeError, match="Failed to initialize Earth Engine"):
                orchestrator.download_climate_projections()
    
    def test_download_climate_projections_exception(self, mock_load_config, mock_ensure_directories, 
                                                   mock_setup_earth_engine):
        """Test handling exceptions during climate data download."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        orchestrator.ee_initialized = True
        
        # Act
        with patch('src.county_projections.download_climate_data', side_effect=Exception("Download error")):
            result = orchestrator.download_climate_projections()
        
        # Assert
        assert result == {}  # Empty dict when all downloads fail
    
    def test_process_climate_data(self, mock_load_config, mock_ensure_directories, mock_ee,
                                 mock_process_counties_with_indicators):
        """Test processing climate data."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        downloaded_files = {
            'tas': 'test_data/tas_data.nc',
            'pr': 'test_data/pr_data.nc'
        }
        
        # Act
        with patch('src.pipeline_orchestrator.get_us_counties'):
            result = orchestrator.process_climate_data(downloaded_files)
        
        # Assert
        assert len(result) == 2
        assert 'tas' in result
        assert 'pr' in result
        assert mock_process_counties_with_indicators.call_count == 2
    
    def test_process_climate_data_exception(self, mock_load_config, mock_ensure_directories, mock_ee):
        """Test handling exceptions during climate data processing."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        downloaded_files = {
            'tas': 'test_data/tas_data.nc',
            'pr': 'test_data/pr_data.nc'
        }
        
        # Act
        with patch('src.pipeline_orchestrator.get_us_counties'), \
             patch('src.county_projections.process_counties_with_indicators', 
                   side_effect=Exception("Processing error")):
            result = orchestrator.process_climate_data(downloaded_files)
        
        # Assert
        assert result == {}  # Empty dict when all processing fails
    
    def test_run_pipeline(self, mock_load_config, mock_ensure_directories, mock_setup_earth_engine,
                         mock_download_climate_data, mock_process_counties_with_indicators, mock_ee):
        """Test running the complete pipeline."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        
        # Act
        result = orchestrator.run_pipeline()
        
        # Assert
        assert result is True
    
    def test_run_pipeline_download_only(self, mock_load_config, mock_ensure_directories, mock_setup_earth_engine,
                                      mock_download_climate_data, mock_ee):
        """Test running the pipeline in download-only mode."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        
        # Act
        result = orchestrator.run_pipeline(download_only=True)
        
        # Assert
        assert result is True
        # Verify process_climate_data is not called
        mock_process_counties_with_indicators.assert_not_called()
    
    def test_run_pipeline_ee_init_failure(self, mock_load_config, mock_ensure_directories):
        """Test pipeline when Earth Engine initialization fails."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        
        # Act
        with patch('src.pipeline_orchestrator.setup_earth_engine', return_value=False):
            result = orchestrator.run_pipeline()
        
        # Assert
        assert result is False
    
    def test_run_pipeline_download_failure(self, mock_load_config, mock_ensure_directories, 
                                          mock_setup_earth_engine):
        """Test pipeline when climate data download fails."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        
        # Act
        with patch.object(orchestrator, 'download_climate_projections', return_value={}):
            result = orchestrator.run_pipeline()
        
        # Assert
        assert result is False
    
    def test_run_pipeline_exception(self, mock_load_config, mock_ensure_directories):
        """Test pipeline with an unexpected exception."""
        # Arrange
        orchestrator = PipelineOrchestrator()
        
        # Act
        with patch.object(orchestrator, 'initialize_earth_engine', side_effect=Exception("Test error")):
            result = orchestrator.run_pipeline()
        
        # Assert
        assert result is False


def test_parse_arguments():
    """Test argument parsing."""
    # Arrange
    test_args = ['--config', 'test_config.yml', '--download-only']
    
    # Act
    with patch('sys.argv', ['script_name'] + test_args):
        args = parse_arguments()
    
    # Assert
    assert args.config == 'test_config.yml'
    assert args.download_only is True 