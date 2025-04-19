import os
import sys
import pytest
import yaml
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

# Import the module to test
from src.pipeline_orchestrator import PipelineOrchestrator, main


class TestPipelineIntegration:
    """Integration tests for the PipelineOrchestrator."""

    @pytest.fixture
    def temp_dirs(self):
        """Create temporary directories for data and output."""
        temp_dir = tempfile.mkdtemp()
        data_dir = os.path.join(temp_dir, 'data')
        output_dir = os.path.join(temp_dir, 'output')
        config_dir = os.path.join(temp_dir, 'config')
        
        os.makedirs(data_dir)
        os.makedirs(output_dir)
        os.makedirs(config_dir)
        
        # Create dummy climate data files
        for variable in ['tas', 'pr']:
            with open(os.path.join(data_dir, f"{variable}_data.nc"), 'w') as f:
                f.write("dummy data")
        
        # Create a test config file
        config_data = {
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
                'dir': data_dir
            },
            'output': {
                'dir': output_dir
            },
            'processing': {
                'chunk_size': 100,
                'max_concurrent_tasks': 10
            }
        }
        
        config_path = os.path.join(config_dir, 'test_config.yml')
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        yield {
            'temp_dir': temp_dir,
            'data_dir': data_dir,
            'output_dir': output_dir,
            'config_path': config_path,
            'config_data': config_data
        }
        
        # Clean up
        shutil.rmtree(temp_dir)

    def test_end_to_end_pipeline(self, temp_dirs, mock_ee):
        """
        Test the pipeline orchestrator end-to-end with mocked external dependencies.
        
        This test verifies that:
        1. The pipeline initializes correctly
        2. Earth Engine authentication works
        3. County boundaries are fetched
        4. Climate data is downloaded 
        5. Data is processed and indicators are generated
        6. Output files are created
        """
        # Arrange
        config_path = temp_dirs['config_path']
        
        # Create mocks for all external function calls
        with patch('src.config_manager.setup_earth_engine', return_value=True), \
             patch('src.county_projections.get_us_counties') as mock_get_counties, \
             patch('src.county_projections.download_climate_data') as mock_download, \
             patch('src.county_projections.process_counties_with_indicators') as mock_process:
            
            # Setup mock county boundaries
            mock_counties = MagicMock()
            mock_get_counties.return_value = mock_counties
            
            # Setup mock download function to return paths to the test files
            mock_download.side_effect = lambda model, scenario, variable, start_year, end_year, output_dir: \
                os.path.join(temp_dirs['data_dir'], f"{variable}_data.nc")
                
            # Setup mock processing function to create output files
            def mock_process_func(climate_data_file, variable, counties, output_dir):
                output_file = os.path.join(output_dir, f"county_{variable}_indicators.csv")
                with open(output_file, 'w') as f:
                    f.write(f"county_id,indicator1,indicator2\n1001,1.5,2.5\n1002,3.5,4.5\n")
                return {"county_id": ["1001", "1002"], 
                        "indicator1": [1.5, 3.5], 
                        "indicator2": [2.5, 4.5]}
                
            mock_process.side_effect = mock_process_func
            
            # Act
            orchestrator = PipelineOrchestrator(config_path=config_path)
            result = orchestrator.run_pipeline()
            
            # Assert
            assert result is True
            
            # Verify Earth Engine was initialized
            assert orchestrator.ee_initialized is True
            
            # Verify download was called for both variables
            assert mock_download.call_count == 2
            
            # Verify process was called for both variables
            assert mock_process.call_count == 2
            
            # Verify output files were created
            tas_output = os.path.join(temp_dirs['output_dir'], 'county_tas_indicators.csv')
            pr_output = os.path.join(temp_dirs['output_dir'], 'county_pr_indicators.csv')
            
            assert os.path.exists(tas_output)
            assert os.path.exists(pr_output)
            
            # Check content of output files
            with open(tas_output, 'r') as f:
                content = f.read()
                assert "county_id,indicator1,indicator2" in content
                assert "1001,1.5,2.5" in content
            
            with open(pr_output, 'r') as f:
                content = f.read()
                assert "county_id,indicator1,indicator2" in content
                assert "1002,3.5,4.5" in content

    def test_end_to_end_download_only(self, temp_dirs, mock_ee):
        """
        Test the pipeline orchestrator in download-only mode.
        
        This test verifies that:
        1. The pipeline initializes correctly
        2. Earth Engine authentication works
        3. Climate data is downloaded 
        4. No processing is performed
        5. No output files are created
        """
        # Arrange
        config_path = temp_dirs['config_path']
        
        # Create mocks for all external function calls
        with patch('src.config_manager.setup_earth_engine', return_value=True), \
             patch('src.county_projections.download_climate_data') as mock_download, \
             patch('src.county_projections.process_counties_with_indicators') as mock_process:
            
            # Setup mock download function to return paths to the test files
            mock_download.side_effect = lambda model, scenario, variable, start_year, end_year, output_dir: \
                os.path.join(temp_dirs['data_dir'], f"{variable}_data.nc")
            
            # Act
            orchestrator = PipelineOrchestrator(config_path=config_path)
            result = orchestrator.run_pipeline(download_only=True)
            
            # Assert
            assert result is True
            
            # Verify download was called
            assert mock_download.call_count == 2
            
            # Verify process was NOT called
            mock_process.assert_not_called()

    def test_main_function(self, temp_dirs, mock_ee):
        """
        Test the main function that runs the pipeline from command line.
        
        This test verifies that:
        1. Command line arguments are parsed correctly
        2. The pipeline is initialized with the correct configuration
        3. The pipeline runs successfully
        """
        # Arrange
        config_path = temp_dirs['config_path']
        
        # Create mocks for all external function calls
        with patch('sys.argv', ['pipeline_orchestrator.py', '--config', config_path]), \
             patch('src.pipeline_orchestrator.PipelineOrchestrator') as mock_orch_class, \
             patch('src.pipeline_orchestrator.sys.exit') as mock_exit:
            
            # Setup mock orchestrator
            mock_orch = MagicMock()
            mock_orch.run_pipeline.return_value = True
            mock_orch_class.return_value = mock_orch
            
            # Act
            main()
            
            # Assert
            mock_orch_class.assert_called_once_with(config_path=config_path)
            mock_orch.run_pipeline.assert_called_once_with(download_only=False)
            mock_exit.assert_called_once_with(0)  # Successful exit

    def test_main_function_failure(self, temp_dirs, mock_ee):
        """
        Test the main function when the pipeline fails.
        
        This test verifies that:
        1. The pipeline runs but fails
        2. The program exits with a non-zero exit code
        """
        # Arrange
        config_path = temp_dirs['config_path']
        
        # Create mocks for all external function calls
        with patch('sys.argv', ['pipeline_orchestrator.py', '--config', config_path]), \
             patch('src.pipeline_orchestrator.PipelineOrchestrator') as mock_orch_class, \
             patch('src.pipeline_orchestrator.sys.exit') as mock_exit:
            
            # Setup mock orchestrator to fail
            mock_orch = MagicMock()
            mock_orch.run_pipeline.return_value = False
            mock_orch_class.return_value = mock_orch
            
            # Act
            main()
            
            # Assert
            mock_exit.assert_called_once_with(1)  # Failed exit 