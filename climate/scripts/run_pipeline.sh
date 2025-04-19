#!/bin/bash

# County-Level Climate Projection Pipeline Runner
# This script sets up the environment and runs the pipeline

# Ensure script fails on any error
set -e

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "The 'uv' package manager is not installed. Installing now..."
    pip install uv
fi

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment with uv..."
    uv venv
fi

# Activate virtual environment
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    # Windows
    source .venv/Scripts/activate
else
    # Unix-like
    source .venv/bin/activate
fi

# Install requirements if needed
if [ ! -f ".requirements_installed" ]; then
    echo "Installing dependencies with uv..."
    uv pip install -r requirements.txt
    touch .requirements_installed
fi

# Ensure config directory exists
if [ ! -d "config" ]; then
    echo "Creating config directory..."
    mkdir -p config
fi

# Check if config file exists, create template if not
if [ ! -f "config/config.yml" ]; then
    echo "Creating template config file..."
    cat > config/config.yml << EOF
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
EOF
    echo "WARNING: Please edit config/config.yml with your specific settings before running the pipeline."
    exit 1
fi

# Create data and output directories
mkdir -p data output

# Run the pipeline with arguments passed to this script
echo "Running the county-level climate projection pipeline..."
python -m src.pipeline_orchestrator "$@"

# Check exit status
if [ $? -eq 0 ]; then
    echo "Pipeline completed successfully!"
    echo "Check the 'output' directory for results."
else
    echo "Pipeline failed. Please check the logs for errors."
    exit 1
fi 