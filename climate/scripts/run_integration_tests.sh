#!/bin/bash
set -e

# Create a virtual environment using uv if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment with uv..."
    uv venv .venv
fi

# Activate the virtual environment
source .venv/bin/activate

# Install dependencies 
echo "Installing dependencies with uv..."
uv pip install -r requirements.txt
uv pip install -r requirements-dev.txt

# Run integration tests
echo "Running integration tests..."
python -m pytest tests/test_integration.py -v

# Optional: Run with coverage
# python -m pytest tests/test_integration.py -v --cov=src

# Generate HTML coverage report if needed
# python -m pytest tests/test_integration.py -v --cov=src --cov-report=html

echo "Integration tests completed!"

# Deactivate the virtual environment
deactivate

echo "Testing complete!" 