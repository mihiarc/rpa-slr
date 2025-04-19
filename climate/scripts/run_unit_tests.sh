#!/bin/bash
set -e

# Create and activate virtual environment using uv
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment with uv..."
    uv venv .venv
fi

# Activate virtual environment
source .venv/bin/activate

# Install dependencies with uv
echo "Installing dependencies with uv..."
uv pip install -r requirements.txt
uv pip install -r requirements-dev.txt

# Run unit tests (exclude integration tests)
echo "Running unit tests..."
python -m pytest tests/ -v -k "not integration"

echo "Unit tests completed!" 