#!/bin/bash

# Script to run the county-level climate projection pipeline tests
# Supports running unit tests, integration tests, or all tests

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

# Install test dependencies
echo "Installing test dependencies..."
uv pip install -r requirements-dev.txt

# Parse command line arguments
TEST_TYPE="all"
if [ $# -gt 0 ]; then
    TEST_TYPE="$1"
fi

echo "Running tests of type: $TEST_TYPE"

# Create required directories for tests
mkdir -p test_data test_output

# Run the appropriate tests
case $TEST_TYPE in
    "unit")
        echo "Running unit tests..."
        python -m pytest tests/unit -v
        ;;
    "integration")
        echo "Running integration tests..."
        python -m pytest tests/integration -v
        ;;
    "all")
        echo "Running all tests with coverage..."
        python -m pytest tests/ --cov=src --cov-report=term-missing -v
        ;;
    *)
        echo "Unknown test type: $TEST_TYPE"
        echo "Usage: $0 [all|unit|integration]"
        exit 1
        ;;
esac

# Check exit status
if [ $? -eq 0 ]; then
    echo "Tests completed successfully!"
else
    echo "Tests failed. Please check the errors above."
    exit 1
fi 