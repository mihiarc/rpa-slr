# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository is for processing county-level tidal flooding data from NOAA, with capabilities for both historical data analysis and future projections. It processes high tide flooding (HTF) data through multiple stages: data fetching, imputation, and assignment to generate county-level flood day estimates.

## Key Commands

### Installation and Setup
```bash
# Create virtual environment
uv venv

# Install package in development mode
uv pip install -e .

# Install with development dependencies  
uv pip install -e .[dev]
```

### Testing
```bash
# Run tests using pytest
uv run pytest tests/

# Run tests with coverage
uv run pytest tests/ --cov=src --cov-report=term-missing --cov-report=html
```

### Main Processing Commands

#### Historical HTF Data Processing
```bash
# Process historical data for specific region
uv run python -m src.noaa.historical.historical_htf_cli \
    --region gulf_coast \
    --start-year 1920 \
    --end-year 2024 \
    --output-dir output/historical \
    --format parquet
```

#### Projected HTF Data Processing
```bash
# Process projected data for specific region
uv run python -m src.noaa.projected.projected_htf_cli \
    --region hawaii \
    --start-decade 2020 \
    --end-decade 2100 \
    --output-dir output/projected \
    --format parquet
```

#### Imputation Process
```bash
# Run imputation for specific region
uv run python src/run_imputation.py --region west_coast --output-dir output/imputation/data
```

#### HTF Assignment
```bash
# Run the HTF assignment process
uv run python src/run_htf_assignment_simple.py
```

#### Data Quality Analysis
```bash
# Generate analysis report for a region
uv run python -m src.analysis.generate_report \
    --region gulf_coast \
    --data-type historical \
    --format markdown \
    --verbose
```

### Code Quality
```bash
# Code formatting
uv run black src/ tests/

# Linting
uv run flake8 src/ tests/

# Type checking
uv run mypy src/
```

## Architecture Overview

### Core Components

1. **NOAA Data Pipeline** (`src/noaa/`)
   - `core/`: API client, rate limiting, and caching
   - `historical/`: Historical HTF data processing
   - `projected/`: Projected HTF data processing

2. **Imputation System** (`src/imputation/`)
   - Maps tide stations to coastal reference points for each county
   - Uses spatial interpolation with distance-based weights
   - Handles missing data through geographic relationships

3. **Assignment System** (`src/assignment/`)
   - Aggregates flood days by county based on imputation mapping
   - Processes both historical and projected data
   - Outputs county-level annual flood day estimates

4. **Analysis Tools** (`src/analysis/`)
   - Data quality assessment
   - Temporal and spatial analysis
   - Report generation with visualizations

5. **Configuration System** (`config/`)
   - Regional mappings and tide station definitions
   - NOAA API settings and data type configurations
   - County-region mapping files

### Data Flow

1. **Fetch**: Download HTF data from NOAA API by region and time period
2. **Impute**: Create spatial relationships between tide stations and coastal counties
3. **Assign**: Aggregate station data to county level using weighted assignments
4. **Analyze**: Generate quality reports and visualizations

### Regional Structure

The system processes data by geographic regions:
- Alaska, Hawaii, Pacific Islands
- Virgin Islands, Puerto Rico  
- North Atlantic, Mid-Atlantic, South Atlantic
- Gulf Coast, West Coast

Each region has its own tide station configuration file in `config/tide_stations/`.

### Key Paths and Configuration

- Configuration: All YAML configs in `config/` directory
- Main script locations: `src/run_imputation.py`, `src/run_htf_assignment_simple.py`
- Project paths defined in `src/config.py`
- Output structure organized by data type: `output/{historical|projected|imputation|analysis}`

### Testing Structure

- Tests located in `tests/` directory
- Focused on NOAA API components and core functionality
- Run with `uv run pytest tests/`
- Coverage reporting available via `--cov` flag

### Dependencies

Core dependencies include pandas, numpy, geopandas, scipy for data processing; matplotlib, seaborn, cartopy for visualization; requests for API calls; and pyarrow for parquet file support. Development tools include pytest, black, flake8, and mypy.