# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-01-23

### Added
- YAML configuration system for NOAA settings
  - Created `noaa_settings.yaml` for centralized NOAA configuration
  - Added `tide-stations-list.yaml` for station metadata
  - Implemented configuration validation in `NOAACache`

### Changed
- Refactored NOAA configuration management
  - Moved hardcoded settings to YAML files
  - Updated `config.py` to use YAML settings
  - Modified `NOAACache` to handle YAML data format
  - Improved station list structure with better metadata

### Technical Details
- Configuration files:
  - `src/noaa/config/noaa_settings.yaml`: Main NOAA configuration
  - `src/noaa/data/tide-stations-list.yaml`: Station metadata
- Modified classes:
  - `NOAACache`: Updated to use YAML configuration
  - Added YAML validation and error handling
  - Improved path management and data type validation
- Updated dependencies:
  - Added PyYAML for YAML file handling 