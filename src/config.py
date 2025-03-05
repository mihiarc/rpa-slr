from pathlib import Path
import yaml

# Project structure configuration
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"

# Main directories
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"

# Data subdirectories
RAW_DATA_DIR = DATA_DIR / "raw"
CACHE_DIR = DATA_DIR / "cache"

# Output subdirectories
PROCESSED_DIR = OUTPUT_DIR / "processed"
LOG_DIR = OUTPUT_DIR / "logs"
ARCHIVE_DIR = OUTPUT_DIR / "archive"
MAPS_DIR = OUTPUT_DIR / "maps"
ANALYSIS_DIR = OUTPUT_DIR / "analysis"
IMPUTATION_DIR = OUTPUT_DIR / "imputation"
HISTORICAL_DIR = OUTPUT_DIR / "historical"
PROJECTED_DIR = OUTPUT_DIR / "projected"
COUNTY_SHORELINE_REF_POINTS_DIR = OUTPUT_DIR / "county_shoreline_ref_points"

# Configuration directories
TIDE_STATIONS_DIR = CONFIG_DIR / "tide_stations"
REGION_CONFIG = CONFIG_DIR / "region_mappings.yaml"
COUNTY_REGION_CONFIG = CONFIG_DIR / "county_region_mappings.yaml"
NOAA_SETTINGS_FILE = CONFIG_DIR / "noaa_api_settings.yaml"

# Load NOAA settings
with open(NOAA_SETTINGS_FILE) as f:
    NOAA_SETTINGS = yaml.safe_load(f)

# Input data paths
CENSUS_COUNTY_SHAPEFILE = DATA_DIR / "input" / "shapefile_county_census" / "tl_2024_us_county.shp"
COASTAL_COUNTIES_CSV = DATA_DIR / "input" / "coastal_counties.csv"

# Common data paths
SHORELINE_DIR = PROCESSED_DIR / "regional_shorelines"
COUNTY_FILE = PROCESSED_DIR / "county.parquet"
COASTAL_COUNTIES_FILE = PROCESSED_DIR / "coastal_counties.parquet"
REFERENCE_POINTS_FILE = COUNTY_SHORELINE_REF_POINTS_DIR / "coastal_reference_points.parquet"

# Historical data paths
HISTORICAL_DATA_DIR = HISTORICAL_DIR / "data"
HISTORICAL_REFERENCE_POINTS = HISTORICAL_DATA_DIR / "reference_points.parquet"
HISTORICAL_TIDE_GAUGE_MAP = HISTORICAL_DATA_DIR / "tide_gauge_county_map.json"

# Projected data paths
PROJECTED_DATA_DIR = PROJECTED_DIR / "data"
PROJECTED_REFERENCE_POINTS = PROJECTED_DATA_DIR / "reference_points.parquet"
PROJECTED_TIDE_GAUGE_MAP = PROJECTED_DATA_DIR / "tide_gauge_county_map.json"

# Imputation paths
IMPUTATION_DATA_DIR = IMPUTATION_DIR / "data"
IMPUTATION_LOGS_DIR = IMPUTATION_DIR / "logs"
IMPUTATION_MAPS_DIR = MAPS_DIR / "imputation"

# Spatial reference systems
ALBERS_CRS = "+proj=aea +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"
WGS84_EPSG = 4326

# Point spacing for reference points (in meters)
POINT_SPACING = 5000  # 5km spacing between coastal reference points

# Data settings from NOAA config
HISTORICAL_SETTINGS = NOAA_SETTINGS['data']['historical']
PROJECTED_SETTINGS = NOAA_SETTINGS['data']['projected']

# Ensure directories exist
def ensure_directories():
    """Create all necessary directories if they don't exist."""
    directories = [
        DATA_DIR,
        RAW_DATA_DIR,
        CACHE_DIR,
        OUTPUT_DIR,
        PROCESSED_DIR,
        LOG_DIR,
        ARCHIVE_DIR,
        MAPS_DIR,
        ANALYSIS_DIR,
        IMPUTATION_DIR,
        HISTORICAL_DIR,
        PROJECTED_DIR,
        HISTORICAL_DATA_DIR,
        PROJECTED_DATA_DIR,
        IMPUTATION_DATA_DIR,
        IMPUTATION_LOGS_DIR,
        IMPUTATION_MAPS_DIR,
        SHORELINE_DIR
    ]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

# Call this when importing the config
ensure_directories() 