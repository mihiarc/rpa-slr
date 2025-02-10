from pathlib import Path
import yaml

# Project structure configuration
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
OUTPUT_DIR = PROJECT_ROOT / "output"
LOG_DIR = OUTPUT_DIR / "logs"
CONFIG_DIR = PROJECT_ROOT / "config"

# Load NOAA settings
NOAA_SETTINGS_FILE = CONFIG_DIR / "noaa_api_settings.yaml"
with open(NOAA_SETTINGS_FILE) as f:
    NOAA_SETTINGS = yaml.safe_load(f)

# Common data paths
SHORELINE_FILE = PROCESSED_DATA_DIR / "shoreline.parquet"
COUNTY_FILE = PROCESSED_DATA_DIR / "county.parquet"
COASTAL_COUNTIES_FILE = PROCESSED_DATA_DIR / "coastal_counties.parquet"
TIDE_STATIONS_LIST = CONFIG_DIR / "tide-stations-list.yaml"
REFERENCE_POINTS_FILE = PROCESSED_DATA_DIR / "reference_points.parquet"

# Historical data paths
HISTORICAL_DATA_DIR = PROCESSED_DATA_DIR / "historical"
HISTORICAL_REFERENCE_POINTS = HISTORICAL_DATA_DIR / "reference_points.parquet"
HISTORICAL_TIDE_GAUGE_MAP = HISTORICAL_DATA_DIR / "tide_gauge_county_map.json"
HISTORICAL_OUTPUT_DIR = OUTPUT_DIR / "historical"

# Projected data paths
PROJECTED_DATA_DIR = PROCESSED_DATA_DIR / "projected"
PROJECTED_REFERENCE_POINTS = PROJECTED_DATA_DIR / "reference_points.parquet"
PROJECTED_TIDE_GAUGE_MAP = PROJECTED_DATA_DIR / "tide_gauge_county_map.json"
PROJECTED_OUTPUT_DIR = OUTPUT_DIR / "projected"

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
        PROCESSED_DATA_DIR,
        OUTPUT_DIR,
        LOG_DIR,
        HISTORICAL_DATA_DIR,
        HISTORICAL_OUTPUT_DIR,
        PROJECTED_DATA_DIR,
        PROJECTED_OUTPUT_DIR
    ]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

# Call this when importing the config
ensure_directories() 