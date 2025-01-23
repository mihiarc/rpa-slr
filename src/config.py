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
NOAA_SETTINGS_FILE = CONFIG_DIR / "noaa_settings.yaml"
with open(NOAA_SETTINGS_FILE) as f:
    NOAA_SETTINGS = yaml.safe_load(f)

# Input data paths (processed parquet files)
SHORELINE_FILE = PROCESSED_DATA_DIR / "shoreline.parquet"
COUNTY_FILE = PROCESSED_DATA_DIR / "county.parquet"
COASTAL_COUNTIES_FILE = PROCESSED_DATA_DIR / "coastal_counties.parquet"
REFERENCE_POINTS_FILE = PROCESSED_DATA_DIR / "coastal_reference_points.parquet"
TIDE_GAUGE_CROSSWALK = PROCESSED_DATA_DIR / "tide_gauge_county_crosswalk.json"
TIDE_STATIONS_LIST = CONFIG_DIR / "tide-stations-list.yaml"

# Spatial reference systems
ALBERS_CRS = "+proj=aea +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m +no_defs"
WGS84_EPSG = 4326

# Distance thresholds (in meters)
POINT_SPACING = 5000  # 5km spacing between coastal reference points
CLOSE_THRESHOLD = NOAA_SETTINGS['stations']['metadata']['close_threshold']  # 1km threshold for "very close" gauges
INITIAL_SEARCH_DISTANCE = 50000  # 50km initial search radius
MAX_SEARCH_DISTANCE = NOAA_SETTINGS['stations']['metadata']['max_search_distance']  # 200km maximum search radius
DISTANCE_INCREMENT = 25000  # 25km increments for expanding search

# Gauge association parameters
MAX_GAUGES_PER_POINT = 3  # Maximum number of gauges to associate with each point
MIN_WEIGHT_THRESHOLD = NOAA_SETTINGS['stations']['metadata']['min_weight_threshold']  # Minimum weight to consider a gauge relevant

# Ensure directories exist
def ensure_directories():
    """Create all necessary directories if they don't exist."""
    for directory in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, OUTPUT_DIR, LOG_DIR]:
        directory.mkdir(parents=True, exist_ok=True)

# Call this when importing the config
ensure_directories() 