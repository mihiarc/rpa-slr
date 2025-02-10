"""
Script to update tide stations list from JSON to YAML format.
"""

import json
import yaml
from pathlib import Path
import logging

from src.config import CONFIG_DIR

logger = logging.getLogger(__name__)

def update_stations():
    """Update tide stations list from JSON to YAML format."""
    # Load JSON file
    json_file = Path("docs/tide-stations-list.json")
    with open(json_file) as f:
        stations_json = json.load(f)
    
    # Create YAML structure
    yaml_data = {
        "metadata": {
            "source": "NOAA Tides and Currents",
            "last_updated": "2024-03-01",
            "description": "List of NOAA tide gauge stations used for high tide flooding analysis",
            "coordinate_system": "WGS84 (EPSG:4326)"
        },
        "stations": {}
    }
    
    # Convert stations to YAML format
    for station in stations_json:
        station_id = station['id']
        yaml_data['stations'][station_id] = {
            'name': station['name'],
            'location': {
                'lat': station['lat'],
                'lon': station['lng']
            }
        }
    
    # Save YAML file
    yaml_file = CONFIG_DIR / "tide-stations-list.yaml"
    with open(yaml_file, 'w') as f:
        yaml.safe_dump(yaml_data, f, sort_keys=False, indent=2)
    
    logger.info(f"Updated {yaml_file} with {len(yaml_data['stations'])} stations")
    
    # Print summary of stations by region
    west_coast = sum(1 for s in stations_json if s['state'] in ['CA', 'OR', 'WA'])
    east_coast = sum(1 for s in stations_json if s['state'] in ['ME', 'NH', 'MA', 'RI', 'CT', 'NY', 'NJ', 'DE', 'MD', 'VA', 'NC', 'SC', 'GA', 'FL'])
    gulf_coast = sum(1 for s in stations_json if s['state'] in ['FL', 'AL', 'MS', 'LA', 'TX'])
    pacific = sum(1 for s in stations_json if s['state'] in ['HI', 'AK'])
    
    print("\nStation Count by Region:")
    print(f"West Coast: {west_coast}")
    print(f"East Coast: {east_coast}")
    print(f"Gulf Coast: {gulf_coast}")
    print(f"Pacific: {pacific}")

def main():
    """Run the station list update."""
    logging.basicConfig(level=logging.INFO)
    update_stations()

if __name__ == "__main__":
    main() 