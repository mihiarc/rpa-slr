"""
Sensitivity analysis for Alaska HTF threshold reference periods.

This script tests how different reference periods for percentile threshold
computation affect the resulting flood day counts. This helps evaluate
the robustness of the methodology and choose an appropriate reference period.

Testing periods:
- 2000-2010 (early period)
- 2000-2015 (mid period)
- 2000-2019 (full period - default)
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import requests

logger = logging.getLogger(__name__)

# Stations without NWS thresholds (the ones that need percentile thresholds)
STATIONS_NO_THRESHOLD = {
    '9455920': {'name': 'Anchorage', 'mhhw': 35.43},
    '9455500': {'name': 'Seward', 'mhhw': 24.83},
    '9455090': {'name': 'Valdez', 'mhhw': 16.77},
    '9454050': {'name': 'Cordova', 'mhhw': 18.88},
    '9457292': {'name': 'Kodiak Island', 'mhhw': 34.04},
    '9459450': {'name': 'Sand Point', 'mhhw': 37.75},
    '9461380': {'name': 'Adak Island', 'mhhw': 6.67},
    '9462620': {'name': 'Unalaska', 'mhhw': 6.20},
    '9463502': {'name': 'Port Moller', 'mhhw': 39.68},
    '9468756': {'name': 'Prudhoe Bay', 'mhhw': 5.29},
    '9497645': {'name': 'Nome', 'mhhw': 36.54}
}

BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"


def fetch_high_low_data(station_id: str, year: int, rate_limit: float = 0.5) -> Optional[List[Dict]]:
    """Fetch high/low water level data for a station and year."""
    params = {
        'begin_date': f'{year}0101',
        'end_date': f'{year}1231',
        'station': station_id,
        'product': 'high_low',
        'datum': 'STND',
        'units': 'english',
        'time_zone': 'gmt',
        'format': 'json'
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        time.sleep(rate_limit)

        if 'data' in data:
            return data['data']
        return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {station_id}/{year}: {e}")
        return None


def compute_percentile_for_period(station_id: str, start_year: int, end_year: int,
                                  percentile: float = 99) -> Optional[float]:
    """Compute percentile threshold for a specific reference period."""
    all_high_values = []

    for year in range(start_year, end_year + 1):
        data = fetch_high_low_data(station_id, year)
        if data:
            for record in data:
                tide_type = record.get('ty', '').strip()
                if tide_type in ['H', 'HH']:
                    try:
                        value = float(record['v'])
                        all_high_values.append(value)
                    except (ValueError, KeyError):
                        continue

    if len(all_high_values) < 100:
        return None

    return np.percentile(all_high_values, percentile)


def compute_flood_days_with_threshold(station_id: str, year: int, threshold: float) -> int:
    """Compute flood days for a year using a given threshold."""
    data = fetch_high_low_data(station_id, year)
    if not data:
        return 0

    flood_dates = set()
    for record in data:
        tide_type = record.get('ty', '').strip()
        if tide_type in ['H', 'HH']:
            try:
                value = float(record['v'])
                if value >= threshold:
                    date = record['t'].split()[0]
                    flood_dates.add(date)
            except (ValueError, KeyError):
                continue

    return len(flood_dates)


def run_sensitivity_analysis(
    test_stations: Optional[List[str]] = None,
    test_year: int = 2023,
    reference_periods: Optional[List[tuple]] = None,
    output_dir: Optional[Path] = None
) -> pd.DataFrame:
    """
    Run sensitivity analysis on reference period choice.

    Args:
        test_stations: List of station IDs to test (defaults to 3 representative stations)
        test_year: Year to compute flood days for
        reference_periods: List of (start, end) tuples for reference periods
        output_dir: Optional output directory for results

    Returns:
        DataFrame with sensitivity analysis results
    """
    if test_stations is None:
        # Select representative stations with different characteristics
        test_stations = ['9455920', '9455090', '9462620']  # Anchorage, Valdez, Unalaska

    if reference_periods is None:
        reference_periods = [
            (2000, 2010, "2000-2010"),
            (2000, 2015, "2000-2015"),
            (2000, 2019, "2000-2019"),
        ]

    results = []

    for station_id in test_stations:
        station_name = STATIONS_NO_THRESHOLD[station_id]['name']
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing station: {station_name} ({station_id})")

        for start_year, end_year, period_label in reference_periods:
            logger.info(f"  Computing threshold for period {period_label}...")
            threshold = compute_percentile_for_period(station_id, start_year, end_year)

            if threshold is None:
                logger.warning(f"    Could not compute threshold for {period_label}")
                continue

            logger.info(f"    99th percentile threshold: {threshold:.2f} ft")

            # Compute flood days for test year
            flood_days = compute_flood_days_with_threshold(station_id, test_year, threshold)
            logger.info(f"    Flood days in {test_year}: {flood_days}")

            results.append({
                'station_id': station_id,
                'station_name': station_name,
                'reference_period': period_label,
                'ref_start': start_year,
                'ref_end': end_year,
                'threshold_99pct_ft': threshold,
                'test_year': test_year,
                'flood_days': flood_days
            })

    df = pd.DataFrame(results)

    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"alaska_htf_sensitivity_{test_year}.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"\nSaved results to {output_path}")

    return df


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run sensitivity analysis on Alaska HTF reference periods")
    parser.add_argument("--test-year", type=int, default=2023, help="Year to test flood days")
    parser.add_argument("--output-dir", type=Path, default=Path("output/analysis"),
                       help="Output directory")
    parser.add_argument("--all-stations", action="store_true",
                       help="Test all stations (slower)")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    test_stations = None
    if args.all_stations:
        test_stations = list(STATIONS_NO_THRESHOLD.keys())

    df = run_sensitivity_analysis(
        test_stations=test_stations,
        test_year=args.test_year,
        output_dir=args.output_dir
    )

    print("\n" + "="*70)
    print("SENSITIVITY ANALYSIS RESULTS")
    print("="*70)

    # Pivot to show comparison
    pivot = df.pivot(index=['station_name'], columns='reference_period', values=['threshold_99pct_ft', 'flood_days'])
    print("\nThreshold comparison (99th percentile, feet):")
    print(pivot['threshold_99pct_ft'].to_string())
    print("\nFlood days comparison:")
    print(pivot['flood_days'].to_string())

    # Calculate sensitivity
    print("\n" + "-"*70)
    print("SENSITIVITY METRICS")
    print("-"*70)
    for station_name in df['station_name'].unique():
        station_df = df[df['station_name'] == station_name]
        thresholds = station_df['threshold_99pct_ft']
        flood_days = station_df['flood_days']
        print(f"\n{station_name}:")
        print(f"  Threshold range: {thresholds.min():.2f} - {thresholds.max():.2f} ft "
              f"(delta: {thresholds.max() - thresholds.min():.2f} ft)")
        print(f"  Flood days range: {flood_days.min()} - {flood_days.max()} "
              f"(delta: {flood_days.max() - flood_days.min()})")


if __name__ == "__main__":
    main()
