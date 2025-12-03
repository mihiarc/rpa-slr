"""
Alaska HTF Flood Days Computer.

NOAA's HTF API does not provide historical flood counts for Alaska stations.
This module computes flood days from raw high/low water level data using
percentile-based thresholds derived from historical observations.

Methodology:
- For stations WITH NWS-defined thresholds: Use the official threshold
- For stations WITHOUT thresholds: Use 99th percentile of historical high tides
  as the minor flood threshold. This represents exceedance events that occur
  approximately 3-4 days per year on average.

The 99th percentile approach is defensible for research because:
1. Data-driven: Threshold emerges from actual water level distribution
2. Comparable across stations: Each station uses its own statistical baseline
3. Statistically rigorous: Clear, reproducible methodology

Reference: Sweet et al. (2018) "2017 State of U.S. High Tide Flooding" uses
similar percentile-based approaches for defining minor flood thresholds.
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import requests
import pandas as pd
import numpy as np
from pathlib import Path

logger = logging.getLogger(__name__)

# Percentile to use for derived thresholds (99th = ~3-4 exceedance days/year)
DEFAULT_THRESHOLD_PERCENTILE = 99

# Reference period for computing percentiles (1990-2000 baseline)
# Using a fixed historical baseline makes flood counts interpretable as
# anomalies relative to late 20th century conditions
REFERENCE_PERIOD_START = 1990
REFERENCE_PERIOD_END = 2000


@dataclass
class AlaskaStation:
    """Alaska tide station with flood threshold information."""
    station_id: str
    name: str
    lat: float
    lon: float
    region: str
    mhhw: float  # Mean Higher High Water (feet, station datum)
    nws_minor: Optional[float]  # NWS minor flood threshold if defined
    percentile_threshold: Optional[float]  # Computed percentile threshold
    threshold_used: Optional[float]  # Final threshold to use
    threshold_source: str  # 'nws', 'percentile_99', etc.


# Alaska station configuration with datums and NWS thresholds (where available)
ALASKA_STATIONS = {
    # Southeast Alaska (have NWS thresholds)
    '9451054': {'name': 'Sitka', 'lat': 57.051667, 'lon': -135.341667, 'region': 'southeast_alaska',
                'mhhw': 14.69, 'nws_minor': 16.76},
    '9451600': {'name': 'Juneau', 'lat': 58.298333, 'lon': -134.411667, 'region': 'southeast_alaska',
                'mhhw': 14.46, 'nws_minor': 17.52},
    '9452210': {'name': 'Skagway', 'lat': 59.450000, 'lon': -135.333333, 'region': 'southeast_alaska',
                'mhhw': 19.45, 'nws_minor': 26.66},
    '9452400': {'name': 'Yakutat', 'lat': 59.548333, 'lon': -139.733333, 'region': 'southeast_alaska',
                'mhhw': 18.71, 'nws_minor': 27.99},
    '9450460': {'name': 'Ketchikan', 'lat': 55.333333, 'lon': -131.625000, 'region': 'southeast_alaska',
                'mhhw': 21.64, 'nws_minor': 27.19},

    # South Central Alaska (no NWS thresholds - use percentile)
    '9455920': {'name': 'Anchorage', 'lat': 61.237778, 'lon': -149.890000, 'region': 'south_central_alaska',
                'mhhw': 35.43, 'nws_minor': None},
    '9455500': {'name': 'Seward', 'lat': 60.120000, 'lon': -149.426667, 'region': 'south_central_alaska',
                'mhhw': 24.83, 'nws_minor': None},
    '9455090': {'name': 'Valdez', 'lat': 61.124444, 'lon': -146.362778, 'region': 'south_central_alaska',
                'mhhw': 16.77, 'nws_minor': None},
    '9454050': {'name': 'Cordova', 'lat': 60.558333, 'lon': -145.753333, 'region': 'south_central_alaska',
                'mhhw': 18.88, 'nws_minor': None},
    '9457292': {'name': 'Kodiak Island', 'lat': 57.731667, 'lon': -152.514444, 'region': 'south_central_alaska',
                'mhhw': 34.04, 'nws_minor': None},

    # Western Alaska (no NWS thresholds - use percentile)
    '9459450': {'name': 'Sand Point', 'lat': 55.337222, 'lon': -160.502222, 'region': 'western_alaska',
                'mhhw': 37.75, 'nws_minor': None},
    '9461380': {'name': 'Adak Island', 'lat': 51.863333, 'lon': -176.632222, 'region': 'western_alaska',
                'mhhw': 6.67, 'nws_minor': None},
    '9462620': {'name': 'Unalaska', 'lat': 53.880000, 'lon': -166.537778, 'region': 'western_alaska',
                'mhhw': 6.20, 'nws_minor': None},
    '9463502': {'name': 'Port Moller', 'lat': 55.988333, 'lon': -160.561667, 'region': 'western_alaska',
                'mhhw': 39.68, 'nws_minor': None},

    # Northern Alaska (no NWS thresholds - use percentile)
    '9468756': {'name': 'Prudhoe Bay', 'lat': 70.411111, 'lon': -148.531944, 'region': 'northern_alaska',
                'mhhw': 5.29, 'nws_minor': None},
    '9497645': {'name': 'Nome', 'lat': 64.495000, 'lon': -165.441667, 'region': 'northern_alaska',
                'mhhw': 36.54, 'nws_minor': None},
}


class AlaskaHTFComputer:
    """Computes HTF flood days for Alaska stations from water level data."""

    BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

    def __init__(self,
                 threshold_percentile: float = DEFAULT_THRESHOLD_PERCENTILE,
                 reference_start: int = REFERENCE_PERIOD_START,
                 reference_end: int = REFERENCE_PERIOD_END,
                 rate_limit: float = 0.5):
        """
        Initialize the Alaska HTF computer.

        Args:
            threshold_percentile: Percentile for derived thresholds (default 99)
            reference_start: Start year for reference period
            reference_end: End year for reference period
            rate_limit: Seconds between API requests
        """
        self.threshold_percentile = threshold_percentile
        self.reference_start = reference_start
        self.reference_end = reference_end
        self.rate_limit = rate_limit
        self.stations: Dict[str, AlaskaStation] = {}
        self._init_stations()

    def _init_stations(self):
        """Initialize station objects without computed thresholds."""
        for station_id, info in ALASKA_STATIONS.items():
            self.stations[station_id] = AlaskaStation(
                station_id=station_id,
                name=info['name'],
                lat=info['lat'],
                lon=info['lon'],
                region=info['region'],
                mhhw=info['mhhw'],
                nws_minor=info['nws_minor'],
                percentile_threshold=None,
                threshold_used=info['nws_minor'],  # Will be updated for non-NWS
                threshold_source='nws' if info['nws_minor'] else 'pending'
            )

    def _fetch_high_low_data(self, station_id: str, year: int) -> Optional[List[Dict]]:
        """
        Fetch high/low water level data for a station and year.

        Args:
            station_id: NOAA station ID
            year: Year to fetch

        Returns:
            List of high/low records or None if no data
        """
        params = {
            'begin_date': f'{year}0101',
            'end_date': f'{year}1231',
            'station': station_id,
            'product': 'high_low',
            'datum': 'STND',  # Station datum (same reference as thresholds)
            'units': 'english',  # feet
            'time_zone': 'gmt',
            'format': 'json'
        }

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if 'data' in data:
                return data['data']
            elif 'error' in data:
                logger.debug(f"API error for {station_id}/{year}: {data.get('error', {}).get('message', 'Unknown')}")
                return None
            else:
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {station_id}/{year}: {e}")
            return None

    def compute_percentile_threshold(self, station_id: str) -> Optional[float]:
        """
        Compute percentile-based flood threshold from reference period data.

        Args:
            station_id: NOAA station ID

        Returns:
            Threshold value in feet, or None if insufficient data
        """
        logger.info(f"Computing {self.threshold_percentile}th percentile threshold for {station_id}")

        all_high_values = []

        for year in range(self.reference_start, self.reference_end + 1):
            data = self._fetch_high_low_data(station_id, year)
            if data:
                for record in data:
                    tide_type = record.get('ty', '').strip()
                    if tide_type in ['H', 'HH']:
                        try:
                            value = float(record['v'])
                            all_high_values.append(value)
                        except (ValueError, KeyError):
                            continue
            time.sleep(self.rate_limit)

        if len(all_high_values) < 100:  # Need sufficient data
            logger.warning(f"Insufficient data for {station_id}: only {len(all_high_values)} high tides")
            return None

        threshold = np.percentile(all_high_values, self.threshold_percentile)
        logger.info(f"  {station_id}: {self.threshold_percentile}th percentile = {threshold:.2f} ft "
                   f"(from {len(all_high_values)} observations)")

        return threshold

    def compute_all_thresholds(self) -> Dict[str, float]:
        """
        Compute percentile thresholds for all stations without NWS thresholds.

        Returns:
            Dictionary of station_id -> threshold
        """
        thresholds = {}

        for station_id, station in self.stations.items():
            if station.nws_minor is not None:
                # Use NWS threshold
                thresholds[station_id] = station.nws_minor
                station.threshold_used = station.nws_minor
                station.threshold_source = 'nws'
                logger.info(f"{station.name}: Using NWS threshold = {station.nws_minor:.2f} ft")
            else:
                # Compute percentile threshold
                pct_threshold = self.compute_percentile_threshold(station_id)
                if pct_threshold:
                    thresholds[station_id] = pct_threshold
                    station.percentile_threshold = pct_threshold
                    station.threshold_used = pct_threshold
                    station.threshold_source = f'percentile_{int(self.threshold_percentile)}'
                else:
                    logger.warning(f"{station.name}: Could not compute threshold")

        return thresholds

    def compute_flood_days(self, station_id: str, year: int) -> Tuple[int, int]:
        """
        Compute number of flood days for a station and year.

        Args:
            station_id: NOAA station ID
            year: Year to compute

        Returns:
            Tuple of (flood_days, total_high_tides)
        """
        station = self.stations.get(station_id)
        if not station:
            raise ValueError(f"Unknown station: {station_id}")

        if station.threshold_used is None:
            logger.warning(f"No threshold for {station_id}, skipping")
            return (0, 0)

        data = self._fetch_high_low_data(station_id, year)
        if not data:
            return (0, 0)

        threshold = station.threshold_used

        # Count days with at least one high tide exceeding threshold
        flood_dates = set()
        high_tide_count = 0

        for record in data:
            tide_type = record.get('ty', '').strip()
            if tide_type in ['H', 'HH']:  # High tide or Higher High tide
                high_tide_count += 1
                try:
                    value = float(record['v'])
                    if value >= threshold:
                        # Extract date from timestamp
                        date = record['t'].split()[0]
                        flood_dates.add(date)
                except (ValueError, KeyError):
                    continue

        return (len(flood_dates), high_tide_count)

    def compute_all_stations(self, start_year: int, end_year: int,
                            station_ids: Optional[List[str]] = None,
                            compute_thresholds: bool = True) -> pd.DataFrame:
        """
        Compute flood days for all stations across a year range.

        Args:
            start_year: First year to process
            end_year: Last year to process (inclusive)
            station_ids: Optional list of specific station IDs to process
            compute_thresholds: If True, compute percentile thresholds first

        Returns:
            DataFrame with columns: station_id, station_name, year, flood_days,
                                    threshold_ft, threshold_source, region
        """
        if station_ids is None:
            station_ids = list(self.stations.keys())

        # Step 1: Compute thresholds for stations that need them
        if compute_thresholds:
            logger.info("Step 1: Computing percentile thresholds for stations without NWS values")
            self.compute_all_thresholds()

        # Step 2: Compute flood days
        logger.info(f"Step 2: Computing flood days for {len(station_ids)} stations, {start_year}-{end_year}")

        results = []
        total_requests = len(station_ids) * (end_year - start_year + 1)
        completed = 0

        for station_id in station_ids:
            station = self.stations[station_id]

            if station.threshold_used is None:
                logger.warning(f"Skipping {station.name}: no threshold available")
                continue

            logger.info(f"Processing {station.name} ({station_id})")

            for year in range(start_year, end_year + 1):
                flood_days, high_tides = self.compute_flood_days(station_id, year)

                results.append({
                    'station_id': station_id,
                    'station_name': station.name,
                    'year': year,
                    'flood_days': flood_days,
                    'high_tides_observed': high_tides,
                    'threshold_ft': station.threshold_used,
                    'threshold_source': station.threshold_source,
                    'percentile_threshold_ft': station.percentile_threshold,
                    'nws_threshold_ft': station.nws_minor,
                    'mhhw_ft': station.mhhw,
                    'region': station.region,
                    'lat': station.lat,
                    'lon': station.lon
                })

                completed += 1
                if completed % 20 == 0:
                    logger.info(f"Progress: {completed}/{total_requests} requests")

                time.sleep(self.rate_limit)

        df = pd.DataFrame(results)
        logger.info(f"Computed {len(df)} station-year records")

        return df

    def save_results(self, df: pd.DataFrame, output_dir: Path,
                    filename: str = "historical_htf_alaska.parquet") -> Path:
        """
        Save results to parquet file.

        Args:
            df: Results DataFrame
            output_dir: Output directory
            filename: Output filename

        Returns:
            Path to output file
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / filename
        df.to_parquet(output_path, index=False)

        logger.info(f"Saved Alaska HTF data to {output_path}")
        return output_path

    def save_threshold_report(self, output_dir: Path,
                             filename: str = "alaska_htf_thresholds.csv") -> Path:
        """
        Save threshold report for documentation.

        Args:
            output_dir: Output directory
            filename: Output filename

        Returns:
            Path to output file
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        records = []
        for station_id, station in self.stations.items():
            records.append({
                'station_id': station_id,
                'station_name': station.name,
                'region': station.region,
                'mhhw_ft': station.mhhw,
                'nws_minor_ft': station.nws_minor,
                f'percentile_{int(self.threshold_percentile)}_ft': station.percentile_threshold,
                'threshold_used_ft': station.threshold_used,
                'threshold_source': station.threshold_source,
                'lat': station.lat,
                'lon': station.lon
            })

        df = pd.DataFrame(records)
        output_path = output_dir / filename
        df.to_csv(output_path, index=False)

        logger.info(f"Saved threshold report to {output_path}")
        return output_path


def compute_alaska_htf(
    start_year: int = 1990,
    end_year: int = 2024,
    output_dir: Optional[Path] = None,
    threshold_percentile: float = DEFAULT_THRESHOLD_PERCENTILE,
    reference_start: int = REFERENCE_PERIOD_START,
    reference_end: int = REFERENCE_PERIOD_END
) -> pd.DataFrame:
    """
    Main function to compute Alaska HTF data.

    Args:
        start_year: First year to process
        end_year: Last year to process
        output_dir: Output directory (optional)
        threshold_percentile: Percentile for derived thresholds
        reference_start: Start year for reference period (for percentile calculation)
        reference_end: End year for reference period

    Returns:
        DataFrame with computed flood days
    """
    computer = AlaskaHTFComputer(
        threshold_percentile=threshold_percentile,
        reference_start=reference_start,
        reference_end=reference_end
    )

    df = computer.compute_all_stations(start_year, end_year)

    if output_dir:
        computer.save_results(df, output_dir)
        computer.save_threshold_report(output_dir)

    return df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Compute Alaska HTF flood days using percentile-based thresholds")
    parser.add_argument("--start-year", type=int, default=1990, help="Start year for flood day computation")
    parser.add_argument("--end-year", type=int, default=2024, help="End year for flood day computation")
    parser.add_argument("--output-dir", type=Path, default=Path("output/historical"),
                       help="Output directory")
    parser.add_argument("--percentile", type=float, default=DEFAULT_THRESHOLD_PERCENTILE,
                       help="Percentile for derived thresholds (default: 99)")
    parser.add_argument("--ref-start", type=int, default=REFERENCE_PERIOD_START,
                       help="Reference period start year for percentile computation")
    parser.add_argument("--ref-end", type=int, default=REFERENCE_PERIOD_END,
                       help="Reference period end year for percentile computation")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    df = compute_alaska_htf(
        start_year=args.start_year,
        end_year=args.end_year,
        output_dir=args.output_dir,
        threshold_percentile=args.percentile,
        reference_start=args.ref_start,
        reference_end=args.ref_end
    )

    print(f"\nAlaska HTF Summary:")
    print(f"  Total records: {len(df)}")
    print(f"  Stations: {df['station_id'].nunique()}")
    print(f"  Year range: {df['year'].min()}-{df['year'].max()}")
    print(f"\n  Threshold sources:")
    for source, count in df.groupby('threshold_source')['station_id'].nunique().items():
        print(f"    {source}: {count} stations")
    print(f"\n  Annual flood days by station (avg):")
    station_avg = df.groupby('station_name')['flood_days'].mean().sort_values(ascending=False)
    for name, avg in station_avg.items():
        print(f"    {name}: {avg:.1f}")
