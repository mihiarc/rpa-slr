# Alaska High Tide Flooding Methodology

This document describes how Alaska tide stations are handled differently from the rest of the US in the HTF (High Tide Flooding) data pipeline.

## Background

NOAA's HTF API provides pre-computed annual flood day counts for most US tide stations. However, **Alaska stations return empty results** from this API - NOAA does not publish HTF data for Alaska.

To include Alaska in county-level flood analysis, we compute flood days directly from raw water level observations using percentile-based thresholds.

## Data Sources

### Other US Regions
- **Source**: NOAA HTF API (Annual Flood Count Product)
- **Endpoint**: `https://api.tidesandcurrents.noaa.gov/dpapi/prod/webapi/product/htf.json`
- **Thresholds**: NOAA-defined minor/moderate/major flood levels
- **Data**: Pre-computed annual flood day counts

### Alaska
- **Source**: NOAA CO-OPS High/Low Water Level API
- **Endpoint**: `https://api.tidesandcurrents.noaa.gov/api/prod/datagetter`
- **Product**: `high_low` (observed high and low tide values)
- **Thresholds**: Derived from historical data (see below)
- **Data**: Computed from raw observations

## Alaska Threshold Methodology

### Stations WITH NWS Flood Thresholds (5 stations)

Five Southeast Alaska stations have official NWS (National Weather Service) minor flood thresholds defined:

| Station ID | Name | NWS Minor Threshold (ft) |
|------------|------|--------------------------|
| 9451054 | Sitka | 16.76 |
| 9451600 | Juneau | 17.52 |
| 9452210 | Skagway | 26.66 |
| 9452400 | Yakutat | 27.99 |
| 9450460 | Ketchikan | 27.19 |

For these stations, we use the official NWS threshold.

### Stations WITHOUT NWS Thresholds (11 stations)

For the remaining 11 stations, we derive thresholds using the **99th percentile** of historical high tide observations:

| Station ID | Name | Region | Baseline Period | 99th Percentile (ft) |
|------------|------|--------|-----------------|---------------------|
| 9455920 | Anchorage | South Central | 1990-2000 | 39.57 |
| 9455500 | Seward | South Central | 1990-2000 | 29.72 |
| 9455090 | Valdez | South Central | 1990-2000 | 19.43 |
| 9454050 | Cordova | South Central | 1990-2000 | 21.80 |
| 9457292 | Kodiak Island | South Central | 1990-2000 | 36.71 |
| 9459450 | Sand Point | Western | 1990-2000 | 39.78 |
| 9461380 | Adak Island | Western | 1990-2000 | 8.28 |
| 9462620 | Unalaska | Western | 1990-2000 | 7.68 |
| 9463502 | Port Moller | Western | 2007-2012* | 41.85 |
| 9468756 | Prudhoe Bay | Northern | 1990-2000 | 7.92 |
| 9497645 | Nome | Northern | 1990-2000 | 38.16 |

*Port Moller data begins in 2006, so uses 2007-2012 as baseline.

## Why 99th Percentile?

The 99th percentile approach is defensible for research because:

1. **Data-driven**: Threshold emerges from actual water level distribution at each station
2. **Comparable across stations**: Each station uses its own statistical baseline
3. **Statistically rigorous**: Clear, reproducible methodology
4. **Consistent with literature**: Sweet et al. (2018) "State of U.S. High Tide Flooding" uses similar percentile-based approaches

The 99th percentile typically corresponds to ~3-4 exceedance days per year during the baseline period.

## Baseline Period Selection

### Standard Baseline: 1990-2000

Most Alaska stations use **1990-2000** as the reference period for computing the 99th percentile threshold. This choice:

- Provides a fixed historical baseline representing late 20th century conditions
- Makes flood counts interpretable as anomalies relative to pre-2000 conditions
- Allows trend analysis showing change from historical baseline
- Contains sufficient observations (~7,000-8,000 high tides per station)

### Fallback Baseline: Port Moller

Port Moller (9463502) has no data before 2006. For this station:

- **Baseline Period**: 2007-2012 (earliest available 6 years)
- **Data Period**: 2013-2024
- **Observations in baseline**: ~4,000 high tides

This is documented in the threshold report with `threshold_source = percentile_99_2007_2012`.

## Sensitivity Analysis

We tested three reference periods (2000-2010, 2000-2015, 2000-2019) to evaluate sensitivity:

| Sensitivity Level | Stations | Description |
|-------------------|----------|-------------|
| Very Low (0 days) | Seward, Kodiak Island | No change across periods |
| Low (1-2 days) | Cordova, Valdez, Sand Point, Port Moller, Anchorage, Unalaska | Minimal variation |
| Moderate (3-4 days) | Adak Island, Prudhoe Bay | Some sensitivity |
| High (9 days) | Nome | Significant sensitivity to period choice |

Most stations show robust results regardless of baseline period choice. Nome shows higher sensitivity due to rapid sea level changes affecting the threshold calculation.

## Flood Day Computation

For each Alaska station and year:

1. Fetch all high/low water level observations from NOAA CO-OPS API
2. Filter to high tides only (type = 'H' or 'HH')
3. Count days where at least one high tide exceeds the threshold
4. Report as annual flood day count

```python
# Pseudocode
threshold = station.threshold  # NWS or 99th percentile
flood_dates = set()
for observation in high_low_data:
    if observation.type in ['H', 'HH']:
        if observation.value >= threshold:
            flood_dates.add(observation.date)
flood_days = len(flood_dates)
```

## Output Files

### Alaska-Specific Files

| File | Description |
|------|-------------|
| `output/historical/historical_htf_alaska.parquet` | Alaska HTF data with full metadata |
| `output/historical/historical_htf_alaska_full.parquet` | Backup with threshold details |
| `output/historical/alaska_htf_thresholds.csv` | Threshold report for all stations |

### Integrated Files

Alaska data is merged into the main HTF file:
- `output/historical/historical_htf_all_stations.parquet`

The assignment pipeline then processes all stations uniformly.

## Data Coverage

### Alaska Station Coverage

| Metric | Value |
|--------|-------|
| Total Stations | 16 |
| Stations with NWS thresholds | 5 |
| Stations with derived thresholds | 11 |
| Data Period | 2000-2024 (most stations) |
| Counties Covered | 25 |

### Regional Subgroups

| Subregion | Stations | Counties |
|-----------|----------|----------|
| Southeast Alaska | 5 | 5 |
| South Central Alaska | 5 | 8 |
| Western Alaska | 4 | 7 |
| Northern Alaska | 2 | 5 |

## Code Location

The Alaska HTF computation is implemented in:

```
src/noaa/historical/alaska_htf_computer.py
```

Key functions:
- `AlaskaHTFComputer.compute_percentile_threshold()` - Derive threshold from baseline data
- `AlaskaHTFComputer.compute_flood_days()` - Count flood days for a station/year
- `AlaskaHTFComputer.compute_all_stations()` - Process all Alaska stations

Sensitivity analysis:
```
src/noaa/historical/alaska_htf_sensitivity.py
```

## Limitations

1. **No moderate/major flood counts**: Only minor flooding threshold is computed
2. **Port Moller**: Shorter baseline period (2007-2012) and data series (2013-2024)
3. **Threshold comparability**: Derived 99th percentile may not be directly comparable to NWS thresholds
4. **Missing data**: Some station-years may have incomplete observations

## References

- Sweet, W.V., et al. (2018). "2017 State of U.S. High Tide Flooding with a 2018 Outlook." NOAA Technical Report NOS CO-OPS 083.
- NOAA CO-OPS API Documentation: https://api.tidesandcurrents.noaa.gov/api/prod/
- NOAA Metadata API: https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/
