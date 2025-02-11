# Imputation Results Report
Generated on: 2025-02-06 14:15:36

## Summary Statistics

### Coverage Overview
- Total reference points: 1298312
- Reference points with assigned gauges: 1298312 (100.0%)
  - Points with 2 gauges: 1298312
  - Points with 1 gauge: 0
- Reference points without gauge assignments: 0

### County Coverage
- Total coastal counties tracked: 429
- Counties with gauge assignments: 429 (100.0%)
- Counties without gauge assignments: 0

## Regional Analysis

The following table shows coverage statistics by region:

Region | Counties | County Coverage | Avg Distance (km) | Max Distance (km)
-------|----------|----------------|-------------------|------------------
Northeast | 80 | 100.0% | 31.6 | 199.2
Southeast | 160 | 100.0% | 27.4 | 203.2
Gulf Coast | 48 | 100.0% | 21.6 | 103.9
Pacific | 69 | 100.0% | 2338.9 | 2586.6
Pacific Islands | 5 | 100.0% | 28.6 | 933.9
Caribbean | 50 | 100.0% | 1605.8 | 1647.4

### Regional Insights

#### Northeast
- 80 of 80 counties have gauge assignments (100.0%)
- Average distance to nearest gauge: 31.6 km
- Maximum distance to nearest gauge: 199.2 km

#### Southeast
- 160 of 160 counties have gauge assignments (100.0%)
- Average distance to nearest gauge: 27.4 km
- Maximum distance to nearest gauge: 203.2 km

#### Gulf Coast
- 48 of 48 counties have gauge assignments (100.0%)
- Average distance to nearest gauge: 21.6 km
- Maximum distance to nearest gauge: 103.9 km

#### Pacific
- 69 of 69 counties have gauge assignments (100.0%)
- Average distance to nearest gauge: 2338.9 km
- Maximum distance to nearest gauge: 2586.6 km

#### Pacific Islands
- 5 of 5 counties have gauge assignments (100.0%)
- Average distance to nearest gauge: 28.6 km
- Maximum distance to nearest gauge: 933.9 km

#### Caribbean
- 50 of 50 counties have gauge assignments (100.0%)
- Average distance to nearest gauge: 1605.8 km
- Maximum distance to nearest gauge: 1647.4 km


## State-Level Analysis


### Northeast

State | Counties | County Coverage | Avg Distance (km) | Max Distance (km)
------|----------|----------------|-------------------|------------------


### Southeast

State | Counties | County Coverage | Avg Distance (km) | Max Distance (km)
------|----------|----------------|-------------------|------------------


### Gulf Coast

State | Counties | County Coverage | Avg Distance (km) | Max Distance (km)
------|----------|----------------|-------------------|------------------


### Pacific

State | Counties | County Coverage | Avg Distance (km) | Max Distance (km)
------|----------|----------------|-------------------|------------------


### Pacific Islands

State | Counties | County Coverage | Avg Distance (km) | Max Distance (km)
------|----------|----------------|-------------------|------------------


### Caribbean

State | Counties | County Coverage | Avg Distance (km) | Max Distance (km)
------|----------|----------------|-------------------|------------------



## Methodology

### Reference Point Assignment
- Each coastal reference point is assigned up to 2 nearest tide gauges
- Weights are calculated using inverse distance squared (1/d²)
- Weights are normalized to sum to 1 for each point
- All reference points are preserved in the output, even those without gauge assignments

### Data Tracking
- Points/counties without gauge assignments are still tracked in the dataset
- This allows for:
  1. Understanding coverage gaps in the monitoring network
  2. Identifying areas that need additional gauge stations
  3. Future updates as new gauge stations come online

## Notes
- All coastal counties are included in the dataset, regardless of gauge coverage
- Counties without gauge assignments are preserved to:
  1. Maintain a complete inventory of coastal counties
  2. Enable future updates as gauge coverage expands
  3. Support analysis of monitoring network gaps
- This version does not restrict gauge assignments so that we can investigate gaps in gauge coverage. We will need to implement a regional based threshold for gauge coverage.

## Data Reliability Insights
- Coverage is generally better in regions with:
  1. Higher population density
  2. Major ports and maritime infrastructure
  3. Historical monitoring programs
- Areas with sparse coverage may need:
  1. Additional gauge installations
  2. Alternative monitoring approaches
  3. Careful interpretation of interpolated values