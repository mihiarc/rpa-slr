# NOAA High Tide Flooding Data Products

This document describes the specific NOAA data products used in this project for high tide flooding analysis.

## Data Products Overview

We utilize two main derived data products from NOAA's Tides & Currents API:
1. Annual Flood Count (historical data)
2. Decadal Projections (future projections)

## 1. Annual Flood Count

Historical data showing the actual count of flooding events at different severity thresholds.

### API Parameters

| Parameter | Description | Format | Required |
|-----------|-------------|---------|----------|
| station   | Station identifier | 7-digit string | No (returns all if omitted) |
| year      | Calendar year | 4-digit integer (YYYY) | No (returns all if omitted) |
| range     | Year range | Integer | No (defaults to 0) |

### Response Data

The API returns the following data for each station-year combination:

- **Station Information**
  - Station ID
  - Station Name
- **Annual Counts**
  - Major flood threshold exceedance days
  - Moderate flood threshold exceedance days
  - Minor flood threshold exceedance days
  - Missing data days

### Example Queries

1. **Get 5 years of data starting from 2010 for a specific station**
   ```
   /htf/htf_annual.json?station=8638610&year=2010&range=5
   ```
   This returns flood count data for station 8638610 from 2010 to 2014 inclusive.

2. **Get single year data for a specific station**
   ```
   /htf/htf_annual.json?station=8638610&year=2010
   ```
   This returns flood count data for station 8638610 for just 2010.

3. **Get all available years for a specific station**
   ```
   /htf/htf_annual.json?station=8638610
   ```
   This returns all available flood count data for station 8638610.

### Sample Response

```json
{
  "count": 5,
  "AnnualFloodCount": [
    {
      "stnId": "8638610",
      "stnName": "Sewells Point, VA",
      "year": 2010,
      "majCount": 0,     // Major flood days
      "modCount": 1,     // Moderate flood days
      "minCount": 6,     // Minor flood days
      "nanCount": 0      // Missing data days
    },
    {
      "stnId": "8638610",
      "stnName": "Sewells Point, VA",
      "year": 2011,
      "majCount": 2,
      "modCount": 2,
      "minCount": 6,
      "nanCount": 0
    }
  ]
}
```

## 2. Decadal Projections

Future projections of flooding frequency under different scenarios.

### API Parameters

| Parameter | Description | Format | Required |
|-----------|-------------|---------|----------|
| station   | Station identifier | 7-digit string | No (returns all if omitted) |
| decade    | Target decade | 4-digit integer ending in 0 | No (returns all if omitted) |
| range     | Decade range | Integer | No (defaults to 0) |

### Response Data

The API returns the following data for each station-decade combination:

- **Station Information**
  - Station ID
  - Station Name
- **Projection Details**
  - Decade
  - Source/basis of projection
- **Scenarios**
  - Low
  - Intermediate-Low
  - Intermediate
  - Intermediate-High
  - High

### Example Queries

1. **Get projections for multiple decades starting from 2050**
   ```
   /htf/htf_projection_decadal.json?station=8638610&decade=2050&range=5
   ```
   This returns projections for station 8638610 for 2050s through 2090s.

2. **Get projections for a single decade**
   ```
   /htf/htf_projection_decadal.json?station=8638610&decade=2050
   ```
   This returns projections for station 8638610 for the 2050s only.

3. **Get all available decade projections for a station**
   ```
   /htf/htf_projection_decadal.json?station=8638610
   ```
   This returns all available decade projections for station 8638610.

### Sample Response

```json
{
  "count": 5,
  "DecadalProjection": [
    {
      "stnId": "8638610",
      "stnName": "Sewells Point, VA",
      "decade": 2050,
      "source": "https://tidesandcurrents.noaa.gov/publications/HTF_Notice_of_Methodology_Update_2023.pdf",
      "low": 85,          // Days per year under low scenario
      "intLow": 100,      // Days per year under intermediate-low scenario
      "intermediate": 125, // Days per year under intermediate scenario
      "intHigh": 150,     // Days per year under intermediate-high scenario
      "high": 185         // Days per year under high scenario
    },
    {
      "stnId": "8638610",
      "stnName": "Sewells Point, VA",
      "decade": 2060,
      "source": "https://tidesandcurrents.noaa.gov/publications/HTF_Notice_of_Methodology_Update_2023.pdf",
      "low": 135,
      "intLow": 170,
      "intermediate": 215,
      "intHigh": 270,
      "high": 310
    }
  ]
}
```

## Usage Notes

1. When station ID is omitted, the API returns data for all available stations
2. Historical data (Annual Flood Count) provides actual observed flooding events
3. Projection data provides estimates under different climate scenarios
4. All flood counts are reported as number of days per year/decade
5. The base URL for all queries is: `https://api.tidesandcurrents.noaa.gov/dpapi/prod/webapi`
6. All responses are in JSON format
7. Historical data counts are broken down by severity (major, moderate, minor)
8. Projection data provides counts for different climate scenarios (low to high)
9. Both endpoints include station metadata in each record 