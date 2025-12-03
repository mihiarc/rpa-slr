# Shoreline Data and Coastal Reference Points

This document describes the shoreline data sources and coastal reference point generation process used in the imputation pipeline.

## Overview

The imputation system maps tide gauge stations to coastal counties using reference points generated along the shoreline. These reference points serve as the spatial backbone for the inverse distance weighting (IDW) interpolation that assigns flood data to counties.

## Data Sources

### 1. NOAA Medium Resolution Shoreline (NOS80K) - Continental US

- **Source**: USGS/NOAA
- **URL**: https://pubs.usgs.gov/of/2012/1187/data/basemaps/shoreline/nos80k/nos80k.zip
- **Scale**: 1:70,000
- **Coverage**: Continental United States (CONUS) only
- **Location**: `data/input/shapefile_shoreline_noaa/nos80k.shp`

**Important Limitation**: The NOS80K dataset explicitly excludes:
- Alaska
- Hawaiian Islands
- Puerto Rico
- US Virgin Islands
- Pacific Island territories (Guam, CNMI, American Samoa)

From the metadata:
> "Alaska, the Hawaiian Islands, Puerto Rico, and all other interests and territories of the United States are not included in the data set."

### 2. GSHHG Global Shoreline Database - Non-CONUS Regions

- **Source**: University of Hawaii / NOAA
- **URL**: https://www.soest.hawaii.edu/pwessel/gshhg/gshhg-shp-2.3.7.zip
- **Resolution**: High resolution (Level 1 - continental boundaries)
- **Coverage**: Global, including all US territories
- **Location**: `data/input/gshhg/GSHHS_shp/h/GSHHS_h_L1.shp`

The GSHHG (Global Self-consistent, Hierarchical, High-resolution Geography) database provides complete global coverage, filling the gaps left by NOS80K for:

| Region | GSHHG Coverage |
|--------|----------------|
| Alaska | 6,670+ shoreline polygons |
| Hawaii | 65+ shoreline polygons |
| Puerto Rico | 128+ shoreline polygons |
| US Virgin Islands | 84+ shoreline polygons |
| Guam | 2+ shoreline polygons |
| Northern Mariana Islands | 16+ shoreline polygons |
| American Samoa | 139+ shoreline polygons |

### 3. US Census TIGER/Line County Boundaries

- **Source**: US Census Bureau
- **URL**: https://www2.census.gov/geo/tiger/TIGER2024/COUNTY/tl_2024_us_county.zip
- **Year**: 2024
- **Coverage**: All US states and territories
- **Location**: `data/input/shapefile_county_census/tl_2024_us_county.shp`

## Reference Point Generation Process

### Step 1: Intersect Shoreline with County Boundaries

For each coastal county, we compute the intersection of the shoreline geometry with the county boundary. This yields the portion of shoreline that belongs to each county.

### Step 2: Generate Points Along Shoreline

Points are generated at regular intervals (default: 5km spacing) along the county's shoreline segment. The spacing ensures adequate coverage while keeping the dataset manageable.

### Step 3: Combine CONUS and Non-CONUS Points

The final reference points file combines:
- Points generated from NOS80K (CONUS)
- Points generated from GSHHG (Non-CONUS)

### Output

- **File**: `output/county_shoreline_ref_points/coastal_reference_points.parquet`
- **Format**: GeoParquet with WGS84 (EPSG:4326) coordinates

## Reference Points Summary

| Category | Counties | Reference Points |
|----------|----------|------------------|
| CONUS | 240 | 110,476 |
| Non-CONUS | 86 | 29,034 |
| **Total** | **326** | **139,510** |

### Points by State/Territory

| State/Territory | Counties | Points |
|-----------------|----------|--------|
| Florida | 34 | 31,704 |
| Alaska | 25 | 27,804 |
| Louisiana | 19 | 23,429 |
| Texas | 17 | 8,956 |
| North Carolina | 18 | 6,826 |
| South Carolina | 6 | 5,740 |
| Maine | 8 | 5,005 |
| Virginia | 24 | 4,602 |
| California | 19 | 4,360 |
| New Jersey | 13 | 3,794 |
| Maryland | 17 | 3,286 |
| Georgia | 7 | 2,475 |
| Oregon | 9 | 2,226 |
| Washington | 18 | 2,116 |
| Massachusetts | 8 | 1,689 |
| New York | 9 | 1,632 |
| Delaware | 3 | 939 |
| Mississippi | 3 | 595 |
| Alabama | 2 | 565 |
| Puerto Rico | 44 | 450 |
| Hawaii | 5 | 441 |
| Rhode Island | 5 | 387 |
| Virgin Islands | 3 | 162 |
| District of Columbia | 1 | 150 |
| Northern Mariana Islands | 4 | 81 |
| American Samoa | 4 | 54 |
| Guam | 1 | 42 |

## Regenerating Reference Points

To regenerate the coastal reference points:

```python
import geopandas as gpd
from shapely.geometry import LineString
from shapely.ops import unary_union
from pathlib import Path

# Load shapefiles
counties = gpd.read_file("data/input/shapefile_county_census/tl_2024_us_county.shp")
nos80k = gpd.read_file("data/input/shapefile_shoreline_noaa/nos80k.shp")
gshhg = gpd.read_file("data/input/gshhg/GSHHS_shp/h/GSHHS_h_L1.shp")

# Convert polygon boundaries to linestrings
def polygon_to_lines(gdf):
    lines = []
    for geom in gdf.geometry:
        if geom.geom_type == 'Polygon':
            lines.append(LineString(geom.exterior.coords))
        elif geom.geom_type == 'MultiPolygon':
            for poly in geom.geoms:
                lines.append(LineString(poly.exterior.coords))
    return unary_union(lines)

# Process CONUS with NOS80K, Non-CONUS with GSHHG
# ... (see inline scripts in session history)
```

## Data Quality Notes

### NOS80K vs GSHHG Comparison

| Metric | NOS80K | GSHHG |
|--------|--------|-------|
| CONUS shoreline length | 216,353 km | 178,869 km |
| Detail level (vertices) | ~21x higher | Lower |
| Source | NOAA official | University of Hawaii |
| Scale | 1:70,000 | Variable |

**Why use both datasets?**

NOS80K provides significantly higher resolution for CONUS (21x more vertices in sample areas like Tampa Bay), capturing complex estuaries, bays, and coastal inlets that GSHHG smooths over. However, NOS80K only covers continental US.

**Could we use GSHHG for everything?**

Yes, with minimal practical impact. Since reference points are generated at 5km spacing, the extra detail in NOS80K has limited effect on the final output. A GSHHG-only approach would:
- Simplify the data pipeline to a single source
- Provide consistent methodology worldwide
- Lose some detail in complex coastal areas (estuaries, barrier islands)

The current hybrid approach prioritizes accuracy for CONUS while enabling coverage of all US territories.

### NOS80K Limitations
- Some coastal features may be simplified at 1:70,000 scale
- Does not include inland waterways
- Last updated varies by region

### GSHHG Considerations
- Global dataset may have varying accuracy by region
- High-resolution version used for best accuracy
- Polygon-based (requires conversion to linestrings)

### Distance Warnings
During imputation, some counties may be far from tide stations:
- Counties >100km from nearest station receive a warning
- Algorithm falls back to nearest station with reduced weight
- Common for remote Alaska boroughs and Pacific islands

## Related Documentation

- [NOAA Data Products](noaa_data_products.md) - HTF data sources
- [Cache System](cache_system.md) - Data caching approach
- [County List](county_list.md) - Full list of coastal counties
