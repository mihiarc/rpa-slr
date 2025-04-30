import ee
import pandas as pd
import os
from datetime import datetime

# Initialize Earth Engine
ee.Initialize()

# Set date range for 2020
startDate = ee.Date('2020-01-01')
endDate = ee.Date('2021-01-01')  # Up to but not including this date

# Get the temperature collection
tmpCollection = ee.ImageCollection('IDAHO_EPSCOR/GRIDMET') \
  .select('tmmx')  # Only select maximum temperature

# Get North Carolina counties (state FIPS code '37')
counties = ee.FeatureCollection('TIGER/2018/Counties') \
  .filter(ee.Filter.eq('STATEFP', '37'))

# Calculate daily max temperatures by county
days = endDate.difference(startDate, "days")
dailyMaxTempByCounty = ee.FeatureCollection(ee.List.sequence(0, days.subtract(1))
  .map(lambda dayOffset: 
    # For each day, create a feature collection
    ee.FeatureCollection(counties.map(lambda county: 
      # For each county, create a feature with max temp data
      ee.Feature(county.geometry())
        .set(tmpCollection
          .filterDate(
            startDate.advance(ee.Number(dayOffset), 'days'), 
            startDate.advance(ee.Number(dayOffset).add(1), 'days')
          )
          .mean()
          .reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=county.geometry(),
            scale=4000
          ))
        .set('date', startDate.advance(ee.Number(dayOffset), 'days').format('yyyy-MM-dd'))
        .set('county_name', county.get('NAME'))
        .set('county_fips', county.get('GEOID'))
    ))
  )).flatten()

# Export to a local CSV file
export_task = ee.batch.Export.table.toDrive(
    collection=dailyMaxTempByCounty,
    description='nc_daily_max_temp_2020',
    fileFormat='CSV',
    folder='earth_engine_exports'
)

# Start the export
export_task.start()
print("Export started. Check your Google Drive folder 'earth_engine_exports' for the results.")
print("The task may take some time to complete.")
