import xclim as xc
import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime


def convert_ee_to_xarray(ee_array, variable_name, lats, lons, times):
    """
    Convert Earth Engine array data to xarray Dataset.
    
    Args:
        ee_array: Earth Engine array data
        variable_name (str): Name of the climate variable
        lats (array): Latitude values
        lons (array): Longitude values
        times (array): Time values
    
    Returns:
        xr.Dataset: xarray Dataset with the climate data
    """
    # Create xarray DataArray from the Earth Engine data
    data_array = xr.DataArray(
        data=ee_array,
        dims=["time", "lat", "lon"],
        coords={
            "time": times,
            "lat": lats,
            "lon": lons,
        },
        name=variable_name
    )
    
    # Convert to dataset
    ds = data_array.to_dataset()
    
    # Set attributes based on variable
    if variable_name == 'tas':
        ds[variable_name].attrs = {
            'standard_name': 'air_temperature',
            'long_name': 'Near-Surface Air Temperature',
            'units': 'K'
        }
    elif variable_name == 'pr':
        ds[variable_name].attrs = {
            'standard_name': 'precipitation_flux',
            'long_name': 'Precipitation',
            'units': 'kg m-2 s-1'
        }
    
    return ds


def calculate_temperature_indicators(ds, output_file=None):
    """
    Calculate temperature-related climate indicators using xclim.
    
    Args:
        ds (xr.Dataset): Dataset containing temperature data
        output_file (str, optional): File path to save results
    
    Returns:
        dict: Dictionary of calculated indicators
    """
    # Convert from K to °C if needed
    if 'tas' in ds and ds.tas.attrs.get('units') == 'K':
        ds['tas'] = ds.tas - 273.15
        ds.tas.attrs['units'] = '°C'
    
    # Define indicators to calculate
    indicators = {}
    
    # Compute annual mean temperature
    indicators['annual_mean_temp'] = xc.indices.tg_mean(ds.tas)
    
    # Compute number of hot days (days with temp > 30°C)
    indicators['hot_days'] = xc.indices.tx_days_above(ds.tas, thresh='30 °C')
    
    # Compute number of frost days (min temp < 0°C)
    indicators['frost_days'] = xc.indices.tn_days_below(ds.tas, thresh='0 °C')
    
    # Compute growing degree days
    indicators['growing_degree_days'] = xc.indices.growing_degree_days(ds.tas)
    
    # Save results if output_file is specified
    if output_file:
        # Convert to DataFrame for easier saving
        result_dict = {}
        for name, indicator in indicators.items():
            result_dict[name] = indicator.values
        
        df = pd.DataFrame(result_dict)
        df.to_csv(output_file, index=False)
        print(f"Saved temperature indicators to {output_file}")
    
    return indicators


def calculate_precipitation_indicators(ds, output_file=None):
    """
    Calculate precipitation-related climate indicators using xclim.
    
    Args:
        ds (xr.Dataset): Dataset containing precipitation data
        output_file (str, optional): File path to save results
    
    Returns:
        dict: Dictionary of calculated indicators
    """
    # Convert units if needed (from kg m-2 s-1 to mm/day)
    if 'pr' in ds and ds.pr.attrs.get('units') == 'kg m-2 s-1':
        ds['pr'] = ds.pr * 86400  # Convert to mm/day (assuming daily data)
        ds.pr.attrs['units'] = 'mm/day'
    
    # Define indicators to calculate
    indicators = {}
    
    # Compute total annual precipitation
    indicators['annual_total_precip'] = xc.indices.prcptot(ds.pr)
    
    # Compute number of heavy precipitation days (> 10mm)
    indicators['heavy_precip_days'] = xc.indices.days_over_precip_thresh(ds.pr, thresh='10 mm/day')
    
    # Compute maximum consecutive dry days
    indicators['max_consec_dry_days'] = xc.indices.maximum_consecutive_dry_days(ds.pr, thresh='1 mm/day')
    
    # Compute simple daily intensity index
    indicators['precip_intensity'] = xc.indices.daily_pr_intensity(ds.pr)
    
    # Save results if output_file is specified
    if output_file:
        # Convert to DataFrame for easier saving
        result_dict = {}
        for name, indicator in indicators.items():
            result_dict[name] = indicator.values
        
        df = pd.DataFrame(result_dict)
        df.to_csv(output_file, index=False)
        print(f"Saved precipitation indicators to {output_file}")
    
    return indicators


def aggregate_by_county(indicators, county_geometries, output_file=None):
    """
    Aggregate climate indicators by county.
    
    Args:
        indicators (dict): Dictionary of calculated indicators
        county_geometries (dict): Dictionary of county geometries
        output_file (str, optional): File path to save results
    
    Returns:
        pd.DataFrame: DataFrame with county-level climate indicators
    """
    # Create empty results list
    results = []
    
    # Loop through each county
    for county_id, geometry in county_geometries.items():
        county_data = {'county_id': county_id}
        
        # Find all grid cells in this county and compute statistics
        for indicator_name, indicator_data in indicators.items():
            # This is a simplified example - in practice, you would use proper
            # spatial intersection methods to identify grid cells within each county
            # and then aggregate (mean, max, etc.) the values for those cells
            
            # Here, we're assuming we can mask the indicator data by county geometry
            county_mask = indicator_data.rio.clip([geometry])
            county_mean = county_mask.mean().values
            
            county_data[indicator_name] = county_mean
        
        results.append(county_data)
    
    # Convert to DataFrame
    df = pd.DataFrame(results)
    
    # Save to file if requested
    if output_file:
        df.to_csv(output_file, index=False)
        print(f"Saved county-level indicators to {output_file}")
    
    return df 