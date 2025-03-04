"""
Generate analysis reports and visualizations for HTF data quality.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import json
import yaml
from datetime import datetime
from typing import Dict, List, Tuple
import numpy as np
from scipy import stats
from statsmodels.nonparametric.smoothers_lowess import lowess
import argparse

from .data_quality import DataQualityAnalyzer

def load_station_names(region: str) -> Dict[str, str]:
    """Load station names from configuration file."""
    config_file = Path('config/tide_stations') / f'{region}_tide_stations.yaml'
    with open(config_file) as f:
        config = yaml.safe_load(f)
    return {
        station_id: station_data['name']
        for station_id, station_data in config['stations'].items()
    }

def load_station_metadata(region: str) -> Dict[str, Dict]:
    """Load full station metadata from configuration file."""
    config_file = Path('config/tide_stations') / f'{region}_tide_stations.yaml'
    with open(config_file) as f:
        config = yaml.safe_load(f)
    return config

def generate_flood_days_heatmap(data: pd.DataFrame, station_names: Dict[str, str], region: str, output_dir: Path) -> None:
    """Generate a heatmap of flood days by station and year."""
    # Create pivot tables
    pivot_data = data.pivot(
        index='station_id',
        columns='year',
        values='flood_days'
    ).fillna(0)
    
    missing_mask = data.pivot(
        index='station_id',
        columns='year',
        values='missing_days'
    ).fillna(0) > 180  # Consider data missing if more than half year is missing
    
    # Replace station IDs with names
    pivot_data.index = [f"{station_names.get(str(idx), idx)} ({idx})" for idx in pivot_data.index]
    missing_mask.index = pivot_data.index
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(20, 10))
    
    # Plot flood days
    sns.heatmap(
        pivot_data,
        cmap='YlOrRd',
        mask=missing_mask,
        cbar_kws={'label': 'Flood Days'},
        xticklabels=2,  # Show every other year
        ax=ax
    )
    
    # Add hatching for missing data
    for i in range(len(pivot_data.index)):
        for j in range(len(pivot_data.columns)):
            if missing_mask.iloc[i, j]:
                ax.add_patch(plt.Rectangle((j, i), 1, 1, fill=True, facecolor='gray', alpha=0.3, hatch='///'))
    
    plt.title(f'Flood Days by Station and Year - {region}\n(Gray hatched areas indicate missing data)')
    plt.xlabel('Year')
    plt.ylabel('Station')
    
    # Save plot
    plt.tight_layout()
    plt.savefig(output_dir / f'{region.lower()}_flood_days_heatmap.png', dpi=300, bbox_inches='tight')
    plt.close()

def generate_completeness_plot(analysis_results: Dict, station_names: Dict[str, str], region: str, output_dir: Path) -> None:
    """Generate a bar plot of data completeness by station."""
    # Extract completeness data
    stations = []
    completeness = []
    for station_id, analysis in analysis_results['station_analyses'].items():
        stations.append(f"{station_names.get(station_id, station_id)} ({station_id})")
        completeness.append(analysis['completeness'] * 100)
    
    # Sort by completeness
    sorted_indices = np.argsort(completeness)
    stations = [stations[i] for i in sorted_indices]
    completeness = [completeness[i] for i in sorted_indices]
    
    # Create bar plot
    plt.figure(figsize=(15, 8))
    bars = plt.bar(stations, completeness)
    plt.title(f'Data Completeness by Station - {region}')
    plt.xlabel('Station')
    plt.ylabel('Completeness (%)')
    plt.xticks(rotation=45, ha='right')
    
    # Add reference line at mean completeness
    mean_completeness = analysis_results['regional_summary']['mean_completeness'] * 100
    plt.axhline(y=mean_completeness, color='r', linestyle='--', label=f'Mean: {mean_completeness:.1f}%')
    plt.legend()
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}%',
                ha='center', va='bottom')
    
    # Save plot
    plt.tight_layout()
    plt.savefig(output_dir / f'{region.lower()}_completeness.png', dpi=300, bbox_inches='tight')
    plt.close()

def generate_flood_days_timeseries(data: pd.DataFrame, station_names: Dict[str, str], metadata: Dict, region: str, output_dir: Path) -> None:
    """Generate a spaghetti plot of flood days since 2005."""
    # Filter data from 2005 onwards
    recent_data = data[data['year'] >= 2005].copy()
    
    # Create figure
    plt.figure(figsize=(15, 8))
    
    # Plot each station's time series
    for station_id in recent_data['station_id'].unique():
        station_data = recent_data[recent_data['station_id'] == station_id]
        if not station_data.empty:
            # Convert station_id to string and ensure it's in metadata
            station_id_str = str(int(station_id))  # Handle both string and integer IDs
            if station_id_str in metadata['stations']:
                # Get sub-region from metadata
                sub_region = metadata['stations'][station_id_str].get('region', 'Unknown')
                station_name = f"{station_names.get(station_id_str, station_id)} ({sub_region})"
                
                # Get years and flood days
                years = station_data['year'].values
                floods = station_data['flood_days'].values
                
                # Compute smoothed line using LOWESS
                if len(years) > 3:  # Only smooth if we have enough points
                    # Create finer grid for smoother line
                    grid_years = np.linspace(years.min(), years.max(), 100)
                    # Use LOWESS smoothing
                    smoothed = lowess(floods, years, frac=0.5, it=1, return_sorted=False)
                    
                    # Plot smoothed line with higher transparency
                    plt.plot(years, smoothed, '-', 
                            label=station_name,
                            alpha=0.4,
                            linewidth=2)
                    
                    # Plot actual points with lower transparency
                    plt.scatter(years, floods, 
                              alpha=0.6,
                              s=20)
    
    plt.title(f'Minor Flood Days by Station (2005-Present) - {region}')
    plt.xlabel('Year')
    plt.ylabel('Flood Days')
    plt.grid(True, alpha=0.3)
    
    # Adjust legend
    plt.legend(bbox_to_anchor=(1.05, 1), 
              loc='upper left',
              fontsize=8,
              title='Stations')
    
    # Save plot
    plt.tight_layout()
    plt.savefig(output_dir / f'{region.lower()}_flood_days_timeseries.png', dpi=300, bbox_inches='tight')
    plt.close()

def generate_markdown_report(data: pd.DataFrame, analysis_results: Dict, station_names: Dict[str, str], region: str, output_dir: Path) -> None:
    """Generate a markdown report summarizing the analysis results."""
    report_path = output_dir / f'{region.lower()}_analysis_report.md'
    
    # Load full station metadata
    metadata = load_station_metadata(region)
    
    with open(report_path, 'w') as f:
        # Header
        f.write(f'# High Tide Flooding Data Quality Analysis - {region}\n\n')
        f.write(f'Analysis generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n')
        
        # Data Overview
        f.write('## Overview\n\n')
        f.write(f'Analysis of high tide flooding data from {data["year"].min()} to {data["year"].max()}.\n\n')
        f.write('### Key Statistics\n\n')
        f.write(f'- Total records analyzed: {len(data)}\n')
        f.write(f'- Average flood days per year (excluding missing data): {data[data["missing_days"] == 0]["flood_days"].mean():.2f}\n')
        f.write(f'- Overall data completeness: {100 - (len(data[data["missing_days"] > 0]) / len(data) * 100):.1f}%\n\n')
        
        # Station Information
        f.write('### Monitoring Stations\n\n')
        f.write('| Station ID | Name | Location | Sub-Region | Data Completeness |\n')
        f.write('|------------|------|----------|------------|-------------------|\n')
        for station_id, station_data in metadata['stations'].items():
            station_analysis = analysis_results['station_analyses'].get(station_id, {})
            completeness = station_analysis.get('completeness', 0) * 100
            sub_region = station_data.get('region', 'Unknown')
            f.write(f"| {station_id} | {station_data['name']} | "
                   f"{station_data['location']['lat']:.2f}°N, {abs(station_data['location']['lon']):.2f}°W | "
                   f"{sub_region} | {completeness:.1f}% |\n")
        f.write('\n')
        
        # Data Quality Visualization
        f.write('## Data Quality Analysis\n\n')
        
        # Completeness Plot
        f.write('### Data Completeness by Station\n\n')
        f.write(f'![Data Completeness]({region.lower()}_completeness.png)\n\n')
        f.write('This visualization shows the percentage of days with valid data for each station:\n')
        f.write('- Stations are ordered by completeness percentage\n')
        f.write('- The red line indicates the regional mean completeness\n')
        f.write(f'- Regional mean completeness: {analysis_results["regional_summary"]["mean_completeness"]*100:.1f}%\n\n')
        
        # Flood Days Heatmap
        f.write('### Flood Days Distribution\n\n')
        f.write(f'![Flood Days Heatmap]({region.lower()}_flood_days_heatmap.png)\n\n')
        f.write('This heatmap shows the distribution of flood days across stations and years:\n')
        f.write('- Color intensity indicates number of flood days\n')
        f.write('- Gray hatched areas indicate missing data (>180 days missing in that year)\n')
        f.write('- White indicates zero flood days with complete data\n\n')
        
        # Recent Flood Days Time Series
        f.write('### Recent Flooding Trends (2005-Present)\n\n')
        f.write(f'![Flood Days Time Series]({region.lower()}_flood_days_timeseries.png)\n\n')
        f.write('This plot shows the trend in minor flood days for each station since 2005:\n')
        f.write('- Each line represents a different monitoring station\n')
        f.write('- Points indicate actual measurements\n')
        f.write('- Gaps in lines indicate missing data\n\n')
        
        # Key Findings
        f.write('## Key Findings\n\n')
        
        # Most Complete Stations
        complete_stations = sorted(
            [(station_id, analysis['completeness']) 
             for station_id, analysis in analysis_results['station_analyses'].items()],
            key=lambda x: x[1], reverse=True
        )[:3]
        f.write('### Most Complete Records\n\n')
        for station_id, completeness in complete_stations:
            station_name = station_names.get(station_id, station_id)
            sub_region = metadata['stations'][station_id].get('region', 'Unknown')
            f.write(f'- {station_name} ({sub_region}, Station {station_id}): {completeness*100:.1f}% complete\n')
        f.write('\n')
        
        # Stations with Most Flooding
        station_floods = []
        for station_id, analysis in analysis_results['station_analyses'].items():
            station_data = data[(data['station_id'] == station_id) & (data['missing_days'] == 0)]
            if not station_data.empty:
                mean_floods = station_data['flood_days'].mean()
                station_floods.append((station_id, mean_floods))
        
        top_flood_stations = sorted(station_floods, key=lambda x: x[1], reverse=True)[:3]
        f.write('### Highest Flooding Activity\n\n')
        for station_id, mean_floods in top_flood_stations:
            station_name = station_names.get(station_id, station_id)
            sub_region = metadata['stations'][station_id].get('region', 'Unknown')
            f.write(f'- {station_name} ({sub_region}, Station {station_id}): {mean_floods:.2f} flood days per year\n')
        f.write('\n')

def generate_analysis_outputs(region: str = 'gulf_coast') -> None:
    """Generate all analysis outputs for a region."""
    # Set up paths
    output_dir = Path('output/analysis')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load data
    data = pd.read_parquet(f'output/historical/historical_htf_{region}.parquet')
    
    # Load station names
    station_names = load_station_names(region)
    
    # Run analysis
    analyzer = DataQualityAnalyzer()
    analysis_results = analyzer.analyze_regional_data(data=data, region=region)
    
    # Generate visualizations
    generate_flood_days_heatmap(data, station_names, region, output_dir)
    generate_completeness_plot(analysis_results, station_names, region, output_dir)
    generate_flood_days_timeseries(data, station_names, load_station_metadata(region), region, output_dir)
    
    # Generate report
    generate_markdown_report(data, analysis_results, station_names, region, output_dir)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate analysis report for a region')
    parser.add_argument('region', type=str, help='Region to analyze (e.g., south_atlantic, gulf_coast)')
    args = parser.parse_args()
    
    generate_analysis_outputs(region=args.region) 