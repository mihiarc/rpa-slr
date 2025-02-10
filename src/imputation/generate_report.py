"""
Script to generate imputation report from existing imputation structure.
Follows Single Responsibility Principle by handling only report generation.
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path
from datetime import datetime
from jinja2 import Template
import logging
from collections import defaultdict
import yaml

from src.config import OUTPUT_DIR, CONFIG_DIR

logger = logging.getLogger(__name__)

REPORT_TEMPLATE = """# Imputation Results Report
Generated on: {{ timestamp }}

## Summary Statistics

### Coverage Overview
- Total reference points: {{ total_points }}
- Reference points with assigned gauges: {{ points_with_gauges }} ({{ "%.1f"|format(100 * points_with_gauges / total_points) }}%)
  - Points with 2 gauges: {{ points_with_two }}
  - Points with 1 gauge: {{ points_with_one }}
- Reference points without gauge assignments: {{ points_without }}

### County Coverage
- Total coastal counties tracked: {{ total_counties }}
- Counties with gauge assignments: {{ counties_with_gauges }} ({{ "%.1f"|format(100 * counties_with_gauges / total_counties) }}%)
- Counties without gauge assignments: {{ counties_without }}

## Regional Analysis

The following table shows coverage statistics by region:

Region | Counties | County Coverage | Avg Distance (km) | Max Distance (km)
-------|----------|----------------|-------------------|------------------
{%- for region, stats in regional_stats.items() %}
{{ region }} | {{ stats['total_counties'] }} | {{ stats['county_coverage'] }}% | {{ "%.1f"|format(stats['avg_distance_km']) }} | {{ "%.1f"|format(stats['max_distance_km']) }}
{%- endfor %}

### Regional Insights
{% for region, stats in regional_stats.items() %}
#### {{ region }}
- {{ stats['counties_with_gauges'] }} of {{ stats['total_counties'] }} counties have gauge assignments ({{ stats['county_coverage'] }}%)
- Average distance to nearest gauge: {{ "%.1f"|format(stats['avg_distance_km']) }} km
- Maximum distance to nearest gauge: {{ "%.1f"|format(stats['max_distance_km']) }} km
{% endfor %}

## State-Level Analysis

{% for region, state_codes in regions.items() %}
### {{ region }}

State | Counties | County Coverage | Avg Distance (km) | Max Distance (km)
------|----------|----------------|-------------------|------------------
{%- for state_code, stats in state_stats.items() %}
{%- if state_code in state_codes %}
{{ stats['name'] }} | {{ stats['total_counties'] }} | {{ stats['county_coverage'] }}% | {{ "%.1f"|format(stats['avg_distance_km']) }} | {{ "%.1f"|format(stats['max_distance_km']) }}
{%- endif %}
{%- endfor %}

{% endfor %}

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
"""

class ReportGenerator:
    """Handles generation of imputation analysis report."""
    
    def __init__(self, 
                 output_dir: Path = OUTPUT_DIR / "imputation",
                 region_config: Path = CONFIG_DIR / "region_mappings.yaml"):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load FIPS mappings and region definitions
        with open(region_config) as f:
            config = yaml.safe_load(f)
            self.regions = config['regions']
            self.state_names = config['state_names']
    
    def analyze_regional_coverage(self, df: pd.DataFrame) -> dict:
        """Analyze coverage statistics by region."""
        regional_stats = {}
        
        for region, info in self.regions.items():
            # Filter points for this region
            region_points = df[df['state_fips'].isin(info['state_codes'])]
            
            if len(region_points) > 0:
                stats = {
                    'total_points': len(region_points),
                    'points_with_gauges': len(region_points[region_points['total_weight'] > 0]),
                    'total_counties': region_points['county_fips'].nunique(),
                    'counties_with_gauges': region_points[region_points['total_weight'] > 0]['county_fips'].nunique(),
                    'avg_distance_km': region_points['distance_1'].mean() / 1000,
                    'max_distance_km': region_points['distance_1'].max() / 1000
                }
                
                # Calculate percentages
                stats['point_coverage'] = round(100 * stats['points_with_gauges'] / stats['total_points'], 1)
                stats['county_coverage'] = round(100 * stats['counties_with_gauges'] / stats['total_counties'], 1)
                
                regional_stats[region] = stats
        
        return regional_stats
    
    def analyze_state_coverage(self, df: pd.DataFrame) -> dict:
        """Analyze coverage statistics by state."""
        state_stats = {}
        
        # Get unique states
        states = df['state_fips'].unique()
        
        for state_code in sorted(states, key=lambda x: self.state_names.get(x, x)):
            # Filter points for this state
            state_points = df[df['state_fips'] == state_code]
            
            if len(state_points) > 0:
                stats = {
                    'name': self.state_names.get(state_code, state_code),
                    'total_points': len(state_points),
                    'points_with_gauges': len(state_points[state_points['total_weight'] > 0]),
                    'total_counties': state_points['county_fips'].nunique(),
                    'counties_with_gauges': state_points[state_points['total_weight'] > 0]['county_fips'].nunique(),
                    'avg_distance_km': state_points['distance_1'].mean() / 1000,
                    'max_distance_km': state_points['distance_1'].max() / 1000
                }
                
                # Calculate percentages
                stats['point_coverage'] = round(100 * stats['points_with_gauges'] / stats['total_points'], 1)
                stats['county_coverage'] = round(100 * stats['counties_with_gauges'] / stats['total_counties'], 1)
                
                state_stats[state_code] = stats
        
        return state_stats
    
    def generate_report(self, df: pd.DataFrame) -> Path:
        """Generate markdown report from imputation structure."""
        logger.info("Analyzing regional coverage...")
        regional_stats = self.analyze_regional_coverage(df)
        
        logger.info("Analyzing state coverage...")
        state_stats = self.analyze_state_coverage(df)
        
        # Load template
        template = Template(REPORT_TEMPLATE)
        
        # Generate report
        markdown = template.render(
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            total_points=len(df),
            points_with_gauges=len(df[df['total_weight'] > 0]),
            points_with_two=len(df[df['gauge_id_2'].notna()]),
            points_with_one=len(df[df['gauge_id_2'].isna() & df['gauge_id_1'].notna()]),
            points_without=len(df[df['total_weight'] == 0]),
            total_counties=df['county_fips'].nunique(),
            counties_with_gauges=df[df['total_weight'] > 0]['county_fips'].nunique(),
            counties_without=df[df['total_weight'] == 0]['county_fips'].nunique(),
            regional_stats=regional_stats,
            state_stats=state_stats,
            regions=self.regions
        )
        
        # Write report
        report_file = self.output_dir / "imputation_report.md"
        report_file.write_text(markdown)
        
        return report_file

def main():
    """Generate report from existing imputation structure."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize report generator
    generator = ReportGenerator()
    
    # Load existing imputation structure
    imputation_file = OUTPUT_DIR / "imputation" / "imputation_structure.parquet"
    if not imputation_file.exists():
        raise FileNotFoundError(f"Imputation structure not found at {imputation_file}")
    
    logger.info("Loading imputation structure...")
    df = gpd.read_parquet(imputation_file)
    
    logger.info("Generating report...")
    report_file = generator.generate_report(df)
    
    logger.info(f"Report generated at: {report_file}")
    return report_file

if __name__ == "__main__":
    main() 