"""
Main module for water level imputation at coastal reference points.
Orchestrates the process of:
1. Loading preprocessed spatial relationships
2. Calculating temporal weights and adjustments
3. Preparing data structures for the next phase

The imputation output is designed to support:
- Historic water level reconstruction
- Future water level projection
- Temporal interpolation between gauge readings
- Handling of missing or incomplete gauge data
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Union
import logging
from datetime import datetime
import json
import matplotlib.pyplot as plt
import seaborn as sns
from jinja2 import Template

from src.config import (
    PROCESSED_DATA_DIR,
    OUTPUT_DIR,
    REFERENCE_POINTS_FILE,
    TIDE_STATIONS_LIST,
    CLOSE_THRESHOLD,
    INITIAL_SEARCH_DISTANCE,
    MAX_SEARCH_DISTANCE,
    DISTANCE_INCREMENT,
    MAX_GAUGES_PER_POINT,
    MIN_WEIGHT_THRESHOLD
)

from .data_loader import DataLoader
from .spatial_ops import NearestGaugeFinder
from .weight_calculator import WeightCalculator

logger = logging.getLogger(__name__)

class ImputationManager:
    """Manages the imputation of water levels at reference points."""
    
    def __init__(self,
                 reference_points_file: Path = REFERENCE_POINTS_FILE,
                 gauge_stations_file: Path = TIDE_STATIONS_LIST,
                 output_dir: Path = OUTPUT_DIR / "imputation",
                 k_nearest: int = MAX_GAUGES_PER_POINT,
                 weight_method: str = 'hybrid'):
        """
        Initialize imputation manager.
        
        Args:
            reference_points_file: Path to reference points file
            gauge_stations_file: Path to gauge stations file
            output_dir: Directory for output files
            k_nearest: Number of nearest gauges to use
            weight_method: Method for calculating weights
        """
        self.reference_points_file = reference_points_file
        self.gauge_stations_file = gauge_stations_file
        self.output_dir = output_dir
        self.k_nearest = k_nearest
        self.weight_method = weight_method
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.data_loader = DataLoader(
            gauge_file=gauge_stations_file,
            points_file=reference_points_file
        )
        self.gauge_finder = NearestGaugeFinder()
        self.weight_calculator = WeightCalculator(
            method=weight_method,
            close_threshold=CLOSE_THRESHOLD,
            max_distance=MAX_SEARCH_DISTANCE
        )
        
        # Setup logging
        self._setup_logging()
    
    def _setup_logging(self):
        """Configure logging for imputation process."""
        log_dir = self.output_dir / "logs"
        log_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"imputation_{timestamp}.log"
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    def _generate_distance_histogram(self, df: pd.DataFrame) -> Path:
        """Generate histogram of distances to nearest gauges."""
        plt.figure(figsize=(10, 6))
        
        for i in range(1, self.k_nearest + 1):
            distances = df[df[f'distance_{i}'].notna()][f'distance_{i}'] / 1000  # Convert to km
            plt.hist(distances, bins=50, alpha=0.5, 
                    label=f'Gauge {i}', range=(0, MAX_SEARCH_DISTANCE/1000))
        
        plt.xlabel('Distance (km)')
        plt.ylabel('Number of Reference Points')
        plt.title('Distribution of Distances to Nearest Gauges')
        plt.legend()
        
        plot_file = self.output_dir / "distance_distribution.png"
        plt.savefig(plot_file)
        plt.close()
        
        return plot_file
    
    def _generate_weight_distribution(self, df: pd.DataFrame) -> Path:
        """Generate plot of weight distributions."""
        plt.figure(figsize=(10, 6))
        
        for i in range(1, self.k_nearest + 1):
            weights = df[df[f'weight_{i}'].notna()][f'weight_{i}']
            plt.hist(weights, bins=50, alpha=0.5, 
                    label=f'Gauge {i}', range=(0, 1))
        
        plt.xlabel('Weight')
        plt.ylabel('Number of Reference Points')
        plt.title('Distribution of Gauge Weights')
        plt.legend()
        
        plot_file = self.output_dir / "weight_distribution.png"
        plt.savefig(plot_file)
        plt.close()
        
        return plot_file
    
    def _generate_report(self, df: pd.DataFrame, 
                        distance_plot: Path, weight_plot: Path) -> Path:
        """Generate markdown report summarizing imputation results."""
        template_str = """# Imputation Results Report
Generated on: {{ timestamp }}

## Summary Statistics

### Coverage
- Total reference points: {{ total_points }}
{% for i in range(1, k_nearest + 1) %}
- Points with {{ i }} gauge(s): {{ gauge_counts[i] }} ({{ (gauge_counts[i]/total_points*100)|round(1) }}%)
{% endfor %}

### Distance Statistics (km)
| Gauge | Mean | Median | Min | Max |
|-------|------|--------|-----|-----|
{% for i in range(1, k_nearest + 1) %}
| {{ i }} | {{ distance_stats[i]['mean']|round(1) }} | {{ distance_stats[i]['median']|round(1) }} | {{ distance_stats[i]['min']|round(1) }} | {{ distance_stats[i]['max']|round(1) }} |
{% endfor %}

## Visualizations

### Distance Distribution
![Distance Distribution]({{ distance_plot.name }})

### Weight Distribution
![Weight Distribution]({{ weight_plot.name }})

## Configuration
- Weight method: {{ weight_method }}
- Maximum gauges per point: {{ k_nearest }}
- Close threshold: {{ close_threshold/1000 }} km
- Maximum search distance: {{ max_distance/1000 }} km
- Minimum weight threshold: {{ min_weight_threshold }}
"""
        
        # Calculate statistics
        gauge_counts = {
            i: df[df[f'gauge_id_{i}'].notna()].shape[0]
            for i in range(1, self.k_nearest + 1)
        }
        
        distance_stats = {}
        for i in range(1, self.k_nearest + 1):
            distances = df[df[f'distance_{i}'].notna()][f'distance_{i}'] / 1000
            distance_stats[i] = {
                'mean': distances.mean(),
                'median': distances.median(),
                'min': distances.min(),
                'max': distances.max()
            }
        
        # Render template
        template = Template(template_str)
        markdown = template.render(
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            total_points=len(df),
            k_nearest=self.k_nearest,
            gauge_counts=gauge_counts,
            distance_stats=distance_stats,
            distance_plot=distance_plot,
            weight_plot=weight_plot,
            weight_method=self.weight_method,
            close_threshold=CLOSE_THRESHOLD,
            max_distance=MAX_SEARCH_DISTANCE,
            min_weight_threshold=MIN_WEIGHT_THRESHOLD
        )
        
        # Save report
        report_file = self.output_dir / "imputation_report.md"
        with open(report_file, 'w') as f:
            f.write(markdown)
        
        return report_file
    
    def prepare_imputation_structure(self) -> pd.DataFrame:
        """
        Prepare data structure for imputation.
        
        Returns:
            DataFrame with reference points and their gauge associations
        """
        logger.info("Loading input data...")
        gauge_stations, reference_points = self.data_loader.load_all()
        
        logger.info("Finding nearest gauges...")
        point_data = self.gauge_finder.find_nearest(
            reference_points,
            gauge_stations,
            k=self.k_nearest,
            initial_max_distance=INITIAL_SEARCH_DISTANCE,
            max_distance_limit=MAX_SEARCH_DISTANCE,
            distance_increment=DISTANCE_INCREMENT
        )
        
        logger.info("Calculating weights...")
        weighted_points = self.weight_calculator.calculate_for_points(point_data)
        
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(weighted_points)
        
        # Add metadata columns
        df['n_gauges'] = df.apply(
            lambda row: sum(1 for i in range(1, self.k_nearest + 1)
                          if row.get(f'gauge_id_{i}') is not None),
            axis=1
        )
        
        df['total_weight'] = df.apply(
            lambda row: sum(row.get(f'weight_{i}', 0)
                          for i in range(1, self.k_nearest + 1)),
            axis=1
        )
        
        return df
    
    def save_imputation_structure(self,
                                df: pd.DataFrame,
                                filename: str = "imputation_structure.parquet") -> Path:
        """
        Save imputation structure to file.
        
        Args:
            df: DataFrame with imputation structure
            filename: Name of output file
            
        Returns:
            Path to saved file
        """
        output_file = self.output_dir / filename
        
        # Convert to GeoDataFrame before saving
        gdf = gpd.GeoDataFrame(df, geometry='geometry')
        
        # Save as parquet with metadata
        gdf.to_parquet(
            output_file,
            compression='snappy',
            index=False
        )
        
        logger.info(f"Saved imputation structure to {output_file}")
        return output_file
    
    def run(self) -> Path:
        """
        Run the complete imputation preparation process.
        
        Returns:
            Path to output file
        """
        logger.info("Starting imputation preparation...")
        
        # Prepare imputation structure
        df = self.prepare_imputation_structure()
        
        # Generate visualizations
        logger.info("Generating visualizations...")
        distance_plot = self._generate_distance_histogram(df)
        weight_plot = self._generate_weight_distribution(df)
        
        # Generate report
        logger.info("Generating report...")
        report_file = self._generate_report(df, distance_plot, weight_plot)
        
        # Save results
        output_file = self.save_imputation_structure(df)
        
        logger.info("Imputation preparation complete.")
        logger.info(f"Report available at: {report_file}")
        
        return output_file

if __name__ == "__main__":
    # Run imputation process
    manager = ImputationManager()
    output_file = manager.run() 