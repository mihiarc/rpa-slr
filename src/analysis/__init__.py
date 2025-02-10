"""
NOAA Data Analysis Package.

This package provides tools for analyzing NOAA high tide flooding data quality
and characteristics.
"""

from .data_quality import DataQualityAnalyzer
from .cli import main as cli_main

__all__ = ['DataQualityAnalyzer', 'cli_main'] 