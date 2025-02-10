"""
NOAA High Tide Flooding Data Package.

This package provides tools for accessing and processing NOAA high tide flooding data,
including both historical observations and future projections.
"""

from . import core
from . import historical
from . import projected

# Import commonly used classes for convenience
from .core import NOAAClient, NOAACache
from .historical import HistoricalHTFFetcher, HistoricalHTFProcessor
from .projected import ProjectedHTFFetcher, ProjectedHTFProcessor

__all__ = [
    # Submodules
    'core',
    'historical',
    'projected',
    
    # Core classes
    'NOAAClient',
    'NOAACache',
    
    # Historical data classes
    'HistoricalHTFFetcher',
    'HistoricalHTFProcessor',
    
    # Projected data classes
    'ProjectedHTFFetcher',
    'ProjectedHTFProcessor'
] 