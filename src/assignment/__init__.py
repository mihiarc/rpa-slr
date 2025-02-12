"""
HTF data assignment module.

This module handles the assignment of HTF data from tide stations to counties,
using a regional approach for both historical and projected data.
"""

from .historical import process_historical_htf
from .common import WeightCalculator

__all__ = [
    'process_historical_htf',
    'WeightCalculator'
]

__version__ = "0.1.0" 