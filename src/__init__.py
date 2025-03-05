"""County-Level Tidal Flooding Data Processing Package."""

from . import noaa
# Conditional import of analysis to avoid dependency issues
try:
    from . import analysis
except ImportError:
    # Skip analysis import if dependencies are missing
    pass
from . import preprocessing
from . import imputation
from . import assignment
from . import config

__version__ = "0.2.0"
__all__ = [
    'noaa',
    'analysis',
    'preprocessing',
    'imputation',
    'assignment',
    'config'
]
