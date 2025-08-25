"""County-Level Tidal Flooding Data Processing Package."""

# Import only essential modules at package load time
from . import config

# Lazy imports - only load when needed
def get_noaa():
    from . import noaa
    return noaa

def get_analysis():
    try:
        from . import analysis
        return analysis
    except ImportError:
        # Skip analysis import if dependencies are missing
        return None

def get_preprocessing():
    from . import preprocessing
    return preprocessing

def get_imputation():
    from . import imputation
    return imputation

def get_assignment():
    try:
        from . import assignment
        return assignment
    except ImportError:
        # Skip assignment import if there are missing config dependencies
        return None

__version__ = "0.2.0"
__all__ = [
    'config',
    'get_noaa',
    'get_analysis', 
    'get_preprocessing',
    'get_imputation',
    'get_assignment'
]
