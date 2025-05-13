"""SwellForecaster - Hawaii Surf Forecast Generator.

The forecast_tool package provides modules for collecting and analyzing
marine data to generate surf forecasts for Hawaii's North and South shores.

This package is the refactored version of the original SwellForecaster,
with improved architecture, async I/O, and stronger typing.
"""

__version__ = "2.0.0"
__author__ = "SwellForecaster Team"
__license__ = "MIT"
__description__ = "AI-powered surf forecasting tool for Oʻahu"

# Set up compatibility bridges when imported
from .compatibility import setup_all_compatibility_bridges
setup_all_compatibility_bridges()

# Convenience imports for package users
try:
    from .run import run_forecaster, main
    from .collector import collect_data
    from .analyzer import analyze_data
except ImportError:
    # These may not be fully implemented yet
    pass

# Import fallback analyzer for reliability
try:
    from .analyzer_fallback import analyze_and_generate as analyze_fallback
except ImportError:
    # Fallback analyzer not available
    pass