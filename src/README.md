# SwellForecaster Source Code

This directory contains the source code for the SwellForecaster application, structured in a clean architecture pattern.

## Project Structure

- `forecast_tool/` - Main package
  - `agents/` - Data collection agents
    - `buoy_agent.py` - NDBC and CDIP buoy data
    - `chart_agent.py` - Charts and imagery
    - `model_agent.py` - Wave model data
    - `api_agent.py` - External API data
    - `region_agent.py` - Regional data (North Pacific, Southern Ocean)
  - `data/` - Base data structures
    - `base_agent.py` - Common functionality for all agents
    - `collection_context.py` - Context manager for data collection
  - `models/` - Pydantic models
    - `settings.py` - Application settings
    - `buoy_data.py` - Buoy data structures
  - `utils/` - Utility functions
    - `file_io.py` - File I/O operations
    - `http.py` - HTTP client with retry logic
    - `logging_config.py` - Structured logging setup
    - `retry.py` - Tenacity-based retry decorators
    - `wave_analysis.py` - Wave analysis utilities
  - `compatibility.py` - Legacy code compatibility layer
  - `collector.py` - Main data collection module
  - `analyzer.py` - Forecast generation module
  - `run.py` - Application entry point

## Usage

The application can be installed as a package using pip:

```bash
pip install -e .
```

After installation, the following commands are available:

- `swellforecaster` - Run both collection and analysis
- `swellcollector` - Run only data collection
- `swellanalyzer` - Run only forecast analysis

## Development

To set up the development environment:

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

3. Install pre-commit hooks:
   ```bash
   pre-commit install
   ```

## Code Style

The codebase follows these conventions:

- Type hints for all functions and methods
- Docstrings in Google style for all public APIs
- Async/await for all I/O operations
- Proper error handling with specific exceptions
- Structured logging with consistent format
- Atomic operations with proper resource cleanup

## Legacy Code Compatibility

The application includes a compatibility layer that allows the new code to work with legacy code. This enables a gradual migration from the old codebase to the new architecture.

Key components of the compatibility layer:

- `forecast_tool.compatibility` module bridges between old and new code
- Global `utils` module is patched with new functionality
- Synchronous versions of async functions are provided for backward compatibility
- Wave analysis functions work in both the old and new codebase

To use the compatibility layer:

```python
# Import and set up the compatibility bridges
from forecast_tool.compatibility import setup_all_compatibility_bridges
setup_all_compatibility_bridges()

# Now import utils, which will use the new functionality
import utils

# Use utils as before
print(utils.utcnow())
data = utils.jload("data.json")
```

The compatibility layer is automatically set up when importing the `forecast_tool` package.