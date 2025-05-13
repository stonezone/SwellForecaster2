# SwellForecaster - Forecasting Tool Package

This directory contains the refactored SwellForecaster package with improved architecture, async I/O, stronger typing, and more robust fallback mechanisms.

## Package Structure

```
forecast_tool/
├── __init__.py           # Package initialization and exports
├── analyzer.py           # Main forecast analyzer
├── analyzer_fallback.py  # Simplified fallback analyzer
├── collector.py          # Data collection orchestrator
├── compatibility.py      # Legacy code compatibility layer
├── run.py                # Main entry point
├── agents/               # Data collection agents
│   ├── __init__.py
│   ├── api_agents.py     # API data source agents
│   ├── buoy_agents.py    # Buoy data collection agents
│   ├── chart_agents.py   # Chart and image collection agents
│   ├── model_agents.py   # Forecast model data agents
│   └── region_agents.py  # Region-specific data agents
├── data/                 # Data models and collection context
│   └── __init__.py
├── models/               # Data and domain models
│   └── __init__.py
└── utils/                # Utility functions and helpers
    ├── __init__.py
    ├── file_io.py        # File I/O utilities
    ├── http.py           # HTTP client with retries
    ├── image_utils.py    # Image processing utilities
    ├── logging_config.py # Logging configuration
    ├── retry.py          # Retry mechanisms
    └── wave_analysis.py  # Wave data analysis
```

## Key Components

### Analyzers

The package provides two analyzer implementations:

1. **Main Analyzer** (`analyzer.py`): The full-featured analyzer that uses OpenAI GPT models to generate sophisticated surf forecasts based on collected data.

2. **Fallback Analyzer** (`analyzer_fallback.py`): A simplified analyzer that works without external API dependencies. This analyzer provides basic forecasts when the main analyzer encounters issues or when OpenAI API access is unavailable.

### Data Collection

The `collector.py` module orchestrates the data collection process:

- Uses async/await patterns for efficient I/O
- Implements proper error handling and retries
- Uses context managers for resource management
- Structures data into organized bundles

### Compatibility Layer

The `compatibility.py` module provides a bridge between the new architecture and legacy code:

- Creates proxy modules for utils, agents, and north_pacific_analysis
- Implements fallback functions when original implementations are unavailable
- Redirects legacy calls to the new implementations

### Utilities

The `utils/` directory contains centralized utility functions:

- **file_io.py**: Async file operations with fallbacks to sync
- **http.py**: Advanced HTTP client with retry, timeout, and SSL validation
- **wave_analysis.py**: Wave data analysis and south swell detection
- **retry.py**: Generic retry mechanisms using tenacity
- **logging_config.py**: Structured logging configuration

## Usage

### Running the Package

The package can be run in several ways:

1. **Using run.py**:
   ```bash
   python -m forecast_tool.run
   ```

2. **Using fallback analyzer**:
   ```bash
   python run_fallback.py [--bundle-id BUNDLE_ID] [--config CONFIG] [--debug]
   ```

3. **Usage from another script**:
   ```python
   from forecast_tool import analyze_fallback
   
   async def main():
       result = await analyze_fallback(config, bundle_id)
       print(f"Forecast generated: {result}")
   ```

### Default Entry Points

- **Collection**: `forecast_tool.collector.collect_data`
- **Analysis**: `forecast_tool.analyzer.analyze_data`
- **Fallback Analysis**: `forecast_tool.analyzer_fallback.analyze_and_generate`

## Configuration

The package uses a `config.ini` file with sections for:

- **API**: API keys for OpenAI and other services
- **GENERAL**: Global settings like data directories
- **FORECAST**: Forecast generation settings
- **SOURCES**: Data source configuration

## Compatibility Notes

The package maintains compatibility with the legacy codebase through:

1. The `compatibility.py` module that bridges old and new APIs
2. Legacy utility functions exposed through the global `utils` module 
3. Fallback implementations for critical components