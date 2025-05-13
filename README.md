# SwellForecaster - Production Version

Oʻahu Surf Forecast Generator that collects marine data from various sources and generates detailed surf forecasts in the style of Pat Caldwell.

## Overview

SwellForecaster is a comprehensive surf forecasting tool that:

- Fetches data from buoys, wave models, weather charts, and API sources
- Analyzes North Pacific and Southern Hemisphere swell patterns
- Uses AI (GPT-4.1) to generate detailed, multi-day forecasts
- Provides equal emphasis on North Shore and South Shore conditions
- Generates forecasts in multiple formats (Markdown, HTML, PDF)

## Key Features

- **Complete Data Collection**: Retrieves data from 15+ sources including NDBC buoys, NOAA models, PacIOOS, Surfline, and Windy
- **Asynchronous Architecture**: Uses asyncio for efficient, parallel data collection
- **Robust Error Handling**: Includes fallbacks, retry logic, and graceful degradation 
- **Multi-Shore Coverage**: Equal emphasis on North Shore and South Shore conditions
- **Pat Caldwell-Style Forecasts**: Detailed multi-phase swell predictions with arrival times and specific directional bands
- **Multiple Output Formats**: Markdown, HTML, and PDF output for easy sharing
- **Intelligent Data Curation**: Optimized data selection for more efficient and focused GPT processing

## Installation

### Prerequisites

- Python 3.9 or higher
- Required packages (install with `pip install -r requirements.txt`):
  - aiohttp
  - configparser
  - openai (for GPT-4.1 integration)
  - Pillow (for image processing)
  - beautifulsoup4 (for HTML parsing)
  - markdown (for format conversion)
  - weasyprint (for PDF generation, optional)

### External Tools (Optional)

For GRIB file processing (enhanced model data):
- wgrib2: `brew install wgrib2` (macOS) or `conda install -c conda-forge wgrib2` (Linux)
- grib2json: `brew install grib2json` (macOS)

### Setup

1. Clone the repository and navigate to the directory:
   ```
   git clone https://github.com/yourusername/SwellForecaster.git
   cd SwellForecaster/new_version
   ```

2. Install Python dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Copy the example configuration and update with your API keys:
   ```
   cp config.example.ini config.ini
   nano config.ini
   ```
   
   At minimum, you need to set:
   - `openai_key` in the [API] section for forecast generation
   - Other optional API keys for enhanced data collection

## Usage

### Running the Complete Pipeline

The default command performs complete data collection and forecast generation:

```bash
./run_production.py
```

This will:
1. Collect data from all configured sources
2. Bundle the data in a timestamped directory
3. Generate a forecast using the collected data
4. Save the forecast in multiple formats

### Command-Line Options

The script supports several options:

```
options:
  -h, --help           show this help message and exit
  --config CONFIG      Path to configuration file
  --bundle-id BUNDLE_ID
                       Analyze a specific bundle ID (skip collection)
  --collect-only       Only collect data
  --analyze-only       Only analyze data
  --debug              Enable debug logging
  --cache-days CACHE_DAYS
                       Days to keep bundles
```

### Example Commands

- Collect data without generating a forecast:
  ```
  ./run_production.py --collect-only
  ```

- Generate a forecast using a specific data bundle:
  ```
  ./run_production.py --analyze-only --bundle-id 0fd034a5ac7641eb93d22b9c6031499d_1747087281
  ```

- Run with more verbose logging:
  ```
  ./run_production.py --debug
  ```

## Configuration

The `config.ini` file contains several sections for customizing the forecaster:

### [API]
API keys for various services:
- `openai_key`: OpenAI API key for forecast generation
- `anthropic_key`: Alternative LLM provider (optional)
- `windy_key`: For Windy Point Forecast API
- `stormglass_key`: For marine data (optional)
- `ecmwf_key`: For European model data (optional)
- `surfline_email` and `surfline_password`: For Surfline API access (optional)

### [GENERAL]
General settings:
- `data_dir`: Directory for storing collected data
- `timeout`: Request timeout in seconds
- `max_retries`: Number of retry attempts
- `agent_model`: Which GPT model to use for forecasting

### [SOURCES]
Enable/disable specific data sources:
- Toggle individual sources like buoys, charts, API sources
- Enable fallbacks for specific sources when primary ones fail

### [FORECAST]
Control forecast output:
- `output_format`: Default output format
- `output_dir`: Directory for forecast output
- `include_images_in_output`: Whether to include charts in output
- `south_swell_emphasis`: Auto, true, or false
- `north_swell_emphasis`: Auto, true, or false

### [SSL_EXCEPTIONS]
Domains where SSL verification should be disabled:
- List of domains that have SSL certificate issues

## Running Enhanced Forecasts

The system offers several specialized forecast runners:

- `run.py`: Standard collection and analysis
- `run_caldwell.py`: Enhanced forecasts in Pat Caldwell's style
- `run_opc_enhanced.py`: Pat Caldwell style with enhanced OPC charts
- `run_fallback.py`: Simple forecasts when other systems fail

The recommended command for the most detailed forecasts is:

```bash
./run_opc_enhanced.py
```

## Output Files

Forecasts are saved in the `forecasts/` directory with the timestamp:

- `forecast_YYYYMMDD_HHMM.md`: Markdown format
- `forecast_YYYYMMDD_HHMM.html`: HTML format with styling
- `forecast_YYYYMMDD_HHMM.pdf`: PDF format (if weasyprint is installed)

## Data Directory Structure

Collected data is stored in `pacific_data/` with this structure:

```
pacific_data/
└── <bundle_id>/
    ├── metadata.json      # Bundle metadata
    ├── ndbc_*.txt         # Buoy data
    ├── opc_*.png          # Ocean Prediction Center charts
    ├── pacioos_*.png      # PacIOOS wave model data
    ├── ww3_*.gif          # WaveWatch III model data
    ├── surfline_*.json    # Surfline API data
    ├── windy_*.json       # Windy API data
    └── ...                # Additional data files
```

## Data Curation System

The forecaster includes an intelligent data curation system that optimizes which data is sent to GPT for analysis:

- **Value Scoring**: Each data source is assigned a score (1-10) based on its forecasting value
- **Prioritization**: Critical data sources like key buoys and OPC charts are always included
- **Filtering**: Low-value or redundant data is automatically excluded
- **Image Optimization**: Images are processed for optimal GPT visual analysis
- **Token Efficiency**: Reduces token usage while maintaining forecast quality

This system is implemented in `data_curator.py` and integrated with the Pat Caldwell analyzer to ensure GPT focuses on the most valuable forecasting data.

### OPC Chart Collection

The system includes dedicated collection of Ocean Prediction Center charts from ocean.weather.gov, which are critical for accurate Pacific storm tracking and swell forecasting. These charts provide essential information about:

- Current surface pressure patterns across the Pacific
- 24/48/72/96 hour surface forecasts
- Wind and wave height forecasts
- Wave period and direction forecasts

Run the enhanced OPC-integrated forecast with:
```bash
./run_opc_enhanced.py
```

## Development

### Adding New Data Sources

To add a new data source:

1. Create a function in the appropriate agent module in `agents/`
2. Follow the common agent interface pattern:
   ```python
   async def new_source(ctx, session):
       # Collect data and return metadata
       return [{"source": "NewSource", "type": "my_type", ...}]
   ```
3. Add the function to the appropriate list in `agents/__init__.py`
4. Update the collector to include your new agent

### Troubleshooting

- **SSL Errors**: Some government sites have certificate issues. Add problematic domains to the `[SSL_EXCEPTIONS]` section.
- **Timeout Errors**: Increase the `timeout` value in `[GENERAL]` section.
- **API Rate Limits**: Many sources have rate limits. Implement appropriate throttling or fallbacks.
- **Missing Data**: Check the logs for error messages. The system will continue even if some sources fail.

## License

[MIT License](LICENSE)# SwellForecaster2
