# SwellForecaster - Fixed Version

This document provides instructions for the fixed version of SwellForecaster, which addresses compatibility issues and ensures proper functionality.

## Overview

The SwellForecaster system collects marine data from various sources (buoys, wave models, weather charts) and generates a detailed surf forecast for Oʻahu in the style of Pat Caldwell. The fixed version ensures that data collection works properly and that the forecast generation process completes successfully.

## Setup Instructions

1. **Configure API Keys**
   - Copy the config.ini file if you haven't already:
     ```
     cp config.example.ini config.ini
     ```
   - Edit config.ini to add your API keys:
     ```
     nano config.ini
     ```
   - At minimum, you'll need:
     - `openai_key` in the [API] section for forecast generation

2. **Install Dependencies**
   - Install Python dependencies:
     ```
     pip install -r requirements.txt
     ```
   - The system requires:
     - Python 3.9 or higher
     - aiohttp, configparser, and other libraries

3. **External Tools (Optional)**
   - For full functionality with GRIB file processing:
     - Install wgrib2: `brew install wgrib2` (macOS)
     - Install grib2json: `brew install grib2json` (macOS)
   - If these aren't available, the system will fall back to alternative data sources

## Running the Fixed Version

### Quick Start

To run a complete data collection and forecast generation:

```bash
python run_fixed.py
```

This will:
1. Collect data from all available sources
2. Bundle the data in a timestamped directory
3. Optimize the data bundle with the data curator system
4. Generate a forecast in multiple formats (Markdown, HTML, PDF)

### Enhanced Forecast Options

For the best forecast quality, use the OPC-enhanced Pat Caldwell style forecast:

```bash
python run_opc_enhanced.py
```

This provides the most detailed forecasts with:
- Enhanced OPC chart collection
- Intelligent data curation and optimization
- Pat Caldwell's distinctive forecasting style
- Equal emphasis on North Shore and South Shore

### Options

- **Collection Only**: Only collect data without generating a forecast
  ```bash
  python run_fixed.py --collect-only
  ```

- **Analysis Only**: Only generate a forecast using previously collected data
  ```bash
  python run_fixed.py --analyze-only
  ```

- **Specific Bundle**: Analyze a specific data bundle
  ```bash
  python run_fixed.py --analyze-only --bundle-id <bundle_id>
  ```

- **Debug Mode**: Enable additional logging
  ```bash
  python run_fixed.py --debug
  ```

- **Custom Config**: Use a different configuration file
  ```bash
  python run_fixed.py --config custom_config.ini
  ```

## Output Files

Forecasts are saved in the `forecasts/` directory with multiple formats:

- **Markdown**: Plain text format readable in any text editor
- **HTML**: Rich format viewable in any web browser
- **PDF**: Printable format (if weasyprint is installed)

## Data Directory Structure

Collected data is stored in `pacific_data/` with this structure:

```
pacific_data/
└── <bundle_id>/    # Unique ID with timestamp
    ├── metadata.json   # Bundle metadata
    ├── ndbc_*.txt      # Buoy data
    ├── opc_*.png       # Ocean Prediction Center charts
    ├── wpc_*.tif       # Weather Prediction Center charts
    ├── pacioos_*.png   # PacIOOS regional wave model data
    └── ...             # Additional data files
```

## Troubleshooting

- **Missing Data**: Some data sources may be unavailable at times. The system includes fallback sources and will continue even if some sources fail.

- **SSL Errors**: Some government websites have certificate issues. The system automatically handles SSL exceptions for known problematic domains.

- **Timeouts**: Network operations may time out. The system includes retry logic with exponential backoff.

- **API Limits**: Some APIs have usage limits. These are handled gracefully, but may result in missing data if limits are exceeded.

## Using the Forecast

The generated forecast provides:

- North Shore (Country) predictions for 3+ days
- South Shore (Town) predictions for 3+ days 
- Wind forecast for different regions
- Extended forecast with developing swell information

Open the HTML or PDF version in your browser for the best viewing experience.

## Data Curation System

The fixed version includes an intelligent data curation system that:

- Prioritizes high-value marine data for GPT processing
- Scores data sources based on their forecasting value (1-10)
- Filters out redundant or irrelevant data
- Optimizes images for GPT's visual processing
- Improves token efficiency and forecast quality

This system is implemented in `data_curator.py` and is automatically used by the Pat Caldwell analyzer when available.