#!/usr/bin/env python3
"""
Production-ready run script for SwellForecaster.

This script provides a complete implementation that:
1. Ensures proper data collection from all configured sources
2. Balances North Shore and South Shore forecasting equally
3. Uses GPT-4.1 for detailed Pat Caldwell-style forecast generation
4. Handles errors gracefully with appropriate fallbacks
5. Generates forecasts in multiple formats (Markdown, HTML, PDF)
"""
import argparse
import asyncio
import configparser
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Ensure the current directory is in the path
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

# Create logs directory if it doesn't exist
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(logs_dir / f"swellforecaster_run_fixed.log")
    ]
)

logger = logging.getLogger("swellforecaster_fixed")

# Import the utils module
import utils

def load_config(config_path):
    """Load configuration from the specified path."""
    config = configparser.ConfigParser()
    
    if os.path.exists(config_path):
        config.read(config_path)
        return config
    
    # Try to find config.ini or config.example.ini as fallback
    for fallback in ["config.ini", "config.example.ini"]:
        if os.path.exists(fallback):
            logger.warning(f"Config {config_path} not found, using {fallback}")
            config.read(fallback)
            return config
    
    logger.error(f"No configuration file found")
    sys.exit(1)

async def collect_data(config, args):
    """
    Collect data using the legacy collector.
    
    Args:
        config: ConfigParser object with configuration
        args: Parsed command line arguments
        
    Returns:
        Path object for the bundle directory
    """
    logger.info("Starting data collection")
    
    # Import the collector module
    from collector_revised import collect
    
    try:
        bundle_dir = await collect(config, args)
        if bundle_dir:
            logger.info(f"Data collection complete: {bundle_dir}")
            return bundle_dir
        else:
            logger.error("Data collection failed")
            return None
    except Exception as e:
        logger.error(f"Error during data collection: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

async def analyze_data(config, bundle_id=None):
    """
    Analyze data to generate a forecast.
    
    Args:
        config: ConfigParser object with configuration
        bundle_id: Optional ID of the bundle to analyze
        
    Returns:
        Dict with paths to generated forecast files
    """
    logger.info(f"Starting forecast analysis with bundle ID: {bundle_id}")
    
    # First try using the fallback analyzer (it's simpler and more robust)
    try:
        from src.forecast_tool.analyzer_fallback import analyze_and_generate
        logger.info("Using fallback analyzer from src/forecast_tool package")
        return await analyze_and_generate(config, bundle_id)
    except ImportError:
        logger.warning("Could not import fallback analyzer, trying simple_analyzer")
    
    # Then try the simple analyzer
    try:
        from simple_analyzer import analyze_and_generate
        logger.info("Using simple_analyzer.py")
        return await analyze_and_generate(config, bundle_id)
    except ImportError:
        logger.warning("Could not import simple_analyzer, trying pacific_forecast_analyzer_revised")
    
    # As a last resort, try the legacy analyzer
    try:
        from pacific_forecast_analyzer_revised import analyze_and_generate
        logger.info("Using pacific_forecast_analyzer_revised.py")
        return await analyze_and_generate(config, bundle_id)
    except ImportError:
        logger.error("Could not import any analyzer module")
        return None
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

async def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="SwellForecaster Fixed Runner")
    parser.add_argument("--config", default="config.ini", help="Path to configuration file")
    parser.add_argument("--bundle-id", help="Analyze a specific bundle ID (skip collection)")
    parser.add_argument("--collect-only", action="store_true", help="Only collect data")
    parser.add_argument("--analyze-only", action="store_true", help="Only analyze data")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--cache-days", type=int, default=7, help="Days to keep bundles")
    args = parser.parse_args()
    
    # Set debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    # Load configuration
    config = load_config(args.config)
    
    # Default to full run if no specific mode is given
    if not (args.collect_only or args.analyze_only):
        args.collect_only = True
        args.analyze_only = True
    
    # Step 1: Data Collection (if needed)
    bundle_dir = None
    if args.collect_only and not args.bundle_id:
        bundle_dir = await collect_data(config, args)
        
        if not bundle_dir and args.analyze_only:
            logger.error("Data collection failed, cannot continue to analysis")
            return 1
    
    # Step 2: Analysis (if needed)
    if args.analyze_only:
        # If we collected data, use that bundle ID
        bundle_id = args.bundle_id
        if not bundle_id and bundle_dir:
            if hasattr(bundle_dir, 'name'):
                bundle_id = bundle_dir.name
            else:
                bundle_id = str(bundle_dir).split('/')[-1]
        
        # Run the analysis
        result = await analyze_data(config, bundle_id)
        
        if result:
            logger.info("Forecast generation successful")
            print("\n========== FORECAST GENERATED SUCCESSFULLY ==========")
            print(f"Bundle ID: {result.get('bundle_id', 'N/A')}")
            print(f"Timestamp: {result.get('timestamp', 'N/A')}")
            print("\nGenerated files:")
            for key, path in result.items():
                if key not in ["timestamp", "bundle_id", "run_id"]:
                    print(f"- {key.capitalize()}: {path}")
            print("\nTo view the forecast:")
            for key, path in result.items():
                if key in ["markdown", "html", "pdf"]:
                    print(f"- Open {key.capitalize()}: open {path}")
            print("===================================================\n")
        else:
            logger.error("Forecast generation failed")
            return 1
    
    return 0

if __name__ == "__main__":
    start_time = datetime.now()
    try:
        exit_code = asyncio.run(main())
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Total execution time: {duration:.2f} seconds")
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)