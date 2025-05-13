#!/usr/bin/env python3
"""
Complete SwellForecaster script that:
- Collects all data from all sources (including API-based ones)
- Generates a fresh forecast based solely on the newly collected data
- Ensures a clean slate for each run

All in one integrated workflow.
"""
import argparse
import asyncio
import configparser
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Ensure we're using modules from the current directory first
current_dir = str(Path(__file__).resolve().parent)
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Make sure the src directory is in the path
src_dir = os.path.join(current_dir, "src")
if os.path.exists(src_dir) and src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Create logs directory if it doesn't exist
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# Set up logging - both to file and console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(logs_dir / f"swellforecaster_{datetime.now().strftime('%Y%m%d')}.log")
    ]
)

logger = logging.getLogger("swellforecaster")

# Import the compatibility bridges to ensure all modules work properly
try:
    # Try to import from the package
    from forecast_tool.compatibility import setup_all_compatibility_bridges
    setup_all_compatibility_bridges()
    logger.info("Set up compatibility bridges")
except Exception as e:
    logger.warning(f"Could not set up compatibility bridges: {e}")

# Import the utilities module
try:
    from forecast_tool.utils import file_io, http
    logger.info("Using forecast_tool.utils")
except ImportError:
    import utils
    logger.info("Using legacy utils module")


def load_config(config_path):
    """Load the configuration file."""
    logger.info(f"Loading configuration from {config_path}")
    config = configparser.ConfigParser()
    
    if os.path.exists(config_path):
        config.read(config_path)
    else:
        # Try to find config.example.ini as fallback
        example_path = Path(current_dir) / "config.example.ini"
        if example_path.exists():
            logger.warning(f"Config {config_path} not found, using {example_path}")
            config.read(example_path)
        else:
            logger.error(f"No configuration file found at {config_path}")
            sys.exit(1)
    
    return config


async def collect_data(config_path):
    """
    Collect all data from all sources.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Path to the data bundle directory, or None if collection failed
    """
    logger.info("Starting data collection phase")
    
    # First, try to use the new collector module
    try:
        from forecast_tool.collector import collect_data as new_collect
        logger.info("Using new collector module from forecast_tool package")
        return await new_collect(config_path)
    except ImportError:
        logger.warning("Could not import the new collector, trying legacy collector")
    
    # Fall back to legacy collector if needed
    try:
        import collector_revised
        logger.info("Using legacy collector_revised.py")
        
        # Initialize the arguments needed by the legacy collector
        class Args:
            def __init__(self):
                self.config = config_path
                self.dry_run = False
                self.debug = False
                self.cache_days = 7
        
        # Load the config
        config = load_config(config_path)
        
        # Call the legacy collector
        return await collector_revised.collect(config, Args())
    except ImportError:
        logger.error("Failed to import any collector module")
        return None
    except Exception as e:
        logger.error(f"Error during data collection: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


async def generate_forecast(bundle_dir, config_path):
    """
    Generate a forecast from the collected data.
    
    Args:
        bundle_dir: Path to the data bundle directory
        config_path: Path to the configuration file
        
    Returns:
        Dictionary with paths to generated forecast files, or None if generation failed
    """
    logger.info(f"Starting forecast generation for bundle: {bundle_dir}")
    
    if not bundle_dir:
        logger.error("No bundle directory provided")
        return None
    
    bundle_id = bundle_dir.name if isinstance(bundle_dir, Path) else bundle_dir.split('/')[-1]
    
    # Try different analyzer modules in order of preference
    try:
        # Try the analyzer_fallback first - most reliable
        try:
            from forecast_tool.analyzer_fallback import analyze_and_generate
            logger.info("Using analyzer_fallback from forecast_tool package")
            config = load_config(config_path)
            return await analyze_and_generate(config, bundle_id)
        except ImportError:
            logger.warning("Could not import analyzer_fallback")
        
        # Try the original analyzer if available
        try:
            import pacific_forecast_analyzer_revised
            logger.info("Using legacy pacific_forecast_analyzer_revised.py")
            config = load_config(config_path)
            return await pacific_forecast_analyzer_revised.analyze_and_generate(config, bundle_id)
        except ImportError:
            logger.warning("Could not import pacific_forecast_analyzer_revised")
        
        # Use the simplest analyzer if all else fails
        try:
            import simple_analyzer
            logger.info("Using simple_analyzer.py")
            config = load_config(config_path)
            return await simple_analyzer.analyze_and_generate(config, bundle_id)
        except ImportError:
            logger.error("Could not import any analyzer module")
            return None
        
    except Exception as e:
        logger.error(f"Error during forecast generation: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


async def main():
    """Main entry point."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="SwellForecaster - Complete Workflow")
    parser.add_argument("--config", default="config.ini", help="Path to configuration file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--collect-only", action="store_true", help="Only collect data, don't generate forecast")
    parser.add_argument("--bundle-id", help="Use a specific bundle ID (skip collection)")
    args = parser.parse_args()
    
    # Set debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    # Start the timer
    start_time = datetime.now()
    logger.info(f"Starting SwellForecaster at {start_time}")
    
    # Collect data if no bundle ID is specified or if collection is explicitly requested
    bundle_dir = None
    if not args.bundle_id or args.collect_only:
        bundle_dir = await collect_data(args.config)
        if not bundle_dir:
            logger.error("Data collection failed, aborting")
            return 1
        logger.info(f"Data collection completed successfully: {bundle_dir}")
    else:
        # Use the specified bundle ID
        logger.info(f"Using specified bundle ID: {args.bundle_id}")
        data_dir = "pacific_data"  # Hardcoded for simplicity
        bundle_dir = Path(data_dir) / args.bundle_id
        if not bundle_dir.exists():
            logger.error(f"Bundle directory not found: {bundle_dir}")
            return 1
    
    # Stop here if only collection was requested
    if args.collect_only:
        logger.info("Collection-only mode, skipping forecast generation")
        return 0
    
    # Generate the forecast
    forecast = await generate_forecast(bundle_dir, args.config)
    if not forecast:
        logger.error("Forecast generation failed")
        return 1
    
    # Display the results
    logger.info(f"Forecast generation completed successfully")
    
    print("\n---------- FORECAST GENERATED SUCCESSFULLY ----------")
    print(f"Bundle ID: {forecast.get('bundle_id', 'N/A')}")
    print(f"Timestamp: {forecast.get('timestamp', 'N/A')}")
    print("\nOutput files:")
    for key, path in forecast.items():
        if key not in ["timestamp", "bundle_id", "run_id"]:
            print(f"- {key.capitalize()}: {path}")
    print("\nUse these files to view the forecast:")
    for key, path in forecast.items():
        if key in ["markdown", "html", "pdf"]:
            print(f"- {key.capitalize()}: open {path}")
    print("---------------------------------------------------\n")
    
    # Calculate and display execution time
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Total execution time: {duration:.2f} seconds")
    
    return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)