#!/usr/bin/env python3
# run.py - Simple script to run the SwellForecaster new version
import argparse
import configparser
import asyncio
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Ensure we're using modules from the new_version directory first
current_dir = str(Path(__file__).resolve().parent)
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Make sure the src directory is in the path
src_dir = os.path.join(current_dir, "src")
if os.path.exists(src_dir) and src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Then add parent directory to path for fallback references if needed
parent_dir = str(Path(__file__).resolve().parent.parent)
if parent_dir not in sys.path:
    sys.path.insert(1, parent_dir)

# Set up basic logging before we import our modules
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join("logs", f"swellforecaster_{datetime.now().strftime('%Y%m%d')}.log"))
    ]
)

# Set up compatibility bridges for legacy code
try:
    # Try to import from the package
    try:
        from forecast_tool.compatibility import setup_all_compatibility_bridges
        setup_all_compatibility_bridges()
        logging.info("Successfully set up compatibility bridges from package")
    except ImportError:
        # Try with direct import from src path
        sys.path.insert(0, os.path.join(current_dir, "src"))
        from forecast_tool.compatibility import setup_all_compatibility_bridges
        setup_all_compatibility_bridges()
        logging.info("Successfully set up compatibility bridges from src path")
except Exception as e:
    logging.warning(f"Could not set up compatibility bridges: {e}")
    logging.warning("Using legacy imports instead")

# Import our local modules
import utils  # This will now use either the compatibility layer or the original module

# Initialize logging
log = utils.log_init("swellforecaster")

# Import collector with fallback mechanism
try:
    # Try to import from our new package
    from forecast_tool.collector import collect_data

    # Create a wrapper function to match the legacy function signature
    async def collect(config, args):
        """Wrapper around collect_data to maintain compatibility with legacy code."""
        config_path = args.config if hasattr(args, 'config') else "config.ini"
        return await collect_data(config_path)

    log.info("Using new collector from forecast_tool package with wrapper")
except ImportError:
    # Fall back to the legacy collector
    try:
        from collector_revised import collect
        log.info("Using legacy collector from collector_revised.py")
    except ImportError:
        log.error("Failed to import any collector module")
        sys.exit(1)

def setup_config(config_file=None):
    """Load and validate configuration."""
    config = configparser.ConfigParser()

    # If config_file is None, default to config.ini
    if config_file is None:
        config_file = "config.ini"

    # First try in the current directory
    if Path(config_file).exists():
        config.read(config_file)
    # Then try in the parent directory
    elif (Path(parent_dir) / config_file).exists():
        config.read(Path(parent_dir) / config_file)
    else:
        # Create example config if it doesn't exist
        log.warning(f"Config file {config_file} not found. Using config.example.ini as a fallback.")
        example_config = Path(parent_dir) / "config.example.ini"
        if example_config.exists():
            config.read(example_config)
        else:
            log.error("No config file found and no example config available.")
            sys.exit(1)

    return config

async def run_collection(args):
    """Run the data collection process."""
    config = setup_config(args.config)
    log.info("Starting data collection...")
    try:
        bundle = await collect(config, args)
        if bundle:
            log.info(f"Collection complete: {bundle}")
            return bundle
        else:
            log.error("Collection failed")
            return None
    except Exception as e:
        log.error(f"Collection error: {e}")
        return None

async def run_analysis(args, bundle=None):
    """Run the forecast analysis and generation."""
    config = setup_config(args.config)
    log.info("Starting forecast analysis...")

    # Use the specified bundle ID, the one from collection, or the latest
    bundle_id = args.bundle_id
    if not bundle_id and bundle:
        bundle_id = bundle.name

    try:
        # Try using the new analyzer from forecast_tool package
        try:
            from forecast_tool.analyzer_fallback import analyze_and_generate
            log.info("Using analyzer_fallback from forecast_tool package")
        except ImportError:
            # Fall back to the legacy analyzer if it exists
            from pacific_forecast_analyzer_revised import analyze_and_generate
            log.info("Using legacy pacific_forecast_analyzer_revised")

        # Run the analyzer
        forecast = await analyze_and_generate(config, bundle_id)
        if forecast:
            log.info(f"Forecast generation complete: {forecast}")
            return forecast
        else:
            log.error("Forecast generation failed")
            return None
    except Exception as e:
        log.error(f"Analysis error: {e}")
        import traceback
        log.error(traceback.format_exc())
        return None

async def main_async(args):
    """Main async function that orchestrates the process."""
    if args.collect_only or args.full_run:
        bundle = await run_collection(args)
        if not bundle and args.full_run:
            log.error("Collection failed, skipping analysis")
            return 1
    else:
        bundle = None
    
    if args.analyze_only or args.full_run:
        forecast = await run_analysis(args, bundle)
        if not forecast:
            log.error("Analysis failed")
            return 1
    
    return 0

def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(description="SwellForecaster - Hawaii Surf Forecast Generator")
    
    # Config options
    parser.add_argument("--config", default="config.ini", help="Path to config file")
    parser.add_argument("--cache-days", type=int, default=7, help="Days to keep data bundles")
    
    # Operation mode
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--collect-only", action="store_true", help="Only collect data, don't generate forecast")
    group.add_argument("--analyze-only", action="store_true", help="Only analyze existing data, don't collect new data")
    group.add_argument("--full-run", action="store_true", help="Run full collection and analysis (default)")
    
    # Bundle selection
    parser.add_argument("--bundle-id", type=str, help="Process specific bundle ID instead of latest")
    
    # Debug options
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without doing it")
    
    args = parser.parse_args()
    
    # Default to full run if no mode specified
    if not (args.collect_only or args.analyze_only or args.full_run):
        args.full_run = True
    
    # Set logging level
    if args.debug:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        exit_code = asyncio.run(main_async(args))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()