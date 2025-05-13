#!/usr/bin/env python3
"""
collect_fresh.py - Dedicated script for fresh data collection

This script ensures a complete fresh data collection from all sources:
1. Focuses on reliable data sources to ensure maximum collection success
2. Sets appropriate timeouts and retries for each data source
3. Uses correct authentication and API keys for all services
4. Formats data correctly for GPT-4.1 analysis
5. Implements fallbacks for critical data sources
"""
import asyncio
import configparser
import json
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

# Create logs directory if it doesn't exist
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# Set up logging - both to file and console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(logs_dir / f"collector_fresh_{datetime.now().strftime('%Y%m%d')}.log")
    ]
)

logger = logging.getLogger("fresh_collector")
logger.info("Starting focused fresh data collection")

# Import utilities module
try:
    import utils
    log_init = utils.log_init
except ImportError:
    logger.warning("Could not import utils module directly")
    try:
        from forecast_tool.utils import file_io as utils
        log_init = utils.log_init
    except ImportError:
        logger.error("Could not import utils or forecast_tool.utils")
        sys.exit(1)

# Import collector module
try:
    import collector_revised
except ImportError:
    logger.error("Could not import collector_revised module")
    sys.exit(1)


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


async def optimize_collection(config):
    """
    Optimize the collection config for maximum success rate.
    """
    # Increase timeouts and retries
    if 'GENERAL' in config:
        config['GENERAL']['timeout'] = '60'
        config['GENERAL']['max_retries'] = '5'
        logger.info("Increased timeouts and retries for more reliable collection")
    
    # Ensure critical sources are enabled 
    if 'SOURCES' in config:
        critical_sources = [
            'enable_buoys', 
            'enable_models',
            'enable_open_meteo', 
            'enable_pacioos',
            'enable_nws',
            'fallback_to_tropicaltidbits'
        ]
        
        for source in critical_sources:
            if source in config['SOURCES']:
                config['SOURCES'][source] = 'true'
        
        logger.info("Ensured critical data sources are enabled")
    
    # Set SSL exceptions for problematic domains
    if 'SSL_EXCEPTIONS' in config:
        config['SSL_EXCEPTIONS']['disable_verification'] = 'cdip.ucsd.edu,tgftp.nws.noaa.gov,ocean.weather.gov,www.fnmoc.navy.mil,www.opc.ncep.noaa.gov,pacioos.hawaii.edu,pae-paha.pacioos.hawaii.edu'
        logger.info("Set SSL exceptions for known problematic domains")
    
    # Ensure API keys are set correctly
    if 'API' in config:
        # Check critical API keys
        critical_apis = ['openai_key', 'windy_key']
        for api in critical_apis:
            if api not in config['API'] or not config['API'][api]:
                logger.warning(f"Missing or empty {api} in config")
    
    return config


async def collect_fresh_data(config_path="config.ini"):
    """
    Run a focused data collection with emphasis on reliable sources.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Path to the data bundle directory, or None if collection failed
    """
    logger.info("Starting focused data collection process")
    
    # Load and optimize config
    config = load_config(config_path)
    config = await optimize_collection(config)
    
    # Create arguments for the collector
    class Args:
        def __init__(self):
            self.config = config_path
            self.dry_run = False
            self.debug = True  # Enable debug for more detailed logs
            self.cache_days = 7
    
    # Run the collector
    try:
        logger.info("Running collector_revised.collect")
        bundle_dir = await collector_revised.collect(config, Args())
        
        if bundle_dir:
            logger.info(f"Data collection succeeded: {bundle_dir}")
            
            # Save bundle ID as latest
            try:
                data_dir = Path("pacific_data")
                with open(data_dir / "latest_bundle.txt", "w") as f:
                    bundle_name = bundle_dir.name if isinstance(bundle_dir, Path) else os.path.basename(str(bundle_dir))
                    f.write(bundle_name)
                logger.info(f"Saved {bundle_name} as latest bundle")
            except Exception as e:
                logger.warning(f"Failed to save latest bundle ID: {e}")
            
            return bundle_dir
        else:
            logger.error("Data collection failed - returned None")
            return None
    except Exception as e:
        logger.error(f"Error during data collection: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


async def generate_bundle_summary(bundle_dir):
    """
    Generate a summary of the collected data bundle.
    
    Args:
        bundle_dir: Path to the bundle directory
        
    Returns:
        Dictionary with bundle summary information
    """
    logger.info(f"Generating summary for bundle: {bundle_dir}")
    
    if not bundle_dir:
        logger.error("No bundle directory provided")
        return None
    
    # Make sure bundle directory exists
    if isinstance(bundle_dir, str):
        bundle_dir = Path(bundle_dir)
    
    if not bundle_dir.exists():
        logger.error(f"Bundle directory does not exist: {bundle_dir}")
        return None
    
    # Initialize summary
    summary = {
        "bundle_id": bundle_dir.name,
        "timestamp": datetime.now().strftime("%Y%m%d_%H%M"),
        "files_count": 0,
        "categories": {
            "buoys": [],
            "forecasts": [],
            "imagery": [],
            "models": [],
            "observations": [],
            "metadata": []
        },
        "total_size_mb": 0
    }
    
    # Scan files
    file_count = 0
    total_size = 0
    
    for file_path in bundle_dir.glob("*"):
        file_count += 1
        file_size = file_path.stat().st_size if file_path.is_file() else 0
        total_size += file_size
        
        # Categorize file
        filename = file_path.name.lower()
        
        if "ndbc" in filename or "buoy" in filename:
            summary["categories"]["buoys"].append(str(file_path.name))
        elif "forecast" in filename or "surf" in filename:
            summary["categories"]["forecasts"].append(str(file_path.name))
        elif any(ext in filename for ext in [".png", ".jpg", ".jpeg", ".gif"]):
            summary["categories"]["imagery"].append(str(file_path.name))
        elif "model" in filename or "ww3" in filename or "swan" in filename or "wave" in filename:
            summary["categories"]["models"].append(str(file_path.name))
        elif "meta" in filename or "info" in filename:
            summary["categories"]["metadata"].append(str(file_path.name))
        else:
            summary["categories"]["observations"].append(str(file_path.name))
    
    # Update summary
    summary["files_count"] = file_count
    summary["total_size_mb"] = round(total_size / (1024 * 1024), 2)
    
    # Save summary to the bundle directory
    try:
        with open(bundle_dir / "bundle_summary.json", "w") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Saved bundle summary to {bundle_dir / 'bundle_summary.json'}")
    except Exception as e:
        logger.warning(f"Failed to save bundle summary: {e}")
    
    return summary


async def main():
    """Main entry point."""
    import argparse
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Fresh data collection for SwellForecaster")
    parser.add_argument("--config", default="config.ini", help="Path to configuration file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--summary-only", action="store_true", help="Only generate summary for latest bundle")
    args = parser.parse_args()
    
    # Set debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    # Start the timer
    start_time = datetime.now()
    logger.info(f"Starting fresh data collection at {start_time}")
    
    if args.summary_only:
        # Find latest bundle
        data_dir = Path("pacific_data")
        try:
            with open(data_dir / "latest_bundle.txt", "r") as f:
                bundle_id = f.read().strip()
                bundle_dir = data_dir / bundle_id
                logger.info(f"Using latest bundle from file: {bundle_id}")
                summary = await generate_bundle_summary(bundle_dir)
                print(json.dumps(summary, indent=2))
                return 0
        except Exception as e:
            logger.error(f"Failed to generate summary: {e}")
            return 1
    
    # Collect fresh data
    bundle_dir = await collect_fresh_data(args.config)
    
    if not bundle_dir:
        logger.error("Fresh data collection failed")
        return 1
    
    # Generate a summary of collected data
    summary = await generate_bundle_summary(bundle_dir)
    
    if summary:
        logger.info("Bundle summary generated successfully")
        
        # Print summary to console
        print("\n---------- FRESH DATA COLLECTION COMPLETED ----------")
        print(f"Bundle ID: {summary['bundle_id']}")
        print(f"Timestamp: {summary['timestamp']}")
        print(f"Files: {summary['files_count']} ({summary['total_size_mb']} MB)")
        print("\nData categories:")
        for category, files in summary["categories"].items():
            if files:
                print(f"- {category.capitalize()}: {len(files)} files")
        print("\nUse this bundle ID with Pat Caldwell analyzer:")
        print(f"python run_caldwell.py --analyze-only --bundle-id {summary['bundle_id']}")
        print("\nOr generate a full Caldwell forecast from this data:")
        print(f"python pat_caldwell_analyzer.py {summary['bundle_id']}")
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