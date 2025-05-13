#!/usr/bin/env python3
"""
collect_test.py - Simplified test version of comprehensive data collection

This is a reduced version of collect_comprehensive.py that uses fewer sources
for faster testing and validation.
"""
import asyncio
import aiohttp
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
        logging.FileHandler(logs_dir / f"test_collector_{datetime.now().strftime('%Y%m%d')}.log")
    ]
)

logger = logging.getLogger("test_collector")
logger.info("Starting test data collection")

# Import utilities and agents
import utils
from agents import buoys, open_meteo
from collector_revised import Ctx

def load_config(config_path="config.ini"):
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

async def optimize_config(config):
    """
    Optimize the collection config for test collection.
    """
    # Increase timeouts and retries
    if 'GENERAL' in config:
        config['GENERAL']['timeout'] = '60'
        config['GENERAL']['max_retries'] = '3'
        logger.info("Increased timeouts and retries for more reliable collection")
    
    # Ensure critical sources are enabled
    if 'SOURCES' in config:
        sources = [
            'enable_buoys', 'enable_open_meteo'
        ]
        
        for source in sources:
            if source in config['SOURCES']:
                config['SOURCES'][source] = 'true'
    
    # Set SSL exceptions for problematic domains
    if 'SSL_EXCEPTIONS' not in config:
        config['SSL_EXCEPTIONS'] = {}
    
    config['SSL_EXCEPTIONS']['disable_verification'] = 'cdip.ucsd.edu,tgftp.nws.noaa.gov'
    logger.info("Set SSL exceptions for known problematic domains")
    
    return config

async def collect_test_data(config_path="config.ini"):
    """Run a limited test collection."""
    # Load and optimize config
    config = load_config(config_path)
    config = await optimize_config(config)
    
    # Create context
    ctx = Ctx(config)
    
    # Use a shorter timeout for network operations
    timeout = aiohttp.ClientTimeout(total=60)
    
    # Create connector with SSL settings
    connector = aiohttp.TCPConnector(ssl=None)
    logger.info("Using domain-specific SSL verification bypass")
    
    results = []
    
    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            # 1. Collect buoy data
            logger.info("Collecting buoy data...")
            buoy_results = await buoys(ctx, session)
            results.extend(buoy_results)
            logger.info(f"Collected {len(buoy_results)} buoy data files")
            
            # 2. Collect Open-Meteo data (reliable and fast)
            logger.info("Collecting Open-Meteo data...")
            meteo_results = await open_meteo(ctx, session)
            results.extend(meteo_results)
            logger.info(f"Collected {len(meteo_results)} Open-Meteo data files")
            
            # Generate metadata
            logger.info(f"Collected a total of {len(results)} files")
            
            # Write metadata
            metadata_content = utils.jdump({
                "run_id": ctx.run_id,
                "timestamp": utils.utcnow(),
                "results": results
            })
            
            bundle_path = ctx.bundle / "metadata.json"
            latest_bundle_path = ctx.base / "latest_bundle.txt"
            
            # Write metadata and update latest bundle
            write_tasks = [
                utils.write_file_async(bundle_path, metadata_content),
                utils.write_file_async(latest_bundle_path, ctx.run_id)
            ]
            await asyncio.gather(*write_tasks)
            
            # Count sources and shore coverage
            total_files = len(results)
            
            # Shore coverage
            north_facing = sum(1 for r in results if "north_facing" in r and r["north_facing"])
            south_facing = sum(1 for r in results if "south_facing" in r and r["south_facing"])
            
            # Get size on disk
            total_size_bytes = 0
            for r in results:
                if "filename" in r:
                    file_path = ctx.bundle / r["filename"]
                    if file_path.exists():
                        total_size_bytes += file_path.stat().st_size
            
            total_size_mb = round(total_size_bytes / (1024 * 1024), 2)
            
            # Save to latest_bundle.txt
            try:
                latest_path = Path("pacific_data") / "latest_bundle.txt"
                with open(latest_path, "w") as f:
                    f.write(ctx.run_id)
                logger.info(f"Saved {ctx.run_id} as latest bundle")
            except Exception as e:
                logger.warning(f"Failed to save latest bundle: {e}")
            
            return ctx.bundle, {
                "total_files": total_files,
                "north_facing": north_facing,
                "south_facing": south_facing,
                "total_size_mb": total_size_mb
            }
    
    except Exception as e:
        logger.error(f"Test collection process failed: {e}")
        return None, None

async def main():
    """Main entry point."""
    import argparse
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Test data collection for SwellForecaster")
    parser.add_argument("--config", default="config.ini", help="Path to configuration file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    # Set debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    # Start the timer
    start_time = datetime.now()
    logger.info(f"Starting test data collection at {start_time}")
    
    # Collect data
    bundle_dir, summary = await collect_test_data(args.config)
    
    if not bundle_dir:
        logger.error("Test data collection failed")
        return 1
    
    # Display summary
    print("\n---------- TEST DATA COLLECTION COMPLETED ----------")
    print(f"Bundle ID: {bundle_dir.name}")
    print(f"Files: {summary['total_files']} ({summary['total_size_mb']} MB)")
    print(f"Shore coverage: North Shore: {summary['north_facing']}, South Shore: {summary['south_facing']}")
    print("\nUse this bundle ID with Pat Caldwell analyzer:")
    print(f"python run_caldwell.py --analyze-only --bundle-id {bundle_dir.name}")
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