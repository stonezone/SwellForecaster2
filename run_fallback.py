#!/usr/bin/env python3
"""
Run script for the fallback surf forecast analyzer.

This script runs the simplified analyzer from the src/forecast_tool package.
"""
import asyncio
import logging
import os
import sys
from pathlib import Path

# Ensure our src directory is in the Python path
src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Set up basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ]
)

logger = logging.getLogger("forecast_runner")

def parse_args():
    """Parse command line arguments."""
    import argparse
    parser = argparse.ArgumentParser(description="Surf Forecast Generator")
    parser.add_argument("--bundle-id", help="ID of the data bundle to use (defaults to latest)")
    parser.add_argument("--config", default="config.ini", help="Path to config file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    return parser.parse_args()

async def main():
    """Main entry point."""
    # Parse arguments
    args = parse_args()
    
    # Configure logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Add file handler for logging
    from datetime import datetime
    file_handler = logging.FileHandler(
        logs_dir / f"swellforecaster_{datetime.now().strftime('%Y%m%d')}.log"
    )
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    ))
    logging.getLogger().addHandler(file_handler)
    
    logger.info("Starting Surf Forecast Generator")
    
    # Load configuration
    import configparser
    config = configparser.ConfigParser()
    if os.path.exists(args.config):
        logger.info(f"Loading config from {args.config}")
        config.read(args.config)
    else:
        logger.warning(f"Config file {args.config} not found, using defaults")
    
    try:
        # Import fallback analyzer
        from forecast_tool.analyzer_fallback import analyze_and_generate
        
        # Run the analyzer
        logger.info(f"Running analyzer with bundle_id: {args.bundle_id}")
        result = await analyze_and_generate(config, args.bundle_id)
        
        if result:
            logger.info("Forecast generation successful")
            print("\n---------- FORECAST GENERATED SUCCESSFULLY ----------")
            print(f"Bundle ID: {result.get('bundle_id')}")
            print(f"Timestamp: {result.get('timestamp')}")
            print("\nOutput files:")
            for key, path in result.items():
                if key not in ["timestamp", "bundle_id", "run_id"]:
                    print(f"- {key.capitalize()}: {path}")
            print("\nUse these files to view the forecast:")
            for key, path in result.items():
                if key in ["markdown", "html", "pdf"]:
                    print(f"- {key.capitalize()}: open {path}")
            print("---------------------------------------------------\n")
            return 0
        else:
            logger.error("Forecast generation failed")
            print("\nForecast generation failed. See logs for details.")
            return 1
            
    except ImportError:
        logger.error("Failed to import analyzer_fallback module")
        print("\nError: Failed to import the analyzer module.")
        print("Make sure the src directory is in your Python path.")
        return 1
        
    except Exception as e:
        logger.error(f"Error during forecast generation: {e}")
        import traceback
        logger.error(traceback.format_exc())
        print(f"\nError: {e}")
        return 1

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        sys.exit(130)