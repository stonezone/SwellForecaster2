#!/usr/bin/env python3
"""
SwellForecaster Production Run Script - Pat Caldwell Style

This script implements a complete workflow for SwellForecaster that:
1. Collects all data from all available sources
2. Generates a detailed forecast using Pat Caldwell's distinctive style
3. Ensures equal emphasis on North Shore ("Country") and South Shore ("Town")
4. Uses GPT-4.1 with specific temperature and token settings for forecasting quality
5. Includes proper error handling and fallbacks throughout the process

Usage:
  python run_production.py                    # Run complete collection and analysis
  python run_production.py --collect-only     # Only collect data, don't generate forecast  
  python run_production.py --analyze-only     # Only analyze using latest bundle
  python run_production.py --bundle-id ID     # Use specific bundle for analysis
  python run_production.py --debug            # Enable debug logging
"""
import argparse
import asyncio
import configparser
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Ensure we're using modules from the current directory first
current_dir = str(Path(__file__).resolve().parent)
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Make sure the src directory is in the path
src_dir = os.path.join(current_dir, "src")
if os.path.exists(src_dir) and src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Also add parent directory to path for finding modules
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(1, parent_dir)

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
logger.info("Starting SwellForecaster Production Run")

# Import the compatibility bridges to ensure all modules work properly
try:
    try:
        from forecast_tool.compatibility import setup_all_compatibility_bridges
        setup_all_compatibility_bridges()
        logger.info("Set up compatibility bridges from package")
    except ImportError:
        # Try with direct import from src path
        sys.path.insert(0, os.path.join(current_dir, "src"))
        try:
            from forecast_tool.compatibility import setup_all_compatibility_bridges
            setup_all_compatibility_bridges()
            logger.info("Set up compatibility bridges from src path")
        except ImportError:
            logger.warning("Compatibility bridges not available in src path")
except Exception as e:
    logger.warning(f"Could not set up compatibility bridges: {e}")
    logger.warning("Using legacy imports instead")

# Import utilities module
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


def load_pat_caldwell_style():
    """Load the Pat Caldwell style guide from the depreciated folder."""
    style_path = Path(current_dir) / "depreciated" / "pat_caldwell_style.md"
    
    if not style_path.exists():
        logger.warning("Pat Caldwell style guide not found")
        return None
    
    try:
        with open(style_path, "r") as f:
            style_guide = f.read()
        logger.info("Loaded Pat Caldwell style guide")
        return style_guide
    except Exception as e:
        logger.warning(f"Failed to load Pat Caldwell style guide: {e}")
        return None


async def collect_data(config_path):
    """
    Collect all data from all sources.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Path to the data bundle directory, or None if collection failed
    """
    logger.info("Starting data collection phase")
    
    config = load_config(config_path)
    
    # First try to use the collector_revised.py in the current directory
    try:
        import collector_revised
        logger.info("Using collector_revised.py from current directory")
        
        # Initialize the arguments needed by the legacy collector
        class Args:
            def __init__(self):
                self.config = config_path
                self.dry_run = False
                self.debug = False
                self.cache_days = 7
        
        return await collector_revised.collect(config, Args())
    except ImportError:
        logger.warning("Could not import collector_revised from current directory")
    except Exception as e:
        logger.error(f"Error with collector_revised: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    # Try importing from the parent directory as fallback
    try:
        sys.path.insert(0, parent_dir)
        import collector_revised
        logger.info("Using collector_revised.py from parent directory")
        
        # Initialize the arguments needed by the legacy collector
        class Args:
            def __init__(self):
                self.config = config_path
                self.dry_run = False
                self.debug = False
                self.cache_days = 7
        
        return await collector_revised.collect(config, Args())
    except ImportError:
        logger.warning("Could not import collector_revised from parent directory")
    except Exception as e:
        logger.error(f"Error with parent collector_revised: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    # As a last resort, try using the forecast_tool package collector
    try:
        from forecast_tool.collector import collect_data
        logger.info("Using collect_data from forecast_tool package")
        return await collect_data(config_path)
    except ImportError:
        logger.error("Could not import any collector module")
        return None
    except Exception as e:
        logger.error(f"Error with forecast_tool collector: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


async def analyze_and_generate(config, bundle_id, pat_caldwell_style=None):
    """
    Custom wrapper around analyze_and_generate that injects Pat Caldwell style.
    
    Args:
        config: ConfigParser object with configuration
        bundle_id: ID of the bundle to analyze
        pat_caldwell_style: Optional Pat Caldwell style guide
        
    Returns:
        Dictionary with paths to generated forecast files, or None if generation failed
    """
    logger.info(f"Starting analysis with Pat Caldwell style for bundle: {bundle_id}")
    
    # Try to use pacific_forecast_analyzer_revised.py with our style enhancements
    try:
        import pacific_forecast_analyzer_revised
        original_module = pacific_forecast_analyzer_revised
        
        # Save original analyze_data function
        if hasattr(original_module, "analyze_data"):
            original_analyze_data = original_module.analyze_data
            
            # Replace with our enhanced version
            async def enhanced_analyze_data(bundle_data, bundle_id, *args, **kwargs):
                logger.info("Using enhanced analyze_data with Pat Caldwell style")
                
                # Set temperature to a more creative value for Pat's style
                kwargs['temperature'] = 0.7
                
                # Set a higher token limit for detailed forecasts
                kwargs['max_tokens'] = 4096
                
                # Ensure equal emphasis on North and South shores
                kwargs['north_emphasis'] = "true"
                kwargs['south_emphasis'] = "true"
                
                # Add Pat Caldwell style guide to the system prompt
                if pat_caldwell_style and 'system_prompt' in kwargs:
                    kwargs['system_prompt'] += f"\n\nFORECASTING STYLE GUIDE:\n{pat_caldwell_style}"
                    logger.info("Added Pat Caldwell style guide to system prompt")
                
                # Call the original function with our enhanced kwargs
                return await original_analyze_data(bundle_data, bundle_id, *args, **kwargs)
            
            # Replace the function
            pacific_forecast_analyzer_revised.analyze_data = enhanced_analyze_data
            
            try:
                # Set equal emphasis in config
                if "FORECAST" in config:
                    config["FORECAST"]["north_swell_emphasis"] = "true"
                    config["FORECAST"]["south_swell_emphasis"] = "true"
                
                # Call the original analyze_and_generate (which will use our enhanced analyze_data)
                result = await pacific_forecast_analyzer_revised.analyze_and_generate(config, bundle_id)
                
                # Restore the original function
                pacific_forecast_analyzer_revised.analyze_data = original_analyze_data
                
                return result
            except Exception as e:
                # Restore the original function if anything goes wrong
                pacific_forecast_analyzer_revised.analyze_data = original_analyze_data
                raise e
        else:
            # If analyze_data isn't available, just call the function directly
            return await pacific_forecast_analyzer_revised.analyze_and_generate(config, bundle_id)
    
    except ImportError:
        logger.warning("Could not import pacific_forecast_analyzer_revised")
    except Exception as e:
        logger.error(f"Error with enhanced analyzer: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    # Try the fallback analyzer if the main one fails
    try:
        from forecast_tool.analyzer_fallback import analyze_and_generate
        logger.info("Using analyzer_fallback from forecast_tool package")
        
        # Set equal emphasis in config
        if "FORECAST" in config:
            config["FORECAST"]["north_swell_emphasis"] = "true"
            config["FORECAST"]["south_swell_emphasis"] = "true"
        
        return await analyze_and_generate(config, bundle_id)
    except ImportError:
        logger.warning("Could not import analyzer_fallback")
    
    # Try the simple analyzer as a last resort
    try:
        import simple_analyzer
        logger.info("Using simple_analyzer.py as last resort")
        return await simple_analyzer.analyze_and_generate(config, bundle_id)
    except ImportError:
        logger.error("Could not import any analyzer module")
    except Exception as e:
        logger.error(f"Error with simple analyzer: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    return None


async def generate_forecast(bundle_dir, config_path):
    """
    Generate a forecast from the collected data with Pat Caldwell styling.
    
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
    
    # Extract bundle ID
    bundle_id = bundle_dir.name if isinstance(bundle_dir, Path) else os.path.basename(str(bundle_dir))
    logger.info(f"Bundle ID: {bundle_id}")
    
    # Load config
    config = load_config(config_path)
    
    # Load Pat Caldwell style guide
    pat_caldwell_style = load_pat_caldwell_style()
    if pat_caldwell_style:
        logger.info("Loaded Pat Caldwell style guide for forecast generation")
    else:
        logger.warning("Could not load Pat Caldwell style guide")
    
    # Try our enhanced analyzer with Pat Caldwell style
    result = await analyze_and_generate(config, bundle_id, pat_caldwell_style)
    
    if result:
        logger.info("Forecast generation successful")
        return result
    else:
        logger.error("All forecast generation attempts failed")
        return None


async def main():
    """Main entry point."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="SwellForecaster - Production Forecast Generator")
    parser.add_argument("--config", default="config.ini", help="Path to configuration file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--collect-only", action="store_true", help="Only collect data, don't generate forecast")
    parser.add_argument("--analyze-only", action="store_true", help="Only analyze existing data, don't collect new data")
    parser.add_argument("--bundle-id", help="Use a specific bundle ID (skip collection)")
    args = parser.parse_args()
    
    # Set debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    # Start the timer
    start_time = datetime.now()
    logger.info(f"Starting SwellForecaster Production Run at {start_time}")
    
    # Collect data if no bundle ID is specified or if collection is explicitly requested
    bundle_dir = None
    if not args.analyze_only and (not args.bundle_id or args.collect_only):
        bundle_dir = await collect_data(args.config)
        if not bundle_dir:
            logger.error("Data collection failed, aborting")
            return 1
        logger.info(f"Data collection completed successfully: {bundle_dir}")
    elif args.bundle_id:
        # Use the specified bundle ID
        logger.info(f"Using specified bundle ID: {args.bundle_id}")
        data_dir = Path("pacific_data")
        bundle_dir = data_dir / args.bundle_id
        if not bundle_dir.exists():
            logger.error(f"Bundle directory not found: {bundle_dir}")
            return 1
    else:
        # Find the latest bundle
        logger.info("Finding the latest data bundle")
        data_dir = Path("pacific_data")
        if not data_dir.exists():
            logger.error("Data directory not found")
            return 1
            
        # Try to read latest_bundle.txt
        try:
            with open(data_dir / "latest_bundle.txt", "r") as f:
                bundle_id = f.read().strip()
                bundle_dir = data_dir / bundle_id
                logger.info(f"Using latest bundle from file: {bundle_id}")
        except:
            # Find the most recent bundle
            bundles = [p for p in data_dir.glob("*_*") if p.is_dir()]
            if not bundles:
                logger.error("No bundles found")
                return 1
                
            # Sort by timestamp (assuming bundle ID ends with timestamp)
            bundles.sort(key=lambda p: p.name.split("_")[-1], reverse=True)
            bundle_dir = bundles[0]
            logger.info(f"Using most recent bundle: {bundle_dir.name}")
    
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