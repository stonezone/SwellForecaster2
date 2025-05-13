#!/usr/bin/env python3
"""
run_caldwell.py - Pat Caldwell Style Forecast Generator

This script runs the Pat Caldwell style forecast generator, which:
1. Collects data from all configured sources (or uses an existing bundle)
2. Generates detailed forecasts with Pat Caldwell's distinctive style
3. Ensures equal emphasis on North Shore ("Country") and South Shore ("Town")
4. Uses GPT-4.1 with temperature settings optimized for Pat Caldwell-style writing

Usage:
  python run_caldwell.py                    # Run complete collection and analysis
  python run_caldwell.py --collect-only     # Only collect data, don't generate forecast  
  python run_caldwell.py --analyze-only     # Only analyze using latest bundle
  python run_caldwell.py --bundle-id ID     # Use specific bundle for analysis
  python run_caldwell.py --debug            # Enable debug logging
"""
import argparse
import asyncio
import configparser
import logging
import os
import sys
from datetime import datetime
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
        logging.FileHandler(logs_dir / f"caldwell_{datetime.now().strftime('%Y%m%d')}.log")
    ]
)

logger = logging.getLogger("caldwell_runner")
logger.info("Starting Pat Caldwell Style Forecast Generator")

# Import the utilities module
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
    
    # Add OPC chart collection to the process for critical charts
    
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
    parent_dir = os.path.dirname(current_dir)
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
    
    # Try our specialized Pat Caldwell analyzer first
    try:
        import pat_caldwell_analyzer
        logger.info("Using specialized Pat Caldwell analyzer")
        
        # Run the analyzer
        result = await pat_caldwell_analyzer.analyze_and_generate(config, bundle_id)
        
        if result:
            logger.info("Forecast generation successful with Pat Caldwell analyzer")
            return result
        else:
            logger.warning("Pat Caldwell analyzer failed, trying fallbacks")
    except ImportError:
        logger.warning("Pat Caldwell analyzer not available, trying fallbacks")
    except Exception as e:
        logger.error(f"Error with Pat Caldwell analyzer: {e}")
        import traceback
        logger.error(traceback.format_exc())
        logger.warning("Trying fallback analyzers")
    
    # Try the pacific_forecast_analyzer_revised.py fallback
    try:
        # First try in current directory
        try:
            import pacific_forecast_analyzer_revised
            logger.info("Using pacific_forecast_analyzer_revised.py")
            
            # Set equal emphasis in config
            if "FORECAST" in config:
                config["FORECAST"]["north_swell_emphasis"] = "true"
                config["FORECAST"]["south_swell_emphasis"] = "true"
                
            if "GENERAL" in config:
                # Set parameters for Pat Caldwell style
                config["GENERAL"]["temperature"] = "0.7"
                config["GENERAL"]["max_tokens"] = "4096"
            
            return await pacific_forecast_analyzer_revised.analyze_and_generate(config, bundle_id)
        except SyntaxError:
            logger.warning("pacific_forecast_analyzer_revised.py has syntax errors")
        except Exception as e:
            logger.error(f"Error with pacific_forecast_analyzer: {e}")
            import traceback
            logger.error(traceback.format_exc())
    except ImportError:
        logger.warning("Could not import pacific_forecast_analyzer_revised")
    
    # Try the fallback analyzer from forecast_tool
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
        return None
    except Exception as e:
        logger.error(f"Error with simple analyzer: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


async def main():
    """Main entry point."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Pat Caldwell Style Surf Forecast Generator")
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
    logger.info(f"Starting Pat Caldwell Forecast Generation at {start_time}")
    
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
    
    print("\n---------- PAT CALDWELL STYLE FORECAST GENERATED ----------")
    print(f"Bundle ID: {forecast.get('bundle_id', 'N/A')}")
    print(f"Timestamp: {forecast.get('timestamp', 'N/A')}")
    print("\nOutput files:")
    for key, path in forecast.items():
        if key not in ["timestamp", "bundle_id", "run_id"]:
            print(f"- {key.capitalize()}: {path}")
    print("\nUse these files to view the forecast:")
    for key, path in forecast.items():
        if key in ["markdown", "html", "pdf"]:
            print(f"- Open {key.capitalize()}: open {path}")
    print("-----------------------------------------------------------\n")
    
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