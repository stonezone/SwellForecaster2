#!/usr/bin/env python3
"""
run_opc_enhanced.py - Enhanced Pat Caldwell Style Forecast with OPC Charts

This script is an enhanced version of run_caldwell.py that:
1. Collects essential OPC charts for wind/wave/swell forecasting
2. Adds them to the data bundle 
3. Uses them as part of the Pat Caldwell style forecast generation

This is the recommended script to run for the most detailed and accurate forecasts,
as it includes critical Pacific surface-pressure patterns, winds, and swell parameters
needed for proper marine forecasting.

Usage:
  python run_opc_enhanced.py                    # Run complete collection and analysis
  python run_opc_enhanced.py --analyze-only     # Only analyze using latest bundle
  python run_opc_enhanced.py --bundle-id ID     # Use specific bundle for analysis
  python run_opc_enhanced.py --debug            # Enable debug logging
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
        logging.FileHandler(logs_dir / f"opc_enhanced_{datetime.now().strftime('%Y%m%d')}.log")
    ]
)

logger = logging.getLogger("opc_enhanced_runner")
logger.info("Starting Enhanced OPC-Integrated Pat Caldwell Style Forecast Generator")

# Import the utilities module
try:
    import utils
    logger.warning("aiofiles not available, falling back to synchronous I/O with asyncio.to_thread")
    logger.warning("Consider installing aiofiles with 'pip install aiofiles' for better performance")
except ImportError:
    try:
        from forecast_tool import utils
        logger.info("Successfully set up compatibility bridges from package")
    except ImportError:
        logger.error("Could not import utils module")
        sys.exit(1)


def load_config(config_path):
    """Load configuration from file and return parsed config."""
    config = configparser.ConfigParser()
    try:
        with open(config_path, "r") as f:
            config.read_file(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading configuration: {e}")
        sys.exit(1)
    
    return config


async def collect_data(config_path):
    """
    Collect all data from all sources, including critical OPC charts.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Path to the data bundle directory, or None if collection failed
    """
    logger.info("Starting enhanced data collection phase with OPC charts")
    
    config = load_config(config_path)
    
    # Try normal data collection first
    bundle_path = None
    
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
        
        # Collect main bundle data
        bundle_path = await collector_revised.collect(config, Args())
    except ImportError:
        logger.warning("Could not import collector_revised from current directory")
    except Exception as e:
        logger.error(f"Error with collector_revised: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    # Try importing from the parent directory if needed
    if not bundle_path:
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
            
            # Collect main bundle data
            bundle_path = await collector_revised.collect(config, Args())
        except ImportError:
            logger.warning("Could not import collector_revised from parent directory")
        except Exception as e:
            logger.error(f"Error with parent collector_revised: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    # Try the forecast_tool package collector as last resort
    if not bundle_path:
        try:
            from forecast_tool.collector import collect_data
            logger.info("Using collect_data from forecast_tool package")
            bundle_path = await collect_data(config_path)
        except ImportError:
            logger.error("Could not import any collector module")
            return None
        except Exception as e:
            logger.error(f"Error with forecast_tool collector: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    # If we have a bundle, add OPC charts to it
    if bundle_path:
        try:
            # Import the specialized OPC chart collector
            from opc_collector import collect_opc_charts
            logger.info(f"Adding critical OPC charts to bundle: {bundle_path}")
            
            # Run the OPC collector
            await collect_opc_charts(bundle_path)
            logger.info("OPC charts added successfully")
        except ImportError:
            logger.warning("OPC collector not available - may affect forecast quality")
        except Exception as e:
            logger.error(f"Error collecting OPC charts: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    return bundle_path


async def generate_forecast(bundle_dir, config_path):
    """
    Generate a forecast from the collected data with Pat Caldwell styling.
    
    Args:
        bundle_dir: Path to the bundle directory
        config_path: Path to the configuration file
        
    Returns:
        True if forecast generation was successful, False otherwise
    """
    logger.info(f"Starting forecast generation for bundle: {bundle_dir}")
    logger.info(f"Bundle ID: {bundle_dir.name}")
    
    # Load configuration
    logger.info(f"Loading configuration from {config_path}")
    config = load_config(config_path)
    
    # Try using pat_caldwell_analyzer.py for specialized Pat Caldwell style
    try:
        import pat_caldwell_analyzer
        logger.info("Using specialized Pat Caldwell analyzer")
        
        # Generate forecast
        result = await pat_caldwell_analyzer.analyze_and_generate(config, bundle_dir.name)
        if result:
            logger.info("Forecast generation successful with Pat Caldwell analyzer")
            return True
        else:
            logger.warning("Pat Caldwell analyzer failed, trying fallbacks")
    except ImportError:
        logger.warning("Pat Caldwell analyzer not available, trying fallbacks")
    except Exception as e:
        logger.warning(f"Pat Caldwell analyzer failed: {e}")
        import traceback
        logger.warning(traceback.format_exc())
    
    # If pat_caldwell_analyzer failed, try pacific_forecast_analyzer_revised.py
    try:
        import pacific_forecast_analyzer_revised
        logger.info("Using pacific_forecast_analyzer_revised.py as fallback")
        
        # Generate forecast
        result = await pacific_forecast_analyzer_revised.analyze_and_generate(config, bundle_dir.name)
        if result:
            logger.info("Forecast generation successful with pacific_forecast_analyzer_revised")
            return True
        else:
            logger.warning("pacific_forecast_analyzer_revised failed")
    except ImportError:
        logger.warning("pacific_forecast_analyzer_revised.py not available")
    except SyntaxError:
        logger.warning("pacific_forecast_analyzer_revised.py has syntax errors")
    except Exception as e:
        logger.warning(f"Error with pacific_forecast_analyzer_revised: {e}")
        import traceback
        logger.warning(traceback.format_exc())
    
    # If that fails, try the analyzer_fallback from forecast_tool
    try:
        from forecast_tool import analyzer_fallback
        logger.info("Using analyzer_fallback from forecast_tool package")
        
        # Generate forecast
        result = await analyzer_fallback.analyze_and_generate(config, bundle_dir.name)
        if result:
            logger.info("Forecast generation successful with analyzer_fallback")
            return True
        else:
            logger.warning("analyzer_fallback failed")
    except ImportError:
        logger.warning("Could not import analyzer_fallback")
    except Exception as e:
        logger.warning(f"Error with analyzer_fallback: {e}")
        import traceback
        logger.warning(traceback.format_exc())
    
    # Last resort - try the simple_analyzer
    try:
        import simple_analyzer
        logger.info("Using simple_analyzer.py as last resort")
        
        # Generate forecast
        result = await simple_analyzer.generate_forecast(bundle_dir.name, config)
        if result:
            logger.info("Forecast generation successful with simple_analyzer")
            return True
        else:
            logger.error("All analyzers failed")
            return False
    except ImportError:
        logger.error("No analyzer modules available")
        return False
    except Exception as e:
        logger.error(f"Error with simple_analyzer: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


async def main():
    """Main entry point for the OPC-enhanced Pat Caldwell forecast generator."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Enhanced Pat Caldwell Style Forecast Generator with OPC Charts")
    parser.add_argument("--config", default="config.ini", help="Path to the configuration file")
    parser.add_argument("--collect-only", action="store_true", help="Only collect data, don't generate forecast")
    parser.add_argument("--analyze-only", action="store_true", help="Only analyze data, don't collect new data")
    parser.add_argument("--bundle-id", help="Use a specific bundle ID for analysis")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    # Set log level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("Debug logging enabled")
    
    # Record start time to calculate total execution time
    start_time = datetime.now()
    logger.info(f"Starting Pat Caldwell Forecast Generation at {start_time}")
    
    # Find the latest data bundle
    if args.bundle_id:
        logger.info(f"Using specified bundle ID: {args.bundle_id}")
    elif args.analyze_only:
        logger.info("Finding the latest data bundle")
    
    # Collect data if no bundle ID is specified or if collection is explicitly requested
    bundle_dir = None
    if not args.analyze_only and (not args.bundle_id or args.collect_only):
        bundle_dir = await collect_data(args.config)
        if not bundle_dir:
            logger.error("Data collection failed")
            return 1
        logger.info(f"Data collection completed successfully: {bundle_dir}")
        
        # Exit if only collection was requested
        if args.collect_only:
            logger.info("Collection only requested, exiting")
            return 0
    
    # Find the bundle directory to use for analysis
    if not bundle_dir:
        if args.bundle_id:
            data_dir = Path("pacific_data")
            bundle_dir = data_dir / args.bundle_id
            if not bundle_dir.exists():
                logger.error(f"Specified bundle directory does not exist: {bundle_dir}")
                return 1
        else:
            # Find the latest bundle
            data_dir = Path("pacific_data")
            if not data_dir.exists():
                logger.error("Data directory not found")
                return 1
                
            # First try reading latest_bundle.txt
            latest_path = data_dir / "latest_bundle.txt"
            if latest_path.exists():
                try:
                    with open(latest_path, "r") as f:
                        bundle_id = f.read().strip()
                        bundle_dir = data_dir / bundle_id
                        logger.info(f"Using latest bundle from file: {bundle_id}")
                except Exception as e:
                    logger.warning(f"Error reading latest bundle file: {e}")
                    bundle_dir = None
            
            # If that failed, use directory timestamps
            if not bundle_dir or not bundle_dir.exists():
                logger.info("Finding latest bundle by timestamp")
                bundles = list(data_dir.glob("*_*"))
                if not bundles:
                    logger.error("No data bundles found")
                    return 1
                
                bundles.sort(key=lambda p: p.name.split("_")[-1], reverse=True)
                bundle_dir = bundles[0]
                logger.info(f"Using most recent bundle: {bundle_dir.name}")
    
    # Generate forecast from the selected bundle
    success = await generate_forecast(bundle_dir, args.config)
    if success:
        logger.info("Forecast generation completed successfully")
        
        # Print summary for the user
        print(f"\n---------- OPC-ENHANCED PAT CALDWELL STYLE FORECAST GENERATED ----------")
        print(f"Bundle ID: {bundle_dir.name}")
        
        # Determine timestamp from generated files
        forecasts_dir = Path("forecasts")
        timestamp = None
        for forecast_file in forecasts_dir.glob(f"forecast_*.md"):
            timestamp = forecast_file.stem.replace("forecast_", "")
            break
        
        if timestamp:
            print(f"Timestamp: {timestamp}")
            print(f"\nOutput files:")
            
            # Check which formats were generated
            file_info = []
            md_path = forecasts_dir / f"forecast_{timestamp}.md"
            if md_path.exists():
                file_info.append(f"- Markdown: {md_path}")
            
            html_path = forecasts_dir / f"forecast_{timestamp}.html"
            if html_path.exists():
                file_info.append(f"- Html: {html_path}")
            
            pdf_path = forecasts_dir / f"forecast_{timestamp}.pdf"
            if pdf_path.exists():
                file_info.append(f"- Pdf: {pdf_path}")
            
            # Print file info
            for info in file_info:
                print(info)
            
            # Print commands to open files
            print(f"\nUse these files to view the forecast:")
            if md_path.exists():
                print(f"- Open Markdown: open {md_path}")
            if html_path.exists():
                print(f"- Open Html: open {html_path}")
            if pdf_path.exists():
                print(f"- Open Pdf: open {pdf_path}")
        
        print(f"-----------------------------------------------------------")
    else:
        logger.error("Forecast generation failed")
        return 1
    
    # Calculate and log execution time
    end_time = datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    logger.info(f"Total execution time: {execution_time:.2f} seconds")
    
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))