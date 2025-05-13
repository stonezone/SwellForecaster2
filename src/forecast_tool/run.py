"""Main entry point for SwellForecaster."""
import argparse
import asyncio
import importlib.util
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union, Any

from .analyzer import analyze_data
from .collector import collect_data
from .models.settings import load_settings
from .utils import setup_logging

logger = logging.getLogger("swellforecaster")


async def run_forecaster(
    config_path: str = "config.ini",
    collect: bool = True,
    analyze: bool = True,
    bundle_id: Optional[str] = None,
    debug: bool = False
) -> int:
    """
    Run the SwellForecaster workflow.

    Args:
        config_path: Path to the configuration file
        collect: Whether to collect data
        analyze: Whether to analyze data and generate a forecast
        bundle_id: Specific bundle ID to analyze (defaults to latest)
        debug: Enable debug mode

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Load settings
    settings = load_settings(config_path)

    # Configure logging based on debug flag
    log_level = "DEBUG" if debug else "INFO"
    setup_logging(log_dir=Path(settings.general.data_dir).parent / "logs", log_level=log_level)

    # Set up compatibility layer for legacy code
    await _setup_compatibility_bridges()

    # Print welcome message
    logger.info("Starting SwellForecaster")

    try:
        # Collect data if requested
        if collect:
            logger.info("Starting data collection")

            try:
                bundle_dir = await collect_data(config_path)

                if bundle_dir:
                    logger.info(f"Data collection complete: {bundle_dir}")

                    # Use this bundle for analysis if none specified
                    if analyze and not bundle_id:
                        bundle_id = bundle_dir.name
                else:
                    logger.error("Data collection failed")
                    return 1

            except Exception as e:
                logger.error(f"Error during data collection: {e}")
                return 1

        # Analyze data if requested
        if analyze:
            logger.info("Starting forecast generation")

            try:
                # Determine if we should use the legacy analyzer
                legacy_analyzer_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                    "pacific_forecast_analyzer_revised.py"
                )

                if os.path.exists(legacy_analyzer_path):
                    logger.info("Using legacy analyzer script")
                    success = await _run_legacy_analyzer(bundle_id, config_path)

                    if not success:
                        logger.error("Legacy analysis failed")
                        return 1

                    logger.info("Legacy forecast generation complete")
                else:
                    # Use the new analyzer implementation
                    forecast = await analyze_data(config_path, bundle_id)

                    if forecast:
                        logger.info(f"Forecast generation complete")
                        logger.info(f"Markdown: {forecast.get('run_id')}/{forecast.get('timestamp')}.md")
                        logger.info(f"HTML: {forecast.get('run_id')}/{forecast.get('timestamp')}.html")
                        logger.info(f"PDF: {forecast.get('run_id')}/{forecast.get('timestamp')}.pdf")
                    else:
                        logger.error("Forecast generation failed")
                        return 1

            except Exception as e:
                logger.error(f"Error during forecast generation: {e}")
                return 1

        logger.info("SwellForecaster completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Unhandled error in SwellForecaster: {e}")
        return 1


async def _setup_compatibility_bridges() -> None:
    """
    Set up compatibility bridges between new utilities and legacy code.

    This creates a compatibility layer that allows the legacy code to use our new utilities.
    """
    logger.debug("Setting up compatibility bridges for legacy code")

    # Import our new utilities
    from . import utils as new_utils

    # Create a proxy module for the legacy code to use
    import types
    import sys

    # Create or modify the existing utils module
    if "utils" not in sys.modules:
        sys.modules["utils"] = types.ModuleType("utils")

    # Add our utility functions to the legacy module
    legacy_utils = sys.modules["utils"]

    # Add the functions that are used in the legacy code
    setattr(legacy_utils, "utcnow", new_utils.utcnow)
    setattr(legacy_utils, "jdump", new_utils.jdump)
    setattr(legacy_utils, "get_south_swell_status", new_utils.get_south_swell_status)

    # Define a log_init function that uses our new logging setup
    def log_init(name: str, level: str = "INFO") -> logging.Logger:
        """Initialize logging with file and console output."""
        logger = logging.getLogger(name)
        return logger

    setattr(legacy_utils, "log_init", log_init)

    # Add other utility functions as needed
    logger.debug("Compatibility bridges set up successfully")


async def _run_legacy_analyzer(bundle_id: Optional[str], config_path: str) -> bool:
    """
    Run the legacy analyzer script.

    Args:
        bundle_id: Optional bundle ID for analysis
        config_path: Path to the configuration file

    Returns:
        True if successful, False otherwise
    """
    try:
        # Find the path to the legacy analyzer
        legacy_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "pacific_forecast_analyzer_revised.py"
        )

        logger.info(f"Running legacy analyzer: {legacy_path}")

        # Import the legacy analyzer
        spec = importlib.util.spec_from_file_location(
            "pacific_forecast_analyzer_revised",
            legacy_path
        )

        legacy_analyzer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(legacy_analyzer)

        # Build the argument list
        args = []

        if config_path and config_path != "config.ini":
            args.extend(["--config", config_path])

        if bundle_id:
            args.extend(["--bundle-id", bundle_id])

        # Check if the module has main or main_async
        if hasattr(legacy_analyzer, "main_async"):
            result = await legacy_analyzer.main_async(args)
            return result == 0
        elif hasattr(legacy_analyzer, "main"):
            # Call the synchronous main function with the arguments
            result = legacy_analyzer.main(args)
            return result == 0
        else:
            # Try to call analyze_and_generate directly
            if hasattr(legacy_analyzer, "analyze_and_generate"):
                result = await legacy_analyzer.analyze_and_generate(bundle_id=bundle_id)
                return result is not None
            else:
                logger.error("Could not find entry point in legacy analyzer")
                return False

    except Exception as e:
        logger.error(f"Error running legacy analyzer: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


async def main_async() -> int:
    """
    Main async function for SwellForecaster.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="SwellForecaster")
    parser.add_argument("--config", default="config.ini", help="Path to configuration file")
    parser.add_argument("--collect-only", action="store_true", help="Only collect data, don't generate forecast")
    parser.add_argument("--analyze-only", action="store_true", help="Only generate forecast, don't collect data")
    parser.add_argument("--bundle-id", help="Specific bundle ID to analyze (defaults to latest)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    # Determine actions based on arguments
    collect = not args.analyze_only
    analyze = not args.collect_only

    # Run the forecaster
    return await run_forecaster(args.config, collect, analyze, args.bundle_id, args.debug)


def main() -> int:
    """
    Main entry point for SwellForecaster.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Setup initial logging (will be reconfigured once settings are loaded)
    logs_dir = Path(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))) / "logs"
    logs_dir.mkdir(exist_ok=True)
    setup_logging(log_dir=logs_dir)

    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nSwellForecaster interrupted by user")
        return 130


if __name__ == "__main__":
    sys.exit(main())