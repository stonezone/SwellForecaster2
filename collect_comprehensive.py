#!/usr/bin/env python3
"""
collect_comprehensive.py - Comprehensive data collection from ALL sources

This script collects data from ALL available sources in the repository:
1. All buoy agents (NDBC, CDIP, CO-OPS)
2. All chart agents (OPC, WPC, NWS)
3. All model agents (WW3, PacIOOS, ECMWF)
4. All API agents (Windy, Open-Meteo, Stormglass, Surfline)
5. All regional agents (Southern Hemisphere, North Pacific)

The script organizes the data collection process with:
- Proper throttling for rate-limited APIs
- SSL verification exceptions for problematic domains
- Fallbacks for sources that may be unavailable
- Proper error handling for robust collection
- Detailed logging and collection statistics
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
from typing import Dict, List, Any, Optional

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
        logging.FileHandler(logs_dir / f"comprehensive_collector_{datetime.now().strftime('%Y%m%d')}.log")
    ]
)

logger = logging.getLogger("comprehensive_collector")
logger.info("Starting comprehensive data collection")

# Import utilities and agents
import utils

# Import all agent modules and functions
from agents import (
    # Buoy agents
    buoys, noaa_coops,
    # Chart agents
    opc, wpc, nws,
    # Model agents
    pacioos, pacioos_swan, model_agent, ww3_model_fallback, ecmwf_wave, ecmwf_agent,
    # API agents
    windy, open_meteo, stormglass, surfline,
    # Regional agents
    southern_hemisphere, north_pacific_enhanced,
    # All agent lists from __init__.py
    BUOY_AGENTS, CHART_AGENTS, MODEL_AGENTS, API_AGENTS, REGION_AGENTS, ALL_AGENTS
)

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
    Optimize the collection config for comprehensive collection.
    """
    # Increase timeouts and retries
    if 'GENERAL' in config:
        config['GENERAL']['timeout'] = '60'
        config['GENERAL']['max_retries'] = '5'
        logger.info("Increased timeouts and retries for more reliable collection")
    
    # Ensure ALL sources are enabled
    if 'SOURCES' in config:
        sources = [
            'enable_buoys', 'enable_coops', 'enable_models', 'enable_open_meteo',
            'enable_windy', 'enable_pacioos', 'enable_pacioos_swan', 'enable_nws',
            'enable_opc', 'enable_wpc', 'enable_stormglass', 'enable_surfline',
            'enable_ecmwf', 'enable_southern_hemisphere', 'enable_north_pacific',
            'fallback_to_tropicaltidbits'
        ]
        
        for source in sources:
            if source in config['SOURCES']:
                config['SOURCES'][source] = 'true'
        
        logger.info("Ensured all data sources are enabled for comprehensive collection")
    
    # Set SSL exceptions for problematic domains based on experience
    if 'SSL_EXCEPTIONS' not in config:
        config['SSL_EXCEPTIONS'] = {}
    
    config['SSL_EXCEPTIONS']['disable_verification'] = 'cdip.ucsd.edu,tgftp.nws.noaa.gov,ocean.weather.gov,www.fnmoc.navy.mil,www.opc.ncep.noaa.gov,pacioos.hawaii.edu,pae-paha.pacioos.hawaii.edu,magicseaweed.com,bom.gov.au,metservice.com'
    logger.info("Set SSL exceptions for known problematic domains")
    
    return config

async def collect_buoy_data(ctx, session):
    """Collect data from all buoy agents."""
    results = []
    
    try:
        # NDBC buoy data
        buoy_results = await buoys(ctx, session)
        results.extend(buoy_results)
        logger.info(f"Collected {len(buoy_results)} NDBC buoy data files")
        
        # NOAA CO-OPS stations
        coops_results = await noaa_coops(ctx, session)
        results.extend(coops_results)
        logger.info(f"Collected {len(coops_results)} NOAA CO-OPS station data files")
        
    except Exception as e:
        logger.error(f"Error in buoy data collection: {e}")
    
    return results

async def collect_chart_data(ctx, session):
    """Collect data from all chart agents."""
    results = []
    
    try:
        # OPC charts
        opc_results = await opc(ctx, session)
        results.extend(opc_results)
        logger.info(f"Collected {len(opc_results)} OPC chart files")
        
        # WPC charts
        wpc_results = await wpc(ctx, session)
        results.extend(wpc_results)
        logger.info(f"Collected {len(wpc_results)} WPC chart files")
        
        # NWS forecasts
        nws_results = await nws(ctx, session)
        results.extend(nws_results)
        logger.info(f"Collected {len(nws_results)} NWS forecast files")
        
    except Exception as e:
        logger.error(f"Error in chart data collection: {e}")
    
    return results

async def collect_model_data(ctx, session):
    """Collect data from all model agents with fallbacks."""
    results = []
    
    try:
        # WaveWatch III model - with fallback mechanism
        try:
            # Try primary WW3 source first
            model_results = await model_agent(ctx, session)
            if model_results:
                results.extend(model_results)
                logger.info(f"Collected {len(model_results)} WW3 model data files")
            else:
                # Use fallback if primary fails
                fallback_results = await ww3_model_fallback(ctx, session)
                results.extend(fallback_results)
                logger.info(f"Collected {len(fallback_results)} WW3 fallback model data files")
        except Exception as e:
            logger.error(f"Error in WW3 model collection: {e}")
            # Try fallback on exception
            try:
                fallback_results = await ww3_model_fallback(ctx, session)
                results.extend(fallback_results)
                logger.info(f"Collected {len(fallback_results)} WW3 fallback model data files")
            except Exception as e:
                logger.error(f"Error in WW3 fallback model collection: {e}")
        
        # PacIOOS regional wave models
        try:
            pacioos_results = await pacioos(ctx, session)
            results.extend(pacioos_results)
            logger.info(f"Collected {len(pacioos_results)} PacIOOS model data files")
        except Exception as e:
            logger.error(f"Error in PacIOOS model collection: {e}")
        
        # PacIOOS SWAN nearshore models
        try:
            swan_results = await pacioos_swan(ctx, session)
            results.extend(swan_results)
            logger.info(f"Collected {len(swan_results)} PacIOOS SWAN model data files")
        except Exception as e:
            logger.error(f"Error in PacIOOS SWAN model collection: {e}")
        
        # ECMWF wave models - try both implementations
        try:
            if ctx.cfg["API"].get("ECMWF_KEY", "").strip():
                # Enhanced ECMWF wave collection
                ecmwf_results = await ecmwf_wave(ctx, session)
                results.extend(ecmwf_results)
                logger.info(f"Collected {len(ecmwf_results)} ECMWF wave model data files")
                
                # Try alternative ECMWF implementation as well for redundancy
                alt_ecmwf_results = await ecmwf_agent(ctx, session)
                results.extend(alt_ecmwf_results)
                logger.info(f"Collected {len(alt_ecmwf_results)} alternative ECMWF model data files")
            else:
                logger.warning("ECMWF API key not found, skipping ECMWF models")
        except Exception as e:
            logger.error(f"Error in ECMWF model collection: {e}")
        
    except Exception as e:
        logger.error(f"Error in model data collection: {e}")
    
    return results

async def collect_api_data(ctx, session):
    """Collect data from all API-based agents."""
    results = []
    
    try:
        # Windy API
        try:
            windy_results = await windy(ctx, session)
            results.extend(windy_results)
            logger.info(f"Collected {len(windy_results)} Windy API data files")
        except Exception as e:
            logger.error(f"Error in Windy API collection: {e}")
        
        # Open-Meteo API
        try:
            openmeteo_results = await open_meteo(ctx, session)
            results.extend(openmeteo_results)
            logger.info(f"Collected {len(openmeteo_results)} Open-Meteo API data files")
        except Exception as e:
            logger.error(f"Error in Open-Meteo API collection: {e}")
        
        # Stormglass API
        try:
            if ctx.cfg["API"].get("STORMGLASS_KEY", "").strip():
                stormglass_results = await stormglass(ctx, session)
                results.extend(stormglass_results)
                logger.info(f"Collected {len(stormglass_results)} Stormglass API data files")
            else:
                logger.warning("Stormglass API key not found, skipping Stormglass")
        except Exception as e:
            logger.error(f"Error in Stormglass API collection: {e}")
        
        # Surfline API
        try:
            surfline_results = await surfline(ctx, session)
            results.extend(surfline_results)
            logger.info(f"Collected {len(surfline_results)} Surfline API data files")
        except Exception as e:
            logger.error(f"Error in Surfline API collection: {e}")
        
    except Exception as e:
        logger.error(f"Error in API data collection: {e}")
    
    return results

async def collect_regional_data(ctx, session):
    """Collect data from all regional agents."""
    results = []
    
    try:
        # Southern Hemisphere data (critical for South Shore forecasts)
        try:
            south_results = await southern_hemisphere(ctx, session)
            results.extend(south_results)
            logger.info(f"Collected {len(south_results)} Southern Hemisphere data files")
        except Exception as e:
            logger.error(f"Error in Southern Hemisphere data collection: {e}")
        
        # North Pacific data (critical for North Shore forecasts)
        try:
            north_results = await north_pacific_enhanced(ctx, session)
            results.extend(north_results)
            logger.info(f"Collected {len(north_results)} North Pacific data files")
        except Exception as e:
            logger.error(f"Error in North Pacific data collection: {e}")
        
    except Exception as e:
        logger.error(f"Error in regional data collection: {e}")
    
    return results

async def generate_summary(bundle_dir, results):
    """Generate a detailed summary of the collected data."""
    summary = {
        "bundle_id": bundle_dir.name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_files": len(results),
        "file_types": {},
        "sources": {},
        "shore_specific": {
            "north_shore": 0,
            "south_shore": 0,
            "both_shores": 0,
            "unspecified": 0
        },
        "priorities": {
            "high": 0,
            "medium": 0,
            "low": 0
        },
        "total_size_bytes": 0
    }
    
    # Process file results
    for result in results:
        # Count by source
        source = result.get("source", "unknown")
        if source not in summary["sources"]:
            summary["sources"][source] = 0
        summary["sources"][source] += 1
        
        # Count by type
        file_type = result.get("type", "unknown")
        if file_type not in summary["file_types"]:
            summary["file_types"][file_type] = 0
        summary["file_types"][file_type] += 1
        
        # Count by shore relevance
        north_facing = result.get("north_facing", False)
        south_facing = result.get("south_facing", False)
        
        if north_facing and south_facing:
            summary["shore_specific"]["both_shores"] += 1
        elif north_facing:
            summary["shore_specific"]["north_shore"] += 1
        elif south_facing:
            summary["shore_specific"]["south_shore"] += 1
        else:
            summary["shore_specific"]["unspecified"] += 1
        
        # Count by priority
        priority = result.get("priority", 2)
        if priority == 0:
            summary["priorities"]["high"] += 1
        elif priority == 1:
            summary["priorities"]["medium"] += 1
        else:
            summary["priorities"]["low"] += 1
        
        # Calculate size if the file exists
        if "filename" in result:
            file_path = bundle_dir / result["filename"]
            if file_path.exists():
                summary["total_size_bytes"] += file_path.stat().st_size
    
    # Convert size to MB for readability
    summary["total_size_mb"] = round(summary["total_size_bytes"] / (1024 * 1024), 2)
    
    # Generate details about shore-specific data
    summary["shore_coverage"] = {
        "north_shore_percentage": round(100 * (summary["shore_specific"]["north_shore"] + summary["shore_specific"]["both_shores"]) / max(1, len(results)), 1),
        "south_shore_percentage": round(100 * (summary["shore_specific"]["south_shore"] + summary["shore_specific"]["both_shores"]) / max(1, len(results)), 1)
    }
    
    # Save summary to the bundle directory
    summary_path = bundle_dir / "collection_summary.json"
    await utils.write_file_async(summary_path, json.dumps(summary, indent=2))
    
    return summary

async def collect_comprehensive_data(config_path="config.ini"):
    """
    Run a comprehensive data collection from all available sources.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Path to the data bundle directory, or None if collection failed
    """
    logger.info("Starting comprehensive data collection")
    
    # Load and optimize config for comprehensive collection
    config = load_config(config_path)
    config = await optimize_config(config)
    
    # Create context
    ctx = Ctx(config)
    
    # Use a longer timeout for network operations
    timeout = aiohttp.ClientTimeout(total=180)  # 3 minutes timeout
    
    # Create connector with SSL settings that will be determined per request
    connector = aiohttp.TCPConnector(ssl=None)  # Will be set per request
    logger.info("Using domain-specific SSL verification bypass")
    
    session = None
    all_results = []
    
    try:
        session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        
        # Collect data from each category, handling errors separately
        # to prevent one category failure from affecting others
        
        # 1. Buoy Data
        logger.info("Collecting buoy data...")
        buoy_results = await collect_buoy_data(ctx, session)
        all_results.extend(buoy_results)
        
        # 2. Chart Data
        logger.info("Collecting chart data...")
        chart_results = await collect_chart_data(ctx, session)
        all_results.extend(chart_results)
        
        # 3. Model Data
        logger.info("Collecting model data...")
        model_results = await collect_model_data(ctx, session)
        all_results.extend(model_results)
        
        # 4. API Data
        logger.info("Collecting API data...")
        api_results = await collect_api_data(ctx, session)
        all_results.extend(api_results)
        
        # 5. Regional Data
        logger.info("Collecting regional data...")
        regional_results = await collect_regional_data(ctx, session)
        all_results.extend(regional_results)
        
        # Generate metadata
        logger.info(f"Collected a total of {len(all_results)} files")
        
        # Write metadata
        metadata_content = utils.jdump({
            "run_id": ctx.run_id,
            "timestamp": utils.utcnow(),
            "results": all_results
        })
        bundle_path = ctx.bundle / "metadata.json"
        latest_bundle_path = ctx.base / "latest_bundle.txt"
        
        # Use parallel tasks for writing metadata and updating latest bundle
        write_tasks = [
            utils.write_file_async(bundle_path, metadata_content),
            utils.write_file_async(latest_bundle_path, ctx.run_id)
        ]
        await asyncio.gather(*write_tasks)
        
        # Generate summary
        summary = await generate_summary(ctx.bundle, all_results)
        
        logger.info(f"Bundle {ctx.run_id} complete ({len(all_results)} files, {summary['total_size_mb']} MB)")
        
        # Save to latest_bundle.txt
        try:
            latest_path = Path("pacific_data") / "latest_bundle.txt"
            with open(latest_path, "w") as f:
                f.write(ctx.run_id)
            logger.info(f"Saved {ctx.run_id} as latest bundle")
        except Exception as e:
            logger.warning(f"Failed to save latest bundle: {e}")
        
        return ctx.bundle, summary
    
    except Exception as e:
        logger.error(f"Comprehensive collection process failed: {e}")
        return None, None
    
    finally:
        if session and not session.closed:
            try:
                await session.close()
                await asyncio.sleep(0.25)  # Allow time for session to fully close
            except Exception as e:
                logger.error(f"Error closing session: {e}")

async def main():
    """Main entry point."""
    import argparse
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Comprehensive data collection for SwellForecaster")
    parser.add_argument("--config", default="config.ini", help="Path to configuration file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    # Set debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    # Start the timer
    start_time = datetime.now()
    logger.info(f"Starting comprehensive data collection at {start_time}")
    
    # Collect data
    bundle_dir, summary = await collect_comprehensive_data(args.config)
    
    if not bundle_dir:
        logger.error("Comprehensive data collection failed")
        return 1
    
    # Display summary
    print("\n---------- COMPREHENSIVE DATA COLLECTION COMPLETED ----------")
    print(f"Bundle ID: {bundle_dir.name}")
    print(f"Files: {summary['total_files']} ({summary['total_size_mb']} MB)")
    print(f"Shore coverage: {summary['shore_coverage']['north_shore_percentage']}% North Shore, {summary['shore_coverage']['south_shore_percentage']}% South Shore")
    print("\nSource breakdown:")
    for source, count in summary["sources"].items():
        print(f"- {source}: {count} files")
    
    print("\nUse this bundle ID with Pat Caldwell analyzer:")
    print(f"python run_caldwell.py --analyze-only --bundle-id {bundle_dir.name}")
    print("\nOr generate a full Caldwell forecast from this data:")
    print(f"python pat_caldwell_analyzer.py {bundle_dir.name}")
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