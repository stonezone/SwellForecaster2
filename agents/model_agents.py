#!/usr/bin/env python3
# agents/model_agents.py - Enhanced wave model data collection agents
from __future__ import annotations
import asyncio
import logging
import json
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List
import utils
from utils import create_placeholder_image, fetch_with_retry

log = logging.getLogger("model_agents")

# Try to import the ecmwf-opendata client
try:
    from ecmwf.opendata import Client as OpenDataClient
    OPENDATA_AVAILABLE = True
except ImportError:
    log.warning("ecmwf-opendata package not installed. Run 'pip install ecmwf-opendata'")
    OPENDATA_AVAILABLE = False
    # Try to import the legacy client as fallback
    try:
        from ecmwfapi import ECMWFDataServer
        LEGACY_AVAILABLE = True
    except ImportError:
        log.warning("ecmwfapi package not installed. Run 'pip install ecmwf-api-client'")
        LEGACY_AVAILABLE = False

async def model_agent(ctx, session):
    """
    Process WaveWatch III GRIB2 data for Hawaiian waters.
    Integrated from the root models.py file to resolve circular dependencies.
    """
    log.info("Fetching WW3 model data via direct model_agent")

    # Format date for NOAA URL pattern
    url = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/wave/prod/wave.{:%Y%m%d}/multi_1.glo_30m.t00z.grib2".format(
        datetime.now(timezone.utc))

    log.info(f"Downloading WW3 GRIB from: {url}")
    data, _ = await fetch_with_retry(ctx, session, url)
    if not data:
        log.warning("Failed to download WW3 GRIB data")
        return []

    # Save to temporary file
    tmp = tempfile.NamedTemporaryFile(delete=False)
    tmp.write(data)
    tmp.close()

    # Define paths for processed files
    slice_path = tmp.name + ".slice"
    json_path = slice_path + ".json"

    try:
        # Extract region around Hawaii (longitude 140-160, latitude 10-30)
        log.info("Slicing GRIB to Hawaiian region")
        slice_process = await asyncio.create_subprocess_shell(
            f"wgrib2 {tmp.name} -small_grib 140:160 10:30 {slice_path}"
        )
        await slice_process.wait()

        if not Path(slice_path).exists():
            log.error("GRIB slicing failed")
            return []

        # Convert GRIB to JSON
        log.info("Converting GRIB to JSON")
        json_process = await asyncio.create_subprocess_shell(
            f"grib2json -d 2 -n 3 -o {json_path} {slice_path}"
        )
        await json_process.wait()

        if not Path(json_path).exists():
            log.error("GRIB to JSON conversion failed")
            return []

        # Read and save the JSON data
        data = Path(json_path).read_bytes()
        fn = ctx.save("ww3_hawaii.json", data)

        # Also get Southern Hemisphere region (for South Shore forecasting)
        south_slice_path = tmp.name + ".south.slice"
        south_json_path = south_slice_path + ".json"

        # Extract Southern Hemisphere region (longitude 180-220, latitude -40-0)
        log.info("Slicing GRIB to Southern Hemisphere region")
        south_slice_process = await asyncio.create_subprocess_shell(
            f"wgrib2 {tmp.name} -small_grib 180:220 -40:0 {south_slice_path}"
        )
        await south_slice_process.wait()

        south_result = []
        if Path(south_slice_path).exists():
            # Convert Southern Hemisphere GRIB to JSON
            log.info("Converting Southern Hemisphere GRIB to JSON")
            south_json_process = await asyncio.create_subprocess_shell(
                f"grib2json -d 2 -n 3 -o {south_json_path} {south_slice_path}"
            )
            await south_json_process.wait()

            if Path(south_json_path).exists():
                # Read and save the Southern Hemisphere JSON data
                south_data = Path(south_json_path).read_bytes()
                south_fn = ctx.save("ww3_south_pacific.json", south_data)

                south_result = [{
                    "source": "WW3-South",
                    "filename": south_fn,
                    "type": "model",
                    "region": "south_pacific",
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "south_facing": True
                }]

        return [{
            "source": "WW3",
            "filename": fn,
            "type": "model",
            "region": "hawaii",
            "priority": 0,
            "timestamp": utils.utcnow()
        }] + south_result

    except Exception as e:
        log.error(f"Error processing WW3 data: {e}")
        return []

    finally:
        # Clean up temporary files
        for path in [tmp.name, slice_path, json_path,
                    south_slice_path if 'south_slice_path' in locals() else "",
                    south_json_path if 'south_json_path' in locals() else ""]:
            try:
                if path and Path(path).exists():
                    Path(path).unlink()
            except Exception as e:
                log.warning(f"Failed to delete temporary file {path}: {e}")

async def pacioos(ctx, session):
    """Fetch PacIOOS WW3 Hawaii data with enhanced reliability and error handling"""
    # Updated variable name based on current PacIOOS API documentation
    # For ww3_hawaii dataset, the correct variable name is 'Thgt'
    VAR = "Thgt"
    base = "https://pae-paha.pacioos.hawaii.edu/erddap/griddap/ww3_hawaii.png"

    # Fix the depth parameter issue - use default depth (0.0) instead of using 18-25 range
    bbox = "[(0.0)][(18.0):(25.0)][(-162.0):(-151.0)]"

    urls = {
        "now": f"{base}?{VAR}[(last)]{bbox}&.draw=surface&.vars=longitude|latitude|{VAR}&.colorBar=||||||",
        "24h": f"{base}?{VAR}[(last-24)]{bbox}&.draw=surface&.vars=longitude|latitude|{VAR}&.colorBar=||||||",
        "48h": f"{base}?{VAR}[(last-48)]{bbox}&.draw=surface&.vars=longitude|latitude|{VAR}&.colorBar=||||||",
    }

    log.info(f"PacIOOS WW3 URL: {urls['now']}")

    out = []
    for tag, u in urls.items():
        data, fn = await fetch_with_retry(ctx, session, u, filename=f"pacioos_{tag}.png")
        if data and fn:
            # Check if the image contains an error message
            if isinstance(data, bytes) and b"Error:" in data and b"wasn't found in datasetID" in data:
                log.error(f"PacIOOS {tag} returned error: {data[:200]}")
                continue

            out.append({
                "source": "PacIOOS",
                "type": f"wave_{tag}",
                "filename": fn,
                "url": u,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "north_facing": True,
                "south_facing": True  # Hawaii models cover both shores
            })
            log.info(f"Successfully fetched PacIOOS {tag}")
        else:
            log.error(f"Failed to fetch PacIOOS {tag}")

    # Add high-resolution North Shore model with corrected variable name and parameters
    ns_base = "https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png"

    # Correct variable name for SWAN dataset (swan_oahu)
    # For swan_oahu dataset, the correct variable name is 'shgt'
    swan_var = "shgt"
    # Fixed coordinates for North Shore area
    ns_urls = {
        "ns_now": f"{ns_base}?{swan_var}[(last)][(21.5):(21.7)][(-158.2):(-158.0)]&.draw=surface&.vars=longitude|latitude|{swan_var}&.colorBar=||||||",
        "ns_24h": f"{ns_base}?{swan_var}[(last-24)][(21.5):(21.7)][(-158.2):(-158.0)]&.draw=surface&.vars=longitude|latitude|{swan_var}&.colorBar=||||||",
    }

    for tag, u in ns_urls.items():
        data, fn = await fetch_with_retry(ctx, session, u, filename=f"pacioos_{tag}.png")
        if data and fn:
            # Check if the image contains an error message
            if isinstance(data, bytes) and b"Error:" in data and b"wasn't found in datasetID" in data:
                log.error(f"PacIOOS NS {tag} returned error: {data[:200]}")
                continue

            out.append({
                "source": "PacIOOS",
                "type": f"wave_{tag}",
                "filename": fn,
                "url": u,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "north_facing": True
            })
            log.info(f"Successfully fetched PacIOOS NS {tag}")
        else:
            log.error(f"Failed to fetch PacIOOS NS {tag}")

    return out

async def pacioos_swan(ctx, session):
    """Fetch PacIOOS SWAN Oahu nearshore wave model data with enhanced error handling"""
    out = []

    # Get metadata for ww3_hawaii to confirm available variables
    ww3_info_url = "https://pae-paha.pacioos.hawaii.edu/erddap/info/ww3_hawaii/index.json"
    data, fn = await fetch_with_retry(ctx, session, ww3_info_url, filename="ww3_hawaii_info.json")
    if data and fn:
        out.append({
            "source": "PacIOOS",
            "type": "ww3_info",
            "filename": fn,
            "url": ww3_info_url,
            "priority": 0,
            "timestamp": utils.utcnow()
        })
        log.info("Successfully fetched WW3 Hawaii dataset info")
    else:
        log.error("Failed to fetch WW3 Hawaii info")

    # For swan_oahu dataset, the correct variable name is 'shgt' (not significant_wave_height)
    swan_var = "shgt"

    # Visualization image using correct variable name with enhanced display settings
    viz_url = f"https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png?{swan_var}%5B(last)%5D&.draw=surface&.vars=longitude|latitude|{swan_var}&.colorBar=Rainbow|||||&.bgColor=0xffccccff"
    data, fn = await fetch_with_retry(ctx, session, viz_url, filename="pacioos_swan_viz.png")
    if data and fn:
        # Check if the image contains an error message
        if isinstance(data, bytes) and b"Error:" in data and b"wasn't found in datasetID" in data:
            log.error(f"PacIOOS SWAN viz returned error: {data[:200]}")
            # Create placeholder with error message
            placeholder_data = create_placeholder_image(
                "Unable to retrieve PacIOOS SWAN visualization. The server returned an error.",
                filename="pacioos_swan_viz"
            )
            fn = ctx.save("pacioos_swan_viz.png", placeholder_data)
            out.append({
                "source": "PacIOOS",
                "type": "swan_viz",
                "filename": fn,
                "url": viz_url,
                "error": True,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "north_facing": True,
                "south_facing": True
            })
        else:
            out.append({
                "source": "PacIOOS",
                "type": "swan_viz",
                "filename": fn,
                "url": viz_url,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "north_facing": True,
                "south_facing": True  # This overview covers both shores
            })
            log.info("Successfully fetched PacIOOS SWAN viz")
    else:
        log.error("Failed to fetch PacIOOS SWAN viz")
        # Create placeholder image
        placeholder_data = create_placeholder_image(
            "Unable to retrieve PacIOOS SWAN visualization. Connection error or service unavailable.",
            filename="pacioos_swan_viz"
        )
        fn = ctx.save("pacioos_swan_viz.png", placeholder_data)
        out.append({
            "source": "PacIOOS",
            "type": "swan_viz",
            "filename": fn,
            "error": True,
            "priority": 1,
            "timestamp": utils.utcnow(),
            "north_facing": True,
            "south_facing": True
        })

    # Metadata JSON - works well
    info_url = "https://pae-paha.pacioos.hawaii.edu/erddap/info/swan_oahu/index.json"
    data, fn = await fetch_with_retry(ctx, session, info_url, filename="pacioos_swan_info.json")
    if data and fn:
        out.append({
            "source": "PacIOOS",
            "type": "swan_info",
            "filename": fn,
            "url": info_url,
            "priority": 0,
            "timestamp": utils.utcnow()
        })
        log.info("Successfully fetched PacIOOS SWAN info")
    else:
        log.error("Failed to fetch PacIOOS SWAN info")
        # Create placeholder JSON with minimal info
        placeholder_data = json.dumps({
            "status": "error",
            "message": "Failed to fetch SWAN model information",
            "timestamp": utils.utcnow(),
            "data_source": "PacIOOS SWAN model",
            "error_type": "fetch_failure"
        })
        fn = ctx.save("pacioos_swan_info.json", placeholder_data)
        out.append({
            "source": "PacIOOS",
            "type": "swan_info",
            "filename": fn,
            "error": True,
            "priority": 0,
            "timestamp": utils.utcnow()
        })

    # South Shore specific wave models with enhanced visualization
    south_shore_url = f"https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png?{swan_var}%5B(last)%5D&.draw=surface&.vars=longitude|latitude|{swan_var}&.colorBar=Rainbow|||||&.bgColor=0xffccccff&.land=under&.lat=21.25:21.30&.lon=-157.85:-157.80"
    data, fn = await fetch_with_retry(ctx, session, south_shore_url, filename="pacioos_swan_south_shore.png")
    if data and fn:
        # Check if the image contains an error message
        if isinstance(data, bytes) and b"Error:" in data and b"wasn't found in datasetID" in data:
            log.error(f"PacIOOS South Shore returned error: {data[:200]}")
            # Create placeholder with error message
            placeholder_data = create_placeholder_image(
                "Unable to retrieve PacIOOS South Shore model. The server returned an error.",
                filename="pacioos_swan_south_shore"
            )
            fn = ctx.save("pacioos_swan_south_shore.png", placeholder_data)
            out.append({
                "source": "PacIOOS",
                "type": "swan_south_shore",
                "filename": fn,
                "url": south_shore_url,
                "error": True,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "south_facing": True
            })
        else:
            out.append({
                "source": "PacIOOS",
                "type": "swan_south_shore",
                "filename": fn,
                "url": south_shore_url,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "south_facing": True
            })
            log.info("Successfully fetched PacIOOS South Shore")
    else:
        log.error("Failed to fetch PacIOOS South Shore")
        # Create placeholder image
        placeholder_data = create_placeholder_image(
            "Unable to retrieve PacIOOS South Shore model. Connection error or service unavailable.",
            filename="pacioos_swan_south_shore"
        )
        fn = ctx.save("pacioos_swan_south_shore.png", placeholder_data)
        out.append({
            "source": "PacIOOS",
            "type": "swan_south_shore",
            "filename": fn,
            "error": True,
            "priority": 1,
            "timestamp": utils.utcnow(),
            "south_facing": True
        })

    # North Shore specific high-resolution SWAN model with enhanced visualization
    ns_url = f"https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png?{swan_var}%5B(last)%5D&.draw=surface&.vars=longitude|latitude|{swan_var}&.colorBar=Rainbow|||||&.bgColor=0xffccccff&.land=under&.lat=21.55:21.75&.lon=-158.20:-158.00"
    data, fn = await fetch_with_retry(ctx, session, ns_url, filename="pacioos_swan_north_shore.png")
    if data and fn:
        # Check if the image contains an error message
        if isinstance(data, bytes) and b"Error:" in data and b"wasn't found in datasetID" in data:
            log.error(f"PacIOOS North Shore returned error: {data[:200]}")
            # Create placeholder with error message
            placeholder_data = create_placeholder_image(
                "Unable to retrieve PacIOOS North Shore model. The server returned an error.",
                filename="pacioos_swan_north_shore"
            )
            fn = ctx.save("pacioos_swan_north_shore.png", placeholder_data)
            out.append({
                "source": "PacIOOS",
                "type": "swan_north_shore",
                "filename": fn,
                "url": ns_url,
                "error": True,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "north_facing": True
            })
        else:
            out.append({
                "source": "PacIOOS",
                "type": "swan_north_shore",
                "filename": fn,
                "url": ns_url,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "north_facing": True
            })
            log.info("Successfully fetched PacIOOS North Shore")
    else:
        log.error("Failed to fetch PacIOOS North Shore")
        # Create placeholder image
        placeholder_data = create_placeholder_image(
            "Unable to retrieve PacIOOS North Shore model. Connection error or service unavailable.",
            filename="pacioos_swan_north_shore"
        )
        fn = ctx.save("pacioos_swan_north_shore.png", placeholder_data)
        out.append({
            "source": "PacIOOS",
            "type": "swan_north_shore",
            "filename": fn,
            "error": True,
            "priority": 1,
            "timestamp": utils.utcnow(),
            "north_facing": True
        })

    # Try to add additional data types if available (direction, period)
    for data_type, var_name in {'direction': 'mpd', 'period': 'tps'}.items():
        try:
            url = f"https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png?{var_name}%5B(last)%5D&.draw=surface&.vars=longitude|latitude|{var_name}&.colorBar=Rainbow|||||&.bgColor=0xffccccff&.land=under"
            data, fn = await fetch_with_retry(ctx, session, url, filename=f"pacioos_swan_{data_type}.png")
            if data and fn:
                # Check if the image contains an error message
                if isinstance(data, bytes) and b"Error:" in data:
                    log.error(f"PacIOOS {data_type} data returned error: {data[:200]}")
                    # Create placeholder with error message
                    placeholder_data = create_placeholder_image(
                        f"Unable to retrieve SWAN wave {data_type} data. The server returned an error.",
                        filename=f"pacioos_swan_{data_type}"
                    )
                    fn = ctx.save(f"pacioos_swan_{data_type}.png", placeholder_data)
                    out.append({
                        "source": "PacIOOS",
                        "type": f"swan_{data_type}",
                        "filename": fn,
                        "url": url,
                        "error": True,
                        "priority": 1,
                        "timestamp": utils.utcnow(),
                        "north_facing": True,
                        "south_facing": True
                    })
                else:
                    out.append({
                        "source": "PacIOOS",
                        "type": f"swan_{data_type}",
                        "filename": fn,
                        "url": url,
                        "priority": 1,
                        "timestamp": utils.utcnow(),
                        "north_facing": True,
                        "south_facing": True
                    })
                    log.info(f"Successfully fetched PacIOOS SWAN wave {data_type}")
            else:
                log.error(f"Failed to fetch PacIOOS {data_type} data")
                # Create placeholder image
                placeholder_data = create_placeholder_image(
                    f"Unable to retrieve SWAN wave {data_type} data. Connection error or service unavailable.",
                    filename=f"pacioos_swan_{data_type}"
                )
                fn = ctx.save(f"pacioos_swan_{data_type}.png", placeholder_data)
                out.append({
                    "source": "PacIOOS",
                    "type": f"swan_{data_type}",
                    "filename": fn,
                    "error": True,
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "north_facing": True,
                    "south_facing": True
                })
        except Exception as e:
            log.error(f"Failed to fetch PacIOOS {data_type} data: {e}")
            # Create placeholder image for exception case
            placeholder_data = create_placeholder_image(
                f"Unable to retrieve SWAN wave {data_type} data. Error: {str(e)}",
                filename=f"pacioos_swan_{data_type}"
            )
            fn = ctx.save(f"pacioos_swan_{data_type}.png", placeholder_data)
            out.append({
                "source": "PacIOOS",
                "type": f"swan_{data_type}",
                "filename": fn,
                "error": True,
                "priority": 1,
                "timestamp": utils.utcnow(),
                "north_facing": True,
                "south_facing": True
            })

    return out

async def ww3_model_fallback(ctx, session):
    """
    Enhanced fallback for WW3 data with more sources and better error handling.
    """
    log.info("Using WW3 model fallback sources")
    out = []
    
    # Multiple global overview images (with different styles/sources for redundancy)
    fallback_urls = [
        # Primary NCEP WW3 global overview image
        ("https://polar.ncep.noaa.gov/waves/latest_run/pac-hs.latest_run.gif", "ww3_pacific_overview.gif", "overview", 1),
        
        # Higher resolution North Pacific data
        ("https://polar.ncep.noaa.gov/waves/latest_run/multi_1.nww3.hs.north_pacific.latest.gif", "ww3_north_pacific_detail.gif", "npac_detail", 1),
        
        # Higher resolution Hawaii region
        ("https://polar.ncep.noaa.gov/waves/latest_run/multi_1.nww3.hs.hawaii.latest.gif", "ww3_hawaii_detail.gif", "hawaii_detail", 1),
        
        # Wave period data - crucial for surf quality assessment
        ("https://polar.ncep.noaa.gov/waves/latest_run/multi_1.nww3.tp.north_pacific.latest.gif", "ww3_north_pacific_period.gif", "npac_period", 1),
        
        # Wave direction data - important for swell direction
        ("https://polar.ncep.noaa.gov/waves/latest_run/multi_1.nww3.dp.north_pacific.latest.gif", "ww3_north_pacific_direction.gif", "npac_direction", 1),
        
        # Southern hemisphere charts for South swells
        ("https://polar.ncep.noaa.gov/waves/latest_run/multi_1.nww3.hs.south_pacific.latest.gif", "ww3_south_pacific_detail.gif", "spac_detail", 1),
        ("https://polar.ncep.noaa.gov/waves/latest_run/multi_1.nww3.tp.south_pacific.latest.gif", "ww3_south_pacific_period.gif", "spac_period", 1),
        
        # Alternative source options if NCEP is down
        ("https://nomads.ncep.noaa.gov/gifs/wave/pac/wave_pac_hs_day1_animate_latest.gif", "ww3_pacific_animate.gif", "pacific_animate", 2),
        ("https://www.opc.ncep.noaa.gov/shtml/P_00hrww.gif", "opc_pacific_ww.gif", "pacific_ww", 2)
    ]

    # Try each source with proper error handling
    for url, filename, subtype, priority in fallback_urls:
        data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
        if data and fn:
            # Determine shore facing based on filename
            north_facing = "north" in filename or "npac" in filename or "pacific" in filename
            south_facing = "south" in filename or "spac" in filename
            
            out.append({
                "source": "WW3-Fallback",
                "type": "wave_model",
                "subtype": subtype,
                "filename": fn,
                "url": url,
                "priority": priority,
                "timestamp": utils.utcnow(),
                "north_facing": north_facing,
                "south_facing": south_facing
            })
            log.info(f"Successfully fetched WW3 fallback: {filename}")
        else:
            log.warning(f"Failed to fetch WW3 fallback: {url}")

    # Try to get WW3 model metadata if available
    try:
        metadata = {
            "model": "WaveWatch III",
            "source": "NCEP/NOAA",
            "description": "Global wave model produced by NOAA/NCEP",
            "forecast_hours": [0, 24, 48, 72, 96, 120],
            "variables": ["significant_wave_height", "peak_period", "mean_direction"],
            "resolution": "0.5 degree",
            "update_frequency": "4 times daily",
            "reliability": "High - operational model",
            "shores": ["North Shore", "South Shore"],
            "chart_sources": fallback_urls
        }
        
        fn = ctx.save("ww3_metadata.json", json.dumps(metadata))
        out.append({
            "source": "WW3-Fallback",
            "type": "metadata",
            "filename": fn,
            "priority": 3,
            "timestamp": utils.utcnow()
        })
    except Exception as e:
        log.error(f"Failed to create WW3 metadata: {e}")

    return out

async def ecmwf_agent(ctx, session):
    """
    Get ECMWF wave forecasts if API key is available.
    Integrated from the root models.py file to resolve circular dependencies.
    """
    logger = logging.getLogger("ecmwf_agent")

    # Check for API key
    api_key = ctx.cfg["API"].get("ECMWF_KEY", "").strip()
    if not api_key:
        logger.info("No ECMWF API key found in config, skipping")
        return []

    # Define regions to fetch - Hawaii and surrounding areas
    regions = [
        {"name": "hawaii", "area": [30, -170, 15, -150]},
        {"name": "north_pacific", "area": [60, -180, 30, -120]},
        {"name": "south_pacific", "area": [0, -180, -40, -120]}
    ]

    results = []

    # Try multiple endpoints - different ECMWF APIs may work
    endpoints = [
        "https://api.ecmwf.int/v1/services/opendata/wave",
        "https://api.ecmwf.int/v1/wave",
        "https://data.ecmwf.int/forecasts/wave",
        "https://api.ecmwf.int/v1/datasets/wave/forecasts"
    ]

    for region in regions:
        region_name = region["name"]
        area = region["area"]

        logger.info(f"Requesting ECMWF wave forecast data for {region_name} using opendata client")

        for endpoint in endpoints:
            try:
                # Prepare request body
                request_data = {
                    "area": area,  # [north, west, south, east]
                    "params": ["swh", "mwd", "pp1d"],  # Significant height, direction, period
                    "apikey": api_key
                }

                # Attempt fetch
                data, _ = await fetch_with_retry(ctx, session, endpoint, method="POST", json_body=request_data)

                if data:
                    # Success! Save data and return
                    fn = ctx.save(f"ecmwf_{region_name}_wave.json", data)
                    results.append({
                        "source": "ECMWF",
                        "filename": fn,
                        "type": "wave_model",
                        "region": region_name,
                        "priority": 1,
                        "timestamp": utils.utcnow(),
                        "north_facing": region_name == "north_pacific",
                        "south_facing": region_name == "south_pacific"
                    })
                    # No need to try other endpoints for this region
                    break

            except Exception as e:
                logger.warning(f"Failed to fetch ECMWF wave data for {region_name} from {endpoint}: {e}")
                # Continue to next endpoint

    if not results:
        logger.error("All ECMWF endpoints failed")

    return results

async def ecmwf_wave(ctx, session):
    """Enhanced ECMWF wave model data collection with robust error handling"""
    # Wave forecast parameters
    WAVE_PARAMS = {
        "swh": "Significant wave height",
        "mwd": "Mean wave direction",
        "mwp": "Mean wave period"
    }
    
    # Regions of interest (for documentation and post-processing)
    REGIONS = {
        "hawaii": {"bounds": [30, -170, 10, -150], "priority": 1},
        "north_pacific": {"bounds": [60, -180, 20, -120], "priority": 1, "north_facing": True},
        "south_pacific": {"bounds": [-15, -180, -60, -120], "priority": 1, "south_facing": True},
    }
    
    # Alternative chart URLs to try if API fails
    CHART_URLS = {
        "swh": "https://charts.ecmwf.int/opencharts-api/v1/products/medium-significant-wave-height?area={region}&format=jpeg",
        "mwd": "https://charts.ecmwf.int/opencharts-api/v1/products/medium-wave-direction?area={region}&format=jpeg",
        "mwp": "https://charts.ecmwf.int/opencharts-api/v1/products/medium-wave-period?area={region}&format=jpeg"
    }
    
    out = []
    api_key = ctx.cfg["API"].get("ECMWF_KEY", "").strip()
    
    # Skip if API key is not available, but try fallback sources
    if not api_key:
        log.warning("ECMWF API key not configured, trying fallback sources")
    else:
        log.info("ECMWF API key found, attempting to fetch model data")
    
    # Create temp directory for downloads
    tmp_dir = Path(tempfile.mkdtemp())
    
    try:
        # Process each region
        for region_name, region_info in REGIONS.items():
            # Check if we should process this region based on configuration
            north_emphasis = ctx.cfg["FORECAST"].get("north_swell_emphasis", "auto").lower()
            if "north_facing" in region_info and north_emphasis not in ["auto", "true"]:
                continue
                
            south_emphasis = ctx.cfg["FORECAST"].get("south_swell_emphasis", "auto").lower()
            if "south_facing" in region_info and south_emphasis not in ["auto", "true"]:
                continue
            
            # Prepare target file path
            target_file = str(tmp_dir / f"ecmwf_{region_name}_wave.grib2")
            success = False
            
            if api_key:
                # Try OpenData client first if available
                if OPENDATA_AVAILABLE:
                    try:
                        client = OpenDataClient(source="ecmwf")  # Options: 'ecmwf', 'azure', 'aws'
                        
                        # Prepare request parameters
                        request = {
                            "stream": "wave",
                            "type": "fc",
                            "step": [0, 6, 12, 18, 24, 30, 36, 42, 48],
                            "param": list(WAVE_PARAMS.keys()),  # Get all wave params
                            "target": target_file
                        }
                            
                        log.info(f"Requesting ECMWF wave forecast data for {region_name} using opendata client")
                        # This is a synchronous call, but we're in an async context
                        # In a real implementation, we'd use asyncio.to_thread for Python 3.9+
                        client.retrieve(**request)
                        success = True
                    except Exception as e:
                        log.error(f"Failed to retrieve ECMWF opendata for {region_name}: {e}")
                
                # Try legacy client if OpenData failed
                if not success and LEGACY_AVAILABLE:
                    try:
                        from ecmwfapi import ECMWFDataServer
                        server = ECMWFDataServer()
                        
                        # Get region-specific bounds
                        bounds = region_info.get("bounds", None)
                        area_param = "/".join(map(str, bounds)) if bounds else None
                        
                        # Prepare request parameters
                        params = {
                            "class": "od",  # Open data
                            "stream": "wave",
                            "type": "fc",  # Forecast
                            "param": "140.128/141.128/142.128",  # swh/mwd/mwp
                            "levtype": "sfc",
                            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                            "time": "00",
                            "step": "0/6/12/18/24/30/36/42/48",
                            "target": target_file
                        }
                        
                        # Try different dataset names
                        dataset_options = ["wave", "waef", "wam"]
                        
                        for dataset in dataset_options:
                            try:
                                params["dataset"] = dataset
                                
                                # Add area if specified
                                if area_param:
                                    params["area"] = area_param
                                    
                                log.info(f"Requesting ECMWF wave forecast data for {region_name} using legacy API with dataset={dataset}")
                                server.retrieve(params)
                                log.info(f"Successfully retrieved data with dataset={dataset}")
                                success = True
                                break
                            except Exception as e:
                                log.warning(f"Failed to retrieve with dataset={dataset}: {e}")
                                continue
                    except Exception as e:
                        log.error(f"Failed to retrieve ECMWF data via legacy API for {region_name}: {e}")
            
            # Process results if successful
            if success and Path(target_file).exists():
                with open(target_file, "rb") as f:
                    data = f.read()
                
                fn = ctx.save(f"ecmwf_{region_name}_wave.grib2", data)
                
                out.append({
                    "source": "ECMWF",
                    "type": "wave_model",
                    "subtype": f"{region_name}_wave",
                    "description": f"ECMWF Wave forecast for {region_name}",
                    "filename": fn,
                    "priority": region_info.get("priority", 1),
                    "timestamp": utils.utcnow(),
                    "north_facing": region_info.get("north_facing", False),
                    "south_facing": region_info.get("south_facing", False)
                })
                log.info(f"Successfully saved ECMWF {region_name} wave data")
            
            # Try fallback chart sources for all regions
            # Map region names to chart region codes
            region_mapping = {
                "hawaii": "pacific",  # Use pacific for Hawaii
                "north_pacific": "north_pacific",
                "south_pacific": "south_pacific"
            }
            
            chart_region = region_mapping.get(region_name, "pacific")
            
            # Try ECMWF charts
            for param, url_template in CHART_URLS.items():
                url = url_template.format(region=chart_region)
                data, fn = await fetch_with_retry(ctx, session, url, filename=f"ecmwf_{region_name}_{param}.jpg")
                if data and fn:
                    out.append({
                        "source": "ECMWF",
                        "type": "chart",
                        "subtype": f"{region_name}_{param}",
                        "description": f"ECMWF {WAVE_PARAMS.get(param, 'Wave')} chart for {region_name}",
                        "filename": fn,
                        "url": url,
                        "priority": 2,
                        "timestamp": utils.utcnow(),
                        "north_facing": region_info.get("north_facing", False),
                        "south_facing": region_info.get("south_facing", False)
                    })
                    log.info(f"Successfully fetched ECMWF {param} chart for {region_name}")
            
            # Try alternative sources like Tropical Tidbits
            alternative_charts = {
                "swh": f"https://www.tropicaltidbits.com/analysis/ocean/global/global_htsgw_0-000.png",
                "mwd": f"https://www.tropicaltidbits.com/analysis/ocean/global/global_mwd_0-000.png",
                "mwp": f"https://www.tropicaltidbits.com/analysis/ocean/global/global_perpw_0-000.png"
            }
            
            for param, url in alternative_charts.items():
                data, fn = await fetch_with_retry(ctx, session, url, filename=f"ecmwf_alt_{region_name}_{param}.png")
                if data and fn:
                    out.append({
                        "source": "ECMWF-Fallback",
                        "type": "chart",
                        "subtype": f"{region_name}_{param}_alt",
                        "description": f"Alternative {WAVE_PARAMS.get(param, 'Wave')} chart for {region_name}",
                        "filename": fn,
                        "url": url,
                        "priority": 3,
                        "timestamp": utils.utcnow(),
                        "north_facing": region_info.get("north_facing", False),
                        "south_facing": region_info.get("south_facing", False)
                    })
                    log.info(f"Successfully fetched alternative {param} chart for {region_name}")
    
    finally:
        # Clean up temporary files
        for file in tmp_dir.glob("*"):
            try:
                file.unlink()
            except Exception:
                pass
        try:
            tmp_dir.rmdir()
        except Exception:
            pass
    
    return out