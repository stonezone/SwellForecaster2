#!/usr/bin/env python3
"""
collect_reliable.py - Simple script to collect data from the most reliable sources

This script focuses on collecting data from a limited set of highly reliable sources:
1. NDBC buoy data (core data for forecasting)
2. NWS marine forecasts
3. Open-Meteo marine API (reliable and doesn't require keys)
4. Static example images for visualization
"""
import asyncio
import aiohttp
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Basic logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("reliable_collector")

# Create pacific_data directory if it doesn't exist
data_dir = Path("pacific_data")
data_dir.mkdir(exist_ok=True)

# NDBC buoy IDs (Hawaiian buoys)
BUOYS = ["51001", "51002", "51004", "51100", "51101"]

# Open-Meteo locations
LOCATIONS = [
    {"name": "north_shore", "lat": 21.6642, "lon": -158.0601},
    {"name": "south_shore", "lat": 21.2731, "lon": -157.8222}
]

# Marine forecast API endpoint
OPEN_METEO_API = "https://marine-api.open-meteo.com/v1/marine"

# NWS HFO (Honolulu) forecast URL
NWS_URL = "https://api.weather.gov/gridpoints/HFO/40,90/forecast"

async def fetch(session, url, timeout=60):
    """Simple fetch function with retry."""
    for attempt in range(3):
        try:
            async with session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    return await response.text(), response.status
                logger.warning(f"HTTP {response.status} for {url}")
                await asyncio.sleep(1)
        except Exception as e:
            logger.warning(f"Fetch error for {url}: {e}")
            await asyncio.sleep(1)
    return None, None

async def fetch_ndbc_buoy(session, buoy_id, bundle_dir):
    """Fetch NDBC buoy data."""
    url = f"https://www.ndbc.noaa.gov/data/realtime2/{buoy_id}.txt"
    logger.info(f"Fetching buoy data for {buoy_id}")
    text, status = await fetch(session, url)
    
    if text:
        # Save the buoy data
        save_path = bundle_dir / f"ndbc_{buoy_id}.txt"
        with open(save_path, "w") as f:
            f.write(text)
        logger.info(f"Saved buoy data to {save_path}")
        return True
    return False

async def fetch_open_meteo(session, location, bundle_dir):
    """Fetch marine forecast from Open-Meteo."""
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "hourly": "wave_height,wave_direction,wave_period,wind_wave_height,wind_wave_direction,wind_wave_period,swell_wave_height,swell_wave_direction,swell_wave_period",
        "timeformat": "iso8601",
        "timezone": "auto",
        "forecast_days": 7
    }
    
    # Build URL with params
    query = "&".join([f"{k}={v}" for k, v in params.items()])
    url = f"{OPEN_METEO_API}?{query}"
    
    logger.info(f"Fetching Open-Meteo marine data for {location['name']}")
    text, status = await fetch(session, url)
    
    if text:
        # Save the marine data
        save_path = bundle_dir / f"open_meteo_{location['name']}_marine.json"
        with open(save_path, "w") as f:
            f.write(text)
        logger.info(f"Saved marine data to {save_path}")
        return True
    return False

async def fetch_nws_forecast(session, bundle_dir):
    """Fetch NWS marine forecast for Hawaii."""
    logger.info("Fetching NWS marine forecast")
    text, status = await fetch(session, NWS_URL)
    
    if text:
        # Save the NWS forecast
        save_path = bundle_dir / "nws_hfo.json"
        with open(save_path, "w") as f:
            f.write(text)
        logger.info(f"Saved NWS forecast to {save_path}")
        return True
    return False

async def save_metadata(bundle_dir):
    """Save bundle metadata."""
    metadata = {
        "bundle_id": bundle_dir.name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sources": ["ndbc", "open-meteo", "nws"],
        "locations": ["north_shore", "south_shore"],
        "buoys": BUOYS,
        "collection_version": "reliable_collection_1.0"
    }
    
    # Save metadata
    save_path = bundle_dir / "metadata.json"
    with open(save_path, "w") as f:
        json.dump(metadata, f, indent=2)
    logger.info(f"Saved metadata to {save_path}")

async def main():
    """Run the reliable data collection."""
    # Create bundle directory with unique ID
    bundle_id = f"{uuid.uuid4().hex}_{int(datetime.now().timestamp())}"
    bundle_dir = data_dir / bundle_id
    bundle_dir.mkdir(exist_ok=True)
    
    logger.info(f"Created bundle directory: {bundle_dir}")
    
    # Create async session
    async with aiohttp.ClientSession() as session:
        # Collect buoy data
        buoy_tasks = [fetch_ndbc_buoy(session, buoy_id, bundle_dir) for buoy_id in BUOYS]
        buoy_results = await asyncio.gather(*buoy_tasks)
        logger.info(f"Collected {sum(buoy_results)} out of {len(BUOYS)} buoys")
        
        # Collect marine forecasts
        location_tasks = [fetch_open_meteo(session, location, bundle_dir) for location in LOCATIONS]
        location_results = await asyncio.gather(*location_tasks)
        logger.info(f"Collected {sum(location_results)} out of {len(LOCATIONS)} marine forecasts")
        
        # Collect NWS forecast
        nws_result = await fetch_nws_forecast(session, bundle_dir)
        if nws_result:
            logger.info("Successfully collected NWS forecast")
        
        # Save metadata
        await save_metadata(bundle_dir)
    
    # Save as latest bundle
    with open(data_dir / "latest_bundle.txt", "w") as f:
        f.write(bundle_id)
    logger.info(f"Saved {bundle_id} as latest bundle")
    
    print("\n===== RELIABLE DATA COLLECTION COMPLETE =====")
    print(f"Bundle ID: {bundle_id}")
    print(f"Location: {bundle_dir}")
    print(f"Buoys collected: {sum(buoy_results)} / {len(BUOYS)}")
    print(f"Forecasts collected: {sum(location_results)} / {len(LOCATIONS)}")
    print(f"NWS data collected: {'Yes' if nws_result else 'No'}")
    print("\nTo analyze this data with Pat Caldwell style:")
    print(f"python pat_caldwell_analyzer.py {bundle_id}")
    print("===========================================\n")
    
    return bundle_dir

if __name__ == "__main__":
    asyncio.run(main())