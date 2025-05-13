#!/usr/bin/env python3
"""
opc_collector.py - Script to collect critical OPC/ocean.weather.gov charts

This script focuses specifically on collecting the most important Ocean Prediction Center
charts needed for accurate wave and swell forecasting for Hawaii. These charts are
crucial for providing visual information about Pacific surface pressure patterns,
winds, and swell parameters.
"""
import asyncio
import aiohttp
import logging
import io
import json
import os
import sys
import uuid
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime, timezone
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("opc_collector")

# Critical OPC charts for forecasting
OPC_URLS = {
    # 1. Current Surface Analysis
    "west_pacific_surface": "https://ocean.weather.gov/P_w_sfc_color.png",
    "full_pacific_surface": "https://ocean.weather.gov/P_sfc_full_ocean_color.png",
    "east_pacific_surface": "https://ocean.weather.gov/P_e_sfc_color.png",
    
    # 2. Surface Forecasts (using full-size, not thumbnails)
    "surface_24h": "https://ocean.weather.gov/P_24hrsfc.gif",
    "surface_48h": "https://ocean.weather.gov/P_48hrsfc.gif",
    "surface_72h": "https://ocean.weather.gov/P_72hrsfc.gif",
    "surface_96h": "https://ocean.weather.gov/P_96hrsfc.gif",
    
    # 3. Wind & Wave Height Forecasts
    "wind_wave_24h": "https://ocean.weather.gov/P_24hrww.gif",
    "wind_wave_48h": "https://ocean.weather.gov/P_48hrww.gif",
    "wind_wave_72h": "https://ocean.weather.gov/P_72hrww.gif",
    "wind_wave_96h": "https://ocean.weather.gov/P_96hrww.gif",
    
    # 4. Wave Period & Direction Forecasts
    "wave_period_24h": "https://ocean.weather.gov/P_24hrwper.gif",
    "wave_period_48h": "https://ocean.weather.gov/P_48hrwper.gif",
    "wave_period_72h": "https://ocean.weather.gov/P_72hrwper.gif",
    "wave_period_96h": "https://ocean.weather.gov/P_96hrwper.gif",
    
    # 5. Upper-air 500mb Charts
    "upper_air_24h": "https://ocean.weather.gov/P_24hr500.gif",
    "upper_air_48h": "https://ocean.weather.gov/P_48hr500.gif",
    "upper_air_72h": "https://ocean.weather.gov/P_72hr500.gif",
    "upper_air_96h": "https://ocean.weather.gov/P_96hr500.gif",
}

async def fetch_image(session, url, retries=3, timeout=30):
    """Fetch image with retries."""
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=timeout) as response:
                if response.status != 200:
                    log.warning(f"HTTP {response.status} for {url}")
                    if attempt < retries - 1:
                        await asyncio.sleep(1 * (attempt + 1))
                        continue
                    return None
                
                # Read as bytes - critical for binary data like images
                return await response.read()
                
        except asyncio.TimeoutError:
            log.warning(f"Timeout fetching {url}, attempt {attempt+1}/{retries}")
            if attempt < retries - 1:
                await asyncio.sleep(1 * (attempt + 1))
                continue
            return None
            
        except Exception as e:
            log.warning(f"Error fetching {url}: {e}, attempt {attempt+1}/{retries}")
            if attempt < retries - 1:
                await asyncio.sleep(1 * (attempt + 1))
                continue
            return None
    
    return None

def create_placeholder_image(name, size=(800, 600), text=None):
    """Create a placeholder image when fetching fails."""
    img = Image.new('RGB', size, color=(240, 240, 240))
    draw = ImageDraw.Draw(img)
    
    # Draw border
    draw.rectangle([(0, 0), (size[0]-1, size[1]-1)], outline=(200, 200, 200), width=2)
    
    # Add text
    if text is None:
        text = f"Placeholder for {name}"
    
    # Calculate text position
    text_w, text_h = draw.textsize(text) if hasattr(draw, 'textsize') else (300, 30)
    position = ((size[0] - text_w) // 2, (size[1] - text_h) // 2)
    
    # Draw text
    draw.text(position, text, fill=(100, 100, 100))
    
    # Convert to bytes
    img_bytes = io.BytesIO()
    img.save(img_bytes, format='PNG')
    return img_bytes.getvalue()

async def fetch_and_save_chart(session, name, url, save_dir):
    """Fetch a chart and save it to disk."""
    log.info(f"Fetching OPC chart: {name}")
    
    # Determine filename based on name
    if name.endswith('h'):
        # For time-based forecasts, use correct extension from URL
        extension = url.split('.')[-1]
        filename = f"opc_{name}.{extension}"
    else:
        # Use the original filename from URL for current analysis charts
        filename = f"opc_{url.split('/')[-1]}"
    
    file_path = save_dir / filename
    
    # Fetch the image
    image_data = await fetch_image(session, url)
    
    # Create placeholder if fetch failed
    if image_data is None:
        log.warning(f"Creating placeholder for failed chart: {name}")
        image_data = create_placeholder_image(name)
    
    # Save to disk
    try:
        with open(file_path, "wb") as f:
            f.write(image_data)
        log.info(f"Saved chart to {file_path}")
        return {"name": name, "file": str(filename), "status": "success"}
    except Exception as e:
        log.error(f"Error saving {name} to {file_path}: {e}")
        return {"name": name, "file": None, "status": "error", "error": str(e)}

async def collect_opc_charts(bundle_dir=None):
    """Collect all OPC charts and save them to the bundle directory."""
    # Create bundle directory if not provided
    if bundle_dir is None:
        now = datetime.now(timezone.utc)
        bundle_id = f"{uuid.uuid4().hex.replace('-', '')[:12]}_{int(now.timestamp())}"
        bundle_dir = Path("pacific_data") / bundle_id
        bundle_dir.mkdir(parents=True, exist_ok=True)
        log.info(f"Created bundle directory: {bundle_dir}")
    else:
        bundle_dir = Path(bundle_dir)
        if not bundle_dir.exists():
            bundle_dir.mkdir(parents=True, exist_ok=True)
            log.info(f"Created bundle directory: {bundle_dir}")
    
    # Create async HTTP session
    async with aiohttp.ClientSession() as session:
        # Create tasks for fetching all charts
        tasks = []
        for name, url in OPC_URLS.items():
            task = fetch_and_save_chart(session, name, url, bundle_dir)
            tasks.append(task)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
    
    # Count successes
    success_count = sum(1 for r in results if r["status"] == "success")
    log.info(f"Collected {success_count} out of {len(OPC_URLS)} OPC charts")
    
    # Update metadata if it exists
    metadata_path = bundle_dir / "metadata.json"
    if metadata_path.exists():
        try:
            with open(metadata_path, "r") as f:
                metadata = json.load(f)
            
            # Add or update OPC charts data
            if "opc_charts" not in metadata:
                metadata["opc_charts"] = {}
            
            metadata["opc_charts"]["count"] = success_count
            metadata["opc_charts"]["total"] = len(OPC_URLS)
            metadata["opc_charts"]["charts"] = [r for r in results if r["status"] == "success"]
            
            # Write updated metadata
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)
            log.info(f"Updated metadata with OPC charts information")
        except Exception as e:
            log.error(f"Error updating metadata: {e}")
    
    # Return bundle directory (useful when creating a new bundle)
    return str(bundle_dir)

async def main():
    """Entry point for OPC chart collection."""
    # Check if bundle ID was provided
    if len(sys.argv) > 1:
        bundle_id = sys.argv[1]
        log.info(f"Using provided bundle ID: {bundle_id}")
        bundle_dir = Path("pacific_data") / bundle_id
    else:
        bundle_dir = None
    
    bundle_path = await collect_opc_charts(bundle_dir)
    print(f"\n===== OPC CHARTS COLLECTION COMPLETE =====")
    print(f"Location: {bundle_path}")
    print(f"===========================================\n")

if __name__ == "__main__":
    asyncio.run(main())