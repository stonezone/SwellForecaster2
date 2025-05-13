#!/usr/bin/env python3
"""
data_curator.py - Curates the most relevant data for GPT forecast analysis

This module optimizes data selection for forecasting by:
1. Intelligently selecting only the most forecast-relevant data
2. Prioritizing high-value sources (buoys, surface maps, wave models)
3. Filtering out low-value or irrelevant data
4. Optimizing image selection and quality for GPT's visual processing

The goal is to maximize forecast quality while minimizing token usage
and processing time.
"""
import logging
import json
import base64
from pathlib import Path
from PIL import Image
from io import BytesIO
from datetime import datetime, timezone
import os
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("data_curator")

# Define high-priority data sources to always include
HIGH_PRIORITY_BUOYS = [
    "51001",  # NW Hawaii - critical for North Shore forecasting
    "51002",  # SW Hawaii - critical for South Shore forecasting
    "51004",  # SE Hawaii - critical for South Shore forecasting
    "51101"   # NW Oahu - directly measures North Shore conditions
]

# Critical OPC charts for forecasting with their relevance score (1-10)
HIGH_PRIORITY_CHARTS = {
    # Current surface pressure maps (highest value)
    "opc_P_sfc_full_ocean_color.png": 10,  # Full Pacific surface map
    "opc_P_w_sfc_color.png": 9,            # West Pacific surface map 
    "opc_P_e_sfc_color.png": 9,            # East Pacific surface map
    
    # Surface forecasts at different time horizons
    "opc_P_24hrsfc.gif": 9,
    "opc_P_48hrsfc.gif": 8,
    "opc_P_72hrsfc.gif": 7,
    
    # Wind & Wave period forecasts
    "opc_P_24hrww.gif": 9,
    "opc_P_48hrww.gif": 8,
    "opc_P_24hrwper.gif": 9,
    "opc_P_48hrwper.gif": 8,
    
    # SWAN model outputs - local Hawaii focused
    "pacioos_swan_north_shore.png": 10,
    "pacioos_swan_south_shore.png": 10,
    "pacioos_swan_direction.png": 9,
    "pacioos_swan_period.png": 9
}

# Data sources that have no direct forecasting value
LOW_VALUE_PATTERNS = [
    # Redundant or less accurate sources
    "nws_akq.json",      # East Coast data - irrelevant
    "nws_sew.json",      # Pacific Northwest data - too distant
    "windy_*_meteogram.png",  # Less accurate than model data
    "coops_*_water_temp.json",  # Only tangentially related to waves
    
    # API data that duplicates buoy/model information
    "windy_*.json",  # Lower quality than models/buoys
    
    # Data files consistently causing issues
    "ww3_hawaii_detail.gif",
    "ww3_north_pacific_detail.gif",
    "ww3_pacific_overview.gif",
    "ww3_south_pacific_detail.gif",
    
    # Non-Pacific or distant region data
    "*carribean*", "*atlantic*", "*east_coast*",
    
    # Local wind data (not oceanic swell)
    "wind_*_local.json"
]

def prepare_image(img_path, max_width=1024, max_bytes=200_000):
    """Optimize image for GPT processing."""
    try:
        if not img_path.exists():
            log.warning(f"Image file does not exist: {img_path}")
            return None, None
            
        # Check size to skip problematic files
        if img_path.stat().st_size < 100:
            log.warning(f"Image file too small, likely corrupt: {img_path}")
            return None, None
            
        # Only these formats are supported by GPT
        valid_formats = ['png', 'jpeg', 'jpg', 'gif', 'webp']
        
        # Special handling for GIF files
        if img_path.suffix.lower() == '.gif':
            with open(img_path, 'rb') as f:
                data = f.read()
                if len(data) > max_bytes:
                    # Convert large GIFs to PNG (safer than JPEG for this case)
                    im = Image.open(img_path)
                    if im.mode == 'P':
                        im = im.convert('RGB')
                    buf = BytesIO()
                    im.save(buf, format="PNG", optimize=True)
                    data = buf.getvalue()
                    return "png", base64.b64encode(data).decode()
                return "gif", base64.b64encode(data).decode()

        # Standard image processing
        im = Image.open(img_path)
        
        # Get the actual format (not just from file extension)
        actual_format = im.format.lower() if im.format else None
        if not actual_format or actual_format not in [f.lower() for f in valid_formats]:
            # Convert to PNG if format is not supported
            actual_format = "png"
        
        # Convert palette mode (P) to RGB for formats that need it
        if im.mode == 'P' and actual_format not in ['gif']:
            im = im.convert('RGB')

        # Ensure we use a supported format (default to PNG)
        out_format = "png"
        if actual_format in valid_formats:
            out_format = actual_format

        # Resize image if it's too large - optimizes token usage
        original_size = im.size
        if original_size[0] > max_width:
            # Keep aspect ratio
            ratio = max_width / original_size[0]
            new_height = int(original_size[1] * ratio)
            im = im.resize((max_width, new_height), Image.LANCZOS)
            log.info(f"Resized image from {original_size} to {im.size}")
        
        # Save to buffer with optimization
        buf = BytesIO()
        im.save(buf, format=out_format.upper(), optimize=True)
        data = buf.getvalue()
        
        # Further reduce size if needed to meet byte limit
        if len(data) > max_bytes:
            # Try higher compression first
            if out_format in ['jpeg', 'jpg']:
                buf = BytesIO()
                im.save(buf, format=out_format.upper(), quality=70, optimize=True)
                data = buf.getvalue()
            
            # If still too large, resize more aggressively
            if len(data) > max_bytes:
                # Calculate needed resize ratio
                ratio = 0.8  # Try 80% of current size
                new_width = int(im.size[0] * ratio)
                new_height = int(im.size[1] * ratio)
                im = im.resize((new_width, new_height), Image.LANCZOS)
                
                buf = BytesIO()
                im.save(buf, format=out_format.upper(), optimize=True)
                data = buf.getvalue()
        
        return out_format, base64.b64encode(data).decode()
        
    except Exception as e:
        log.warning(f"Error preparing image {img_path}: {e}")
        return None, None

def is_high_value_file(file_path):
    """Determine if a file contains high-value forecasting data."""
    # Get filename
    filename = file_path.name
    
    # Check for high-priority buoys
    if filename.startswith("ndbc_"):
        buoy_id = filename.replace("ndbc_", "").split(".")[0].split("_")[0]
        if buoy_id in HIGH_PRIORITY_BUOYS:
            return True
    
    # Check for high-priority charts
    if filename in HIGH_PRIORITY_CHARTS:
        return True
        
    # Check for other valuable files
    if (filename.startswith("pacioos_") or 
        filename.startswith("opc_") or 
        "swan" in filename or 
        "wave" in filename or
        "swell" in filename):
        # Check against low-value patterns
        for pattern in LOW_VALUE_PATTERNS:
            if pattern.startswith("*") and pattern.endswith("*"):
                # Middle wildcard
                if pattern[1:-1] in filename:
                    return False
            elif pattern.startswith("*"):
                # Suffix match
                if filename.endswith(pattern[1:]):
                    return False
            elif pattern.endswith("*"):
                # Prefix match
                if filename.startswith(pattern[:-1]):
                    return False
            elif pattern == filename:
                # Exact match
                return False
        return True
        
    # Metadata is always valuable
    if filename == "metadata.json":
        return True
        
    # Default to not high value
    return False

def get_data_value_score(file_path):
    """Score a file's value for forecasting from 0-10."""
    filename = file_path.name
    
    # High priority charts have predefined scores
    if filename in HIGH_PRIORITY_CHARTS:
        return HIGH_PRIORITY_CHARTS[filename]
    
    # High priority buoys
    if filename.startswith("ndbc_"):
        buoy_id = filename.replace("ndbc_", "").split(".")[0].split("_")[0]
        if buoy_id in HIGH_PRIORITY_BUOYS:
            return 10
    
    # SWAN model outputs are high priority
    if "swan" in filename.lower() and "pacioos" in filename.lower():
        return 9
    
    # OPC charts are generally valuable
    if filename.startswith("opc_P_"):
        # More recent forecasts are more valuable
        if "24hr" in filename:
            return 8
        elif "48hr" in filename:
            return 7
        elif "72hr" in filename:
            return 6
        return 5
    
    # Wave models
    if "wave" in filename.lower() or "ww3" in filename.lower():
        return 7
    
    # Wind data
    if "wind" in filename.lower():
        return 5
    
    # Default moderate value
    return 3

def curate_bundle_data(bundle_dir, max_images=8, max_buoys=5):
    """Curate the most relevant data from a bundle for GPT ingestion."""
    log.info(f"Curating high-value data from bundle: {bundle_dir}")
    
    # Ensure bundle_dir is a Path
    bundle_dir = Path(bundle_dir)
    if not bundle_dir.exists():
        log.error(f"Bundle directory does not exist: {bundle_dir}")
        return None
    
    # Initialize curated data structure
    curated_data = {
        "metadata": {},
        "buoy_data": [],
        "wave_models": [],
        "images": [],
        "other_data": [],
        "timestamps": {},
    }
    
    # Load metadata first
    metadata_path = bundle_dir / "metadata.json"
    if metadata_path.exists():
        try:
            with open(metadata_path, "r") as f:
                content = f.read().strip()
                if content:
                    curated_data["metadata"] = json.loads(content)
                    log.info("Loaded bundle metadata")
        except Exception as e:
            log.warning(f"Error loading metadata: {e}")
    
    # Find all data files in the bundle
    all_files = list(bundle_dir.glob("*.*"))
    
    # Filter out low-value files
    high_value_files = [f for f in all_files if is_high_value_file(f)]
    log.info(f"Found {len(high_value_files)} high-value files out of {len(all_files)} total files")
    
    # Process buoy data (prioritizing important buoys)
    buoy_files = [f for f in high_value_files if f.name.startswith("ndbc_") and f.name.endswith(".txt")]
    buoy_files = sorted(buoy_files, key=lambda f: 
                  10 if any(b in f.name for b in HIGH_PRIORITY_BUOYS) else 5)
    
    # Limit to most important buoys
    for buoy_file in buoy_files[:max_buoys]:
        try:
            with open(buoy_file, "r") as f:
                buoy_data = f.read().strip()
                
            if not buoy_data:
                continue
                
            buoy_name = buoy_file.stem.replace("ndbc_", "")
            curated_data["buoy_data"].append({
                "name": buoy_name,
                "data": buoy_data,
                "file": str(buoy_file.name),
            })
            log.info(f"Added high-priority buoy data: {buoy_name}")
        except Exception as e:
            log.warning(f"Error processing buoy file {buoy_file}: {e}")
    
    # Process wave model data
    model_files = [f for f in high_value_files if "info.json" in f.name]
    for model_file in model_files:
        try:
            with open(model_file, "r") as f:
                content = f.read().strip()
                
            if not content:
                continue
                
            model_data = json.loads(content)
            model_name = model_file.stem.replace("_info", "")
            curated_data["wave_models"].append({
                "name": model_name,
                "data": model_data,
                "file": str(model_file.name),
            })
            log.info(f"Added wave model data: {model_name}")
        except Exception as e:
            log.warning(f"Error processing model file {model_file}: {e}")
    
    # Process images (prioritize by value score)
    image_files = [f for f in high_value_files if f.suffix.lower() in ['.png', '.jpg', '.jpeg', '.gif', '.webp']]
    # Sort by forecast value
    image_files = sorted(image_files, key=get_data_value_score, reverse=True)
    
    # Process highest value images first, up to max_images
    for img_file in image_files[:max_images]:
        try:
            # Prepare and optimize the image for GPT
            img_format, img_base64 = prepare_image(img_file)
            if img_format and img_base64:
                curated_data["images"].append({
                    "name": img_file.stem,
                    "format": img_format,
                    "data": img_base64,
                    "file": str(img_file.name),
                    "value_score": get_data_value_score(img_file)
                })
                log.info(f"Added high-value image: {img_file.name} (score: {get_data_value_score(img_file)})")
        except Exception as e:
            log.warning(f"Error processing image file {img_file}: {e}")
    
    # Add timestamps
    curated_data["timestamps"]["curated"] = datetime.now(timezone.utc).isoformat()
    if "collected" in curated_data["metadata"]:
        curated_data["timestamps"]["collected"] = curated_data["metadata"]["collected"]
    
    # Log summary
    log.info(f"Curated {len(curated_data['buoy_data'])} buoys, {len(curated_data['wave_models'])} models, " +
             f"and {len(curated_data['images'])} images for GPT processing")
    
    return curated_data

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python data_curator.py BUNDLE_DIR")
        sys.exit(1)
    
    bundle_dir = sys.argv[1]
    curated_data = curate_bundle_data(bundle_dir)
    
    if curated_data:
        print(f"\nSuccessfully curated high-value data from {bundle_dir}")
        print(f"- Buoys: {len(curated_data['buoy_data'])}")
        print(f"- Wave Models: {len(curated_data['wave_models'])}")
        print(f"- Images: {len(curated_data['images'])}")
        print("\nTop priority images:")
        for img in sorted(curated_data['images'], key=lambda x: x.get('value_score', 0), reverse=True)[:5]:
            print(f"- {img['file']} (value score: {img.get('value_score', 'unknown')})")
    else:
        print(f"Failed to curate data from {bundle_dir}")
        sys.exit(1)