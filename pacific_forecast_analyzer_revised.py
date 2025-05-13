#!/usr/bin/env python3
# pacific_forecast_analyzer_revised.py – Enhanced analyzer that turns data bundle into surf forecast via GPT‑4.1
from __future__ import annotations
import os
import base64, json, logging, sys, utils, configparser, os, httpx, re, asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path
from io import BytesIO
from PIL import Image
from openai import OpenAI, AsyncOpenAI
import requests
import markdown
from weasyprint import HTML, CSS

# Import the new North Pacific analysis module
import north_pacific_analysis

# Import the South Swell Calibrator
from south_swell_calibration import SwellCalibrator

log = utils.log_init("analyzer")

# Load prompt templates from the JSON file
def load_prompts(prompts_file="prompts.json"):
    """
    Load prompt templates from the JSON configuration file.

    This function provides a flexible prompt templating system that allows
    customization of AI-generated forecasts without modifying code. The templates
    use string interpolation with {variable} syntax to inject dynamic data at runtime.

    Structure of prompts.json:
    - forecast: Main forecasting prompts
      - intro: System instruction for the AI forecaster
      - emphasis: Configurable emphasis sections (north/south/both shores)
      - data_sources: Template for listing available data sources
      - structure: Templates for different forecast structural components
      - specialized: Domain-specific analysis templates
    - chart_generation: Templates for generating visual charts

    Args:
        prompts_file (str): Path to the JSON file containing prompt templates.
                           Defaults to "prompts.json" in the current directory.

    Returns:
        dict: A nested dictionary of prompt templates, or None if loading fails.
              When None is returned, the code falls back to hardcoded prompts.

    See also:
        /docs/prompt_templates.md for a comprehensive guide to the templating system.
    """
    try:
        with open(prompts_file, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log.warning(f"Failed to load prompt templates from {prompts_file}: {e}")
        return None

def prepare_image(img_path: Path, max_bytes=200_000) -> tuple[str,str]:
    """Format an image for the OpenAI API (returns format, base64str)"""
    # Only these formats are supported by OpenAI's API
    valid_formats = ['png', 'jpeg', 'jpg', 'gif', 'webp']
    try:
        # Check if the file exists first
        if not img_path.exists():
            log.warning(f"Image file does not exist: {img_path}")
            return None, None
            
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
        
        # Get the actual format (not just from the file extension)
        actual_format = im.format.lower() if im.format else None
        if not actual_format or actual_format not in [f.lower() for f in valid_formats]:
            # Convert to PNG if format is not supported
            actual_format = "png"
        
        # Convert palette mode (P) to RGB for formats that need it
        if im.mode == 'P' and actual_format not in ['gif']:
            im = im.convert('RGB')

        # Ensure we use a supported format for OpenAI (default to PNG, which is well-supported)
        out_format = "png"
        if actual_format in valid_formats:
            out_format = actual_format

        buf = BytesIO()
        im.save(buf, format=out_format.upper(), optimize=True)
        data = buf.getvalue()
        
        # Resize if needed to meet byte limit
        if len(data) > max_bytes:
            im.thumbnail((1024,1024))
            buf = BytesIO() 
            im.save(buf, format=out_format.upper())
            data = buf.getvalue()
            
            # If still too large, reduce more
            if len(data) > max_bytes:
                im.thumbnail((800,800))
                buf = BytesIO()
                im.save(buf, format=out_format.upper())
                data = buf.getvalue()
        
        return out_format, base64.b64encode(data).decode()
    except Exception as e:
        log.warning(f"Error preparing image {img_path}: {e}")
        return None, None

def should_skip_file(file_path):
    """Check if a file should be skipped based on known problematic patterns."""
    # List of known problematic files
    problematic_files = [
        "nws_akq.json", "nws_sew.json", 
        "coops_honolulu_water_temp.json", "coops_kaneohe_water_temp.json",
        "windy_north_shore_meteogram.png", "windy_south_shore_meteogram.png",
        "coops_honolulu_wind.json", "coops_kaneohe_wind.json",
        "windy_ala_moana.json", "windy_buoy_51001.json", "windy_diamond_head.json",
        "windy_haleiwa.json", "windy_makaha.json", "windy_makapuu.json", 
        "windy_north_shore.json", "windy_pipeline.json", "windy_south_shore.json", 
        "windy_sunset.json", "windy_waikiki.json", "windy_waimea.json",
        "ww3_hawaii_detail.gif", "ww3_north_pacific_detail.gif", 
        "ww3_north_pacific_direction.gif", "ww3_north_pacific_period.gif",
        "ww3_pacific_overview.gif", "ww3_south_pacific_detail.gif",
        "ww3_south_pacific_period.gif"
    ]
    
    # Check if this file is in our problematic list
    if file_path.name in problematic_files:
        log.warning(f"Skipping known problematic file: {file_path.name}")
        return True
    
    # Check file size - skip suspiciously small files
    try:
        file_size = file_path.stat().st_size
        if file_size < 100:  # Files under 100 bytes are likely empty or corrupt
            log.warning(f"Skipping suspiciously small file ({file_size} bytes): {file_path.name}")
            return True
    except Exception:
        # If we can't check the file size, better to skip it
        log.warning(f"Could not check file size, skipping: {file_path.name}")
        return True
        
    return False

def load_bundle(data_dir, bundle_id=None):
    """Load a data bundle.
    
    Args:
        data_dir: Path to the data directory
        bundle_id: Optional ID of a specific bundle to load
        
    Returns:
        (metadata, bundle_dir): Tuple of metadata dictionary and bundle directory path
    """
    data_dir = Path(data_dir)
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
        
    # If bundle_id is provided, use that
    if bundle_id:
        bundle_dir = data_dir / bundle_id
        if not bundle_dir.exists():
            raise FileNotFoundError(f"Bundle not found: {bundle_dir}")
    else:
        # Otherwise, find the latest bundle
        try:
            # First check for the latest_bundle.txt file
            latest_bundle_file = data_dir / "latest_bundle.txt"
            if latest_bundle_file.exists():
                with open(latest_bundle_file, "r") as f:
                    bundle_id = f.read().strip()
                bundle_dir = data_dir / bundle_id
                if bundle_dir.exists():
                    log.info(f"Using latest bundle from file: {bundle_id}")
                else:
                    log.warning(f"Latest bundle from file not found: {bundle_id}")
                    bundle_id = None
            
            # If that fails, find the most recent bundle by timestamp
            if not bundle_id:
                bundles = sorted([p for p in data_dir.glob("*_*") if p.is_dir()], 
                              key=lambda p: p.name.split("_")[-1], reverse=True)
                if not bundles:
                    raise FileNotFoundError(f"No bundles found in {data_dir}")
                bundle_dir = bundles[0]
        except Exception as e:
            log.error(f"Error finding latest bundle: {e}")
            raise
    
    log.info(f"Loading bundle: {bundle_dir}")
    
    # Look for metadata.json
    meta_path = bundle_dir / "metadata.json"
    metadata = {}
    
    if meta_path.exists() and not should_skip_file(meta_path):
        try:
            with open(meta_path, "r") as f:
                content = f.read().strip()
                if content:  # Skip empty metadata
                    metadata = json.loads(content)
        except json.JSONDecodeError as e:
            log.warning(f"Failed to parse metadata.json: {e}")
        except Exception as e:
            log.warning(f"Error loading metadata: {e}")
    
    return metadata, bundle_dir

def select(metadata, n=8, bundle_dir=None):
    """Select the most relevant data for analysis.
    
    Args:
        metadata: The bundle metadata dictionary
        n: Maximum number of items to select
        bundle_dir: Optional path to the bundle directory
        
    Returns:
        A dictionary of selected data
    """
    selected = {
        "buoys": [],
        "charts": [],
        "models": [],
        "forecasts": [],
        "weather": []
    }
    
    # Skip specific problematic files
    if bundle_dir:
        # First load NDBC buoy data - very reliable, high priority
        buoy_files = [f for f in bundle_dir.glob("ndbc_*.txt") if not should_skip_file(f)]
        for buoy_file in buoy_files[:5]:  # Limit to 5 buoys
            try:
                with open(buoy_file, "r") as f:
                    content = f.read()
                
                # Skip if content is too short
                if len(content) < 50:
                    continue
                    
                buoy_id = buoy_file.stem.replace("ndbc_", "")
                selected["buoys"].append({
                    "id": buoy_id,
                    "name": f"NDBC Buoy {buoy_id}",
                    "data": content
                })
            except Exception as e:
                log.warning(f"Error loading buoy data from {buoy_file}: {e}")
    
        # Load SWAN model data
        swan_files = [f for f in bundle_dir.glob("*swan*.json") if not should_skip_file(f)]
        for swan_file in swan_files[:2]:
            try:
                with open(swan_file, "r") as f:
                    data = json.load(f)
                selected["models"].append({
                    "name": "PacIOOS SWAN Model",
                    "source": "pacioos",
                    "data": data
                })
            except Exception as e:
                log.warning(f"Error loading SWAN data from {swan_file}: {e}")
        
        # Load relevant images
        image_files = []
        for ext in [".png", ".jpg", ".jpeg", ".gif"]:
            image_files.extend([img for img in bundle_dir.glob(f"*{ext}") 
                                if not should_skip_file(img)])
        
        # Prioritize images with relevant keywords in filename
        relevant_keywords = ["swan", "pacioos", "pacific", "wave", "swell"]
        prioritized_images = []
        
        for keyword in relevant_keywords:
            matches = [img for img in image_files if keyword in img.name.lower()]
            prioritized_images.extend(matches)
            # Remove matched files to avoid duplicates
            image_files = [img for img in image_files if img not in matches]
        
        # Add up to 4 top priority images
        for img_file in prioritized_images[:4]:
            img_format, img_base64 = prepare_image(img_file)
            if img_format and img_base64:
                selected["charts"].append({
                    "name": img_file.stem,
                    "source": "pacioos" if "pacioos" in img_file.name.lower() else "noaa",
                    "format": img_format,
                    "data": img_base64
                })
    
    return selected

def extract_significant_south_swells(metadata, bundle_dir):
    """Extract data about significant south swells from the bundle.
    
    Returns:
        List of dictionaries containing south swell information
    """
    # Default return if no data available
    default_result = [{
        "direction": 190,
        "period": 16,
        "height": 2.5,
        "start_date": None,
        "peak_date": None,
        "end_date": None,
        "confidence": "medium"
    }]
    
    # Sophisticated swell detection (placeholder)
    result = []
    
    # Try to find better south swell data
    try:
        south_patterns_path = bundle_dir / "south_swell_patterns.json"
        if south_patterns_path.exists() and not should_skip_file(south_patterns_path):
            with open(south_patterns_path, "r") as f:
                patterns = json.load(f)
                
            if patterns and isinstance(patterns, list):
                result = patterns
                log.info(f"Found {len(patterns)} south swell patterns")
            else:
                log.warning("South swell patterns file exists but contains no valid data")
                result = default_result
        else:
            # No patterns file, try to infer from buoy data
            log.info("No south swell patterns file, using buoy data")
            buoy_files = [f for f in bundle_dir.glob("ndbc_51002*.txt") if not should_skip_file(f)]
            
            if buoy_files:
                with open(buoy_files[0], "r") as f:
                    buoy_data = f.read()
                
                # Simple regex to extract south swell data
                # For a proper implementation, we would use a more sophisticated parsing approach
                period_match = re.search(r"(\d+\.\d+)\s+sec", buoy_data)
                height_match = re.search(r"WVHT\s+(\d+\.\d+)\s+m", buoy_data)
                direction_match = re.search(r"MWD\s+(\d+)\s+deg", buoy_data)
                
                if period_match and height_match:
                    period = float(period_match.group(1))
                    height = float(height_match.group(1))
                    direction = int(direction_match.group(1)) if direction_match else 190
                    
                    # Filter for south swells (160-220 degrees)
                    if 160 <= direction <= 220:
                        result = [{
                            "direction": direction,
                            "period": period,
                            "height": height,
                            "confidence": "medium"
                        }]
                        log.info(f"Extracted south swell from buoy: {direction}° at {period}s, {height}m")
            
            if not result:
                log.warning("Could not extract south swell data, using defaults")
                result = default_result
                
    except Exception as e:
        log.warning(f"Error extracting south swells: {e}")
        result = default_result
        
    return result

async def generate_forecast(cfg, selected, bundle_dir, significant_swells=None, 
                           north_shore_analysis=None):
    """Generate forecast using GPT-4.1 with enhanced prompts.
    
    Args:
        cfg: Configuration dictionary
        selected: Dictionary of selected data
        bundle_dir: Path to the bundle directory
        significant_swells: Optional list of significant south swells
        north_shore_analysis: Optional North Pacific analysis
        
    Returns:
        Generated forecast text
    """
    try:
        api_key = cfg["API"]["OPENAI_KEY"].strip()
        if not api_key:
            log.error("OpenAI API key not configured")
            return None
    except (KeyError, AttributeError):
        log.error("Error accessing API key from config")
        return None
        
    # Load prompt templates
    prompts = load_prompts()
    if not prompts:
        # Fallback to hardcoded prompts
        log.warning("Using hardcoded prompts (prompts.json not found or invalid)")
        system_prompt = """
        You are a veteran Hawaiian surf forecaster with over 30 years of experience analyzing 
        Pacific storm systems and delivering detailed, educational surf forecasts for Hawaii.

        Use your deep expertise in swell mechanics, Pacific climatology, and historical analogs 
        to analyze the marine data (surf, wind, swell) and generate a comprehensive 10-day surf 
        forecast for Oʻahu.

        IMPORTANT: Both North Pacific and South Pacific activity are significant. Your forecast 
        should provide detailed analysis for both North Shore ('Country') and South Shore ('Town'), 
        with equal emphasis and detail for both shore forecasts.
        """
        
        user_prompt = f"""
        Using the marine data collected, generate a detailed surf forecast for Oʻahu.

        Focus equally on both North Shore ("Country") and South Shore ("Town") forecasts, with
        comprehensive multi-phase analysis for both hemispheres.

        Bundle ID: {bundle_dir.name}

        FORMAT YOUR FORECAST WITH THESE SECTIONS:
        1. Brief overview of current conditions
        2. "Moon's view" section for high-level assessment
        3. North Shore ("Country") detailed analysis with:
            - Break down systems into clearly labeled phases
            - Include "Backstory" for previous phases
            - Add "Pulse Status" with current buoy readings
            - Provide "Prognosis" with specific direction ranges
        4. South Shore ("Town") analysis with same level of detail
        5. Include "Wooly Worm" references for long-range forecasts
        6. End with a tabular forecast in standard format

        Use precise directional ranges (e.g., "320-340 degrees") rather than general cardinal directions.
        """
    else:
        log.info("Using templates from prompts.json")
        # Apply template system using interpolation
        try:
            system_prompt = prompts["forecast"]["intro"]
            
            # Choose emphasis based on config and seasonal patterns
            try:
                emphasis_type = cfg["FORECAST"]["emphasis"].lower()
            except (KeyError, AttributeError):
                emphasis_type = "both"
                
            if emphasis_type not in ["north", "south", "both"]:
                emphasis_type = "both"
                
            # Add emphasis section
            system_prompt += "\n\n" + prompts["forecast"]["emphasis"][emphasis_type]
            
            # Prepare data sources summary
            data_sources = []
            if selected["buoys"]:
                buoy_ids = [b["id"] for b in selected["buoys"]]
                data_sources.append(f"Buoys: {', '.join(buoy_ids)}")
            if selected["models"]:
                model_names = [m["name"] for m in selected["models"]]
                data_sources.append(f"Models: {', '.join(model_names)}")
            if selected["charts"]:
                chart_names = [c["name"] for c in selected["charts"]]
                data_sources.append(f"Charts: {', '.join(chart_names)}")
                
            # Build user prompt with data sources and structure
            user_prompt = prompts["forecast"]["data_sources"].format(
                bundle_id=bundle_dir.name,
                sources="\n".join([f"- {s}" for s in data_sources])
            )
            
            # Add forecast structure
            user_prompt += "\n\n" + prompts["forecast"]["structure"]["detailed"]
            
            # Add specialized sections if available
            if significant_swells:
                south_swells = []
                for swell in significant_swells:
                    direction = swell.get("direction", 190)
                    period = swell.get("period", 16)
                    height = swell.get("height", 2.5)
                    south_swells.append(f"{direction}° at {period}s, {height}m")
                
                if south_swells:
                    user_prompt += "\n\n" + prompts["forecast"]["specialized"]["south_swell"].format(
                        south_swells="\n".join([f"- {s}" for s in south_swells])
                    )
                    
            if north_shore_analysis and "storms" in north_shore_analysis:
                storms = []
                for storm in north_shore_analysis.get("storms", []):
                    location = storm.get("location", "Unknown")
                    direction = storm.get("direction", 0)
                    strength = storm.get("strength", "Unknown")
                    storms.append(f"{strength} storm at {location}, {direction}°")
                
                if storms:
                    user_prompt += "\n\n" + prompts["forecast"]["specialized"]["north_pacific"].format(
                        storms="\n".join([f"- {s}" for s in storms])
                    )
        except Exception as e:
            log.error(f"Error applying prompt templates: {e}")
            # Fallback to hardcoded prompts
            system_prompt = """
            You are a veteran Hawaiian surf forecaster with deep expertise in
            analyzing Pacific storm systems and delivering detailed, educational
            surf forecasts for Hawaii. Generate a comprehensive 10-day forecast
            with equal emphasis on North Shore and South Shore conditions.
            """
            
            user_prompt = f"""
            Using the marine data collected, generate a detailed
            surf forecast for Oʻahu with sections for current conditions,
            North Shore analysis, South Shore analysis, and a tabular forecast.
            
            Bundle ID: {bundle_dir.name}
            """
    
    # Initialize OpenAI client
    client = AsyncOpenAI(api_key=api_key)
    
    # Build API call messages
    messages = [
        {"role": "system", "content": system_prompt}
    ]
    
    # Build user message with content array (text + images)
    user_content = [{"type": "text", "text": user_prompt}]
    
    # Add images to the message
    for chart in selected["charts"]:
        if "format" in chart and "data" in chart:
            user_content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/{chart['format']};base64,{chart['data']}",
                    "detail": "high"
                }
            })
    
    messages.append({"role": "user", "content": user_content})
    
    # Call the API
    try:
        log.info("Calling GPT-4.1 to generate forecast...")
        response = await client.chat.completions.create(
            model="gpt-4.1",  # Using GPT-4.1 as specified
            messages=messages,
            temperature=0.7,  # Slightly creative for forecaster's voice
            max_tokens=4000,  # Substantial response for detailed forecast
            top_p=1.0,
            frequency_penalty=0.0,
            presence_penalty=0.0
        )
        
        forecast_text = response.choices[0].message.content
        log.info("Successfully generated forecast")
        return forecast_text
    except Exception as e:
        log.error(f"Error calling OpenAI API: {e}")
        return None

def markdown_to_html(markdown_content, title="O'ahu Surf Forecast"):
    """Create an HTML document with CSS styling."""
    html_content = f"""<!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{title}</title>
        <style>
            body {{
                font-family: 'Helvetica Neue', Arial, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 800px;
                margin: 0 auto;
                padding: 20px;
            }}
            h1, h2, h3 {{
                color: #0066cc;
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
                margin: 20px 0;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
            tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            code {{
                background-color: #f5f5f5;
                padding: 2px 4px;
                border-radius: 4px;
            }}
            blockquote {{
                border-left: 4px solid #0066cc;
                padding-left: 15px;
                color: #666;
            }}
            .forecast-date {{
                color: #666;
                font-style: italic;
            }}
            .moon-view {{
                background-color: #f7f7f7;
                padding: 15px;
                border-radius: 5px;
                margin: 15px 0;
            }}
            .north-shore, .south-shore {{
                border-top: 3px solid #0066cc;
                padding-top: 10px;
                margin-top: 30px;
            }}
            .forecast-table {{
                overflow-x: auto;
            }}
            .wooly-worm {{
                font-style: italic;
                color: #555;
                background-color: #f7f7f7;
                padding: 15px;
                border-radius: 5px;
            }}
        </style>
    </head>
    <body>
        {markdown.markdown(markdown_content, extensions=['tables'])}
    </body>
    </html>
    """
    return html_content

def generate_charts(data, forecasts, timestamp_str):
    """Generate HTML charts for the forecast."""
    chart_html = f"""
    <div class="forecast-charts">
        <h2>Forecast Charts</h2>
        <p class="chart-date">Generated: {timestamp_str}</p>
    """
    
    # Add 5-day forecast charts
    for day in range(1, 6):
        # Get forecast date
        from datetime import datetime, timedelta
        forecast_date = (datetime.now() + timedelta(days=day-1)).strftime("%A, %B %d")
        
        chart_html += f"""<h2>Day {day} Forecast: {forecast_date}</h2>"""
        
        # Add forecast row
        chart_html += f"""<div class="chart-row" style="display: flex; flex-wrap: wrap; justify-content: space-between;">"""
        
        # North Shore chart
        chart_html += f"""
        <div class="chart-cell" style="width: 48%; margin-bottom: 20px;">
            <h3>North Shore</h3>
            <div class="chart-content" style="border: 1px solid #ccc; padding: 10px;">
                <p>Wave Height: 3-5 ft</p>
                <p>Period: 10-12 seconds</p>
                <p>Direction: 330-350°</p>
            </div>
        </div>
        """
        
        # South Shore chart
        chart_html += f"""
        <div class="chart-cell" style="width: 48%; margin-bottom: 20px;">
            <h3>South Shore</h3>
            <div class="chart-content" style="border: 1px solid #ccc; padding: 10px;">
                <p>Wave Height: 1-3 ft</p>
                <p>Period: 14-16 seconds</p>
                <p>Direction: 180-200°</p>
            </div>
        </div>
        """
        
        chart_html += """</div>"""
    
    chart_html += """</div>"""
    return chart_html

def generate_pdf(html_content, pdf_path):
    """Generate PDF from HTML content."""
    try:
        html = HTML(string=html_content)
        html.write_pdf(pdf_path)
        return True
    except Exception as e:
        log.error(f"Error generating PDF: {e}")
        return False

async def main(cfg, meta, bundle_dir, out_dir, timestamp):
    """
    Main function for generating forecasts called from analyze_and_generate
    """
    # Extract data from the bundle
    selected = select(meta, n=12, bundle_dir=bundle_dir)

    # Auto-detect significant south swells
    significant_swells = extract_significant_south_swells(meta, bundle_dir)

    # Get North Shore analysis
    try:
        # Note: north_pacific_analysis module function not available, using dummy data
        north_shore_analysis = {"storms": [], "swell_forecast": {}, "buoy_data": {}}
    except Exception as e:
        log.warning(f"Error getting North Pacific analysis: {e}")
        north_shore_analysis = {}

    # Handle "auto" settings properly with seasonal biases
    try:
        south_emphasis = cfg["FORECAST"]["south_swell_emphasis"].lower()
    except (KeyError, AttributeError, TypeError):
        south_emphasis = "auto"

    # Apply seasonal bias for south swell using enhanced utility function
    # This provides a more nuanced probability based on the time of year
    south_swell_probability = utils.get_south_swell_status()

    # South swell season: April-September (stronger bias in Jun-Aug)
    south_seasonal_bias = south_swell_probability >= 0.5  # Use 50% as threshold for seasonal bias

    if south_seasonal_bias:
        # Add additional logic for south swell season
        log.info(f"South swell seasonal bias active (probability: {south_swell_probability:.2f})")
        if south_emphasis == "auto":
            south_emphasis = "true"
        log.info(f"Applying South Shore seasonal bias (probability: {south_swell_probability:.2f})")
        # Log extra information for peak season
        if south_swell_probability >= 0.7:
            log.info("Peak South swell season - applying stronger bias")
    else:
        # Apply North Shore bias in winter months
        log.info(f"North Shore seasonal bias active (probability: {1-south_swell_probability:.2f})")
        if south_emphasis == "auto":
            south_emphasis = "false"

    # Generate the forecast text
    forecast_text = await generate_forecast(cfg, selected, bundle_dir, 
                                         significant_swells, north_shore_analysis)
                                         
    if not forecast_text:
        log.error("Failed to generate forecast text")
        return None

    # Make sure output directory exists
    Path(out_dir).mkdir(exist_ok=True)

    # Create timestamp string
    ts_str = timestamp.strftime("%Y%m%d_%H%M")

    # Write markdown file
    md_path = out_dir / f"forecast_{ts_str}.md"
    try:
        with open(md_path, "w") as f:
            f.write(forecast_text)
        log.info(f"Wrote markdown forecast to {md_path}")
    except Exception as e:
        log.error(f"Error writing markdown file: {e}")
        return None

    # Create HTML version
    html_content = markdown_to_html(forecast_text)
    
    # Save HTML file
    html_path = out_dir / f"forecast_{ts_str}.html"
    try:
        with open(html_path, "w") as f:
            f.write(html_content)
        log.info(f"Wrote HTML forecast to {html_path}")
    except Exception as e:
        log.error(f"Error writing HTML file: {e}")

    # Generate PDF
    pdf_path = out_dir / f"forecast_{ts_str}.pdf"
    success = generate_pdf(html_content, pdf_path)
    if success:
        log.info(f"Wrote PDF forecast to {pdf_path}")
    else:
        log.warning("Failed to generate PDF forecast")

    # Return file paths and metadata
    return {
        "md_path": str(md_path),
        "html_path": str(html_path),
        "pdf_path": str(pdf_path),
        "timestamp": timestamp.isoformat(),
        "bundle_id": bundle_dir.name
    }

# Import the NorthPacificAnalyzer class - must be imported before it's used
# from north_pacific_analysis import NorthPacificAnalyzer

async def analyze_and_generate(cfg, bundle_id=None):
    """Main entry point for analysis and forecast generation from the run script"""
    data_dir = Path(cfg["GENERAL"]["data_dir"]).expanduser()
    out_dir = Path(cfg["FORECAST"]["output_dir"]).expanduser()
    out_dir.mkdir(exist_ok=True)

    api_key = cfg["API"]["OPENAI_KEY"].strip()
    if not api_key:
        log.error("OpenAI API key not configured")
        return None

    timestamp = datetime.now(tz=timezone(timedelta(hours=-10), "HST"))  # Hawaii time

    try:
        # Load the data bundle
        meta, bundle_dir = load_bundle(data_dir, bundle_id)
        log.info(f"Analyzing bundle: {bundle_dir}")

        # Generate the forecast
        return await main(cfg, meta, bundle_dir, out_dir, timestamp)
    except Exception as e:
        log.error(f"Analyze and generate error: {e}")
        return None
        
# Add this class inside pacific_forecast_analyzer_revised.py just after imports
# to provide a fallback implementation

class NorthPacificAnalyzer:
    """
    Analysis class for North Pacific swells.
    
    This is a fallback implementation for the north_pacific_analysis module.
    """
    
    def __init__(self, bundle_dir, metadata):
        """
        Initialize the analyzer with metadata.
        
        Args:
            bundle_dir: Path to the data bundle
            metadata: Bundle metadata dictionary
        """
        self.bundle_dir = bundle_dir
        self.metadata = metadata
        self.log = logging.getLogger("north_pacific.analyzer")
        
        # Create output directory if needed
        output_dir = Path("forecasts")
        output_dir.mkdir(exist_ok=True)
        
        # State for analysis results
        self.storms = []
        self.predictions = {}
        self.breaks = {}
    
    def analyze(self):
        """
        Analyze the North Pacific data.
        
        Returns:
            Dictionary of analysis results
        """
        from datetime import datetime, timezone
        import utils
        
        self.log.info("Analyzing North Pacific data with fallback implementation")
        
        # Simulate storm detection for testing
        self.storms = [
            {
                "location": "Off Japan",
                "pressure": 970,
                "fetch": 600,
                "direction": 310,
                "strength": "Strong",
                "wave_potential": 12
            }
        ]
        
        # Simulate wave predictions for breaks
        for break_name in ["Pipeline", "Sunset", "Waimea"]:
            self.breaks[break_name] = {
                "wave_height_range": [4, 8],
                "arrival_date": (datetime.now(timezone.utc)).isoformat(),
                "primary_direction": 310,
                "primary_period": 14
            }
        
        # Return analysis results
        return {
            "storms": self.storms,
            "swell_forecast": self.breaks,
            "analysis_timestamp": utils.utcnow()
        }