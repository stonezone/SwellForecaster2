#!/usr/bin/env python3
"""
pat_caldwell_analyzer.py - Simplified analyzer focused on Pat Caldwell style forecasts

This module provides a streamlined implementation of the forecast analyzer that:
1. Uses GPT-4.1 with tailored parameters for Pat Caldwell style
2. Gives equal emphasis to North Shore ("Country") and South Shore ("Town")
3. Applies the distinctive Pat Caldwell forecasting style
4. Maintains backward compatibility with the core SwellForecaster architecture
"""
import asyncio
import base64
import configparser
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from io import BytesIO

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("pat_caldwell_analyzer")

# Try to import dependencies with appropriate error handling
try:
    import markdown
    from PIL import Image
    from weasyprint import HTML, CSS
    from openai import AsyncOpenAI
except ImportError as e:
    log.error(f"Required dependency not found: {e}")
    log.error("Please install missing packages with: pip install openai pillow markdown weasyprint")
    sys.exit(1)

# Try to import utils module with fallbacks
try:
    import utils
except ImportError:
    log.warning("Could not import utils module directly")
    try:
        from forecast_tool.utils import file_io as utils
        log.info("Using forecast_tool.utils.file_io as utils")
    except ImportError:
        log.error("Could not import utils or forecast_tool.utils.file_io")
        sys.exit(1)


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


def load_pat_caldwell_style():
    """Load the Pat Caldwell style guide from the depreciated folder."""
    # Try a few possible locations for the style guide
    possible_paths = [
        Path("depreciated") / "pat_caldwell_style.md",
        Path("..") / "depreciated" / "pat_caldwell_style.md",
        Path(__file__).parent / "depreciated" / "pat_caldwell_style.md",
    ]
    
    for style_path in possible_paths:
        if style_path.exists():
            try:
                with open(style_path, "r") as f:
                    style_guide = f.read()
                log.info(f"Loaded Pat Caldwell style guide from {style_path}")
                return style_guide
            except Exception as e:
                log.warning(f"Failed to load Pat Caldwell style guide from {style_path}: {e}")
    
    log.warning("Pat Caldwell style guide not found in any expected location")
    return None


def find_latest_bundle():
    """Find the latest data bundle in the pacific_data directory."""
    data_dir = Path("pacific_data")
    if not data_dir.exists():
        log.error("Data directory not found")
        return None
        
    # Try to read latest_bundle.txt first
    try:
        with open(data_dir / "latest_bundle.txt", "r") as f:
            bundle_id = f.read().strip()
            bundle_dir = data_dir / bundle_id
            if bundle_dir.exists():
                log.info(f"Using latest bundle from file: {bundle_id}")
                return bundle_id
    except:
        pass
    
    # Find the most recent bundle by timestamp
    bundles = [p for p in data_dir.glob("*_*") if p.is_dir()]
    if not bundles:
        log.error("No bundles found")
        return None
        
    # Sort by timestamp (assuming bundle ID ends with timestamp)
    bundles.sort(key=lambda p: p.name.split("_")[-1], reverse=True)
    bundle_id = bundles[0].name
    log.info(f"Using most recent bundle: {bundle_id}")
    return bundle_id


def validate_bundle(bundle_id):
    """Validate that a bundle exists and has required files."""
    if not bundle_id:
        return False, "No bundle ID provided"
    
    bundle_dir = Path("pacific_data") / bundle_id
    if not bundle_dir.exists():
        return False, f"Bundle directory does not exist: {bundle_dir}"
    
    # Check for basic required files (minimal validation)
    if not any(bundle_dir.glob("*.json")) and not any(bundle_dir.glob("*.txt")):
        return False, f"Bundle directory appears empty: {bundle_dir}"
    
    return True, bundle_dir


async def collect_bundle_data(bundle_dir):
    """Collect data from the bundle directory for analysis."""
    log.info(f"Collecting data from bundle: {bundle_dir}")

    # Initialize data collection containers
    bundle_data = {
        "metadata": {},
        "buoy_data": [],
        "wave_models": [],
        "images": [],
        "other_data": [],
        "timestamps": {},
    }

    # Look for metadata first
    metadata_path = bundle_dir / "metadata.json"
    if metadata_path.exists():
        try:
            with open(metadata_path, "r") as f:
                content = f.read().strip()
                if not content:  # Skip empty files
                    log.warning(f"Empty metadata file: {metadata_path}")
                else:
                    bundle_data["metadata"] = json.loads(content)
                    log.info("Loaded bundle metadata")
        except json.JSONDecodeError as e:
            log.warning(f"JSON decode error in metadata: {e}")
        except Exception as e:
            log.warning(f"Error loading metadata: {e}")

    # Process NDBC buoy data
    buoy_files = list(bundle_dir.glob("ndbc_*.txt"))
    for buoy_file in buoy_files:
        try:
            with open(buoy_file, "r") as f:
                buoy_data = f.read()

            # Skip empty files
            if not buoy_data.strip():
                log.warning(f"Empty buoy data file: {buoy_file}")
                continue

            buoy_name = buoy_file.stem.replace("ndbc_", "")
            bundle_data["buoy_data"].append({
                "name": buoy_name,
                "data": buoy_data,
                "file": str(buoy_file.name),
            })
        except Exception as e:
            log.warning(f"Error loading buoy data from {buoy_file}: {e}")

    # Process wave model data (WW3, SWAN, etc.)
    wave_model_files = list(bundle_dir.glob("*info.json"))
    for model_file in wave_model_files:
        try:
            with open(model_file, "r") as f:
                content = f.read().strip()

            # Skip empty files
            if not content:
                log.warning(f"Empty wave model file: {model_file}")
                continue

            model_data = json.loads(content)

            # Skip files with empty objects or arrays
            if not model_data or (isinstance(model_data, dict) and not model_data) or (isinstance(model_data, list) and not model_data):
                log.warning(f"Empty wave model data in: {model_file}")
                continue

            model_name = model_file.stem.replace("_info", "")
            bundle_data["wave_models"].append({
                "name": model_name,
                "data": model_data,
                "file": str(model_file.name),
            })
        except json.JSONDecodeError as e:
            log.warning(f"JSON decode error in wave model data: {model_file}, {e}")
        except Exception as e:
            log.warning(f"Error loading wave model data from {model_file}: {e}")

    # Collect image files (for visualization)
    image_extensions = ['.png', '.jpg', '.jpeg', '.gif']
    image_files = []

    # Define problematic image files to skip
    skip_patterns = [
        "windy_*_meteogram.png",  # Skip problematic meteogram PNGs
        "windy_north_shore_meteogram.png",
        "windy_south_shore_meteogram.png",
        "ww3_hawaii_detail.gif",
        "ww3_north_pacific_detail.gif",
        "ww3_north_pacific_direction.gif",
        "ww3_north_pacific_period.gif",
        "ww3_pacific_overview.gif",
        "ww3_south_pacific_detail.gif",
        "ww3_south_pacific_period.gif"
    ]

    # Function to check if file should be skipped
    def should_skip(file_path):
        from fnmatch import fnmatch

        # Skip empty files
        if file_path.stat().st_size == 0:
            log.warning(f"Skipping empty image file: {file_path}")
            return True

        # Skip problematic filenames
        for pattern in skip_patterns:
            if fnmatch(file_path.name, pattern):
                log.warning(f"Skipping problematic image file: {file_path}")
                return True

        return False

    # Collect non-problematic image files
    for ext in image_extensions:
        for file_path in bundle_dir.glob(f"*{ext}"):
            if not should_skip(file_path):
                image_files.append(file_path)

    # Sort by relevance (simple approach - can be customized further)
    prioritized_images = []
    for pattern in ["swan", "ww3", "pacific", "swell", "wave", "buoy"]:
        matching = [img for img in image_files if pattern in img.name.lower()]
        prioritized_images.extend(matching)
        # Remove the matched files to avoid duplicates
        image_files = [img for img in image_files if img not in matching]

    # Add remaining images
    prioritized_images.extend(image_files)

    # Take up to 8 most relevant images
    relevant_images = prioritized_images[:8]
    log.info(f"Selected {len(relevant_images)} images for forecast visualization")

    for img_file in relevant_images:
        try:
            # Skip very small files that are likely corrupted
            if img_file.stat().st_size < 100:
                log.warning(f"Skipping suspiciously small image file: {img_file}")
                continue

            img_format, img_base64 = prepare_image(img_file)
            if img_format and img_base64:
                bundle_data["images"].append({
                    "name": img_file.stem,
                    "format": img_format,
                    "data": img_base64,
                    "file": str(img_file.name),
                })
            else:
                log.warning(f"Failed to prepare image: {img_file}")
        except Exception as e:
            log.warning(f"Error processing image file {img_file}: {e}")

    # Get additional JSON data files
    json_files = [f for f in bundle_dir.glob("*.json")
                  if not f.name.endswith("info.json") and f.name != "metadata.json"]

    # Define problematic JSON files to skip
    problematic_patterns = [
        "nws_*.json",
        "nws_akq.json",
        "nws_sew.json",
        "coops_*.json",
        "coops_honolulu_water_temp.json",
        "coops_kaneohe_water_temp.json",
        "coops_honolulu_wind.json",
        "coops_kaneohe_wind.json",
        "windy_*.json",
        "windy_ala_moana.json",
        "windy_buoy_51001.json",
        "windy_diamond_head.json",
        "windy_haleiwa.json",
        "windy_makaha.json",
        "windy_makapuu.json",
        "windy_north_shore.json",
        "windy_pipeline.json",
        "windy_south_shore.json",
        "windy_sunset.json",
        "windy_waikiki.json",
        "windy_waimea.json",
        "empty_*.json"
    ]

    # Function to check if JSON file should be skipped
    def should_skip_json(file_path):
        from fnmatch import fnmatch

        # Skip empty files
        if file_path.stat().st_size == 0:
            log.warning(f"Skipping empty JSON file: {file_path}")
            return True

        # Skip known problematic files
        for pattern in problematic_patterns:
            if fnmatch(file_path.name, pattern):
                log.info(f"Skipping known problematic JSON file: {file_path}")
                return True

        return False

    for json_file in json_files:
        if should_skip_json(json_file):
            continue

        try:
            with open(json_file, "r") as f:
                content = f.read().strip()

            # Skip empty files
            if not content:
                log.warning(f"Empty JSON file: {json_file}")
                continue

            json_data = json.loads(content)

            # Skip files with empty objects or arrays
            if not json_data or (isinstance(json_data, dict) and not json_data) or (isinstance(json_data, list) and not json_data):
                log.warning(f"Empty JSON data in: {json_file}")
                continue

            bundle_data["other_data"].append({
                "name": json_file.stem,
                "data": json_data,
                "file": str(json_file.name),
            })
        except json.JSONDecodeError as e:
            log.warning(f"JSON decode error in {json_file}: {e}")
        except Exception as e:
            log.warning(f"Error loading JSON data from {json_file}: {e}")

    # Add collection timestamp
    bundle_data["timestamps"]["collected"] = datetime.now(timezone.utc).isoformat()

    return bundle_data


async def analyze_data(bundle_data, config, bundle_id, style_guide=None):
    """
    Analyze data and generate forecast using GPT-4.1 with Pat Caldwell style.
    
    Args:
        bundle_data: Collected bundle data
        config: Configuration object 
        bundle_id: ID of the bundle
        style_guide: Optional Pat Caldwell style guide text
        
    Returns:
        Generated forecast text
    """
    log.info("Starting forecast analysis with Pat Caldwell style...")
    
    # Get OpenAI API key from config
    if "API" not in config or "openai_key" not in config["API"]:
        log.error("OpenAI API key not found in configuration")
        return None
    
    openai_key = config["API"]["openai_key"]
    
    # Initialize OpenAI client
    client = AsyncOpenAI(api_key=openai_key)
    
    # Prepare system prompt with Pat Caldwell style
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
    
    # Add Pat Caldwell style guide if available
    if style_guide:
        system_prompt += f"\n\nUSE THIS DISTINCTIVE FORECASTING STYLE:\n{style_guide}"
    
    # Prepare user prompt
    user_prompt = f"""
    Using the marine data collected on {datetime.now().strftime('%Y-%m-%d')}, generate a detailed
    surf forecast for Oʻahu in Pat Caldwell's distinctive style.
    
    Focus equally on both North Shore ("Country") and South Shore ("Town") forecasts, with
    comprehensive multi-phase analysis for both hemispheres.
    
    Bundle ID: {bundle_id}
    
    Available Data Summary:
    - Buoys: {', '.join([b['name'] for b in bundle_data['buoy_data']])}
    - Wave Models: {', '.join([m['name'] for m in bundle_data['wave_models']])}
    - Images: {', '.join([i['name'] for i in bundle_data['images']])}
    - Other Data: {', '.join([d['name'] for d in bundle_data['other_data']])}
    
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
    
    # Prepare image messages for the API
    image_messages = []
    for img in bundle_data["images"]:
        # Skip images with no format or data
        if not img['format'] or not img['data']:
            log.warning(f"Skipping image with missing format or data: {img.get('name', 'unknown')}")
            continue

        # Validate format is one of the allowed formats
        valid_formats = ['png', 'jpeg', 'jpg', 'gif', 'webp']
        format_to_use = img['format']
        if format_to_use not in valid_formats:
            log.warning(f"Converting image {img.get('name', 'unknown')} from {format_to_use} to png")
            format_to_use = 'png'

        image_messages.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:image/{format_to_use};base64,{img['data']}",
                "detail": "high"
            }
        })
    
    # Create the full message structure for the API
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": [
            {"type": "text", "text": user_prompt},
            *image_messages
        ]}
    ]
    
    # Call the OpenAI API to generate the forecast
    try:
        log.info("Calling GPT-4.1 to generate forecast...")
        response = await client.chat.completions.create(
            model="gpt-4.1",
            messages=messages,
            temperature=0.7,  # More creative for Pat's distinctive style
            max_tokens=4096,  # Long response for detailed forecast
            top_p=1.0,
            frequency_penalty=0.0,
            presence_penalty=0.0
        )
        
        # Extract the generated forecast text
        forecast_text = response.choices[0].message.content
        log.info("Successfully generated forecast")
        
        return forecast_text
    except Exception as e:
        log.error(f"Error generating forecast: {e}")
        return None


def generate_html(markdown_content, title="O'ahu Surf Forecast"):
    """Generate HTML from Markdown content."""
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


def generate_pdf(html_content, output_path):
    """Generate PDF from HTML content."""
    try:
        html = HTML(string=html_content)
        html.write_pdf(output_path)
        return True
    except Exception as e:
        log.error(f"Error generating PDF: {e}")
        return False


async def analyze_and_generate(config, bundle_id=None):
    """
    Main entry point for analysis and forecast generation.
    
    Args:
        config: Configuration object or path to config file
        bundle_id: Optional ID of the bundle to analyze
        
    Returns:
        Dictionary with paths to generated forecast files, or None if generation failed
    """
    # Initialize timestamp for output files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    
    # Load config if a path was provided
    if isinstance(config, (str, Path)):
        config_path = config
        config = configparser.ConfigParser()
        config.read(config_path)
    
    # Find bundle ID if not provided
    if not bundle_id:
        bundle_id = find_latest_bundle()
        if not bundle_id:
            log.error("Could not find a valid bundle")
            return None
    
    # Validate the bundle
    valid, result = validate_bundle(bundle_id)
    if not valid:
        log.error(result)
        return None
    
    bundle_dir = result
    
    # Load Pat Caldwell style guide
    style_guide = load_pat_caldwell_style()
    
    # Use optimized data curation to select the most relevant data
    try:
        # Try to import the data curator
        from data_curator import curate_bundle_data
        log.info("Using optimized data curation for more effective GPT processing")
        bundle_data = curate_bundle_data(bundle_dir)
        if not bundle_data:
            log.warning("Data curation failed, falling back to standard collection")
            bundle_data = await collect_bundle_data(bundle_dir)
    except ImportError:
        log.info("Data curator not available, using standard data collection")
        # Collect all data from the bundle using the standard approach
        bundle_data = await collect_bundle_data(bundle_dir)
    
    # Log what we're sending to GPT
    log.info(f"Sending to GPT: {len(bundle_data['buoy_data'])} buoy records, " +
            f"{len(bundle_data['wave_models'])} wave models, " +
            f"{len(bundle_data['images'])} images")
    
    # Generate the forecast
    forecast_text = await analyze_data(bundle_data, config, bundle_id, style_guide)
    if not forecast_text:
        log.error("Failed to generate forecast text")
        return None
    
    # Create the output directory if it doesn't exist
    output_dir = Path("forecasts")
    output_dir.mkdir(exist_ok=True)
    
    # Generate output paths
    md_path = output_dir / f"forecast_{timestamp}.md"
    html_path = output_dir / f"forecast_{timestamp}.html"
    pdf_path = output_dir / f"forecast_{timestamp}.pdf"
    
    # Write markdown file
    try:
        with open(md_path, "w") as f:
            f.write(forecast_text)
        log.info(f"Wrote markdown forecast to {md_path}")
    except Exception as e:
        log.error(f"Error writing markdown file: {e}")
        return None
    
    # Generate and write HTML file
    try:
        html_content = generate_html(forecast_text)
        with open(html_path, "w") as f:
            f.write(html_content)
        log.info(f"Wrote HTML forecast to {html_path}")
    except Exception as e:
        log.error(f"Error writing HTML file: {e}")
    
    # Generate and write PDF file
    try:
        if generate_pdf(html_content, pdf_path):
            log.info(f"Wrote PDF forecast to {pdf_path}")
        else:
            log.warning("Failed to generate PDF forecast")
    except Exception as e:
        log.error(f"Error writing PDF file: {e}")
    
    # Return the result
    return {
        "markdown": str(md_path),
        "html": str(html_path),
        "pdf": str(pdf_path),
        "bundle_id": bundle_id,
        "timestamp": timestamp,
        "run_id": timestamp
    }


async def main():
    """Test the analyzer directly."""
    import sys
    
    # Parse bundle ID from command line argument
    bundle_id = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Load config
    config = configparser.ConfigParser()
    config.read("config.ini")
    
    # Run the analyzer
    result = await analyze_and_generate(config, bundle_id)
    
    if result:
        print("\n========== FORECAST GENERATED SUCCESSFULLY ==========")
        print(f"Bundle ID: {result.get('bundle_id', 'N/A')}")
        print(f"Timestamp: {result.get('timestamp', 'N/A')}")
        print("\nGenerated files:")
        for key, path in result.items():
            if key not in ["timestamp", "bundle_id", "run_id"]:
                print(f"- {key.capitalize()}: {path}")
        print("\nTo view the forecast:")
        for key, path in result.items():
            if key in ["markdown", "html", "pdf"]:
                print(f"- Open {key.capitalize()}: open {path}")
        print("===================================================\n")
    else:
        print("\nForecast generation failed. See logs for details.")
        return 1
    
    return 0


if __name__ == "__main__":
    asyncio.run(main())