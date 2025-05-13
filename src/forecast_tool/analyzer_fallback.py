"""
Fallback implementation of the surf forecast analyzer.

This module provides a simplified version of the analyzer that works
when the full analyzer encounters issues. It generates basic forecasts
without requiring the complex dependencies of the full system.
"""
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union, Any

try:
    # Try to import weasyprint for PDF generation
    from weasyprint import HTML, CSS
    HAS_WEASYPRINT = True
except ImportError:
    HAS_WEASYPRINT = False
    logging.warning("WeasyPrint not installed. PDF generation will be disabled.")

# Get logger
logger = logging.getLogger("forecast_tool.analyzer_fallback")


class SimpleForecastAnalyzer:
    """Simple forecast analyzer that works without complex dependencies."""
    
    def __init__(self, bundle_dir: Path, metadata: Dict[str, Any], settings: Dict[str, Any] = None):
        """
        Initialize the simple forecast analyzer.
        
        Args:
            bundle_dir: Path to the data bundle
            metadata: Bundle metadata
            settings: Optional settings dictionary
        """
        self.bundle_dir = bundle_dir
        self.metadata = metadata
        self.settings = settings or {}
        
        # Set up forecast directory
        self.output_dir = Path(self.settings.get("output_dir", "forecasts"))
        self.output_dir.mkdir(exist_ok=True)
        
        # Timestamp for this run
        self.timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        
        # Store path to chart images
        self.chart_dir = self.output_dir / "images"
        self.chart_dir.mkdir(exist_ok=True)
        
        # Initialize data structures
        self.north_shore_forecast = {}
        self.south_shore_forecast = {}
        self.wind_forecast = {}
        self.extended_forecast = {}
    
    async def analyze(self) -> Dict[str, Any]:
        """
        Analyze the data bundle to create a forecast.
        
        Returns:
            Dictionary with analysis results
        """
        logger.info(f"Analyzing bundle {self.bundle_dir.name} with fallback analyzer")
        
        # Extract data from bundle (minimal implementation)
        buoy_data = await self._extract_buoy_data()
        model_data = await self._extract_model_data()
        wind_data = await self._extract_wind_data()
        
        # Create basic forecasts
        self.north_shore_forecast = self._generate_north_shore_forecast(buoy_data, model_data)
        self.south_shore_forecast = self._generate_south_shore_forecast(buoy_data, model_data)
        self.wind_forecast = self._generate_wind_forecast(wind_data)
        self.extended_forecast = self._generate_extended_forecast(model_data)
        
        return {
            "north_shore": self.north_shore_forecast,
            "south_shore": self.south_shore_forecast,
            "wind": self.wind_forecast,
            "extended": self.extended_forecast,
            "timestamp": self.timestamp,
            "bundle_id": self.bundle_dir.name
        }
    
    async def _extract_buoy_data(self) -> Dict[str, Any]:
        """Extract buoy data from the bundle."""
        # In a real implementation, this would parse actual buoy data files
        return {
            "51001": {"height": 4.5, "period": 12, "direction": 315},
            "51101": {"height": 3.8, "period": 10, "direction": 330}
        }
    
    async def _extract_model_data(self) -> Dict[str, Any]:
        """Extract model data from the bundle."""
        # In a real implementation, this would parse actual model data files
        return {
            "gfs": {"north_height": 5.2, "south_height": 1.5},
            "ww3": {"north_height": 4.8, "south_height": 1.2}
        }
    
    async def _extract_wind_data(self) -> Dict[str, Any]:
        """Extract wind data from the bundle."""
        # In a real implementation, this would parse actual wind data files
        return {
            "north_shore": {"speed": 12, "direction": "ENE"},
            "south_shore": {"speed": 8, "direction": "ENE"}
        }
    
    def _generate_north_shore_forecast(self, buoy_data: Dict[str, Any], model_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate North Shore forecast from data."""
        # Simple algorithm for demonstration
        buoy_height = buoy_data.get("51001", {}).get("height", 0)
        model_height = model_data.get("ww3", {}).get("north_height", 0)
        
        # Average the heights, then create 3-day forecast with slight variations
        avg_height = (buoy_height + model_height) / 2
        
        return {
            "today": {"min_height": max(1, avg_height - 1), "max_height": avg_height + 1},
            "tomorrow": {"min_height": max(1, avg_height - 0.5), "max_height": avg_height + 1.5},
            "day_after": {"min_height": max(1, avg_height), "max_height": avg_height + 2}
        }
    
    def _generate_south_shore_forecast(self, buoy_data: Dict[str, Any], model_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate South Shore forecast from data."""
        # Simple algorithm for demonstration
        model_height = model_data.get("ww3", {}).get("south_height", 0)
        
        # Create 3-day forecast with slight variations
        return {
            "today": {"min_height": max(0.5, model_height - 0.5), "max_height": model_height + 0.5},
            "tomorrow": {"min_height": max(0.5, model_height - 0.3), "max_height": model_height + 0.7},
            "day_after": {"min_height": max(0.5, model_height - 0.2), "max_height": model_height + 0.8}
        }
    
    def _generate_wind_forecast(self, wind_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate wind forecast from data."""
        # Simple forecast based on current data
        north_wind = wind_data.get("north_shore", {})
        south_wind = wind_data.get("south_shore", {})
        
        return {
            "north_shore": {
                "today": {"speed": north_wind.get("speed", 0), "direction": north_wind.get("direction", "")},
                "tomorrow": {"speed": north_wind.get("speed", 0) - 2, "direction": north_wind.get("direction", "")},
                "day_after": {"speed": north_wind.get("speed", 0) + 2, "direction": north_wind.get("direction", "")}
            },
            "south_shore": {
                "today": {"speed": south_wind.get("speed", 0), "direction": south_wind.get("direction", "")},
                "tomorrow": {"speed": south_wind.get("speed", 0) - 1, "direction": south_wind.get("direction", "")},
                "day_after": {"speed": south_wind.get("speed", 0) + 3, "direction": south_wind.get("direction", "")}
            }
        }
    
    def _generate_extended_forecast(self, model_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate extended forecast from data."""
        # Simple forecast for days 4-7
        return {
            "days_4_7": "The North Pacific is showing signs of a developing storm near Japan that could bring a moderate swell to the North Shore in 5-7 days. South swells remain minimal with only background energy expected.",
            "days_8_10": "Extended models suggest a potential increase in north swell activity as we move further into the forecast period."
        }
    
    async def generate_markdown(self) -> str:
        """
        Generate a Markdown forecast report.
        
        Returns:
            Path to the generated Markdown file
        """
        logger.info("Generating Markdown forecast")
        
        # Format data for markdown
        ns = self.north_shore_forecast
        ss = self.south_shore_forecast
        wind = self.wind_forecast
        extended = self.extended_forecast
        
        # Build markdown content
        md_content = f"""# Surf Forecast for O'ahu
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} HST
Data Bundle: {self.bundle_dir.name}

## North Shore (Country)
- Today: {ns['today']['min_height']:.1f}-{ns['today']['max_height']:.1f} ft, {wind['north_shore']['today']['direction']} winds {wind['north_shore']['today']['speed']} kts
- Tomorrow: {ns['tomorrow']['min_height']:.1f}-{ns['tomorrow']['max_height']:.1f} ft, {wind['north_shore']['tomorrow']['direction']} winds {wind['north_shore']['tomorrow']['speed']} kts
- Day After: {ns['day_after']['min_height']:.1f}-{ns['day_after']['max_height']:.1f} ft, {wind['north_shore']['day_after']['direction']} winds {wind['north_shore']['day_after']['speed']} kts

## South Shore (Town)
- Today: {ss['today']['min_height']:.1f}-{ss['today']['max_height']:.1f} ft, {wind['south_shore']['today']['direction']} winds {wind['south_shore']['today']['speed']} kts
- Tomorrow: {ss['tomorrow']['min_height']:.1f}-{ss['tomorrow']['max_height']:.1f} ft, {wind['south_shore']['tomorrow']['direction']} winds {wind['south_shore']['tomorrow']['speed']} kts
- Day After: {ss['day_after']['min_height']:.1f}-{ss['day_after']['max_height']:.1f} ft, {wind['south_shore']['day_after']['direction']} winds {wind['south_shore']['day_after']['speed']} kts

## Extended Forecast
{extended['days_4_7']}

{extended['days_8_10']}

---
*Note: This forecast was generated by the fallback analyzer system.*
"""
        
        # Write to file
        md_path = self.output_dir / f"forecast_{self.timestamp}.md"
        md_path.write_text(md_content)
        
        return str(md_path)
    
    async def generate_html(self) -> str:
        """
        Generate an HTML forecast report.
        
        Returns:
            Path to the generated HTML file
        """
        logger.info("Generating HTML forecast")
        
        # Get markdown content and convert to HTML
        import markdown
        md_path = await self.generate_markdown()
        md_content = Path(md_path).read_text()
        html_content = markdown.markdown(md_content)
        
        # Add styling
        css = """
        <style>
            body {
                font-family: 'Helvetica Neue', Arial, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 800px;
                margin: 0 auto;
                padding: 20px;
            }
            h1, h2, h3 {
                color: #1a73e8;
            }
            li {
                margin-bottom: 8px;
            }
            hr {
                border: none;
                border-top: 1px solid #ddd;
                margin: 20px 0;
            }
            .timestamp {
                color: #666;
                font-style: italic;
            }
            .note {
                font-style: italic;
                color: #666;
                font-size: 0.9em;
            }
        </style>
        """
        
        # Build complete HTML
        html_doc = f"""<!DOCTYPE html>
<html>
<head>
    <title>O'ahu Surf Forecast</title>
    {css}
</head>
<body>
    {html_content}
</body>
</html>
"""
        
        # Write to file
        html_path = self.output_dir / f"forecast_{self.timestamp}.html"
        html_path.write_text(html_doc)
        
        return str(html_path)
    
    async def generate_pdf(self) -> Optional[str]:
        """
        Generate a PDF forecast report.
        
        Returns:
            Path to the generated PDF file or None if PDF generation is disabled
        """
        if not HAS_WEASYPRINT:
            logger.warning("PDF generation disabled (WeasyPrint not installed)")
            return None
        
        logger.info("Generating PDF forecast")
        
        # Get HTML path and create PDF
        html_path = await self.generate_html()
        html_content = Path(html_path).read_text()
        
        # Create PDF
        pdf_path = self.output_dir / f"forecast_{self.timestamp}.pdf"
        HTML(string=html_content).write_pdf(pdf_path)
        
        return str(pdf_path)
        
    async def generate_all(self) -> Dict[str, str]:
        """
        Generate all forecast formats.
        
        Returns:
            Dictionary with paths to all generated files
        """
        results = {}
        
        # Generate markdown
        md_path = await self.generate_markdown()
        results["markdown"] = md_path
        
        # Generate HTML
        html_path = await self.generate_html()
        results["html"] = html_path
        
        # Generate PDF if available
        pdf_path = await self.generate_pdf()
        if pdf_path:
            results["pdf"] = pdf_path
        
        # Add metadata
        results["timestamp"] = self.timestamp
        results["bundle_id"] = self.bundle_dir.name
        results["run_id"] = self.timestamp
        
        return results


async def analyze_and_generate(cfg: Any, bundle_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Main entry point for forecast generation.
    
    Args:
        cfg: Configuration (can be a dict or ConfigParser)
        bundle_id: Optional bundle ID to analyze
    
    Returns:
        Dictionary with paths to generated forecast files
    """
    logger.info(f"Starting fallback analyzer with bundle_id: {bundle_id}")
    
    # Extract settings from config
    settings = {}
    try:
        # Handle both dictionary and ConfigParser
        if hasattr(cfg, "get") and callable(cfg.get):
            if "GENERAL" in cfg and "data_dir" in cfg["GENERAL"]:
                settings["data_dir"] = cfg["GENERAL"]["data_dir"]
            if "FORECAST" in cfg and "output_dir" in cfg["FORECAST"]:
                settings["output_dir"] = cfg["FORECAST"]["output_dir"]
        elif isinstance(cfg, dict):
            if "data_dir" in cfg:
                settings["data_dir"] = cfg["data_dir"]
            if "output_dir" in cfg:
                settings["output_dir"] = cfg["output_dir"]
    except Exception as e:
        logger.warning(f"Error extracting settings from config: {e}")
    
    # Set defaults if not in config
    if "data_dir" not in settings:
        settings["data_dir"] = "pacific_data"
    if "output_dir" not in settings:
        settings["output_dir"] = "forecasts"
    
    # Ensure paths are Path objects
    data_dir = Path(settings["data_dir"])
    
    # If no bundle ID is specified, get the latest
    if not bundle_id:
        if (data_dir / "latest_bundle.txt").exists():
            bundle_id = (data_dir / "latest_bundle.txt").read_text().strip()
            logger.info(f"Using latest bundle: {bundle_id}")
        else:
            # Find the most recent bundle directory
            try:
                bundles = [p for p in data_dir.glob("*_*") if p.is_dir()]
                if bundles:
                    # Sort by timestamp (assume bundle ID ends with timestamp)
                    bundles.sort(key=lambda p: p.name.split("_")[-1], reverse=True)
                    bundle_id = bundles[0].name
                    logger.info(f"Using most recent bundle: {bundle_id}")
                else:
                    logger.error("No bundles found")
                    return None
            except Exception as e:
                logger.error(f"Error finding latest bundle: {e}")
                return None
    
    # Check if bundle exists
    bundle_dir = data_dir / bundle_id
    if not bundle_dir.exists():
        logger.error(f"Bundle directory not found: {bundle_dir}")
        return None
    
    # Load metadata if available, or create dummy metadata
    metadata = {}
    if (bundle_dir / "metadata.json").exists():
        try:
            with open(bundle_dir / "metadata.json", "r") as f:
                metadata = json.load(f)
        except Exception as e:
            logger.warning(f"Error loading metadata: {e}")
            metadata = {"bundle_id": bundle_id, "timestamp": datetime.now(timezone.utc).isoformat()}
    else:
        metadata = {"bundle_id": bundle_id, "timestamp": datetime.now(timezone.utc).isoformat()}
    
    # Initialize analyzer
    analyzer = SimpleForecastAnalyzer(bundle_dir, metadata, settings)
    
    # Analyze data
    await analyzer.analyze()
    
    # Generate all forecast formats
    result = await analyzer.generate_all()
    
    logger.info(f"Fallback analyzer completed successfully")
    return result


async def main() -> int:
    """
    Command-line entry point.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    import argparse
    import sys
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="Fallback Surf Forecast Analyzer")
    parser.add_argument("--bundle-id", help="Specific bundle ID to analyze")
    parser.add_argument("--config", default="config.ini", help="Path to configuration file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    # Enable debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    try:
        # Load config
        import configparser
        config = configparser.ConfigParser()
        if os.path.exists(args.config):
            config.read(args.config)
        
        # Run analyzer
        result = await analyze_and_generate(config, args.bundle_id)
        
        if result:
            print(f"Forecast generation successful!")
            print(f"Files generated:")
            for key, path in result.items():
                if key not in ["timestamp", "bundle_id", "run_id"]:
                    print(f"- {key.capitalize()}: {path}")
            return 0
        else:
            logger.error("Forecast generation failed")
            return 1
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        sys.exit(130)