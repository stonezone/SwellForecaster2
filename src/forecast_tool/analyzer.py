"""Forecast analyzer module for SwellForecaster."""
import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union, Any

import markdown
from weasyprint import HTML

from .data.collection_context import CollectionResult
from .models.settings import Settings, load_settings
from .utils import (
    setup_logging, read_file, save_json, write_file, 
    load_json, file_exists, utcnow
)

logger = logging.getLogger("analyzer")


class ForecastAnalyzer:
    """
    Analyzes collected data and generates surf forecasts.
    
    Uses OpenAI API for natural language processing of the data.
    """
    
    def __init__(self, settings: Settings):
        """
        Initialize the forecast analyzer.
        
        Args:
            settings: Application settings
        """
        self.settings = settings
        self.api_key = settings.api.openai_key
        self.model = settings.forecast.openai_model
        self.temperature = settings.forecast.temperature
        self.north_shore_emphasis = settings.forecast.north_swell_emphasis
        self.south_shore_emphasis = settings.forecast.south_swell_emphasis
        self.output_format = settings.forecast.output_format
        
        # Helper functions for north/south shore analysis
        # These would be imported from the appropriate modules in a full implementation
        self.north_shore_analyzer = None
        self.south_shore_analyzer = None
    
    async def load_bundle(self, bundle_id: Optional[str] = None) -> Tuple[Dict[str, Any], Path]:
        """
        Load a data bundle for analysis.
        
        Args:
            bundle_id: Specific bundle ID to load (defaults to latest)
            
        Returns:
            Tuple of (metadata, bundle directory)
            
        Raises:
            ValueError: If the bundle does not exist or is invalid
        """
        # Get the base directory for data bundles
        data_dir = self.settings.general.data_dir
        
        # If no bundle ID is specified, get the latest
        if not bundle_id:
            if await file_exists(data_dir / "latest_bundle.txt"):
                bundle_id = (await read_file(data_dir / "latest_bundle.txt")).strip()
            else:
                raise ValueError("No latest_bundle.txt file found")
        
        # Check that the bundle directory exists
        bundle_dir = data_dir / bundle_id
        if not await file_exists(bundle_dir):
            raise ValueError(f"Bundle directory not found: {bundle_dir}")
        
        # Load metadata
        if not await file_exists(bundle_dir / "metadata.json"):
            raise ValueError(f"Metadata file not found in bundle: {bundle_dir}")
        
        metadata = await load_json(bundle_dir / "metadata.json")
        
        # Add full paths to results
        for result in metadata.get("results", []):
            result["path"] = bundle_dir / result["filename"]
        
        logger.info(f"Loaded bundle {bundle_id} with {len(metadata.get('results', []))} files")
        
        return metadata, bundle_dir
    
    async def generate_forecast(self, bundle_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate a surf forecast from the data in a bundle.
        
        Args:
            bundle_id: Specific bundle ID to use (defaults to latest)
            
        Returns:
            Dictionary with forecast results
            
        Raises:
            ValueError: If the bundle does not exist or is invalid
        """
        # Load bundle
        metadata, bundle_dir = await self.load_bundle(bundle_id)
        
        # Process data for forecast
        forecast_data = await self._prepare_forecast_data(metadata, bundle_dir)
        
        # Generate forecast text
        forecast_text = await self._generate_forecast_text(forecast_data)
        
        # Format forecast for output
        formatted_forecast = await self._format_forecast(forecast_text, metadata)
        
        # Save forecast to files
        await self._save_forecast(formatted_forecast, metadata)
        
        return formatted_forecast
    
    async def _prepare_forecast_data(self, metadata: Dict[str, Any], bundle_dir: Path) -> Dict[str, Any]:
        """
        Prepare data for forecast generation.
        
        Args:
            metadata: Bundle metadata
            bundle_dir: Path to bundle directory
            
        Returns:
            Dictionary with prepared forecast data
        """
        logger.info("Preparing data for forecast generation")
        
        # Get the ID and timestamp
        run_id = metadata["run_id"]
        timestamp = metadata["timestamp"]
        
        # Select and prepare buoy data
        buoy_data = await self._prepare_buoy_data(metadata, bundle_dir)
        
        # Select and prepare chart data
        chart_data = await self._prepare_chart_data(metadata, bundle_dir)
        
        # Select and prepare model data
        model_data = await self._prepare_model_data(metadata, bundle_dir)
        
        # Analyze North Shore conditions if enabled
        north_shore_analysis = None
        if self.north_shore_emphasis.lower() in ["auto", "true"]:
            north_shore_analysis = await self._analyze_north_shore(metadata, bundle_dir)
        
        # Analyze South Shore conditions if enabled
        south_shore_analysis = None
        if self.south_shore_emphasis.lower() in ["auto", "true"]:
            south_shore_analysis = await self._analyze_south_shore(metadata, bundle_dir)
        
        # Combine data into a single structure
        forecast_data = {
            "run_id": run_id,
            "timestamp": timestamp,
            "buoy_data": buoy_data,
            "chart_data": chart_data,
            "model_data": model_data,
            "north_shore": north_shore_analysis,
            "south_shore": south_shore_analysis,
            "emphasis": {
                "north": self.north_shore_emphasis.lower() in ["auto", "true"],
                "south": self.south_shore_emphasis.lower() in ["auto", "true"]
            }
        }
        
        return forecast_data
    
    async def _prepare_buoy_data(self, metadata: Dict[str, Any], bundle_dir: Path) -> Dict[str, Any]:
        """
        Prepare buoy data for forecast generation.
        
        Args:
            metadata: Bundle metadata
            bundle_dir: Path to bundle directory
            
        Returns:
            Dictionary with prepared buoy data
        """
        logger.info("Preparing buoy data")
        
        buoy_data = {}
        
        # Collect all buoy data from the bundle
        buoy_results = [
            result for result in metadata.get("results", [])
            if result.get("type") == "buoy"
        ]
        
        # Process by source and buoy ID
        for result in buoy_results:
            source = result.get("source", "unknown")
            file_path = result.get("path")
            subtype = result.get("subtype", "standard")
            
            if not file_path or not await file_exists(file_path):
                logger.warning(f"Buoy file not found: {file_path}")
                continue
            
            # Extract buoy ID from filename pattern
            # Examples: ndbc_51001.txt, cdip_098.txt, ndbc_51001_spec.txt
            filename = Path(file_path).name
            buoy_id = None
            
            if "ndbc_" in filename:
                # Extract ID between ndbc_ and .txt or _spec.txt
                parts = filename.replace("ndbc_", "").split("_")
                buoy_id = parts[0]
                source = "NDBC"
            elif "cdip_" in filename:
                # Extract ID between cdip_ and .txt or _spec.txt
                parts = filename.replace("cdip_", "").split("_")
                buoy_id = parts[0]
                source = "CDIP"
            
            if not buoy_id:
                logger.warning(f"Could not extract buoy ID from filename: {filename}")
                continue
            
            # Initialize buoy entry if needed
            if buoy_id not in buoy_data:
                buoy_data[buoy_id] = {
                    "source": source,
                    "data": {},
                    "north_facing": result.get("north_facing", True),
                    "south_facing": result.get("south_facing", False)
                }
            
            # Read and store the data
            try:
                content = await read_file(file_path)
                buoy_data[buoy_id]["data"][subtype] = content
                logger.info(f"Added {source} buoy {buoy_id} {subtype} data")
            except Exception as e:
                logger.error(f"Failed to read buoy data {file_path}: {e}")
        
        return buoy_data
    
    async def _prepare_chart_data(self, metadata: Dict[str, Any], bundle_dir: Path) -> Dict[str, Any]:
        """
        Prepare chart data for forecast generation.
        
        Args:
            metadata: Bundle metadata
            bundle_dir: Path to bundle directory
            
        Returns:
            Dictionary with prepared chart data
        """
        logger.info("Preparing chart data")
        
        chart_data = {
            "surface": [],
            "wave": [],
            "forecast": [],
            "other": []
        }
        
        # Collect all chart data from the bundle
        chart_results = [
            result for result in metadata.get("results", [])
            if result.get("type") in ["surface_analysis", "wave_height", "surface_forecast", 
                                      "wave_forecast", "chart"]
        ]
        
        # Sort by priority
        chart_results.sort(key=lambda x: x.get("priority", 10))
        
        # Process charts and categorize them
        for result in chart_results:
            chart_type = result.get("type", "other")
            file_path = result.get("path")
            
            if not file_path or not await file_exists(file_path):
                logger.warning(f"Chart file not found: {file_path}")
                continue
            
            # Categorize charts
            category = "other"
            if chart_type in ["surface_analysis"]:
                category = "surface"
            elif chart_type in ["wave_height", "wave_model"]:
                category = "wave"
            elif chart_type in ["surface_forecast", "wave_forecast"]:
                category = "forecast"
            
            # Add chart info
            chart_info = {
                "file_path": str(file_path),
                "filename": Path(file_path).name,
                "source": result.get("source", "unknown"),
                "type": chart_type,
                "subtype": result.get("subtype"),
                "url": result.get("url"),
                "north_facing": result.get("north_facing", True),
                "south_facing": result.get("south_facing", False)
            }
            
            chart_data[category].append(chart_info)
            logger.info(f"Added {chart_type} chart: {Path(file_path).name}")
        
        return chart_data
    
    async def _prepare_model_data(self, metadata: Dict[str, Any], bundle_dir: Path) -> Dict[str, Any]:
        """
        Prepare model data for forecast generation.
        
        Args:
            metadata: Bundle metadata
            bundle_dir: Path to bundle directory
            
        Returns:
            Dictionary with prepared model data
        """
        logger.info("Preparing model data")
        
        model_data = {
            "ww3": [],
            "pacioos": [],
            "ecmwf": [],
            "other": []
        }
        
        # Collect all model data from the bundle
        model_results = [
            result for result in metadata.get("results", [])
            if result.get("type") in ["model", "wave_model", "ww3_info", "swan_info"]
        ]
        
        # Sort by priority
        model_results.sort(key=lambda x: x.get("priority", 10))
        
        # Process models and categorize them
        for result in model_results:
            source = result.get("source", "unknown").lower()
            file_path = result.get("path")
            
            if not file_path or not await file_exists(file_path):
                logger.warning(f"Model file not found: {file_path}")
                continue
            
            # Categorize models
            category = "other"
            if "ww3" in source:
                category = "ww3"
            elif "pacioos" in source:
                category = "pacioos"
            elif "ecmwf" in source:
                category = "ecmwf"
            
            # Add model info
            model_info = {
                "file_path": str(file_path),
                "filename": Path(file_path).name,
                "source": result.get("source", "unknown"),
                "type": result.get("type", "model"),
                "subtype": result.get("subtype"),
                "region": result.get("region"),
                "url": result.get("url"),
                "north_facing": result.get("north_facing", True),
                "south_facing": result.get("south_facing", False)
            }
            
            # For JSON models, load the content for processing
            if Path(file_path).suffix == ".json":
                try:
                    content = await read_file(file_path)
                    model_info["data"] = content
                except Exception as e:
                    logger.error(f"Failed to read model data {file_path}: {e}")
            
            model_data[category].append(model_info)
            logger.info(f"Added {category} model: {Path(file_path).name}")
        
        return model_data
    
    async def _analyze_north_shore(self, metadata: Dict[str, Any], bundle_dir: Path) -> Dict[str, Any]:
        """
        Analyze North Shore conditions.
        
        Args:
            metadata: Bundle metadata
            bundle_dir: Path to bundle directory
            
        Returns:
            Dictionary with North Shore analysis
            
        Note:
            In a full implementation, this would use the North Pacific analysis module
        """
        logger.info("Analyzing North Shore conditions")
        
        # This is a placeholder for the actual implementation
        # In a real implementation, this would use the north_pacific_analysis module
        
        # Just return a simple structure for now
        return {
            "analyzed": True,
            "timestamp": utcnow(),
            "summary": "North Shore analysis would be performed here",
            "buoys": {},
            "swells": []
        }
    
    async def _analyze_south_shore(self, metadata: Dict[str, Any], bundle_dir: Path) -> Dict[str, Any]:
        """
        Analyze South Shore conditions.
        
        Args:
            metadata: Bundle metadata
            bundle_dir: Path to bundle directory
            
        Returns:
            Dictionary with South Shore analysis
            
        Note:
            In a full implementation, this would use the south_swell_calibration module
        """
        logger.info("Analyzing South Shore conditions")
        
        # This is a placeholder for the actual implementation
        # In a real implementation, this would use the south_swell_calibration module
        
        # Just return a simple structure for now
        return {
            "analyzed": True,
            "timestamp": utcnow(),
            "summary": "South Shore analysis would be performed here",
            "buoys": {},
            "swells": []
        }
    
    async def _generate_forecast_text(self, forecast_data: Dict[str, Any]) -> str:
        """
        Generate forecast text using OpenAI API.
        
        Args:
            forecast_data: Prepared forecast data
            
        Returns:
            Generated forecast text
        """
        logger.info("Generating forecast text")
        
        # In a full implementation, this would use the OpenAI API
        # For now, just return a placeholder forecast
        
        # Example forecast template
        forecast = f"""# Oʻahu Surf Forecast

## Generated on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC

### Overview

This is a placeholder forecast. In a real implementation, this would be generated using the OpenAI API based on the analyzed data.

### North Shore Forecast

Waves: 3-5 feet
Conditions: Clean in the morning, light onshore winds in the afternoon
Trend: Holding steady

### South Shore Forecast

Waves: 1-2 feet
Conditions: Small background swell
Trend: Slight increase expected in the coming days

### Day-by-Day Forecast

#### Today
North Shore: 3-5 feet, clean conditions
South Shore: 1-2 feet, clean conditions

#### Tomorrow
North Shore: 4-6 feet, clean conditions in the morning
South Shore: 1-3 feet, clean conditions

#### Day After Tomorrow
North Shore: 5-7 feet, potential for larger sets
South Shore: 2-3 feet, clean conditions

### Extended Outlook

A new northwest swell is expected to arrive in 3-4 days, potentially bringing waves in the 6-8 foot range to the North Shore.

### Data Sources

This forecast is based on data from NDBC buoys, PacIOOS models, and NOAA wave models.

### Disclaimer

This is an automated forecast and should be used as guidance only. Always check with local authorities and lifeguards for current conditions.
"""
        
        return forecast
    
    async def _format_forecast(self, forecast_text: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format forecast for output.
        
        Args:
            forecast_text: Generated forecast text
            metadata: Bundle metadata
            
        Returns:
            Dictionary with formatted forecast
        """
        logger.info("Formatting forecast for output")
        
        # Get the bundle ID and timestamp for filenames
        run_id = metadata["run_id"]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        
        # Create formatted versions
        html_content = markdown.markdown(forecast_text)
        
        # Add HTML wrappers
        html_doc = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Oʻahu Surf Forecast</title>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; margin: 40px; }}
                h1, h2, h3, h4 {{ color: #2c3e50; }}
                h1 {{ border-bottom: 2px solid #3498db; padding-bottom: 10px; }}
                h2 {{ border-bottom: 1px solid #bdc3c7; padding-bottom: 5px; margin-top: 30px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                table, th, td {{ border: 1px solid #ddd; }}
                th, td {{ padding: 12px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .disclaimer {{ background-color: #f8f9fa; padding: 10px; border-left: 4px solid #e74c3c; margin-top: 30px; }}
            </style>
        </head>
        <body>
            {html_content}
            <div class="disclaimer">
                <p><strong>Generated by SwellForecaster on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</strong></p>
                <p>This is an automated forecast and should be used as guidance only. Always check with local authorities and lifeguards for current conditions.</p>
            </div>
        </body>
        </html>
        """
        
        # Create PDF content from HTML
        # In a full implementation, this would use weasyprint to convert HTML to PDF
        pdf_content = b"Placeholder PDF content"
        
        # Return formatted forecast
        return {
            "run_id": run_id,
            "timestamp": timestamp,
            "markdown": forecast_text,
            "html": html_doc,
            "pdf": pdf_content
        }
    
    async def _save_forecast(self, formatted_forecast: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """
        Save forecast to files.
        
        Args:
            formatted_forecast: Formatted forecast
            metadata: Bundle metadata
        """
        logger.info("Saving forecast to files")
        
        # Get the forecast directory
        forecast_dir = Path(self.settings.general.data_dir).parent / "forecasts"
        forecast_dir.mkdir(exist_ok=True, parents=True)
        
        # Get the timestamp for filenames
        timestamp = formatted_forecast["timestamp"]
        
        # Save markdown version
        markdown_path = forecast_dir / f"forecast_{timestamp}.md"
        await write_file(markdown_path, formatted_forecast["markdown"])
        logger.info(f"Saved markdown forecast to {markdown_path}")
        
        # Save HTML version
        html_path = forecast_dir / f"forecast_{timestamp}.html"
        await write_file(html_path, formatted_forecast["html"])
        logger.info(f"Saved HTML forecast to {html_path}")
        
        # Save PDF version
        pdf_path = forecast_dir / f"forecast_{timestamp}.pdf"
        await write_file(pdf_path, formatted_forecast["pdf"])
        logger.info(f"Saved PDF forecast to {pdf_path}")
        
        # Create forecast image charts
        # In a full implementation, this would generate charts for the forecast
        # For now, just log a message
        logger.info("Forecast charts would be generated here")


async def analyze_data(
    config_path: str = "config.ini",
    bundle_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Analyze data and generate a forecast.
    
    Args:
        config_path: Path to the configuration file
        bundle_id: Specific bundle ID to analyze (defaults to latest)
        
    Returns:
        Dictionary with forecast results
    """
    # Load settings
    settings = load_settings(config_path)
    
    # Set up logging
    setup_logging(log_dir=Path(settings.general.data_dir).parent / "logs")
    
    # Create analyzer and generate forecast
    analyzer = ForecastAnalyzer(settings)
    return await analyzer.generate_forecast(bundle_id)


async def main_async() -> int:
    """
    Main async function for forecast generation.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="SwellForecaster Analyzer")
    parser.add_argument("--config", default="config.ini", help="Path to configuration file")
    parser.add_argument("--bundle-id", help="Specific bundle ID to analyze (defaults to latest)")
    args = parser.parse_args()
    
    try:
        # Analyze data and generate forecast
        forecast = await analyze_data(args.config, args.bundle_id)
        
        if forecast:
            print(f"Forecast generation complete")
            print(f"Markdown: {forecast.get('run_id')}/{forecast.get('timestamp')}.md")
            print(f"HTML: {forecast.get('run_id')}/{forecast.get('timestamp')}.html")
            print(f"PDF: {forecast.get('run_id')}/{forecast.get('timestamp')}.pdf")
            return 0
        else:
            print("Forecast generation failed")
            return 1
    
    except Exception as e:
        print(f"Error during forecast generation: {e}")
        return 1


def main() -> int:
    """
    Main entry point for forecast generation.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nForecast generation interrupted by user")
        return 130


if __name__ == "__main__":
    sys.exit(main())