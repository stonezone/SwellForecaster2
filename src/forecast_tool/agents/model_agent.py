"""Wave model data collection agent for SwellForecaster."""
import asyncio
import json
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union, Any

from ..data.base_agent import BaseAgent
from ..data.collection_context import CollectionContext, CollectionResult
from ..models.settings import Settings
from ..utils import create_placeholder_image

logger = logging.getLogger("agents.model")


class ModelAgent(BaseAgent):
    """Agent for collecting wave model data from various sources."""
    
    def __init__(self, settings: Settings):
        """
        Initialize the model agent.
        
        Args:
            settings: Application settings
        """
        super().__init__(settings)
        
        # Check for ECMWF OpenData client
        self.has_opendata = False
        self.has_legacy_ecmwf = False
        
        try:
            from ecmwf.opendata import Client as OpenDataClient
            self.has_opendata = True
        except ImportError:
            self.logger.warning("ecmwf-opendata package not installed. Run 'pip install ecmwf-opendata'")
            # Try to import the legacy client as fallback
            try:
                from ecmwfapi import ECMWFDataServer
                self.has_legacy_ecmwf = True
            except ImportError:
                self.logger.warning("ecmwfapi package not installed. Run 'pip install ecmwf-api-client'")
    
    @property
    def name(self) -> str:
        """Agent name."""
        return "ModelAgent"
    
    @property
    def enabled(self) -> bool:
        """Check if model collection is enabled."""
        return (
            self.settings.sources.enable_models or 
            self.settings.sources.enable_pacioos or 
            self.settings.sources.enable_pacioos_swan or
            self.settings.sources.enable_ecmwf
        )
    
    async def collect(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Collect model data.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        if not self.enabled:
            self.logger.info("Model data collection is disabled")
            return []
        
        self.logger.info("Starting model data collection")
        results = []
        tasks = []
        
        # Add collection tasks based on enabled sources
        if self.settings.sources.enable_models:
            tasks.append(self.collect_ww3_model(ctx))
        
        if self.settings.sources.enable_pacioos:
            tasks.append(self.collect_pacioos(ctx))
        
        if self.settings.sources.enable_pacioos_swan:
            tasks.append(self.collect_pacioos_swan(ctx))
        
        if self.settings.sources.enable_ecmwf and self.settings.api.ecmwf_key:
            tasks.append(self.collect_ecmwf(ctx))
        
        # Collect all model data in parallel
        model_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in model_results:
            if isinstance(result, Exception):
                self.logger.error(f"Model collection task failed: {result}")
            elif result:
                if isinstance(result, list):
                    results.extend(result)
                else:
                    results.append(result)
        
        self.logger.info(f"Completed model data collection with {len(results)} results")
        return results
    
    async def collect_ww3_model(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Collect Wave Watch III model data.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        self.logger.info("Collecting WW3 model data")
        results = []
        
        # Format date for NOAA URL pattern
        now = datetime.now(timezone.utc)
        url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/wave/prod/wave.{now:%Y%m%d}/multi_1.glo_30m.t00z.grib2"
        
        try:
            # Download the GRIB file
            self.logger.info(f"Downloading WW3 GRIB from: {url}")
            data = await self.fetch(ctx, url)
            
            if not data:
                self.logger.warning("Failed to download WW3 GRIB data")
                return await self.ww3_model_fallback(ctx)
            
            # Save to temporary file
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(data)
                tmp_path = Path(tmp.name)
            
            try:
                # Define paths for processed files
                slice_path = tmp_path.with_suffix(".slice")
                json_path = slice_path.with_suffix(".json")
                
                # Extract region around Hawaii (longitude 140-160, latitude 10-30)
                self.logger.info("Slicing GRIB to Hawaiian region")
                slice_process = await asyncio.create_subprocess_shell(
                    f"wgrib2 {tmp_path} -small_grib 140:160 10:30 {slice_path}"
                )
                await slice_process.wait()
                
                if not slice_path.exists():
                    self.logger.error("GRIB slicing failed")
                    return await self.ww3_model_fallback(ctx)
                
                # Convert GRIB to JSON
                self.logger.info("Converting GRIB to JSON")
                json_process = await asyncio.create_subprocess_shell(
                    f"grib2json -d 2 -n 3 -o {json_path} {slice_path}"
                )
                await json_process.wait()
                
                if not json_path.exists():
                    self.logger.error("GRIB to JSON conversion failed")
                    return await self.ww3_model_fallback(ctx)
                
                # Read and save the JSON data
                with open(json_path, "rb") as f:
                    json_data = f.read()
                
                result = await self.create_result(
                    ctx=ctx,
                    source="WW3",
                    filename="ww3_hawaii.json",
                    data=json_data,
                    type_name="model",
                    region="hawaii",
                    priority=0,
                    url=url,
                    north_facing=True,
                    south_facing=False
                )
                
                results.append(result)
                
                # Also get Southern Hemisphere region (for South Shore forecasting)
                south_slice_path = tmp_path.with_suffix(".south.slice")
                south_json_path = south_slice_path.with_suffix(".json")
                
                # Extract Southern Hemisphere region (longitude 180-220, latitude -40-0)
                self.logger.info("Slicing GRIB to Southern Hemisphere region")
                south_slice_process = await asyncio.create_subprocess_shell(
                    f"wgrib2 {tmp_path} -small_grib 180:220 -40:0 {south_slice_path}"
                )
                await south_slice_process.wait()
                
                if south_slice_path.exists():
                    # Convert Southern Hemisphere GRIB to JSON
                    self.logger.info("Converting Southern Hemisphere GRIB to JSON")
                    south_json_process = await asyncio.create_subprocess_shell(
                        f"grib2json -d 2 -n 3 -o {south_json_path} {south_slice_path}"
                    )
                    await south_json_process.wait()
                    
                    if south_json_path.exists():
                        # Read and save the Southern Hemisphere JSON data
                        with open(south_json_path, "rb") as f:
                            south_json_data = f.read()
                        
                        south_result = await self.create_result(
                            ctx=ctx,
                            source="WW3-South",
                            filename="ww3_south_pacific.json",
                            data=south_json_data,
                            type_name="model",
                            region="south_pacific",
                            priority=1,
                            url=url,
                            north_facing=False,
                            south_facing=True
                        )
                        
                        results.append(south_result)
            
            finally:
                # Clean up temporary files
                for path in [
                    tmp_path,
                    slice_path,
                    json_path,
                    south_slice_path if 'south_slice_path' in locals() else None,
                    south_json_path if 'south_json_path' in locals() else None
                ]:
                    if path and path.exists():
                        try:
                            path.unlink()
                        except Exception as e:
                            self.logger.warning(f"Failed to delete temporary file {path}: {e}")
        
        except Exception as e:
            self.logger.error(f"Error processing WW3 data: {e}")
            return await self.ww3_model_fallback(ctx)
        
        return results
    
    async def ww3_model_fallback(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Fallback for WW3 data when GRIB processing fails.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        self.logger.info("Using WW3 model fallback sources")
        results = []
        
        # Multiple global overview images (with different styles/sources for redundancy)
        fallback_urls = [
            # Primary NCEP WW3 global overview image
            ("https://polar.ncep.noaa.gov/waves/latest_run/pac-hs.latest_run.gif", 
             "ww3_pacific_overview.gif", "overview", 1),
            
            # Higher resolution North Pacific data
            ("https://polar.ncep.noaa.gov/waves/latest_run/multi_1.nww3.hs.north_pacific.latest.gif", 
             "ww3_north_pacific_detail.gif", "npac_detail", 1),
            
            # Higher resolution Hawaii region
            ("https://polar.ncep.noaa.gov/waves/latest_run/multi_1.nww3.hs.hawaii.latest.gif", 
             "ww3_hawaii_detail.gif", "hawaii_detail", 1),
            
            # Wave period data - crucial for surf quality assessment
            ("https://polar.ncep.noaa.gov/waves/latest_run/multi_1.nww3.tp.north_pacific.latest.gif", 
             "ww3_north_pacific_period.gif", "npac_period", 1),
            
            # Wave direction data - important for swell direction
            ("https://polar.ncep.noaa.gov/waves/latest_run/multi_1.nww3.dp.north_pacific.latest.gif", 
             "ww3_north_pacific_direction.gif", "npac_direction", 1),
            
            # Southern hemisphere charts for South swells
            ("https://polar.ncep.noaa.gov/waves/latest_run/multi_1.nww3.hs.south_pacific.latest.gif", 
             "ww3_south_pacific_detail.gif", "spac_detail", 1)
        ]
        
        # Try each source with proper error handling
        tasks = []
        for url, filename, subtype, priority in fallback_urls:
            tasks.append(self.fetch_model_chart(ctx, url, filename, subtype, priority))
        
        # Wait for all tasks to complete
        chart_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in chart_results:
            if isinstance(result, Exception):
                self.logger.error(f"WW3 fallback task failed: {result}")
            elif result:
                results.append(result)
        
        self.logger.info(f"Completed WW3 fallback collection with {len(results)} results")
        return results
    
    async def fetch_model_chart(
        self,
        ctx: CollectionContext,
        url: str,
        filename: str,
        subtype: str,
        priority: int
    ) -> Optional[CollectionResult]:
        """
        Fetch a model chart.
        
        Args:
            ctx: Collection context
            url: URL to fetch
            filename: Filename to save as
            subtype: Chart subtype
            priority: Priority (lower is higher)
            
        Returns:
            Collection result or None if fetch failed
        """
        try:
            # Determine if this is for north or south shores
            north_facing = "north" in filename.lower() or "npac" in filename.lower() or "pacific" in filename.lower()
            south_facing = "south" in filename.lower() or "spac" in filename.lower()
            
            # Fetch the chart
            data = await self.fetch(ctx, url)
            
            # Create result
            result = await self.create_result(
                ctx=ctx,
                source="WW3-Fallback",
                filename=filename,
                data=data,
                type_name="wave_model",
                subtype=subtype,
                priority=priority,
                url=url,
                north_facing=north_facing,
                south_facing=south_facing
            )
            
            self.logger.info(f"Successfully fetched WW3 fallback: {filename}")
            return result
        
        except Exception as e:
            self.logger.warning(f"Failed to fetch WW3 fallback: {url} - {e}")
            return None
    
    async def collect_pacioos(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Collect PacIOOS wave model data.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        self.logger.info("Collecting PacIOOS wave model data")
        results = []
        
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
        
        self.logger.info(f"PacIOOS WW3 URL: {urls['now']}")
        
        # Fetch each URL in parallel
        tasks = []
        for tag, url in urls.items():
            tasks.append(self.fetch_pacioos_chart(ctx, url, f"pacioos_{tag}.png", tag))
        
        # Wait for all tasks to complete
        chart_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in chart_results:
            if isinstance(result, Exception):
                self.logger.error(f"PacIOOS collection task failed: {result}")
            elif result:
                results.append(result)
        
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
        
        # Fetch North Shore data in parallel
        ns_tasks = []
        for tag, url in ns_urls.items():
            ns_tasks.append(self.fetch_pacioos_chart(ctx, url, f"pacioos_{tag}.png", tag, north_facing=True, south_facing=False))
        
        # Wait for all tasks to complete
        ns_results = await asyncio.gather(*ns_tasks, return_exceptions=True)
        
        # Process results
        for result in ns_results:
            if isinstance(result, Exception):
                self.logger.error(f"PacIOOS North Shore collection task failed: {result}")
            elif result:
                results.append(result)
        
        self.logger.info(f"Completed PacIOOS collection with {len(results)} results")
        return results
    
    async def fetch_pacioos_chart(
        self,
        ctx: CollectionContext,
        url: str,
        filename: str,
        tag: str,
        north_facing: bool = True,
        south_facing: bool = True
    ) -> Optional[CollectionResult]:
        """
        Fetch a PacIOOS chart.
        
        Args:
            ctx: Collection context
            url: URL to fetch
            filename: Filename to save as
            tag: Chart tag
            north_facing: Whether relevant for north-facing shores
            south_facing: Whether relevant for south-facing shores
            
        Returns:
            Collection result or None if fetch failed
        """
        try:
            # Fetch the chart
            data = await self.fetch(ctx, url)
            
            # Check if the image contains an error message
            if isinstance(data, bytes) and b"Error:" in data and b"wasn't found in datasetID" in data:
                self.logger.error(f"PacIOOS {tag} returned error: {data[:200]}")
                
                # Create placeholder with error message
                error_msg = data.decode("utf-8", errors="replace")[:200] if isinstance(data, bytes) else "Unknown error"
                placeholder_data = create_placeholder_image(
                    f"Unable to retrieve PacIOOS {tag}. Error: {error_msg}",
                    filename=filename
                )
                
                # Create result with error flag
                result = await self.create_result(
                    ctx=ctx,
                    source="PacIOOS",
                    filename=filename,
                    data=placeholder_data,
                    type_name=f"wave_{tag}",
                    priority=10,  # Lower priority for error
                    url=url,
                    error=True,
                    north_facing=north_facing,
                    south_facing=south_facing
                )
                
                return result
            
            # Create result
            result = await self.create_result(
                ctx=ctx,
                source="PacIOOS",
                filename=filename,
                data=data,
                type_name=f"wave_{tag}",
                priority=1,
                url=url,
                north_facing=north_facing,
                south_facing=south_facing
            )
            
            self.logger.info(f"Successfully fetched PacIOOS {tag}")
            return result
        
        except Exception as e:
            self.logger.error(f"Failed to fetch PacIOOS {tag}: {e}")
            
            # Create placeholder image
            placeholder_data = create_placeholder_image(
                f"Unable to retrieve PacIOOS {tag}. Error: {str(e)}",
                filename=filename
            )
            
            # Create result with error flag
            result = await self.create_result(
                ctx=ctx,
                source="PacIOOS",
                filename=filename,
                data=placeholder_data,
                type_name=f"wave_{tag}",
                priority=10,  # Lower priority for error
                url=url,
                error=True,
                north_facing=north_facing,
                south_facing=south_facing
            )
            
            return result
    
    async def collect_pacioos_swan(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Collect PacIOOS SWAN model data.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        self.logger.info("Collecting PacIOOS SWAN model data")
        results = []
        
        # Get metadata for ww3_hawaii to confirm available variables
        ww3_info_url = "https://pae-paha.pacioos.hawaii.edu/erddap/info/ww3_hawaii/index.json"
        
        try:
            # Fetch metadata
            info_data = await self.fetch(ctx, ww3_info_url)
            
            if info_data:
                # Save metadata
                info_result = await self.create_result(
                    ctx=ctx,
                    source="PacIOOS",
                    filename="ww3_hawaii_info.json",
                    data=info_data,
                    type_name="ww3_info",
                    priority=0,
                    url=ww3_info_url
                )
                
                results.append(info_result)
                self.logger.info("Successfully fetched WW3 Hawaii dataset info")
        except Exception as e:
            self.logger.error(f"Failed to fetch WW3 Hawaii info: {e}")
        
        # For swan_oahu dataset, the correct variable name is 'shgt' (not significant_wave_height)
        swan_var = "shgt"
        
        # Collection tasks for SWAN charts
        tasks = []
        
        # Visualization image
        viz_url = f"https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png?{swan_var}%5B(last)%5D&.draw=surface&.vars=longitude|latitude|{swan_var}&.colorBar=Rainbow|||||&.bgColor=0xffccccff"
        tasks.append(self.fetch_pacioos_swan_chart(ctx, viz_url, "pacioos_swan_viz.png", "swan_viz", True, True))
        
        # Metadata JSON
        info_url = "https://pae-paha.pacioos.hawaii.edu/erddap/info/swan_oahu/index.json"
        tasks.append(self.fetch_pacioos_json(ctx, info_url, "pacioos_swan_info.json", "swan_info"))
        
        # South Shore specific wave models
        south_shore_url = f"https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png?{swan_var}%5B(last)%5D&.draw=surface&.vars=longitude|latitude|{swan_var}&.colorBar=Rainbow|||||&.bgColor=0xffccccff&.land=under&.lat=21.25:21.30&.lon=-157.85:-157.80"
        tasks.append(self.fetch_pacioos_swan_chart(ctx, south_shore_url, "pacioos_swan_south_shore.png", "swan_south_shore", False, True))
        
        # North Shore specific high-resolution SWAN model
        ns_url = f"https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png?{swan_var}%5B(last)%5D&.draw=surface&.vars=longitude|latitude|{swan_var}&.colorBar=Rainbow|||||&.bgColor=0xffccccff&.land=under&.lat=21.55:21.75&.lon=-158.20:-158.00"
        tasks.append(self.fetch_pacioos_swan_chart(ctx, ns_url, "pacioos_swan_north_shore.png", "swan_north_shore", True, False))
        
        # Wait for all tasks to complete
        swan_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in swan_results:
            if isinstance(result, Exception):
                self.logger.error(f"PacIOOS SWAN collection task failed: {result}")
            elif result:
                results.append(result)
        
        # Try to add additional data types if available (direction, period)
        for data_type, var_name in {'direction': 'mpd', 'period': 'tps'}.items():
            url = f"https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu.png?{var_name}%5B(last)%5D&.draw=surface&.vars=longitude|latitude|{var_name}&.colorBar=Rainbow|||||&.bgColor=0xffccccff&.land=under"
            
            try:
                # Fetch the chart
                result = await self.fetch_pacioos_swan_chart(ctx, url, f"pacioos_swan_{data_type}.png", f"swan_{data_type}", True, True)
                
                if result:
                    results.append(result)
                    self.logger.info(f"Successfully fetched PacIOOS SWAN wave {data_type}")
            
            except Exception as e:
                self.logger.error(f"Failed to fetch PacIOOS {data_type} data: {e}")
        
        self.logger.info(f"Completed PacIOOS SWAN collection with {len(results)} results")
        return results
    
    async def fetch_pacioos_swan_chart(
        self,
        ctx: CollectionContext,
        url: str,
        filename: str,
        subtype: str,
        north_facing: bool,
        south_facing: bool
    ) -> Optional[CollectionResult]:
        """
        Fetch a PacIOOS SWAN chart.
        
        Args:
            ctx: Collection context
            url: URL to fetch
            filename: Filename to save as
            subtype: Chart subtype
            north_facing: Whether relevant for north-facing shores
            south_facing: Whether relevant for south-facing shores
            
        Returns:
            Collection result or None if fetch failed
        """
        try:
            # Fetch the chart
            data = await self.fetch(ctx, url)
            
            # Check if the image contains an error message
            if isinstance(data, bytes) and b"Error:" in data:
                self.logger.error(f"PacIOOS {subtype} returned error: {data[:200]}")
                
                # Create placeholder with error message
                error_msg = data.decode("utf-8", errors="replace")[:200] if isinstance(data, bytes) else "Unknown error"
                placeholder_data = create_placeholder_image(
                    f"Unable to retrieve PacIOOS {subtype}. Error: {error_msg}",
                    filename=filename
                )
                
                # Create result with error flag
                result = await self.create_result(
                    ctx=ctx,
                    source="PacIOOS",
                    filename=filename,
                    data=placeholder_data,
                    type_name=subtype,
                    priority=10,  # Lower priority for error
                    url=url,
                    error=True,
                    north_facing=north_facing,
                    south_facing=south_facing
                )
                
                return result
            
            # Create result
            result = await self.create_result(
                ctx=ctx,
                source="PacIOOS",
                filename=filename,
                data=data,
                type_name=subtype,
                priority=1,
                url=url,
                north_facing=north_facing,
                south_facing=south_facing
            )
            
            self.logger.info(f"Successfully fetched PacIOOS {subtype}")
            return result
        
        except Exception as e:
            self.logger.error(f"Failed to fetch PacIOOS {subtype}: {e}")
            
            # Create placeholder image
            placeholder_data = create_placeholder_image(
                f"Unable to retrieve PacIOOS {subtype}. Error: {str(e)}",
                filename=filename
            )
            
            # Create result with error flag
            result = await self.create_result(
                ctx=ctx,
                source="PacIOOS",
                filename=filename,
                data=placeholder_data,
                type_name=subtype,
                priority=10,  # Lower priority for error
                url=url,
                error=True,
                north_facing=north_facing,
                south_facing=south_facing
            )
            
            return result
    
    async def fetch_pacioos_json(
        self,
        ctx: CollectionContext,
        url: str,
        filename: str,
        subtype: str
    ) -> Optional[CollectionResult]:
        """
        Fetch PacIOOS JSON metadata.
        
        Args:
            ctx: Collection context
            url: URL to fetch
            filename: Filename to save as
            subtype: JSON subtype
            
        Returns:
            Collection result or None if fetch failed
        """
        try:
            # Fetch the data
            data = await self.fetch(ctx, url)
            
            # Create result
            result = await self.create_result(
                ctx=ctx,
                source="PacIOOS",
                filename=filename,
                data=data,
                type_name=subtype,
                priority=0,
                url=url
            )
            
            self.logger.info(f"Successfully fetched PacIOOS {subtype}")
            return result
        
        except Exception as e:
            self.logger.error(f"Failed to fetch PacIOOS {subtype}: {e}")
            
            # Create placeholder JSON with minimal info
            placeholder_data = json.dumps({
                "status": "error",
                "message": f"Failed to fetch {subtype} information",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data_source": f"PacIOOS {subtype}",
                "error_type": "fetch_failure",
                "error": str(e)
            })
            
            # Create result with error flag
            result = await self.create_result(
                ctx=ctx,
                source="PacIOOS",
                filename=filename,
                data=placeholder_data,
                type_name=subtype,
                priority=10,  # Lower priority for error
                url=url,
                error=True
            )
            
            return result
    
    async def collect_ecmwf(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Collect ECMWF model data.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        self.logger.info("Collecting ECMWF model data")
        results = []
        
        # Check for API key
        api_key = self.settings.api.ecmwf_key
        if not api_key:
            self.logger.info("No ECMWF API key found in config, skipping")
            return []
        
        # Define regions to fetch
        regions = [
            {"name": "hawaii", "area": [30, -170, 15, -150], "north_facing": True, "south_facing": True},
            {"name": "north_pacific", "area": [60, -180, 30, -120], "north_facing": True, "south_facing": False},
            {"name": "south_pacific", "area": [0, -180, -40, -120], "north_facing": False, "south_facing": True}
        ]
        
        # Try multiple endpoints - different ECMWF APIs may work
        endpoints = [
            "https://api.ecmwf.int/v1/services/opendata/wave",
            "https://api.ecmwf.int/v1/wave",
            "https://data.ecmwf.int/forecasts/wave",
            "https://api.ecmwf.int/v1/datasets/wave/forecasts"
        ]
        
        # Fetch data for each region
        for region in regions:
            region_name = region["name"]
            area = region["area"]
            north_facing = region.get("north_facing", False)
            south_facing = region.get("south_facing", False)
            
            self.logger.info(f"Requesting ECMWF wave forecast data for {region_name}")
            
            success = False
            for endpoint in endpoints:
                try:
                    # Prepare request body
                    request_data = {
                        "area": area,  # [north, west, south, east]
                        "params": ["swh", "mwd", "pp1d"],  # Significant height, direction, period
                        "apikey": api_key
                    }
                    
                    # Attempt fetch
                    data = await self.fetch(
                        ctx,
                        endpoint,
                        method="POST",
                        json_body=request_data,
                        headers={"Content-Type": "application/json"}
                    )
                    
                    if data:
                        # Success! Save data
                        result = await self.create_result(
                            ctx=ctx,
                            source="ECMWF",
                            filename=f"ecmwf_{region_name}_wave.json",
                            data=data,
                            type_name="wave_model",
                            region=region_name,
                            priority=1,
                            url=endpoint,
                            north_facing=north_facing,
                            south_facing=south_facing
                        )
                        
                        results.append(result)
                        self.logger.info(f"Successfully collected ECMWF data for {region_name}")
                        success = True
                        break  # No need to try other endpoints
                
                except Exception as e:
                    self.logger.warning(f"Failed to fetch ECMWF wave data for {region_name} from {endpoint}: {e}")
                    # Continue to next endpoint
            
            if not success:
                self.logger.error(f"All ECMWF endpoints failed for {region_name}")
                
                # Try alternative sources (charts)
                try:
                    # Try a basic chart URL as fallback
                    chart_url = f"https://charts.ecmwf.int/opencharts-api/v1/products/medium-significant-wave-height?area={region_name}&format=jpeg"
                    chart_data = await self.fetch(ctx, chart_url)
                    
                    if chart_data:
                        # Save chart
                        chart_result = await self.create_result(
                            ctx=ctx,
                            source="ECMWF-Chart",
                            filename=f"ecmwf_{region_name}_wave.jpg",
                            data=chart_data,
                            type_name="wave_chart",
                            region=region_name,
                            priority=2,
                            url=chart_url,
                            north_facing=north_facing,
                            south_facing=south_facing
                        )
                        
                        results.append(chart_result)
                        self.logger.info(f"Successfully collected ECMWF chart for {region_name}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to fetch ECMWF chart for {region_name}: {e}")
        
        self.logger.info(f"Completed ECMWF collection with {len(results)} results")
        return results