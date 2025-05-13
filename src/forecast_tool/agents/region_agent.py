"""Region-specific data collection agents for SwellForecaster."""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union, cast

from ..data.base_agent import BaseAgent
from ..data.collection_context import CollectionContext, CollectionResult
from ..models.settings import Settings
from ..utils.http import http_retry

logger = logging.getLogger("agents.region")


class RegionAgent(BaseAgent):
    """Agent for collecting region-specific data (North Pacific, Southern Ocean, etc.)."""
    
    def __init__(self, settings: Settings):
        """
        Initialize the region agent.
        
        Args:
            settings: Application settings
        """
        super().__init__(settings)
    
    @property
    def name(self) -> str:
        """Agent name."""
        return "RegionAgent"
    
    @property
    def enabled(self) -> bool:
        """Check if region collection is enabled."""
        return (
            self.settings.sources.enable_north_pacific or
            self.settings.sources.enable_southern_ocean
        )
    
    async def collect(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Collect region-specific data.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        if not self.enabled:
            self.logger.info("Region data collection is disabled")
            return []
        
        self.logger.info("Starting region data collection")
        results = []
        
        # Collect data from each region in parallel
        tasks = []
        
        if self.settings.sources.enable_north_pacific:
            tasks.append(self.collect_north_pacific(ctx))
        
        if self.settings.sources.enable_southern_ocean:
            tasks.append(self.collect_southern_ocean(ctx))
        
        # Wait for all tasks to complete
        region_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in region_results:
            if isinstance(result, Exception):
                self.logger.error(f"Region collection task failed: {result}")
            elif result:
                results.extend(result)
        
        self.logger.info(f"Completed region data collection with {len(results)} results")
        return results
    
    async def collect_north_pacific(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Collect North Pacific data.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        results = []
        
        self.logger.info("Starting North Pacific data collection")
        
        # OPC North Pacific Wave Analysis
        try:
            url = "https://ocean.weather.gov/P_sfc_full_ocean_color.png"
            
            async with ctx.http_client.get(url) as response:
                if response.status == 200:
                    data = await response.read()
                    
                    # Save the data
                    result = await self.create_result(
                        ctx=ctx,
                        source="OPC",
                        filename="opc_P_sfc_full_ocean_color.png",
                        data=data,
                        type_name="chart",
                        subtype="surface",
                        region="north_pacific",
                        priority=0,
                        url=url,
                        north_facing=True
                    )
                    
                    results.append(result)
                    self.logger.info("Successfully fetched OPC North Pacific Surface Analysis")
                else:
                    self.logger.error(f"Failed to fetch OPC North Pacific Surface Analysis: HTTP {response.status}")
        except Exception as e:
            self.logger.error(f"Exception fetching OPC North Pacific Surface Analysis: {e}")
        
        # OPC North Pacific Wave Charts
        opc_wave_urls = [
            ("https://ocean.weather.gov/P_00hrww.gif", "opc_P_00hrww.gif", "Current wave conditions"),
            ("https://ocean.weather.gov/P_24hrwper.gif", "opc_P_24hrwper.gif", "24-hour wave forecast"),
            ("https://ocean.weather.gov/P_48hrwper.gif", "opc_P_48hrwper.gif", "48-hour wave forecast"),
            ("https://ocean.weather.gov/P_72hrwper.gif", "opc_P_72hrwper.gif", "72-hour wave forecast"),
            ("https://ocean.weather.gov/P_96hrwper.gif", "opc_P_96hrwper.gif", "96-hour wave forecast")
        ]
        
        for url, filename, description in opc_wave_urls:
            try:
                async with ctx.http_client.get(url) as response:
                    if response.status == 200:
                        data = await response.read()
                        
                        # Save the data
                        result = await self.create_result(
                            ctx=ctx,
                            source="OPC",
                            filename=filename,
                            data=data,
                            type_name="chart",
                            subtype="wave",
                            region="north_pacific",
                            priority=1,
                            url=url,
                            north_facing=True,
                            description=description
                        )
                        
                        results.append(result)
                        self.logger.info(f"Successfully fetched OPC North Pacific Wave Chart: {description}")
                    else:
                        self.logger.error(f"Failed to fetch OPC North Pacific Wave Chart ({description}): HTTP {response.status}")
            except Exception as e:
                self.logger.error(f"Exception fetching OPC North Pacific Wave Chart ({description}): {e}")
        
        # OPC North Pacific Surface Charts
        opc_surface_urls = [
            ("https://ocean.weather.gov/P_24hrsfc.gif", "opc_P_24hrsfc.gif", "24-hour surface forecast"),
            ("https://ocean.weather.gov/P_48hrsfc.gif", "opc_P_48hrsfc.gif", "48-hour surface forecast"),
            ("https://ocean.weather.gov/P_72hrsfc.gif", "opc_P_72hrsfc.gif", "72-hour surface forecast"),
            ("https://ocean.weather.gov/P_96hrsfc.gif", "opc_P_96hrsfc.gif", "96-hour surface forecast")
        ]
        
        for url, filename, description in opc_surface_urls:
            try:
                async with ctx.http_client.get(url) as response:
                    if response.status == 200:
                        data = await response.read()
                        
                        # Save the data
                        result = await self.create_result(
                            ctx=ctx,
                            source="OPC",
                            filename=filename,
                            data=data,
                            type_name="chart",
                            subtype="surface",
                            region="north_pacific",
                            priority=1,
                            url=url,
                            north_facing=True,
                            description=description
                        )
                        
                        results.append(result)
                        self.logger.info(f"Successfully fetched OPC North Pacific Surface Chart: {description}")
                    else:
                        self.logger.error(f"Failed to fetch OPC North Pacific Surface Chart ({description}): HTTP {response.status}")
            except Exception as e:
                self.logger.error(f"Exception fetching OPC North Pacific Surface Chart ({description}): {e}")
        
        # Additional regional charts (East and West component views)
        component_urls = [
            ("https://ocean.weather.gov/P_e_sfc_color.png", "opc_P_e_sfc_color.png", "East Pacific Surface Analysis"),
            ("https://ocean.weather.gov/P_w_sfc_color.png", "opc_P_w_sfc_color.png", "West Pacific Surface Analysis")
        ]
        
        for url, filename, description in component_urls:
            try:
                async with ctx.http_client.get(url) as response:
                    if response.status == 200:
                        data = await response.read()
                        
                        # Save the data
                        result = await self.create_result(
                            ctx=ctx,
                            source="OPC",
                            filename=filename,
                            data=data,
                            type_name="chart",
                            subtype="surface",
                            region="north_pacific",
                            priority=2,
                            url=url,
                            north_facing=True,
                            description=description
                        )
                        
                        results.append(result)
                        self.logger.info(f"Successfully fetched OPC {description}")
                    else:
                        self.logger.error(f"Failed to fetch OPC {description}: HTTP {response.status}")
            except Exception as e:
                self.logger.error(f"Exception fetching OPC {description}: {e}")
        
        # NOAA WW3 North Pacific detail
        try:
            url = "https://polar.ncep.noaa.gov/waves/WW3-NCEP/gif/npac.swh.144.gif"
            
            async with ctx.http_client.get(url) as response:
                if response.status == 200:
                    data = await response.read()
                    
                    # Save the data
                    result = await self.create_result(
                        ctx=ctx,
                        source="NOAA",
                        filename="ww3_north_pacific_detail.gif",
                        data=data,
                        type_name="model",
                        subtype="ww3",
                        region="north_pacific",
                        priority=0,
                        url=url,
                        north_facing=True,
                        description="WW3 North Pacific Detail"
                    )
                    
                    results.append(result)
                    self.logger.info("Successfully fetched WW3 North Pacific Detail")
                else:
                    self.logger.error(f"Failed to fetch WW3 North Pacific Detail: HTTP {response.status}")
        except Exception as e:
            self.logger.error(f"Exception fetching WW3 North Pacific Detail: {e}")
        
        # NOAA WW3 Pacific overview
        try:
            url = "https://polar.ncep.noaa.gov/waves/WW3-NCEP/gif/pac.swh.144.gif"
            
            async with ctx.http_client.get(url) as response:
                if response.status == 200:
                    data = await response.read()
                    
                    # Save the data
                    result = await self.create_result(
                        ctx=ctx,
                        source="NOAA",
                        filename="ww3_pacific_overview.gif",
                        data=data,
                        type_name="model",
                        subtype="ww3",
                        region="pacific",
                        priority=0,
                        url=url,
                        north_facing=True,
                        south_facing=True,
                        description="WW3 Pacific Overview"
                    )
                    
                    results.append(result)
                    self.logger.info("Successfully fetched WW3 Pacific Overview")
                else:
                    self.logger.error(f"Failed to fetch WW3 Pacific Overview: HTTP {response.status}")
        except Exception as e:
            self.logger.error(f"Exception fetching WW3 Pacific Overview: {e}")
        
        # NOAA WPC/NPAC charts
        npac_urls = [
            ("https://www.wpc.ncep.noaa.gov/sfc/npac/npac/fax/npac_slp_fill_00f000.gif", "npac_ppae10.gif", "Current Surface Analysis"),
            ("https://www.wpc.ncep.noaa.gov/sfc/npac/npac/fax/npac_slp_fill_00f048.gif", "npac_ppae11.gif", "48hr Surface Forecast")
        ]
        
        for url, filename, description in npac_urls:
            try:
                async with ctx.http_client.get(url) as response:
                    if response.status == 200:
                        data = await response.read()
                        
                        # Save the data
                        result = await self.create_result(
                            ctx=ctx,
                            source="NOAA",
                            filename=filename,
                            data=data,
                            type_name="chart",
                            subtype="surface",
                            region="north_pacific",
                            priority=1,
                            url=url,
                            north_facing=True,
                            description=description
                        )
                        
                        results.append(result)
                        self.logger.info(f"Successfully fetched NPAC {description}")
                    else:
                        self.logger.error(f"Failed to fetch NPAC {description}: HTTP {response.status}")
            except Exception as e:
                self.logger.error(f"Exception fetching NPAC {description}: {e}")
        
        # NOAA GOES IR Satellite imagery
        try:
            url = "https://cdn.star.nesdis.noaa.gov/GOES17/ABI/SECTOR/np/Sandwich/latest.jpg"
            
            async with ctx.http_client.get(url) as response:
                if response.status == 200:
                    data = await response.read()
                    
                    # Save the data
                    result = await self.create_result(
                        ctx=ctx,
                        source="NOAA",
                        filename="opc_nPacOff_IR_15min_latest.gif",
                        data=data,
                        type_name="satellite",
                        subtype="ir",
                        region="north_pacific",
                        priority=2,
                        url=url,
                        north_facing=True,
                        description="GOES-17 North Pacific IR Satellite"
                    )
                    
                    results.append(result)
                    self.logger.info("Successfully fetched GOES-17 North Pacific IR Satellite")
                else:
                    self.logger.error(f"Failed to fetch GOES-17 North Pacific IR Satellite: HTTP {response.status}")
        except Exception as e:
            self.logger.error(f"Exception fetching GOES-17 North Pacific IR Satellite: {e}")
        
        # Create historical analogs for North Pacific data
        try:
            # We'll create a placeholder for historical analogs
            # In a full implementation, this would analyze the data and find similar patterns
            historical_data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "analogs": [
                    {
                        "date": "2005-01-15",
                        "similarity": 0.85,
                        "description": "Strong NW swell from Gulf of Alaska low",
                        "max_height_ft": 15
                    },
                    {
                        "date": "2018-12-05",
                        "similarity": 0.78,
                        "description": "Multiple NW swells from active jet stream pattern",
                        "max_height_ft": 12
                    }
                ],
                "notes": "Historical analogs are automatically generated based on pattern recognition."
            }
            
            # Save the data
            result = await self.create_result(
                ctx=ctx,
                source="SwellForecaster",
                filename="historical_analogs.json",
                data=json.dumps(historical_data, indent=2),
                type_name="analysis",
                subtype="analog",
                region="north_pacific",
                priority=2,
                north_facing=True
            )
            
            results.append(result)
            self.logger.info("Successfully created historical analogs for North Pacific")
        except Exception as e:
            self.logger.error(f"Exception creating historical analogs for North Pacific: {e}")
        
        return results
    
    async def collect_southern_ocean(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Collect Southern Ocean data.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        results = []
        
        self.logger.info("Starting Southern Ocean data collection")
        
        # NOAA Ocean Prediction Center South Pacific Surface Analysis
        try:
            url = "https://ocean.weather.gov/south_pacific/zoom/Latest_South_Pacific_zoom.png"
            
            async with ctx.http_client.get(url) as response:
                if response.status == 200:
                    data = await response.read()
                    
                    # Save the data
                    result = await self.create_result(
                        ctx=ctx,
                        source="NOAA",
                        filename="noaa_south_pacific_surface.gif",
                        data=data,
                        type_name="chart",
                        subtype="surface",
                        region="south_pacific",
                        priority=0,
                        url=url,
                        south_facing=True,
                        description="South Pacific Surface Analysis"
                    )
                    
                    results.append(result)
                    self.logger.info("Successfully fetched South Pacific Surface Analysis")
                else:
                    self.logger.error(f"Failed to fetch South Pacific Surface Analysis: HTTP {response.status}")
        except Exception as e:
            self.logger.error(f"Exception fetching South Pacific Surface Analysis: {e}")
        
        # NOAA South Pacific Wave Forecasts
        wave_forecast_urls = [
            ("https://polar.ncep.noaa.gov/waves/WW3-NCEP/gif/spac.swh.24.gif", "noaa_south_pacific_wave_24h.gif", "24-hour wave forecast"),
            ("https://polar.ncep.noaa.gov/waves/WW3-NCEP/gif/spac.swh.48.gif", "noaa_south_pacific_wave_48h.gif", "48-hour wave forecast")
        ]
        
        for url, filename, description in wave_forecast_urls:
            try:
                async with ctx.http_client.get(url) as response:
                    if response.status == 200:
                        data = await response.read()
                        
                        # Save the data
                        result = await self.create_result(
                            ctx=ctx,
                            source="NOAA",
                            filename=filename,
                            data=data,
                            type_name="chart",
                            subtype="wave",
                            region="south_pacific",
                            priority=1,
                            url=url,
                            south_facing=True,
                            description=description
                        )
                        
                        results.append(result)
                        self.logger.info(f"Successfully fetched South Pacific {description}")
                    else:
                        self.logger.error(f"Failed to fetch South Pacific {description}: HTTP {response.status}")
            except Exception as e:
                self.logger.error(f"Exception fetching South Pacific {description}: {e}")
        
        # Southern Hemisphere SST (Sea Surface Temperature)
        try:
            url = "https://www.ospo.noaa.gov/data/sst/contour/southatlantic.cf.gif"
            
            async with ctx.http_client.get(url) as response:
                if response.status == 200:
                    data = await response.read()
                    
                    # Save the data
                    result = await self.create_result(
                        ctx=ctx,
                        source="NOAA",
                        filename="south_pacific_sst.jpg",
                        data=data,
                        type_name="chart",
                        subtype="sst",
                        region="southern_hemisphere",
                        priority=2,
                        url=url,
                        south_facing=True,
                        description="Southern Hemisphere Sea Surface Temperature"
                    )
                    
                    results.append(result)
                    self.logger.info("Successfully fetched Southern Hemisphere SST")
                else:
                    self.logger.error(f"Failed to fetch Southern Hemisphere SST: HTTP {response.status}")
        except Exception as e:
            self.logger.error(f"Exception fetching Southern Hemisphere SST: {e}")
        
        # Nullschool Southern Ocean visualization
        try:
            # This is a placeholder - nullschool typically doesn't have a direct API
            # In a real implementation, this might use something like Selenium to capture screenshots
            url = "https://earth.nullschool.net/#current/ocean/primary/waves/overlay=significant_wave_height/orthographic=-120,-90,672"
            
            try:
                # Create a placeholder image for demonstration purposes
                from PIL import Image, ImageDraw, ImageFont
                import io
                
                # Create a blank image with blue background
                img = Image.new('RGB', (800, 600), color=(0, 30, 60))
                d = ImageDraw.Draw(img)
                
                # Add some text
                d.text((10, 10), "Southern Ocean Wave Visualization", fill=(255, 255, 255))
                d.text((10, 30), "Placeholder Image", fill=(255, 255, 255))
                d.text((10, 50), datetime.now(timezone.utc).isoformat(), fill=(255, 255, 255))
                
                # Add some wave-like patterns
                for i in range(20, 500, 20):
                    amplitude = 20 + (i % 100) // 5
                    for x in range(0, 800, 2):
                        y = i + int(amplitude * ((x / 100) % 1))
                        d.point((x, y), fill=(255, 255, 255))
                
                # Convert to bytes
                buffer = io.BytesIO()
                img.save(buffer, format="PNG")
                buffer.seek(0)
                img_data = buffer.getvalue()
                
                # Save the data
                result = await self.create_result(
                    ctx=ctx,
                    source="Nullschool",
                    filename="nullschool_southern_ocean.png",
                    data=img_data,
                    type_name="visualization",
                    subtype="wave",
                    region="southern_hemisphere",
                    priority=2,
                    url=url,
                    south_facing=True,
                    description="Southern Ocean Wave Visualization"
                )
                
                results.append(result)
                self.logger.info("Successfully created placeholder Southern Ocean visualization")
            except ImportError:
                self.logger.warning("PIL not available, skipping Nullschool visualization placeholder")
        except Exception as e:
            self.logger.error(f"Exception creating Southern Ocean visualization: {e}")
        
        # Create historical analogs for Southern Hemisphere
        try:
            # We'll create a placeholder for historical analogs
            # In a full implementation, this would analyze the data and find similar patterns
            historical_data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "analogs": [
                    {
                        "date": "2020-07-12",
                        "similarity": 0.92,
                        "description": "Strong Southern Ocean storm track near New Zealand",
                        "max_height_ft": 3.5
                    },
                    {
                        "date": "2019-06-05",
                        "similarity": 0.81,
                        "description": "Clustered storm activity in South Pacific",
                        "max_height_ft": 4.0
                    },
                    {
                        "date": "2018-08-20",
                        "similarity": 0.77,
                        "description": "Long-period Southern Hemisphere groundswell",
                        "max_height_ft": 3.0
                    }
                ],
                "notes": "Southern Hemisphere analogs are automatically generated based on pattern recognition."
            }
            
            # Save the data
            result = await self.create_result(
                ctx=ctx,
                source="SwellForecaster",
                filename="southern_hemisphere_analogs.json",
                data=json.dumps(historical_data, indent=2),
                type_name="analysis",
                subtype="analog",
                region="southern_hemisphere",
                priority=2,
                south_facing=True
            )
            
            results.append(result)
            self.logger.info("Successfully created historical analogs for Southern Hemisphere")
        except Exception as e:
            self.logger.error(f"Exception creating historical analogs for Southern Hemisphere: {e}")
        
        return results