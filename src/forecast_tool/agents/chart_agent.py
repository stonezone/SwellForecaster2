"""Chart collection agent for SwellForecaster."""
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Union, Any

from ..data.base_agent import BaseAgent
from ..data.collection_context import CollectionContext, CollectionResult
from ..models.settings import Settings
from ..utils import create_placeholder_image

logger = logging.getLogger("agents.chart")


class ChartAgent(BaseAgent):
    """Agent for collecting OPC, WPC, and other meteorological chart data."""
    
    def __init__(self, settings: Settings):
        """
        Initialize the chart agent.
        
        Args:
            settings: Application settings
        """
        super().__init__(settings)
        self.chart_sources = {
            # Ocean Prediction Center (OPC) charts
            "opc": {
                "enabled": settings.sources.enable_opc,
                "charts": {
                    # Current surface analysis
                    "P_sfc_full_ocean_color.png": {
                        "url": "https://www.opc.ncep.noaa.gov/P_sfc_full_ocean_color.png",
                        "type": "surface_analysis",
                        "priority": 1,
                        "north_facing": True,
                        "south_facing": False,
                    },
                    # Current wave analysis
                    "P_00hrww.gif": {
                        "url": "https://www.opc.ncep.noaa.gov/P_00hrww.gif",
                        "type": "wave_height",
                        "priority": 1,
                        "north_facing": True,
                        "south_facing": False,
                    },
                    # Regional wave analysis
                    "P_reg_00hrww.gif": {
                        "url": "https://www.opc.ncep.noaa.gov/P_reg_00hrww.gif",
                        "type": "wave_height",
                        "subtype": "regional",
                        "priority": 2,
                        "north_facing": True,
                        "south_facing": False,
                    },
                    # 24-hour surface forecast
                    "P_24hrsfc.gif": {
                        "url": "https://www.opc.ncep.noaa.gov/P_24hrsfc.gif",
                        "type": "surface_forecast",
                        "subtype": "24hr",
                        "priority": 1,
                        "north_facing": True,
                        "south_facing": False,
                    },
                    # 24-hour wave forecast
                    "P_24hrwper.gif": {
                        "url": "https://www.opc.ncep.noaa.gov/P_24hrwper.gif",
                        "type": "wave_forecast",
                        "subtype": "24hr",
                        "priority": 1,
                        "north_facing": True,
                        "south_facing": False,
                    },
                    # 48-hour surface forecast
                    "P_48hrsfc.gif": {
                        "url": "https://www.opc.ncep.noaa.gov/P_48hrsfc.gif",
                        "type": "surface_forecast",
                        "subtype": "48hr",
                        "priority": 2,
                        "north_facing": True,
                        "south_facing": False,
                    },
                    # 48-hour wave forecast
                    "P_48hrwper.gif": {
                        "url": "https://www.opc.ncep.noaa.gov/P_48hrwper.gif",
                        "type": "wave_forecast",
                        "subtype": "48hr",
                        "priority": 2,
                        "north_facing": True,
                        "south_facing": False,
                    },
                    # 72-hour surface forecast
                    "P_72hrsfc.gif": {
                        "url": "https://www.opc.ncep.noaa.gov/P_72hrsfc.gif",
                        "type": "surface_forecast",
                        "subtype": "72hr",
                        "priority": 3,
                        "north_facing": True,
                        "south_facing": False,
                    },
                    # 72-hour wave forecast
                    "P_72hrwper.gif": {
                        "url": "https://www.opc.ncep.noaa.gov/P_72hrwper.gif",
                        "type": "wave_forecast",
                        "subtype": "72hr",
                        "priority": 3,
                        "north_facing": True,
                        "south_facing": False,
                    },
                    # Eastern Pacific surface analysis
                    "P_e_sfc_color.png": {
                        "url": "https://www.opc.ncep.noaa.gov/P_e_sfc_color.png",
                        "type": "surface_analysis",
                        "subtype": "eastern_pacific",
                        "priority": 2,
                        "north_facing": True,
                        "south_facing": False,
                    },
                    # Western Pacific surface analysis
                    "P_w_sfc_color.png": {
                        "url": "https://www.opc.ncep.noaa.gov/P_w_sfc_color.png",
                        "type": "surface_analysis",
                        "subtype": "western_pacific",
                        "priority": 2,
                        "north_facing": True,
                        "south_facing": False,
                    },
                    # North Pacific satellite imagery
                    "nPacOff_IR_15min_latest.gif": {
                        "url": "https://www.opc.ncep.noaa.gov/IR_imagery/nPacOff_IR_15min_latest.gif",
                        "type": "satellite",
                        "subtype": "north_pacific",
                        "priority": 3,
                        "north_facing": True,
                        "south_facing": False,
                    },
                }
            },
            # Weather Prediction Center (WPC) charts
            "wpc": {
                "enabled": settings.sources.enable_wpc,
                "charts": {
                    # 24-hour precipitation forecast
                    "24hr.tif": {
                        "url": "https://www.wpc.ncep.noaa.gov/qpf/p24i.gif",
                        "type": "precipitation",
                        "subtype": "24hr",
                        "priority": 3,
                        "north_facing": True,
                        "south_facing": True,
                    },
                    # 48-hour precipitation forecast
                    "48hr.tif": {
                        "url": "https://www.wpc.ncep.noaa.gov/qpf/p48i.gif",
                        "type": "precipitation",
                        "subtype": "48hr",
                        "priority": 3,
                        "north_facing": True,
                        "south_facing": True,
                    },
                }
            },
            # NOAA Wave Watch III (WW3) model charts
            "ww3": {
                "enabled": settings.sources.enable_models,
                "charts": {
                    # North Pacific significant wave height
                    "north_pacific_detail.gif": {
                        "url": "https://polar.ncep.noaa.gov/waves/latest_run/multi_1.nww3.hs.north_pacific.latest.gif",
                        "type": "wave_model",
                        "subtype": "north_pacific",
                        "priority": 1,
                        "north_facing": True,
                        "south_facing": False,
                    },
                    # Pacific overview
                    "pacific_overview.gif": {
                        "url": "https://polar.ncep.noaa.gov/waves/latest_run/pac-hs.latest_run.gif",
                        "type": "wave_model",
                        "subtype": "pacific_overview",
                        "priority": 1,
                        "north_facing": True,
                        "south_facing": True,
                    },
                }
            },
            # Southern Hemisphere charts
            "southern": {
                "enabled": settings.sources.enable_southern_ocean,
                "charts": {
                    # South Pacific surface analysis
                    "south_pacific_surface.gif": {
                        "url": "https://www.bom.gov.au/marine/charts/pac/PackPSL00.pdf",
                        "type": "surface_analysis",
                        "subtype": "south_pacific",
                        "priority": 1,
                        "north_facing": False,
                        "south_facing": True,
                    },
                    # South Pacific wave height
                    "south_pacific_wave_24h.gif": {
                        "url": "https://www.bom.gov.au/marine/charts/pac/PackWAVE01.pdf",
                        "type": "wave_forecast",
                        "subtype": "south_pacific_24h",
                        "priority": 1,
                        "north_facing": False,
                        "south_facing": True,
                    },
                    # South Pacific wave height 48h
                    "south_pacific_wave_48h.gif": {
                        "url": "https://www.bom.gov.au/marine/charts/pac/PackWAVE02.pdf",
                        "type": "wave_forecast",
                        "subtype": "south_pacific_48h",
                        "priority": 2,
                        "north_facing": False,
                        "south_facing": True,
                    },
                }
            }
        }
    
    @property
    def name(self) -> str:
        """Agent name."""
        return "ChartAgent"
    
    @property
    def enabled(self) -> bool:
        """Check if chart collection is enabled."""
        # At least one chart source must be enabled
        return any(source["enabled"] for source in self.chart_sources.values())
    
    async def collect(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Collect chart data.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        if not self.enabled:
            self.logger.info("Chart data collection is disabled")
            return []
        
        self.logger.info("Starting chart data collection")
        results = []
        tasks = []
        
        # Process each chart source
        for source_name, source_info in self.chart_sources.items():
            if not source_info["enabled"]:
                self.logger.info(f"Skipping disabled chart source: {source_name}")
                continue
            
            self.logger.info(f"Processing chart source: {source_name}")
            
            # Process each chart in parallel
            for filename, chart_info in source_info["charts"].items():
                task = self.fetch_chart(
                    ctx,
                    source_name,
                    filename,
                    chart_info["url"],
                    chart_info["type"],
                    chart_info.get("subtype"),
                    chart_info.get("priority", 1),
                    chart_info.get("north_facing"),
                    chart_info.get("south_facing")
                )
                tasks.append(task)
        
        # Wait for all tasks to complete
        chart_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in chart_results:
            if isinstance(result, Exception):
                self.logger.error(f"Chart collection task failed: {result}")
            elif result:
                results.append(result)
        
        self.logger.info(f"Completed chart data collection with {len(results)} results")
        return results
    
    async def fetch_chart(
        self,
        ctx: CollectionContext,
        source: str,
        filename: str,
        url: str,
        type_name: str,
        subtype: Optional[str] = None,
        priority: int = 1,
        north_facing: Optional[bool] = None,
        south_facing: Optional[bool] = None
    ) -> Optional[CollectionResult]:
        """
        Fetch a chart from a URL and save it.
        
        Args:
            ctx: Collection context
            source: Source name
            filename: Filename to save as
            url: URL to fetch
            type_name: Type of chart
            subtype: Subtype of chart
            priority: Priority (lower is higher)
            north_facing: Whether relevant for north-facing shores
            south_facing: Whether relevant for south-facing shores
            
        Returns:
            Collection result or None if fetch failed
        """
        try:
            # Fetch the chart
            self.logger.info(f"Fetching chart: {url}")
            data = await self.fetch(ctx, url)
            
            # Create result
            result = await self.create_result(
                ctx=ctx,
                source=source,
                filename=filename,
                data=data,
                type_name=type_name,
                subtype=subtype,
                priority=priority,
                url=url,
                north_facing=north_facing,
                south_facing=south_facing
            )
            
            self.logger.info(f"Successfully fetched chart: {filename}")
            return result
        
        except Exception as e:
            self.logger.error(f"Failed to fetch chart {url}: {str(e)}")
            
            # Create placeholder image for failed chart
            try:
                self.logger.info(f"Creating placeholder image for failed chart: {filename}")
                placeholder_data = create_placeholder_image(
                    f"Unable to retrieve {source} chart. Error: {str(e)}",
                    filename=filename
                )
                
                # Create result with error flag
                result = await self.create_result(
                    ctx=ctx,
                    source=source,
                    filename=filename,
                    data=placeholder_data,
                    type_name=type_name,
                    subtype=subtype,
                    priority=priority + 10,  # Lower priority for placeholder
                    url=url,
                    error=True,
                    north_facing=north_facing,
                    south_facing=south_facing
                )
                
                self.logger.info(f"Created placeholder for failed chart: {filename}")
                return result
            
            except Exception as placeholder_error:
                self.logger.error(f"Failed to create placeholder for {filename}: {placeholder_error}")
                return None