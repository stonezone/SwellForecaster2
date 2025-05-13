"""NDBC and CDIP buoy data collection agent for SwellForecaster."""
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union, Any

from ..data.base_agent import BaseAgent
from ..data.collection_context import CollectionContext, CollectionResult
from ..models.buoy_data import BuoyReading, BuoyData
from ..models.settings import Settings
from ..utils import http_retry, fetch, fetch_json

logger = logging.getLogger("agents.buoy")


class BuoyAgent(BaseAgent):
    """Agent for collecting NDBC and CDIP buoy data."""
    
    def __init__(self, settings: Settings):
        """
        Initialize the buoy agent.
        
        Args:
            settings: Application settings
        """
        super().__init__(settings)
        self.buoy_ids = {
            # NDBC buoys near Hawaii (Core buoys)
            "51000": {"name": "NDBC 51000 - NW Hawaii", "north_facing": True},
            "51001": {"name": "NDBC 51001 - NW Hawaii", "north_facing": True},
            "51002": {"name": "NDBC 51002 - SW Hawaii", "south_facing": True},
            "51003": {"name": "NDBC 51003 - W Hawaii", "north_facing": True, "south_facing": True},
            "51004": {"name": "NDBC 51004 - SE Hawaii", "south_facing": True},
            "51101": {"name": "NDBC 51101 - NW Oahu", "north_facing": True},
            
            # CDIP buoys (Additional buoys)
            "098": {"name": "CDIP 098 - Mokapu Point", "type": "CDIP", "north_facing": True, "south_facing": True},
            "106": {"name": "CDIP 106 - Waimea Bay", "type": "CDIP", "north_facing": True},
            "188": {"name": "CDIP 188 - Kaumalapau", "type": "CDIP", "south_facing": True},
            "225": {"name": "CDIP 225 - Pauwela", "type": "CDIP", "north_facing": True},
            "238": {"name": "CDIP 238 - Hanalei", "type": "CDIP", "north_facing": True},
        }
    
    @property
    def name(self) -> str:
        """Agent name."""
        return "BuoyAgent"
    
    @property
    def enabled(self) -> bool:
        """Check if buoy collection is enabled."""
        return self.settings.sources.enable_buoys
    
    async def collect(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Collect NDBC and CDIP buoy data.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        if not self.enabled:
            self.logger.info("Buoy data collection is disabled")
            return []
        
        self.logger.info("Starting buoy data collection")
        results = []
        
        # Collect data for each buoy in parallel
        tasks = []
        for buoy_id, buoy_info in self.buoy_ids.items():
            # Skip CDIP buoys if they're not enabled
            if buoy_info.get("type") == "CDIP" and not self.settings.sources.enable_buoys:
                continue
            
            # Determine buoy type (default to NDBC)
            buoy_type = buoy_info.get("type", "NDBC")
            
            # Create collection task
            if buoy_type == "CDIP":
                tasks.append(self.collect_cdip_buoy(ctx, buoy_id, buoy_info))
            else:
                tasks.append(self.collect_ndbc_buoy(ctx, buoy_id, buoy_info))
        
        # Wait for all tasks to complete
        buoy_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in buoy_results:
            if isinstance(result, Exception):
                self.logger.error(f"Buoy collection task failed: {result}")
            elif result:
                results.extend(result)
        
        self.logger.info(f"Completed buoy data collection with {len(results)} results")
        return results
    
    async def collect_ndbc_buoy(
        self, ctx: CollectionContext, buoy_id: str, buoy_info: Dict[str, Any]
    ) -> List[CollectionResult]:
        """
        Collect data for a specific NDBC buoy.
        
        Args:
            ctx: Collection context
            buoy_id: Buoy ID
            buoy_info: Buoy metadata
            
        Returns:
            List of collection results
        """
        results = []
        buoy_name = buoy_info.get("name", f"NDBC {buoy_id}")
        self.logger.info(f"Collecting data for {buoy_name}")
        
        # Determine if this buoy is relevant for specific shores
        north_facing = buoy_info.get("north_facing", False)
        south_facing = buoy_info.get("south_facing", False)
        
        # Base URL for NDBC data
        base_url = "https://www.ndbc.noaa.gov/data/realtime2"
        
        # Collect standard meteorological data
        try:
            url = f"{base_url}/{buoy_id}.txt"
            data = await fetch(url)
            
            if data:
                # Save the data
                result = await self.create_result(
                    ctx=ctx,
                    source="NDBC",
                    filename=f"ndbc_{buoy_id}.txt",
                    data=data,
                    type_name="buoy",
                    subtype="meteorological",
                    region="hawaii",
                    priority=1,
                    url=url,
                    north_facing=north_facing,
                    south_facing=south_facing
                )
                
                results.append(result)
                self.logger.info(f"Successfully collected meteorological data for {buoy_name}")
        except Exception as e:
            self.logger.error(f"Failed to collect meteorological data for {buoy_name}: {e}")
        
        # Collect spectral wave data
        try:
            url = f"{base_url}/{buoy_id}.spec"
            data = await fetch(url)
            
            if data:
                # Save the data
                result = await self.create_result(
                    ctx=ctx,
                    source="NDBC",
                    filename=f"ndbc_{buoy_id}_spec.txt",
                    data=data,
                    type_name="buoy",
                    subtype="spectral",
                    region="hawaii",
                    priority=2,
                    url=url,
                    north_facing=north_facing,
                    south_facing=south_facing
                )
                
                results.append(result)
                self.logger.info(f"Successfully collected spectral data for {buoy_name}")
        except Exception as e:
            self.logger.error(f"Failed to collect spectral data for {buoy_name}: {e}")
        
        return results
    
    async def collect_cdip_buoy(
        self, ctx: CollectionContext, buoy_id: str, buoy_info: Dict[str, Any]
    ) -> List[CollectionResult]:
        """
        Collect data for a specific CDIP buoy.
        
        Args:
            ctx: Collection context
            buoy_id: Buoy ID
            buoy_info: Buoy metadata
            
        Returns:
            List of collection results
        """
        results = []
        buoy_name = buoy_info.get("name", f"CDIP {buoy_id}")
        self.logger.info(f"Collecting data for {buoy_name}")
        
        # Determine if this buoy is relevant for specific shores
        north_facing = buoy_info.get("north_facing", False)
        south_facing = buoy_info.get("south_facing", False)
        
        # URL for CDIP data with parameters for wave data
        url = f"https://cdip.ucsd.edu/data_access/justdar.cdip?069p1/{buoy_id}/tc"
        
        try:
            # Fetch the data
            data = await fetch(url)
            
            if data:
                # Save the data
                result = await self.create_result(
                    ctx=ctx,
                    source="CDIP",
                    filename=f"cdip_{buoy_id}.txt",
                    data=data,
                    type_name="buoy",
                    subtype="realtime",
                    region="hawaii",
                    priority=1,
                    url=url,
                    north_facing=north_facing,
                    south_facing=south_facing
                )
                
                results.append(result)
                self.logger.info(f"Successfully collected realtime data for {buoy_name}")
        except Exception as e:
            self.logger.error(f"Failed to collect realtime data for {buoy_name}: {e}")
        
        # Try to get spectral data if available (JSON format)
        try:
            spec_url = f"https://cdip.ucsd.edu/data_access/MEM_2dspectra.cdip?{buoy_id}p1_d2"
            spec_data = await fetch(spec_url)
            
            if spec_data:
                # Save the data
                result = await self.create_result(
                    ctx=ctx,
                    source="CDIP",
                    filename=f"cdip_{buoy_id}_spec.txt",
                    data=spec_data,
                    type_name="buoy",
                    subtype="spectral",
                    region="hawaii",
                    priority=2,
                    url=spec_url,
                    north_facing=north_facing,
                    south_facing=south_facing
                )
                
                results.append(result)
                self.logger.info(f"Successfully collected spectral data for {buoy_name}")
        except Exception as e:
            self.logger.error(f"Failed to collect spectral data for {buoy_name}: {e}")
        
        return results