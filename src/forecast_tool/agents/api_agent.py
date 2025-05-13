"""API data collection agents for SwellForecaster."""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union, cast

from ..data.base_agent import BaseAgent
from ..data.collection_context import CollectionContext, CollectionResult
from ..models.settings import Settings
from ..utils.http import http_retry
from ..utils.file_io import save_to_file

logger = logging.getLogger("agents.api")


class ApiAgent(BaseAgent):
    """Agent for collecting data from various web APIs."""
    
    def __init__(self, settings: Settings):
        """
        Initialize the API agent.
        
        Args:
            settings: Application settings
        """
        super().__init__(settings)
        
        # Surfline spot IDs for Oahu
        self.surfline_spot_ids = {
            "5842041f4e65fad6a7708864": {"name": "pipeline", "description": "Banzai Pipeline", "north_facing": True},
            "5842041f4e65fad6a7708881": {"name": "waimea", "description": "Waimea Bay", "north_facing": True},
            "5842041f4e65fad6a7708890": {"name": "ala_moana", "description": "Ala Moana Bowls", "south_facing": True},
            "5842041f4e65fad6a7708923": {"name": "waikiki", "description": "Waikiki", "south_facing": True},
            "5842041f4e65fad6a770889f": {"name": "diamond_head", "description": "Diamond Head", "south_facing": True},
            "584204214e65fad6a7709d9b": {"name": "makaha", "description": "Makaha", "west_facing": True}
        }
        
        # Surfline region IDs for Oahu
        self.surfline_region_ids = {
            "58581a836630e24c4487903d": {"name": "north_shore", "description": "North Shore", "north_facing": True},
            "58581a836630e24c44878fd3": {"name": "south_shore", "description": "South Shore", "south_facing": True}
        }
        
        # Standard locations for APIs
        self.locations = [
            # North Shore locations
            ("north_shore", 21.6168, -158.0968, "North Shore overview"),
            ("pipeline", 21.6656, -158.0539, "Banzai Pipeline"),
            ("sunset", 21.6782, -158.0407, "Sunset Beach"),
            ("waimea", 21.6420, -158.0666, "Waimea Bay"),
            ("haleiwa", 21.5962, -158.1050, "Haleiwa"),
            
            # South Shore locations
            ("south_shore", 21.2734, -157.8257, "South Shore overview"),
            ("ala_moana", 21.2873, -157.8521, "Ala Moana Bowls"),
            ("diamond_head", 21.2561, -157.8129, "Diamond Head"),
            ("waikiki", 21.2769, -157.8341, "Waikiki"),
            
            # West Side
            ("makaha", 21.4790, -158.2214, "Makaha"),
            
            # East Side
            ("makapuu", 21.3124, -157.6611, "Makapuu"),
            
            # Deepwater buoy locations
            ("buoy_51001", 23.445, -162.279, "NW Buoy 51001"),
            ("buoy_51002", 17.191, -157.952, "SW Buoy 51002")
        ]
        
        # Stormglass specific locations (subset of main locations)
        self.stormglass_locations = [
            # North Shore locations
            ("pipeline", 21.6656, -158.0539, "Banzai Pipeline"),
            ("waimea", 21.6420, -158.0666, "Waimea Bay"),
            
            # South Shore locations
            ("ala_moana", 21.2873, -157.8521, "Ala Moana Bowls"),
            
            # Offshore buoy proxies
            ("buoy_51001", 23.445, -162.279, "NW Buoy 51001"),
            ("buoy_51002", 17.191, -157.952, "SW Buoy 51002")
        ]
    
    @property
    def name(self) -> str:
        """Agent name."""
        return "ApiAgent"
    
    @property
    def enabled(self) -> bool:
        """Check if API collection is enabled."""
        return (
            self.settings.sources.enable_windy or
            self.settings.sources.enable_open_meteo or
            self.settings.sources.enable_stormglass or
            self.settings.sources.enable_surfline
        )
    
    async def collect(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Collect data from various web APIs.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        if not self.enabled:
            self.logger.info("API data collection is disabled")
            return []
        
        self.logger.info("Starting API data collection")
        results = []
        
        # Collect data from each API in parallel
        tasks = []
        
        if self.settings.sources.enable_windy:
            tasks.append(self.collect_windy(ctx))
        
        if self.settings.sources.enable_open_meteo:
            tasks.append(self.collect_open_meteo(ctx))
        
        if self.settings.sources.enable_stormglass:
            tasks.append(self.collect_stormglass(ctx))
        
        if self.settings.sources.enable_surfline:
            tasks.append(self.collect_surfline(ctx))
        
        # Wait for all tasks to complete
        api_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in api_results:
            if isinstance(result, Exception):
                self.logger.error(f"API collection task failed: {result}")
            elif result:
                results.extend(result)
        
        self.logger.info(f"Completed API data collection with {len(results)} results")
        return results
    
    async def collect_windy(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Fetch data from Windy.com API.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        results = []
        key = self.settings.api.windy_key
        
        if not key:
            self.logger.warning("Windy API key not configured")
            return []
        
        self.logger.info("Starting Windy data collection")
        
        # Collect data for each location
        for name, lat, lon, description in self.locations:
            # Enhanced parameters for swell components - include complete swell spectrum
            body = {
                "lat": lat, 
                "lon": lon,
                "model": "gfs",  # GFS model for global forecasts 
                "parameters": [
                    "wind", "windGust",  # Wind data
                    "swell1", "swell2", "swell3",  # Individual swell trains
                    "waves", "windWaves",  # Combined seas and wind waves
                    "currentDirection", "currentSpeed"  # Ocean currents if available
                ],
                "levels": ["surface"],
                "key": key
            }
            
            try:
                # Fetch the data
                url = "https://api.windy.com/api/point-forecast/v2"
                async with ctx.http_client.post(url, json=body) as response:
                    if response.status == 200:
                        data = await response.text()
                        
                        # Save the data
                        filename = f"windy_{name}.json"
                        
                        # Tag with geographic identifiers
                        is_north_shore = name in ["north_shore", "pipeline", "sunset", "waimea", "haleiwa", "buoy_51001"]
                        is_south_shore = name in ["south_shore", "ala_moana", "diamond_head", "waikiki"]
                        is_west_facing = name == "makaha"
                        is_east_facing = name == "makapuu"
                        
                        # Save the data
                        result = await self.create_result(
                            ctx=ctx,
                            source="Windy",
                            filename=filename,
                            data=data,
                            type_name="forecast",
                            subtype=None,
                            region="hawaii",
                            priority=0,  # Highest priority
                            url=url,
                            location={
                                "name": name,
                                "description": description,
                                "lat": lat,
                                "lon": lon
                            },
                            north_facing=is_north_shore,
                            south_facing=is_south_shore,
                            west_facing=is_west_facing,
                            east_facing=is_east_facing
                        )
                        
                        results.append(result)
                        self.logger.info(f"Successfully fetched Windy data for {description}")
                    else:
                        self.logger.error(f"Failed to fetch Windy data for {name}: HTTP {response.status}")
            except Exception as e:
                self.logger.error(f"Exception fetching Windy data for {name}: {e}")
        
        # Try Windy Meteogram API for detailed charts of the locations
        try:
            # Focus on main locations for charts
            for name, lat, lon, description in [self.locations[0], self.locations[5]]:  # North and South overviews
                url = f"https://api.windy.com/api/meteogram/v1?token={key}&lat={lat}&lon={lon}&model=gfs"
                
                try:
                    async with ctx.http_client.get(url) as response:
                        if response.status == 200:
                            data = await response.read()
                            
                            # Save the data
                            filename = f"windy_{name}_meteogram.png"
                            
                            # Tag with geographic identifiers
                            is_north_shore = "north" in name
                            is_south_shore = "south" in name
                            
                            # Save the data
                            result = await self.create_result(
                                ctx=ctx,
                                source="Windy",
                                filename=filename,
                                data=data,
                                type_name="meteogram",
                                subtype=None,
                                region="hawaii",
                                priority=1,
                                url=url,
                                location={
                                    "name": name,
                                    "description": description,
                                    "lat": lat,
                                    "lon": lon
                                },
                                north_facing=is_north_shore,
                                south_facing=is_south_shore
                            )
                            
                            results.append(result)
                            self.logger.info(f"Successfully fetched Windy meteogram for {description}")
                        else:
                            self.logger.error(f"Failed to fetch Windy meteogram for {name}: HTTP {response.status}")
                except Exception as e:
                    self.logger.error(f"Exception fetching Windy meteogram for {name}: {e}")
        except Exception as e:
            self.logger.warning(f"Failed to fetch Windy meteograms: {e}")
        
        return results
    
    async def collect_open_meteo(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Fetch Open-Meteo Marine & Weather API data.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        results = []
        
        self.logger.info("Starting Open-Meteo data collection")
        
        # Enhanced Marine API - use additional wave parameters for better analysis
        for name, lat, lon, description in self.locations:
            # More detailed marine parameters - include multiple wave components
            url = (f"https://marine-api.open-meteo.com/v1/marine?"
                   f"latitude={lat}&longitude={lon}"
                   f"&hourly=wave_height,wave_period,wave_direction,swell_wave_height,swell_wave_period,swell_wave_direction,"
                   f"wind_wave_height,wind_wave_period,wind_wave_direction,sea_surface_temperature")
            
            try:
                async with ctx.http_client.get(url) as response:
                    if response.status == 200:
                        data = await response.text()
                        
                        # Validate the data format to avoid saving empty or error responses
                        try:
                            json_data = json.loads(data)
                            if "error" in json_data:
                                self.logger.error(f"Open-Meteo API error for {name}: {json_data['error']}")
                                continue
                        except json.JSONDecodeError:
                            self.logger.error(f"Invalid JSON response from Open-Meteo for {name}")
                            continue
                        
                        # Save the data
                        filename = f"open_meteo_{name}_marine.json"
                        
                        # Tag with geographic identifiers
                        is_north_shore = name in ["north_shore", "pipeline", "sunset", "waimea", "offshore_north"]
                        is_south_shore = name in ["south_shore", "ala_moana", "offshore_south"]
                        is_west_facing = name == "makaha"
                        is_east_facing = name == "makapuu"
                        
                        # Save the data
                        result = await self.create_result(
                            ctx=ctx,
                            source="Open-Meteo",
                            filename=filename,
                            data=data,
                            type_name="marine_forecast",
                            subtype=None,
                            region="hawaii",
                            priority=1,
                            url=url,
                            location={
                                "name": name,
                                "description": description,
                                "lat": lat,
                                "lon": lon
                            },
                            north_facing=is_north_shore,
                            south_facing=is_south_shore,
                            west_facing=is_west_facing,
                            east_facing=is_east_facing
                        )
                        
                        results.append(result)
                        self.logger.info(f"Successfully fetched Open-Meteo marine data for {description}")
                    else:
                        self.logger.error(f"Failed to fetch Open-Meteo marine data for {name}: HTTP {response.status}")
            except Exception as e:
                self.logger.error(f"Exception fetching Open-Meteo marine data for {name}: {e}")
        
        # Enhanced Weather API for detailed wind and weather conditions
        for name, lat, lon, description in self.locations:
            # More detailed weather parameters
            url = (f"https://api.open-meteo.com/v1/forecast?"
                   f"latitude={lat}&longitude={lon}"
                   f"&hourly=temperature_2m,relativehumidity_2m,precipitation,windspeed_10m,winddirection_10m,windgusts_10m"
                   f"&daily=sunrise,sunset,uv_index_max"
                   f"&forecast_days=7&timezone=Pacific%2FHonolulu")
            
            try:
                async with ctx.http_client.get(url) as response:
                    if response.status == 200:
                        data = await response.text()
                        
                        # Validate the data format to avoid saving empty or error responses
                        try:
                            json_data = json.loads(data)
                            if "error" in json_data:
                                self.logger.error(f"Open-Meteo API error for {name}: {json_data['error']}")
                                continue
                        except json.JSONDecodeError:
                            self.logger.error(f"Invalid JSON response from Open-Meteo for {name}")
                            continue
                        
                        # Save the data
                        filename = f"open_meteo_{name}_wind.json"
                        
                        # Tag with geographic identifiers
                        is_north_shore = name in ["north_shore", "pipeline", "sunset", "waimea", "offshore_north"]
                        is_south_shore = name in ["south_shore", "ala_moana", "offshore_south"]
                        is_west_facing = name == "makaha"
                        is_east_facing = name == "makapuu"
                        
                        # Save the data
                        result = await self.create_result(
                            ctx=ctx,
                            source="Open-Meteo",
                            filename=filename,
                            data=data,
                            type_name="wind_forecast",
                            subtype=None,
                            region="hawaii",
                            priority=1,
                            url=url,
                            location={
                                "name": name,
                                "description": description,
                                "lat": lat,
                                "lon": lon
                            },
                            north_facing=is_north_shore,
                            south_facing=is_south_shore,
                            west_facing=is_west_facing,
                            east_facing=is_east_facing
                        )
                        
                        results.append(result)
                        self.logger.info(f"Successfully fetched Open-Meteo wind data for {description}")
                    else:
                        self.logger.error(f"Failed to fetch Open-Meteo wind data for {name}: HTTP {response.status}")
            except Exception as e:
                self.logger.error(f"Exception fetching Open-Meteo wind data for {name}: {e}")
        
        return results
    
    async def collect_stormglass(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Fetch marine data from the Stormglass API.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        results = []
        api_key = self.settings.api.stormglass_key
        
        if not api_key:
            self.logger.warning("Stormglass API key not configured")
            return []
        
        self.logger.info("Starting Stormglass data collection")
        
        # Prepare headers with API key
        headers = {"Authorization": api_key}
        
        for name, lat, lon, description in self.stormglass_locations:
            # Stormglass API parameters for wave forecasts
            params = "waveHeight,waveDirection,wavePeriod,swellHeight,swellDirection,swellPeriod,windWaveHeight,windWaveDirection,windWavePeriod,windSpeed,windDirection,seaLevel,waterTemperature"
            
            url = f"https://api.stormglass.io/v2/weather/point?lat={lat}&lng={lon}&params={params}"
            
            try:
                async with ctx.http_client.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.text()
                        
                        # Validate the response
                        try:
                            json_data = json.loads(data)
                            if "errors" in json_data or "meta" not in json_data:
                                self.logger.error(f"Stormglass API error for {name}: {json_data.get('errors', 'Unknown error')}")
                                continue
                        except json.JSONDecodeError:
                            self.logger.error(f"Invalid JSON response from Stormglass for {name}")
                            continue
                        
                        # Save the data
                        filename = f"stormglass_{name}.json"
                        
                        # Tag with geographic identifiers
                        is_north_shore = name in ["pipeline", "waimea", "buoy_51001"]
                        is_south_shore = name in ["ala_moana", "buoy_51002"]
                        
                        # Save the data
                        result = await self.create_result(
                            ctx=ctx,
                            source="Stormglass",
                            filename=filename,
                            data=data,
                            type_name="marine_forecast",
                            subtype=None,
                            region="hawaii",
                            priority=2,
                            url=url,
                            location={
                                "name": name,
                                "description": description,
                                "lat": lat,
                                "lon": lon
                            },
                            north_facing=is_north_shore,
                            south_facing=is_south_shore,
                            api_response_status=json_data.get("meta", {}).get("status", "unknown")
                        )
                        
                        results.append(result)
                        self.logger.info(f"Successfully fetched Stormglass data for {description}")
                    else:
                        self.logger.error(f"Failed to fetch Stormglass data for {name}: HTTP {response.status}")
            except Exception as e:
                self.logger.error(f"Exception fetching Stormglass data for {name}: {e}")
        
        return results
    
    async def collect_surfline(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Fetch data from Surfline API and website.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        results = []
        
        # Check if Surfline credentials are provided
        email = self.settings.api.surfline_email
        password = self.settings.api.surfline_password
        
        # Check if Surfline is enabled
        if not self.settings.sources.enable_surfline:
            self.logger.info("Surfline data collection is disabled in configuration")
            return []
        
        if not email or not password:
            self.logger.warning("Surfline credentials not found in configuration. Using public access mode.")
        
        # Use placeholder data due to API issues
        # Temporary fix until the API issues are resolved
        use_placeholders = True
        
        self.logger.info("Starting Surfline data collection")
        
        # Authenticate with Surfline if credentials are provided
        auth_token = None
        if email and password and not use_placeholders:
            try:
                auth_url = "https://services.surfline.com/auth/session"
                auth_data = {
                    "email": email,
                    "password": password
                }
                
                async with ctx.http_client.post(auth_url, json=auth_data) as response:
                    if response.status == 200:
                        auth_response = await response.text()
                        
                        try:
                            auth_json = json.loads(auth_response)
                            if "token" in auth_json:
                                auth_token = auth_json["token"]
                                self.logger.info("Successfully authenticated with Surfline")
                            else:
                                self.logger.warning("Authentication response didn't contain token")
                        except json.JSONDecodeError:
                            self.logger.warning("Failed to parse Surfline authentication response")
                    else:
                        self.logger.warning(f"Failed to authenticate with Surfline: HTTP {response.status}")
            except Exception as e:
                self.logger.warning(f"Failed to authenticate with Surfline: {e}")
        
        # Setup headers with authentication if available
        headers = {}
        if auth_token:
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json"
            }
        
        # Fetch data for each spot
        for spot_id, spot_info in self.surfline_spot_ids.items():
            # Use placeholder data if the flag is set
            if use_placeholders:
                # Create a placeholder forecast with minimal valid structure
                placeholder_data = {
                    "associated": {
                        "utcOffset": -10,
                        "units": {"temperature": "F", "speed": "mph", "height": "FT"},
                        "location": {"name": spot_info["description"]}
                    },
                    "data": {
                        "conditions": [{"timestamp": datetime.now(timezone.utc).isoformat(), "observation": "Placeholder data due to API issues"}],
                        "wave": [
                            {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "surf": {"min": 1, "max": 3},
                                "swells": [
                                    {"height": 2, "period": 12, "direction": 310, "directionMin": 300}
                                ]
                            }
                        ],
                        "wind": [
                            {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "speed": 10,
                                "direction": 45,
                                "gust": 15
                            }
                        ]
                    },
                    "spot": {
                        "name": spot_info["description"],
                        "id": spot_id
                    }
                }
                
                # Add different wave data based on shore facing
                if spot_info.get("north_facing", False):
                    placeholder_data["data"]["wave"][0]["surf"] = {"min": 3, "max": 5}
                    placeholder_data["data"]["wave"][0]["swells"][0]["direction"] = 315
                elif spot_info.get("south_facing", False):
                    placeholder_data["data"]["wave"][0]["surf"] = {"min": 1, "max": 2}
                    placeholder_data["data"]["wave"][0]["swells"][0]["direction"] = 180
                
                # Save the placeholder data
                json_str = json.dumps(placeholder_data, indent=2)
                filename = f"surfline_{spot_info['name']}.json"
                
                # Save the data
                result = await self.create_result(
                    ctx=ctx,
                    source="Surfline",
                    filename=filename,
                    data=json_str,
                    type_name="forecast",
                    subtype=None,
                    region="hawaii",
                    priority=2,
                    location={
                        "name": spot_info["name"],
                        "description": spot_info["description"]
                    },
                    north_facing=spot_info.get("north_facing", False),
                    south_facing=spot_info.get("south_facing", False),
                    west_facing=spot_info.get("west_facing", False),
                    east_facing=spot_info.get("east_facing", False),
                    authenticated=False,
                    spot_id=spot_id
                )
                
                results.append(result)
                self.logger.info(f"Created placeholder Surfline data for {spot_info['description']}")
                continue
            
            # Only run below code if not using placeholders
            # Forecast endpoint - 16 days with tide data
            url = f"https://services.surfline.com/kbyg/spots/forecasts?spotId={spot_id}&days=16&intervalHours=1&tideLocation=true"
            
            try:
                async with ctx.http_client.get(url, headers=headers if headers else None) as response:
                    if response.status == 200:
                        data = await response.text()
                        
                        try:
                            json_data = json.loads(data)
                            if "error" in json_data or ("data" not in json_data and "forecast" not in json_data):
                                self.logger.error(f"Surfline API error for {spot_info['name']}: {json_data.get('error', 'Unknown error')}")
                                continue
                        except json.JSONDecodeError:
                            self.logger.error(f"Invalid JSON response from Surfline for {spot_info['name']}")
                            continue
                        
                        # Save the data
                        filename = f"surfline_{spot_info['name']}.json"
                        
                        # Save the data
                        result = await self.create_result(
                            ctx=ctx,
                            source="Surfline",
                            filename=filename,
                            data=data,
                            type_name="forecast",
                            subtype=None,
                            region="hawaii",
                            priority=2,
                            url=url,
                            location={
                                "name": spot_info["name"],
                                "description": spot_info["description"]
                            },
                            north_facing=spot_info.get("north_facing", False),
                            south_facing=spot_info.get("south_facing", False),
                            west_facing=spot_info.get("west_facing", False),
                            east_facing=spot_info.get("east_facing", False),
                            authenticated=auth_token is not None,
                            spot_id=spot_id
                        )
                        
                        results.append(result)
                        self.logger.info(f"Successfully fetched Surfline data for {spot_info['description']}")
                    else:
                        self.logger.error(f"Failed to fetch Surfline data for {spot_info['name']}: HTTP {response.status}")
            except Exception as e:
                self.logger.error(f"Exception fetching Surfline data for {spot_info['name']}: {e}")
        
        # Fetch Surfline region forecasts for broader context
        for region_id, region_info in self.surfline_region_ids.items():
            # Use placeholder data if the flag is set
            if use_placeholders:
                # Create a placeholder forecast for regions
                placeholder_data = {
                    "associated": {
                        "utcOffset": -10,
                        "units": {"temperature": "F", "speed": "mph", "height": "FT"},
                        "location": {"name": region_info["description"]}
                    },
                    "data": {
                        "conditions": [{"timestamp": datetime.now(timezone.utc).isoformat(), "observation": "Placeholder data due to API issues"}],
                        "wave": [
                            {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "surf": {"min": 1, "max": 3},
                                "swells": [
                                    {"height": 2, "period": 12, "direction": 310, "directionMin": 300}
                                ]
                            }
                        ],
                        "wind": [
                            {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "speed": 10,
                                "direction": 45,
                                "gust": 15
                            }
                        ]
                    },
                    "subregion": {
                        "name": region_info["description"],
                        "id": region_id
                    }
                }
                
                # Add different wave data based on region
                if region_info.get("north_facing", False):
                    placeholder_data["data"]["wave"][0]["surf"] = {"min": 3, "max": 5}
                    placeholder_data["data"]["wave"][0]["swells"][0]["direction"] = 315
                elif region_info.get("south_facing", False):
                    placeholder_data["data"]["wave"][0]["surf"] = {"min": 1, "max": 2}
                    placeholder_data["data"]["wave"][0]["swells"][0]["direction"] = 180
                
                # Save the placeholder data
                json_str = json.dumps(placeholder_data, indent=2)
                filename = f"surfline_region_{region_info['name']}.json"
                
                # Save the data
                result = await self.create_result(
                    ctx=ctx,
                    source="Surfline",
                    filename=filename,
                    data=json_str,
                    type_name="region_forecast",
                    subtype=None,
                    region="hawaii",
                    priority=1,
                    location={
                        "name": region_info["name"],
                        "description": region_info["description"]
                    },
                    north_facing=region_info.get("north_facing", False),
                    south_facing=region_info.get("south_facing", False),
                    authenticated=False,
                    region_id=region_id
                )
                
                results.append(result)
                self.logger.info(f"Created placeholder Surfline region data for {region_info['description']}")
                continue
            
            # Only run below code if not using placeholders
            url = f"https://services.surfline.com/kbyg/regions/forecasts?subregionId={region_id}&days=16"
            
            try:
                async with ctx.http_client.get(url, headers=headers if headers else None) as response:
                    if response.status == 200:
                        data = await response.text()
                        
                        try:
                            json_data = json.loads(data)
                            if "error" in json_data or ("data" not in json_data and "forecast" not in json_data):
                                self.logger.error(f"Surfline region API error for {region_info['name']}: {json_data.get('error', 'Unknown error')}")
                                continue
                        except json.JSONDecodeError:
                            self.logger.error(f"Invalid JSON response from Surfline region API for {region_info['name']}")
                            continue
                        
                        # Save the data
                        filename = f"surfline_region_{region_info['name']}.json"
                        
                        # Save the data
                        result = await self.create_result(
                            ctx=ctx,
                            source="Surfline",
                            filename=filename,
                            data=data,
                            type_name="region_forecast",
                            subtype=None,
                            region="hawaii",
                            priority=1,
                            url=url,
                            location={
                                "name": region_info["name"],
                                "description": region_info["description"]
                            },
                            north_facing=region_info.get("north_facing", False),
                            south_facing=region_info.get("south_facing", False),
                            authenticated=auth_token is not None,
                            region_id=region_id
                        )
                        
                        results.append(result)
                        self.logger.info(f"Successfully fetched Surfline region data for {region_info['description']}")
                    else:
                        self.logger.error(f"Failed to fetch Surfline region data for {region_info['name']}: HTTP {response.status}")
            except Exception as e:
                self.logger.error(f"Exception fetching Surfline region data for {region_info['name']}: {e}")
        
        # Fallback to scraping approach if direct API access failed
        if not results and not use_placeholders:
            try:
                self.logger.info("Attempting fallback to Surfline public data scraping")
                # Scrape basic forecast data from the public website
                public_url = "https://www.surfline.com/surf-report/pipeline/5842041f4e65fad6a7708864"
                
                async with ctx.http_client.get(public_url) as response:
                    if response.status == 200:
                        data = await response.text()
                        
                        # Save the data
                        filename = "surfline_public_pipeline.html"
                        
                        # Save the data
                        result = await self.create_result(
                            ctx=ctx,
                            source="Surfline",
                            filename=filename,
                            data=data,
                            type_name="public_page",
                            subtype=None,
                            region="hawaii",
                            priority=3,
                            url=public_url,
                            north_facing=True
                        )
                        
                        results.append(result)
                        self.logger.info("Successfully fetched Surfline public page data")
                    else:
                        self.logger.error(f"Failed to fetch Surfline public page: HTTP {response.status}")
            except Exception as e:
                self.logger.error(f"Failed to scrape Surfline public data: {e}")
        
        return results