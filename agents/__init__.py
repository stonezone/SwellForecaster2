"""
Agent modules for SwellForecaster data collection.

Each agent follows a common interface pattern:
- async def agent_name(ctx, session) -> List[dict]

Where:
- ctx: A context object that provides fetch(), save(), and cfg access
- session: An aiohttp.ClientSession for HTTP requests
- Return value: A list of metadata dictionaries for collected data

Common metadata fields:
- source: Source name (e.g., "NDBC", "Windy")
- type: Data type (e.g., "chart", "forecast", "realtime")
- filename: Filename saved in the bundle
- priority: Priority level (lower = higher priority)
- timestamp: ISO-8601 timestamp
- Optional geographic tags:
  - south_facing: True for South Shore relevant data
  - north_facing: True for North Shore relevant data

"""

# Structure imports for easier access
from agents.buoy_agents import buoys, noaa_coops
from agents.chart_agents import opc, wpc, nws
from agents.model_agents import (
    pacioos, pacioos_swan, ww3_model_fallback, ecmwf_wave,
    model_agent, ecmwf_agent
)
from agents.api_agents import windy, open_meteo, stormglass, surfline
from agents.region_agents import southern_hemisphere, north_pacific_enhanced

# Group agents by category for easier management
BUOY_AGENTS = [buoys, noaa_coops]
CHART_AGENTS = [opc, wpc, nws]
MODEL_AGENTS = [
    model_agent,        # Primary WW3 model agent (integrated from models.py)
    pacioos,            # PacIOOS wave model
    pacioos_swan,       # PacIOOS SWAN nearshore model
    ww3_model_fallback, # Fallback for WaveWatch III
    ecmwf_wave,         # Enhanced ECMWF wave agent
    ecmwf_agent         # Standard ECMWF agent (integrated from models.py)
]
API_AGENTS = [windy, open_meteo, stormglass, surfline]
REGION_AGENTS = [southern_hemisphere, north_pacific_enhanced]

# All available agents
ALL_AGENTS = BUOY_AGENTS + CHART_AGENTS + MODEL_AGENTS + API_AGENTS + REGION_AGENTS