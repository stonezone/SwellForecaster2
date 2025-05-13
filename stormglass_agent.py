#!/usr/bin/env python3
# stormglass_agent.py - Agent for collecting wave data from Stormglass API
import logging
import json
from datetime import datetime, timezone, timedelta

log = logging.getLogger("stormglass_agent")

async def stormglass(ctx, session):
    """Fetch wave and ocean data from Stormglass API for Hawaiian locations"""
    
    # Check if API key is available
    if "STORMGLASS_KEY" not in ctx.cfg["API"]:
        log.warning("No StormGlass API key in config, skipping")
        return []
    
    api_key = ctx.cfg["API"]["STORMGLASS_KEY"]
    if not api_key or api_key.startswith("your_"):
        log.warning("Invalid StormGlass API key, skipping")
        return []
    
    headers = {
        "Authorization": api_key
    }
    
    # Key locations around Oahu
    locations = [
        {"name": "pipeline", "lat": 21.6648, "lng": -158.0528, "north_facing": True},
        {"name": "sunset", "lat": 21.6789, "lng": -158.0354, "north_facing": True},
        {"name": "waimea", "lat": 21.6419, "lng": -158.0658, "north_facing": True},
        {"name": "ala_moana", "lat": 21.2754, "lng": -157.8463, "south_facing": True},
        {"name": "diamond_head", "lat": 21.2562, "lng": -157.8050, "south_facing": True}
    ]
    
    # Parameters to request
    params = {
        "params": ",".join([
            "waveHeight", 
            "wavePeriod", 
            "waveDirection", 
            "windSpeed", 
            "windDirection", 
            "seaLevel",
            "swellHeight",
            "swellPeriod",
            "swellDirection",
            "secondarySwellHeight", 
            "secondarySwellPeriod", 
            "secondarySwellDirection"
        ]),
        "start": (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat(),
        "end": (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
    }
    
    out = []
    
    for location in locations:
        loc_params = {**params, "lat": location["lat"], "lng": location["lng"]}
        url = "https://api.stormglass.io/v2/weather/point"
        
        try:
            data = await ctx.fetch(session, url, headers=headers, method="GET", json_body=loc_params)
            
            if data:
                fn = ctx.save(f"stormglass_{location['name']}.json", data)
                out.append({
                    "source": "StormGlass",
                    "type": "wave_data",
                    "location": location["name"],
                    "filename": fn,
                    "url": url,
                    "priority": 1,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "north_facing": location.get("north_facing", False),
                    "south_facing": location.get("south_facing", False)
                })
                log.info(f"Fetched StormGlass data for {location['name']}")
            else:
                log.warning(f"Failed to fetch StormGlass data for {location['name']}")
                
        except Exception as e:
            log.error(f"Error fetching StormGlass data for {location['name']}: {e}")
    
    return out