#!/usr/bin/env python3
# agents/buoy_agents.py - NDBC, CDIP, and other buoy data collection agents
from __future__ import annotations
import asyncio
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
import utils
from utils import clean_buoy_value, fetch_with_retry

log = logging.getLogger("buoy_agents")

async def buoys(ctx, session):
    """Enhanced NDBC buoy implementation with time series data and robust error handling"""
    # Get both standard and North Shore specific buoys
    buoy_ids = ["51001", "51002", "51101"]  # Standard buoys
    north_shore_buoys = ["51000", "51003", "51004", "51101"]  # North Pacific buoys
    cdip_buoys = ["106", "188", "098", "096"]  # CDIP buoys (Waimea, Mokapu, etc.)
    
    # Additional buoys for southern hemisphere tracking
    south_buoys = ["51002", "51004", "51S12"]  # South-facing (for South Shore)
    
    # Combine lists, removing duplicates while preserving order
    all_buoy_ids = []
    for bid in buoy_ids + north_shore_buoys + south_buoys:
        if bid not in all_buoy_ids:
            all_buoy_ids.append(bid)
    
    out = []
    
    # Process NDBC buoys with enhanced error handling
    for bid in all_buoy_ids:
        # Get recent data (latest observation)
        url = f"https://www.ndbc.noaa.gov/data/realtime2/{bid}.txt"
        try:
            d = await ctx.fetch(session, url)
            if d:
                fn = ctx.save(f"ndbc_{bid}.txt", d)
                out.append({
                    "source": "NDBC", 
                    "filename": fn, 
                    "buoy": bid,
                    "type": "realtime", 
                    "priority": 0, 
                    "timestamp": utils.utcnow(),
                    "south_facing": bid in south_buoys,
                    "north_facing": bid in north_shore_buoys
                })
                log.debug(f"Successfully fetched NDBC buoy {bid}")
            else:
                log.warning(f"Failed to fetch NDBC buoy {bid} - no data returned")
        except Exception as e:
            log.warning(f"Failed to fetch NDBC buoy {bid}: {e}")
            
        # Get spectral data if available
        try:
            spec_url = f"https://www.ndbc.noaa.gov/data/realtime2/{bid}.spec"
            d = await ctx.fetch(session, spec_url)
            if d:
                fn = ctx.save(f"ndbc_{bid}_spec.txt", d)
                out.append({
                    "source": "NDBC", 
                    "filename": fn, 
                    "buoy": bid,
                    "type": "spectral", 
                    "priority": 1, 
                    "timestamp": utils.utcnow(),
                    "south_facing": bid in south_buoys,
                    "north_facing": bid in north_shore_buoys
                })
                log.debug(f"Successfully fetched NDBC spectral data for buoy {bid}")
            else:
                log.debug(f"Failed to fetch NDBC spectral data for buoy {bid} - no data returned")
        except Exception as e:
            log.debug(f"Failed to fetch NDBC spectral data for buoy {bid}: {e}")

    # Process CDIP buoys with enhanced handling of data formats
    for cdip_id in cdip_buoys:
        # Get recent data (latest observation)
        url = f"https://cdip.ucsd.edu/data_access/rest/buoy_data/{cdip_id}/latest"
        try:
            d = await ctx.fetch(session, url)
            if d:
                fn = ctx.save(f"cdip_{cdip_id}.json", d)
                out.append({
                    "source": "CDIP", 
                    "filename": fn, 
                    "buoy": cdip_id,
                    "type": "realtime", 
                    "priority": 0, 
                    "timestamp": utils.utcnow()
                })
                log.debug(f"Successfully fetched CDIP buoy {cdip_id}")
            else:
                log.warning(f"Failed to fetch CDIP buoy {cdip_id} - no data returned")
        except Exception as e:
            log.warning(f"Failed to fetch CDIP buoy {cdip_id}: {e}")
    
    return out

async def noaa_coops(ctx, session):
    """NOAA CO-OPS tide and current stations in Hawaii"""
    # Stations:
    # 1612340 = Honolulu
    # 1612480 = Mokuoloe (Kaneohe Bay)
    # 1615680 = Kahului Harbor, Maui
    out = []
    
    stations = {
        "1612340": "Honolulu",
        "1612480": "Kaneohe"
    }
    
    products = {
        "wind": {"name": "Wind", "product": "wind", "interval": "hourly", "priority": 1},
        "water_temp": {"name": "Water Temperature", "product": "water_temperature", "interval": "hourly", "priority": 2}
    }
    
    for station_id, station_name in stations.items():
        for product_id, product_info in products.items():
            url = (f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?"
                   f"station={station_id}&product={product_info['product']}&"
                   f"date=latest&units=english&time_zone=gmt&"
                   f"interval={product_info['interval']}&format=json&application=SwellForecaster")
            
            data, fn = await fetch_with_retry(ctx, session, url, 
                                            filename=f"coops_{station_name.lower()}_{product_id}.json")
            
            if data and fn:
                out.append({
                    "source": "NOAA CO-OPS",
                    "station": station_id,
                    "station_name": station_name,
                    "type": product_id,
                    "filename": fn,
                    "url": url,
                    "priority": product_info["priority"],
                    "timestamp": utils.utcnow()
                })
                log.info(f"Successfully fetched NOAA CO-OPS {product_info['name']} for {station_name}")
            else:
                log.warning(f"Failed to fetch NOAA CO-OPS {product_info['name']} for {station_name}")
    
    return out

async def noaa_ndfd(ctx, session):
    """NOAA NDFD winds and other marine forecast data"""
    out = []
    
    # NDFD endpoints for Hawaii marine zones
    url_base = "https://digital.weather.gov/rest/ndfd/marine"
    
    # Marine weather elements
    elements = {
        "wind_speed": "wspd",
        "wind_dir": "wdir", 
        "wave_height": "waveh",
        "wind_gust": "gust"
    }
    
    # Marine zones for Hawaii
    areas = {
        "north_shore": "OPZ150",
        "south_shore": "OPZ180", 
        "windward": "OPZ130"
    }
    
    # Try to get each element for each area
    for area_name, area_code in areas.items():
        for element_name, element_code in elements.items():
            url = f"{url_base}/{area_code}/{element_code}"
            
            data, fn = await fetch_with_retry(ctx, session, url, 
                                            filename=f"ndfd_{area_name}_{element_name}.xml")
            
            if data and fn:
                out.append({
                    "source": "NOAA NDFD",
                    "area": area_name,
                    "type": element_name,
                    "filename": fn,
                    "url": url,
                    "priority": 2,
                    "timestamp": utils.utcnow()
                })
                log.info(f"Successfully fetched NOAA NDFD {element_name} for {area_name}")
            else:
                log.warning(f"Failed to fetch NOAA NDFD {element_name} for {area_name}")
    
    return out