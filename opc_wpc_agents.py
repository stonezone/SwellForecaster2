#!/usr/bin/env python3
# opc_wpc_agents.py - Agents for Ocean Prediction Center and Weather Prediction Center data

import asyncio
import logging
import re
from pathlib import Path
from datetime import datetime, timezone

# Set up logging
log = logging.getLogger("opc_wpc_agents")

async def opc(ctx, session):
    """
    Fetch Ocean Prediction Center charts with enhanced reliability.
    
    Args:
        ctx: Collection context with fetch and save methods
        session: aiohttp ClientSession for HTTP requests
        
    Returns:
        List of metadata dictionaries for collected data
    """
    # Key URLs for OPC charts
    critical_urls = [
        # Current surface analysis - most important for current conditions
        ("https://ocean.weather.gov/P_sfc_full_ocean_color.png", "opc_P_sfc_full_ocean_color.png", "pacific_surface", 1),
        
        # Surface forecasts - critical for future storm development
        ("https://ocean.weather.gov/shtml/P_24hrsfc.gif", "opc_P_24hrsfc.gif", "pacific_surface_24hr", 1),
        ("https://ocean.weather.gov/shtml/P_48hrsfc.gif", "opc_P_48hrsfc.gif", "pacific_surface_48hr", 1),
        ("https://ocean.weather.gov/shtml/P_72hrsfc.gif", "opc_P_72hrsfc.gif", "pacific_surface_72hr", 1),
        
        # Wave height forecasts - essential for detecting significant swells
        ("https://ocean.weather.gov/shtml/P_24hrwhs.gif", "opc_P_24hrwhs.gif", "pacific_wave_height_24hr", 1),
        ("https://ocean.weather.gov/shtml/P_48hrwhs.gif", "opc_P_48hrwhs.gif", "pacific_wave_height_48hr", 1),
        
        # Wave period forecasts - critical for swell quality assessment
        ("https://ocean.weather.gov/shtml/P_24hrwper.gif", "opc_P_24hrwper.gif", "pacific_wave_period_24hr", 1),
        ("https://ocean.weather.gov/shtml/P_48hrwper.gif", "opc_P_48hrwper.gif", "pacific_wave_period_48hr", 1),
        
        # Add satellite imagery
        ("https://ocean.weather.gov/nPacOff_IR_15min_latest.gif", "opc_nPacOff_IR_15min_latest.gif", "ir_satellite", 1),
    ]
    
    out = []
    
    # Fetch the critical charts with proper error handling
    for url, filename, subtype, priority in critical_urls:
        try:
            data = await ctx.fetch(session, url)
            if data:
                fn = ctx.save(filename, data)
                out.append({
                    "source": "OPC",
                    "type": "chart",
                    "subtype": subtype,
                    "filename": fn,
                    "url": url,
                    "priority": priority,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "north_facing": "north" in subtype.lower() or "pacific" in subtype.lower()
                })
                log.info(f"Fetched OPC chart: {filename}")
            else:
                log.warning(f"Failed to fetch OPC chart: {url}")
        except Exception as e:
            log.warning(f"Error fetching OPC chart {url}: {e}")
    
    return out

async def wpc(ctx, session):
    """
    Fetch Weather Prediction Center charts with enhanced robustness.
    
    Args:
        ctx: Collection context with fetch and save methods
        session: aiohttp ClientSession for HTTP requests
        
    Returns:
        List of metadata dictionaries for collected data
    """
    out = []
    
    # Try original WPC URLs first
    charts = [
        ("https://tgftp.nws.noaa.gov/fax/PWFA12.TIF", "wpc_24hr.tif", "24h_surface"),
        ("https://tgftp.nws.noaa.gov/fax/PWFE12.TIF", "wpc_48hr.tif", "48h_surface"),
        ("https://tgftp.nws.noaa.gov/fax/PWFE90.TIF", "wpc_south_pacific_surface.tif", "south_pacific_surface")
    ]
    
    for url, filename, subtype in charts:
        try:
            data = await ctx.fetch(session, url)
            if data:
                fn = ctx.save(filename, data)
                out.append({
                    "source": "WPC", 
                    "filename": fn, 
                    "url": url,
                    "type": "surface",
                    "subtype": subtype,
                    "priority": 2, 
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "north_facing": "south" not in subtype.lower(),
                    "south_facing": "south" in subtype.lower()
                })
                log.info(f"Fetched WPC chart: {filename}")
            else:
                log.warning(f"Failed to fetch WPC chart: {url}")
        except Exception as e:
            log.warning(f"Error fetching WPC chart {url}: {e}")
    
    # Add North Pacific surface analysis charts
    npac_urls = [
        ("https://tgftp.nws.noaa.gov/fax/PPAE10.gif", "npac_ppae10.gif", "surface_analysis"),
        ("https://tgftp.nws.noaa.gov/fax/PPAE11.gif", "npac_ppae11.gif", "surface_forecast_24h"),
    ]
    
    for url, filename, subtype in npac_urls:
        try:
            data = await ctx.fetch(session, url)
            if data:
                fn = ctx.save(filename, data)
                out.append({
                    "source": "WPC", 
                    "filename": fn, 
                    "url": url,
                    "type": "chart",
                    "subtype": subtype,
                    "priority": 1,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "north_facing": True
                })
                log.info(f"Fetched North Pacific chart: {filename}")
            else:
                log.warning(f"Failed to fetch North Pacific chart: {url}")
        except Exception as e:
            log.warning(f"Error fetching North Pacific chart {url}: {e}")
    
    return out

async def nws(ctx, session):
    """
    Fetch National Weather Service forecasts for Hawaii.
    
    Args:
        ctx: Collection context with fetch and save methods
        session: aiohttp ClientSession for HTTP requests
        
    Returns:
        List of metadata dictionaries for collected data
    """
    out = []
    
    # Get Hawaii forecast office headlines
    url = "https://api.weather.gov/offices/HFO/headlines"
    try:
        data = await ctx.fetch(session, url)
        if data:
            fn = ctx.save("nws_hfo.json", data)
            out.append({
                "source": "NWS", 
                "type": "forecast_headlines",
                "office": "Hawaii",
                "filename": fn, 
                "url": url, 
                "priority": 3, 
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "north_facing": True,
                "south_facing": True
            })
            log.info("Fetched NWS Hawaii headlines")
        else:
            log.warning("Failed to fetch NWS Hawaii headlines")
    except Exception as e:
        log.warning(f"Error fetching NWS Hawaii headlines: {e}")
    
    # Get Hawaii marine forecast page
    url = "https://www.weather.gov/hfo/marine"
    try:
        data = await ctx.fetch(session, url)
        if data:
            fn = ctx.save("nws_hawaii_marine.html", data)
            out.append({
                "source": "NWS",
                "type": "marine_page",
                "filename": fn,
                "url": url,
                "priority": 1,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "north_facing": True,
                "south_facing": True
            })
            log.info("Fetched NWS Hawaii marine page")
        else:
            log.warning("Failed to fetch NWS Hawaii marine page")
    except Exception as e:
        log.warning(f"Error fetching NWS Hawaii marine page: {e}")
    
    return out