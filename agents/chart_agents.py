#!/usr/bin/env python3
# agents/chart_agents.py - OPC, WPC and other chart collection agents with enhanced robustness
from __future__ import annotations
import asyncio
import logging
import re
from pathlib import Path
import utils

log = logging.getLogger("chart_agents")

# Regular expression for excluded chart types
EXCL = re.compile(r"(logo|header|thumb_|usa_gov|twitter)", re.I)

async def fetch_with_retry(ctx, session, url, max_retries=3, filename=None):
    """Fetch with retry logic and SSL fallback"""
    for attempt in range(max_retries):
        try:
            data = await ctx.fetch(session, url)
            if data and filename:
                fn = ctx.save(filename, data)
                return data, fn
            return data, None
        except Exception as e:
            if attempt == max_retries - 1:
                log.warning(f"Failed to fetch after {max_retries} attempts: {url} - {e}")
                return None, None
            log.info(f"Retry {attempt+1}/{max_retries} for {url} after error: {e}")
            await asyncio.sleep(2 ** attempt)  # Exponential backoff

async def opc(ctx, session):
    """Fetch Ocean Prediction Center charts with enhanced reliability"""
    # CRITICAL: These surface models with isobars are essential for forecasting
    # Explicit list of critical Pacific surface analysis and forecast charts
    critical_urls = [
        # Current surface analysis - most important for current conditions
        ("https://ocean.weather.gov/P_sfc_full_ocean_color.png", "opc_P_sfc_full_ocean_color.png", "pacific_surface", 1),
        ("https://ocean.weather.gov/P_w_sfc_color.png", "opc_P_w_sfc_color.png", "west_pacific_surface", 1),
        ("https://ocean.weather.gov/P_e_sfc_color.png", "opc_P_e_sfc_color.png", "east_pacific_surface", 1),

        # Surface forecasts - critical for future storm development
        ("https://ocean.weather.gov/shtml/P_24hrsfc.gif", "opc_P_24hrsfc.gif", "pacific_surface_24hr", 1),
        ("https://ocean.weather.gov/shtml/P_48hrsfc.gif", "opc_P_48hrsfc.gif", "pacific_surface_48hr", 1),
        ("https://ocean.weather.gov/shtml/P_72hrsfc.gif", "opc_P_72hrsfc.gif", "pacific_surface_72hr", 1),
        ("https://ocean.weather.gov/shtml/P_96hrsfc.gif", "opc_P_96hrsfc.gif", "pacific_surface_96hr", 1),

        # Wave height forecasts - essential for detecting significant swells
        ("https://ocean.weather.gov/shtml/P_24hrwhs.gif", "opc_P_24hrwhs.gif", "pacific_wave_height_24hr", 1),
        ("https://ocean.weather.gov/shtml/P_48hrwhs.gif", "opc_P_48hrwhs.gif", "pacific_wave_height_48hr", 1),
        ("https://ocean.weather.gov/shtml/P_72hrwhs.gif", "opc_P_72hrwhs.gif", "pacific_wave_height_72hr", 1),
        ("https://ocean.weather.gov/shtml/P_96hrwhs.gif", "opc_P_96hrwhs.gif", "pacific_wave_height_96hr", 1),  # Added 96-hour forecast

        # Wave period forecasts - critical for swell quality assessment
        ("https://ocean.weather.gov/shtml/P_24hrwper.gif", "opc_P_24hrwper.gif", "pacific_wave_period_24hr", 1),
        ("https://ocean.weather.gov/shtml/P_48hrwper.gif", "opc_P_48hrwper.gif", "pacific_wave_period_48hr", 1),
        ("https://ocean.weather.gov/shtml/P_72hrwper.gif", "opc_P_72hrwper.gif", "pacific_wave_period_72hr", 1),
        ("https://ocean.weather.gov/shtml/P_96hrwper.gif", "opc_P_96hrwper.gif", "pacific_wave_period_96hr", 1),  # Added 96-hour forecast
        
        # Wave direction forecasts - critical for determining fetch angle
        ("https://ocean.weather.gov/P_24hrwdir.gif", "opc_P_24hrwdir.gif", "pacific_wave_direction_24hr", 1),
        ("https://ocean.weather.gov/P_48hrwdir.gif", "opc_P_48hrwdir.gif", "pacific_wave_direction_48hr", 1),
        ("https://ocean.weather.gov/P_72hrwdir.gif", "opc_P_72hrwdir.gif", "pacific_wave_direction_72hr", 1),
        
        # Wind/wave charts - important for tracking developing systems
        ("https://ocean.weather.gov/P_00hrww.gif", "opc_P_00hrww.gif", "pacific_wind_wave_current", 1),
        ("https://ocean.weather.gov/P_reg_00hrww.gif", "opc_P_reg_00hrww.gif", "pacific_regional_current", 1),
        
        # Special region charts
        ("https://ocean.weather.gov/UA/overland_alaska_f024.png", "opc_overland_alaska_f024.png", "alaska_surface_24hr", 2),
    ]

    out = []

    # First fetch all the critical forecast models with isobars
    for url, filename, subtype, priority in critical_urls:
        data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
        if data and fn:
            out.append({
                "source": "OPC",
                "type": "chart",
                "subtype": subtype,
                "filename": fn,
                "url": url,
                "priority": priority,  # Priority 1 for critical surface models
                "timestamp": utils.utcnow(),
                "isobars": True,  # Flag to indicate this chart has isobars
                "north_facing": "north" in subtype.lower() or "pacific" in subtype.lower(),
                "south_facing": "south" in subtype.lower()
            })
            log.info(f"Fetched critical OPC chart: {filename}")
        else:
            log.warning(f"Failed to fetch critical OPC chart: {url}")

    # Also fetch thumbnails for preview
    thumbnail_urls = [
        ("https://ocean.weather.gov/P_024hrwper_thumb.gif", "opc_P_024hrwper_thumb.gif", "pacific_wave_period_24hr_thumb", 2),
        ("https://ocean.weather.gov/P_048hrwper_thumb.gif", "opc_P_048hrwper_thumb.gif", "pacific_wave_period_48hr_thumb", 2),
        ("https://ocean.weather.gov/P_072hrwper_thumb.gif", "opc_P_072hrwper_thumb.gif", "pacific_wave_period_72hr_thumb", 2),
        ("https://ocean.weather.gov/P_096hrwper_thumb.gif", "opc_P_096hrwper_thumb.gif", "pacific_wave_period_96hr_thumb", 2),
    ]
    
    for url, filename, subtype, priority in thumbnail_urls:
        data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
        if data and fn:
            out.append({
                "source": "OPC",
                "type": "thumbnail",
                "subtype": subtype,
                "filename": fn,
                "url": url,
                "priority": priority,
                "timestamp": utils.utcnow(),
                "north_facing": True
            })
            log.info(f"Fetched OPC thumbnail: {filename}")
        else:
            log.warning(f"Failed to fetch OPC thumbnail: {url}")

    # Then also fetch additional charts from the web page
    html, _ = await fetch_with_retry(ctx, session, "https://ocean.weather.gov/Pac_tab.php")
    if not html:
        return out  # Return what we already have if page fetch fails

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    for img in soup.find_all("img"):
        src = img.get("src", "")
        if not src or not re.search(r"\.(png|gif)$", src, re.I):
            continue

        if EXCL.search(src):
            continue

        # Skip files we've already fetched explicitly
        if any(src.endswith(Path(u[0]).name) for u in critical_urls + thumbnail_urls):
            continue

        url = "https://ocean.weather.gov/" + src.lstrip("/")
        data, fn = await fetch_with_retry(ctx, session, url, filename=f"opc_{Path(url).name}")

        if not data or len(data) < 25_000:
            continue

        out.append({
            "source": "OPC",
            "filename": fn,
            "url": url,
            "type": "chart",
            "subtype": "supplementary",
            "priority": 2,  # Lower priority for supplementary charts
            "timestamp": utils.utcnow(),
            "north_facing": True
        })
        log.info(f"Fetched supplementary OPC chart: {Path(url).name}")

    # Add satellite imagery
    satellite_urls = [
        ("https://ocean.weather.gov/nPacOff_IR_15min_latest.gif", "opc_nPacOff_IR_15min_latest.gif", "ir_satellite", 1),
    ]

    for url, filename, subtype, priority in satellite_urls:
        data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
        if data and fn:
            out.append({
                "source": "OPC",
                "type": "satellite",
                "subtype": subtype,
                "filename": fn,
                "url": url,
                "priority": priority,
                "timestamp": utils.utcnow(),
                "north_facing": True
            })
            log.info(f"Fetched OPC satellite imagery: {filename}")
        else:
            log.warning(f"Failed to fetch OPC satellite imagery: {url}")

    # Get reference charts
    reference_urls = [
        ("https://www.weather.gov/images/opc/ndfd_example.png", "opc_ndfd_example.png", "ndfd", 3),
        ("https://www.weather.gov/images/opc/radiofax_pac.png", "opc_radiofax_pac.png", "radiofax", 3),
        ("https://tgftp.nws.noaa.gov/weather/weather_gov_Epac_800px_2018-06-27.png", "opc_weather_gov_Epac_800px_2018-06-27.png", "reference", 3),
    ]

    for url, filename, subtype, priority in reference_urls:
        data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
        if data and fn:
            out.append({
                "source": "OPC",
                "type": "reference",
                "subtype": subtype,
                "filename": fn,
                "url": url,
                "priority": priority,
                "timestamp": utils.utcnow()
            })
            log.info(f"Fetched OPC reference chart: {filename}")
        else:
            log.warning(f"Failed to fetch OPC reference chart: {url}")

    return out

async def wpc(ctx, session):
    """Fetch Weather Prediction Center charts with enhanced robustness"""
    out = []
    
    # Try original URLs first
    charts = [
        ("https://tgftp.nws.noaa.gov/fax/PWFA12.TIF", "wpc_pwfa12.tif", "24h_wave"),
        ("https://tgftp.nws.noaa.gov/fax/PWFE12.TIF", "wpc_pwfe12.tif", "48h_wave"),
        ("https://tgftp.nws.noaa.gov/fax/PWFE90.TIF", "wpc_pwfe90.tif", "south_pacific_wave")  # Added South Pacific wave
    ]
    
    for url, filename, subtype in charts:
        data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
        if data and fn:
            out.append({
                "source": "WPC", 
                "filename": fn, 
                "url": url,
                "type": "forecast",
                "subtype": subtype,
                "priority": 2, 
                "timestamp": utils.utcnow(),
                "north_facing": "pacific" not in subtype.lower() and "south" not in subtype.lower(),
                "south_facing": "south" in subtype.lower()
            })
            log.info(f"Fetched WPC chart: {filename}")
        else:
            log.warning(f"Failed to fetch WPC chart: {url}")
    
    # Try alternate URL (from older collector)
    if not out:
        alt_urls = [
            ("https://tgftp.nws.noaa.gov/fax/PWFA11.TIF", "wpc_24hr.tif", "24h_surface"),
            ("https://tgftp.nws.noaa.gov/fax/PWFE11.TIF", "wpc_48hr.tif", "48h_surface"),
            ("https://tgftp.nws.noaa.gov/fax/PYFE10.TIF", "wpc_south_pacific_surface.tif", "south_pacific_surface")
        ]
        
        for url, filename, subtype in alt_urls:
            data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
            if data and fn:
                out.append({
                    "source": "WPC", 
                    "type": "surface",
                    "subtype": subtype,
                    "filename": fn, 
                    "url": url, 
                    "priority": 2, 
                    "timestamp": utils.utcnow(),
                    "north_facing": "pacific" not in subtype.lower() and "south" not in subtype.lower(),
                    "south_facing": "south" in subtype.lower()
                })
                log.info(f"Fetched alternate WPC chart: {filename}")
            else:
                log.warning(f"Failed to fetch alternate WPC chart: {url}")
    
    # Add more North Pacific charts with detailed metadata
    npac_urls = [
        ("https://tgftp.nws.noaa.gov/fax/PPAE10.gif", "npac_ppae10.gif", "surface_analysis", "current"),
        ("https://tgftp.nws.noaa.gov/fax/PPAE11.gif", "npac_ppae11.gif", "surface_forecast", "24h"),
        ("https://tgftp.nws.noaa.gov/fax/PPAE12.gif", "npac_ppae12.gif", "surface_forecast", "48h"),
        ("https://tgftp.nws.noaa.gov/fax/PPAE13.gif", "npac_ppae13.gif", "surface_forecast", "72h"),  # Added 72hr
        ("https://tgftp.nws.noaa.gov/fax/PJFA10.gif", "npac_pjfa10.gif", "wave_analysis", "current"),
        ("https://tgftp.nws.noaa.gov/fax/PJFA11.gif", "npac_pjfa11.gif", "wave_forecast", "24h"),
        ("https://tgftp.nws.noaa.gov/fax/PJFA12.gif", "npac_pjfa12.gif", "wave_forecast", "48h"),
        ("https://tgftp.nws.noaa.gov/fax/PJFA13.gif", "npac_pjfa13.gif", "wave_forecast", "72h"),  # Added 72hr
    ]
    
    for url, filename, chart_type, forecast_time in npac_urls:
        data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
        if data and fn:
            out.append({
                "source": "WPC", 
                "filename": fn, 
                "url": url,
                "type": chart_type,
                "subtype": f"npac_{forecast_time}",
                "priority": 1,
                "forecast_time": forecast_time,
                "timestamp": utils.utcnow(),
                "north_facing": True
            })
            log.info(f"Fetched North Pacific chart: {filename}")
        else:
            log.warning(f"Failed to fetch North Pacific chart: {url}")
    
    # Add experimental model products
    model_urls = [
        ("https://mag.ncep.noaa.gov/data/gfs/latest/gfs_namer_024_1000_500_thick.gif", "wpc_gfs_namer_024_thick.gif"),
        ("https://mag.ncep.noaa.gov/data/gfs/latest/gfs_namer_024_precip_mslp.gif", "wpc_gfs_namer_024_precip.gif"), 
        ("https://mag.ncep.noaa.gov/data/gfs/latest/gfs_namer_024_wv_jet.gif", "wpc_gfs_namer_024_jet.gif"),
        ("https://mag.ncep.noaa.gov/data/gfs/latest/gfs_npac_024_1000_500_thick.gif", "wpc_gfs_npac_024_thick.gif"),
        ("https://mag.ncep.noaa.gov/data/gfs/latest/gfs_npac_024_wv_jet.gif", "wpc_gfs_npac_024_jet.gif"),
    ]
    
    for url, filename in model_urls:
        data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
        if data and fn:
            out.append({
                "source": "WPC", 
                "filename": fn, 
                "url": url,
                "type": "model", 
                "subtype": Path(filename).stem,
                "priority": 2, 
                "timestamp": utils.utcnow(),
                "north_facing": "npac" in filename.lower() or "namer" in filename.lower()
            })
            log.info(f"Fetched GFS model chart: {filename}")
        else:
            log.warning(f"Failed to fetch GFS model chart: {url}")
    
    return out

async def nws(ctx, session):
    """Fetch National Weather Service forecasts and headlines with enhanced robustness"""
    out = []
    
    # Get Office Headlines
    offices = [
        ("HFO", "Hawaii"),  # Honolulu
        ("GUM", "Guam"),    # Guam - important for tracking Western Pacific
        ("AKQ", "Alaska"),  # Alaska - important for North Pacific systems
        ("SEW", "Seattle")  # Seattle - important for Eastern North Pacific
    ]
    
    for office_code, office_name in offices:
        url = f"https://api.weather.gov/offices/{office_code}/headlines"
        data, fn = await fetch_with_retry(ctx, session, url, filename=f"nws_{office_code.lower()}.json")
        if data and fn:
            out.append({
                "source": "NWS", 
                "type": "forecast_headlines",
                "office": office_name,
                "office_code": office_code,
                "filename": fn, 
                "url": url, 
                "priority": 3, 
                "timestamp": utils.utcnow(),
                "north_facing": office_code in ["HFO", "AKQ", "SEW"],
                "south_facing": office_code == "HFO"
            })
            log.info(f"Fetched NWS {office_name} headlines")
        else:
            log.warning(f"Failed to fetch NWS {office_name} headlines")
    
    # Get marine forecasts for zones with enhanced metadata
    zones = [
        # North Shore zones
        ("PHZ116", "North Shore Oahu", "north"),
        ("PHZ126", "Kauai North", "north"),
        ("PHZ136", "Maui Windward", "north"),
        ("PHZ146", "Big Island North and East", "north"),
        
        # South Shore zones
        ("PHZ117", "South Shore Oahu", "south"),
        ("PHZ127", "Kauai South", "south"),
        ("PHZ137", "Maui Leeward", "south"),
        ("PHZ147", "Big Island South", "south"),
        
        # West Shore zones (can receive both north and south swells)
        ("PHZ118", "West Shore Oahu", "west"),
        ("PHZ128", "Kauai West", "west")
    ]
    
    for zone_id, zone_name, shore_type in zones:
        url = f"https://api.weather.gov/zones/forecast/{zone_id}/forecast"
        data, fn = await fetch_with_retry(ctx, session, url, filename=f"nws_{zone_id.lower()}_forecast.json")
        if data and fn:
            out.append({
                "source": "NWS", 
                "type": "zone_forecast",
                "zone": zone_name,
                "zone_id": zone_id,
                "shore_type": shore_type,
                "filename": fn, 
                "url": url, 
                "priority": 2, 
                "timestamp": utils.utcnow(),
                "north_facing": shore_type in ["north", "west"],
                "south_facing": shore_type in ["south", "west"]
            })
            log.info(f"Fetched NWS forecast for {zone_name}")
        else:
            log.warning(f"Failed to fetch NWS forecast for {zone_name}")
    
    # Get marine zone forecasts (offshore waters)
    marine_zones = [
        ("PZZ170", "Hawaiian Waters", "offshore"),
        ("PZZ171", "Oahu to 40nm", "offshore"),
        ("PZZ175", "Hawaiian Waters North", "north_offshore"),
        ("PZZ176", "Hawaiian Waters South", "south_offshore")
    ]
    
    for zone_id, zone_name, zone_type in marine_zones:
        url = f"https://api.weather.gov/zones/forecast/{zone_id}/forecast"
        data, fn = await fetch_with_retry(ctx, session, url, filename=f"nws_{zone_id.lower()}_marine.json")
        if data and fn:
            out.append({
                "source": "NWS", 
                "type": "marine_forecast",
                "zone": zone_name,
                "zone_id": zone_id,
                "zone_type": zone_type,
                "filename": fn, 
                "url": url, 
                "priority": 1,  # Higher priority for marine forecasts
                "timestamp": utils.utcnow(),
                "north_facing": "north" in zone_type,
                "south_facing": "south" in zone_type
            })
            log.info(f"Fetched NWS marine forecast for {zone_name}")
        else:
            log.warning(f"Failed to fetch NWS marine forecast for {zone_name}")
    
    # NOTE: Pat Caldwell's forecast is no longer collected here
    # We're using weather data only to create our own Pat-style forecasts
    # This avoids reliance on subscription-based human-prepared forecasts
    log.info("Using weather data only for forecast creation (no external forecasts)")

    # Get NWS marine products for Hawaii for full technical data
    url = "https://www.weather.gov/hfo/marine"
    data, fn = await fetch_with_retry(ctx, session, url, filename="nws_hawaii_marine.html")
    if data and fn:
        out.append({
            "source": "NWS",
            "type": "marine_page",
            "filename": fn,
            "url": url,
            "priority": 1,
            "timestamp": utils.utcnow(),
            "north_facing": True,
            "south_facing": True
        })
        log.info("Fetched NWS Hawaii marine products page")
    else:
        log.warning("Failed to fetch NWS Hawaii marine products page")
    
    return out