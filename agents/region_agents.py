#!/usr/bin/env python3
# agents/region_agents.py - Regional data collection agents with enhanced pattern matching
from __future__ import annotations
import asyncio
import logging
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
import utils

log = logging.getLogger("region_agents")

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

async def southern_hemisphere(ctx, session):
    """Track Southern Hemisphere storm systems for south swell forecasting"""
    out = []

    # Southern Ocean and South Pacific data sources - ENHANCED for better detection
    south_sources = [
        # Australian Bureau of Meteorology - Southern Ocean Analysis
        ("https://tgftp.nws.noaa.gov/fax/PYFE10.gif", "noaa_south_pacific_surface.gif", "surface_analysis"),
        ("https://tgftp.nws.noaa.gov/fax/PWFA11.gif", "noaa_south_pacific_wave_24h.gif", "wave_24h"),
        ("https://tgftp.nws.noaa.gov/fax/PWFE11.gif", "noaa_south_pacific_wave_48h.gif", "wave_48h"),
        ("https://tgftp.nws.noaa.gov/fax/PWFE12.gif", "noaa_south_pacific_wave_72h.gif", "wave_72h"),  # Added 72hr forecast

        # South Pacific Wave Height Analysis - critical for south swell detection
        ("https://tgftp.nws.noaa.gov/fax/PJFA90.gif", "south_pacific_wave_analysis.gif", "wave_analysis"),

        # NOAA South Pacific Streamlines and SST data - updated URL
        ("https://www.ospo.noaa.gov/data/sst/contour/global.c.gif", "south_pacific_sst.jpg", "sst"),
        # Alternative SST source
        ("https://www.ospo.noaa.gov/data/sst/contour/southpac.c.gif", "south_pacific_sst_alt.jpg", "sst_alt"),

        # New Zealand MetService - excellent for tracking Southern Ocean storms
        ("https://www.metservice.com/publicData/marineSST", "nz_metservice_sst.png", "nz_sst"),
        ("https://www.metservice.com/publicData/marineSfc1", "nz_metservice_surface_1.png", "nz_surface_1"),
        ("https://www.metservice.com/publicData/marineSfc2", "nz_metservice_surface_2.png", "nz_surface_2"),
        ("https://www.metservice.com/publicData/marineSfc3", "nz_metservice_surface_3.png", "nz_surface_3"),
        ("https://www.metservice.com/publicData/marineSfc4", "nz_metservice_surface_4.png", "nz_surface_4"),  # Added 4th day

        # Australia BOM - essential for tracking systems near New Zealand
        ("http://www.bom.gov.au/marine/wind.shtml?unit=p0&location=wa1&tz=AEDT", "bom_australia_wind.html", "australia_wind"),
        ("http://www.bom.gov.au/australia/charts/synoptic_col.shtml", "bom_australia_charts.html", "australia_charts"),

        # Global wave models - great for tracking South Pacific swell patterns
        ("https://polar.ncep.noaa.gov/waves/WEB/gfswave.latest/plots/Global.small/Global.small.whs_withsnow.jpeg", "global_wave_heights.jpg", "global_waves"),
        ("https://polar.ncep.noaa.gov/waves/WEB/gfswave.latest/plots/Global.small/Global.small.per_withsnow.jpeg", "global_wave_period.jpg", "global_period"),  # Added wave period

        # Satellite imagery - for visually tracking South Pacific storms
        ("https://www.goes.noaa.gov/dml/south/nhem/ssa/vis.jpg", "south_pacific_vis.jpg", "satellite_vis"),
        ("https://www.oceanweather.com/data/SPAC-WAM/WAVE.GIF", "oceanweather_south_pacific.gif", "oceanweather"),

        # Wave period forecasts - critical for south swell quality
        ("https://www.surf-forecast.com/maps/New-Zealand/six_day/wave-energy-period", "nz_wave_period.jpg", "wave_period"),
        ("https://www.surf-forecast.com/maps/New-Zealand/six_day/swell-height", "nz_swell_height.jpg", "swell_height"),

        # Additional Southern Ocean Storm Tracks - Nullschool visualization
        ("https://earth.nullschool.net/#current/ocean/primary/waves/overlay=primary_waves/orthographic=-180,-30,293", "nullschool_southern_ocean.png", "nullschool"),
    ]

    # Fetch standard Southern Hemisphere charts with enhanced error handling
    for url, filename, subtype in south_sources:
        try:
            data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
            if data and fn:
                out.append({
                    "source": "SouthernHemisphere",
                    "type": "chart",
                    "subtype": subtype,
                    "filename": fn,
                    "url": url,
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "south_facing": True
                })
                log.info(f"Successfully fetched Southern Hemisphere data: {subtype}")
        except Exception as e:
            log.warning(f"Failed to fetch Southern Hemisphere data from {url}: {e}")

    # Surfline South Pacific Swell tracking - critical for south swell detection
    surfline_url = "https://services.surfline.com/kbyg/regions/south-pacific?subregionId=58581a836630e24c44878fd3"
    try:
        data, fn = await fetch_with_retry(ctx, session, surfline_url, filename="surfline_south_pacific.json")
        if data and fn:
            out.append({
                "source": "SouthernHemisphere",
                "type": "forecast",
                "provider": "surfline",
                "filename": fn,
                "url": surfline_url,
                "priority": 2,
                "timestamp": utils.utcnow(),
                "south_facing": True
            })
    except Exception as e:
        log.warning(f"Failed to fetch Surfline data: {e}")

    # Surfline individual spots for South Shore (multiple spots for better coverage)
    spot_ids = {
        "5842041f4e65fad6a7708890": "ala_moana",
        "5842041f4e65fad6a7708923": "waikiki",
        "5842041f4e65fad6a770889f": "diamond_head"
    }
    
    for spot_id, spot_name in spot_ids.items():
        spot_url = f"https://services.surfline.com/kbyg/spots/forecasts?spotId={spot_id}&days=16"
        try:
            data, fn = await fetch_with_retry(ctx, session, spot_url, filename=f"surfline_{spot_name}.json")
            if data and fn:
                out.append({
                    "source": "SouthernHemisphere",
                    "type": "forecast",
                    "provider": "surfline_spot",
                    "spot_name": spot_name,
                    "filename": fn,
                    "url": spot_url,
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "south_facing": True
                })
        except Exception as e:
            log.warning(f"Failed to fetch Surfline spot data for {spot_name}: {e}")

    # Surf News Network - text forecast for South Shore
    snn_url = "https://www.surfnewsnetwork.com/forecast/"
    try:
        data, fn = await fetch_with_retry(ctx, session, snn_url, filename="snn_forecast.html")
        if data and fn:
            out.append({
                "source": "SouthernHemisphere",
                "type": "text_forecast",
                "provider": "snn",
                "filename": fn,
                "url": snn_url,
                "priority": 2,
                "timestamp": utils.utcnow(),
                "south_facing": True
            })
    except Exception as e:
        log.warning(f"Failed to fetch SNN forecast: {e}")

    # NOTE: We no longer collect Pat Caldwell's forecast
    # Instead, we'll use similar pattern matching techniques to extract data from
    # various weather sources for our own Pat-style forecast generation

    # Enhanced patterns for detecting South Pacific swell events from various data sources
    # We'll save these pattern templates for the analyzer to use later
    south_patterns_template = {
        "swell_patterns": [
            # Similar patterns to what we used for Caldwell's forecast, but now applied to
            # other data sources like Surfline, SNN, MagicSeaweed, etc.
            r"(\d+\.?\d?)(?:-(\d+\.?\d?))?\s*(?:ft|foot|feet).*?(?:SSW|S|SW|South|South-southwest|Southwest|Southerly).*?(\d+)(?:-(\d+))?\s*(?:s(?:ec)?|second)",
            r"(?:SSW|S|SW|South|South-southwest|Southwest|Southerly).*?(\d+\.?\d?)(?:-(\d+\.?\d?))?\s*(?:ft|foot|feet).*?(\d+)(?:-(\d+))?\s*(?:s(?:ec)?|second)",
            r"(\d+\.?\d?)\s+((?:SSW|S|SW|SSE|SE))\s+(\d+)(?:\s+\d+\s+\d+)?",
            r"(?:peaking|building).*?(\d+\/\d+).*?(?:SSW|S|SW|South|South-southwest|Southwest|Southerly)",
        ],
        "fetch_patterns": [
            r"(?:Classic|Favorable|captured|fetch)(?:[^.]*?)(?:New Zealand|NZ|Tasman)(?:[^.]*?)(?:low|storm|gale)",
            r"(?:Low|Storm|Gale)(?:[^.]*?)(?:east|west|south|north)(?:[^.]*?)(?:New Zealand|NZ|Tasmania|Australia)",
            r"(?:ASCAT|JASON)(?:[^.]*?)(?:validated|confirmed|showed)(?:[^.]*?)(?:winds|seas)(?:[^.]*?)(\d+)(?:\'|\-foot|ft)"
        ],
        "direction_patterns": [
            r"(\d+)(?:-(\d+))?\s*degrees?",
            r"from\s+(\d+)(?:-(\d+))?\s*degrees?"
        ],
        "caldwell_style_extraction": {
            "description": "Pattern templates for extracting swell information from various sources",
            "usage": "Apply these patterns to text sources to extract Pat Caldwell-style swell data",
            "direction_mapping": {
                "S": "180°",
                "SSW": "190-200°",
                "SW": "225°",
                "SSE": "160-170°",
                "SE": "135°"
            }
        }
    }

    # Save the pattern templates to use in forecast generation
    fn = ctx.save("south_swell_patterns.json", json.dumps(south_patterns_template))
    out.append({
        "source": "SouthernHemisphere",
        "type": "pattern_template",
        "filename": fn,
        "priority": 2,
        "timestamp": utils.utcnow(),
        "south_facing": True
    })

    # Add Surfline South Pacific region forecast
    surfline_south_pacific_url = "https://www.surfline.com/surf-forecasts/south-pacific/3679"
    try:
        data, fn = await fetch_with_retry(ctx, session, surfline_south_pacific_url, filename="surfline_south_pacific_forecast.html")
        if data and fn:
            out.append({
                "source": "SouthernHemisphere",
                "type": "text_forecast",
                "provider": "surfline_region",
                "filename": fn,
                "url": surfline_south_pacific_url,
                "priority": 2,
                "timestamp": utils.utcnow(),
                "south_facing": True
            })
    except Exception as e:
        log.warning(f"Failed to fetch Surfline South Pacific forecast: {e}")

    # Add MagicSeaweed South Pacific forecast
    magicseaweed_url = "https://magicseaweed.com/Southern-Pacific-Ocean-Surf-Forecast/10/"
    try:
        data, fn = await fetch_with_retry(ctx, session, magicseaweed_url, filename="magicseaweed_south_pacific.html")
        if data and fn:
            out.append({
                "source": "SouthernHemisphere",
                "type": "text_forecast",
                "provider": "magicseaweed",
                "filename": fn,
                "url": magicseaweed_url,
                "priority": 2,
                "timestamp": utils.utcnow(),
                "south_facing": True
            })
    except Exception as e:
        log.warning(f"Failed to fetch MagicSeaweed South Pacific forecast: {e}")

    # Historical South Pacific storm analogs for current date period
    # This helps identify seasonal patterns by comparing with past years
    current_month = datetime.now().month
    current_day = datetime.now().day

    # Calculate dates for previous years in same season
    date_analogs = []
    for year in range(2020, 2025):  # Look at last 5 years for better historical context
        analog_date = datetime(year, current_month, current_day).strftime("%Y-%m-%d")
        date_analogs.append(analog_date)

        # Also check +/- 7 days for better coverage
        for offset in [-14, -7, 7, 14]:  # Extended window
            offset_date = (datetime(year, current_month, current_day) + timedelta(days=offset)).strftime("%Y-%m-%d")
            date_analogs.append(offset_date)

    # Enhanced historical information
    historical_data = {
        "current_date": datetime.now().strftime("%Y-%m-%d"),
        "analogs": date_analogs,
        "notes": "Historical analogs for South Pacific storm patterns",
        "seasonal_context": {
            "summer": current_month in [5, 6, 7, 8, 9, 10],
            "winter": current_month in [11, 12, 1, 2, 3, 4],
            "peak_south_swell": current_month in [6, 7, 8],
            "peak_north_swell": current_month in [12, 1, 2]
        },
        "reference_events": [
            {"year": 2020, "month": 7, "description": "XL July South swell"},
            {"year": 2019, "month": 6, "description": "Fiji Pro swell"},
            {"year": 2018, "month": 5, "description": "Early season SSW"}
        ]
    }

    fn = ctx.save("southern_hemisphere_analogs.json", json.dumps(historical_data))
    out.append({
        "source": "SouthernHemisphere",
        "type": "historical_analogs",
        "filename": fn,
        "priority": 3,
        "timestamp": utils.utcnow(),
        "south_facing": True
    })

    return out

async def north_pacific_enhanced(ctx, session):
    """Enhanced North Pacific data collection for detailed storm tracking"""
    out = []
    
    # FNMOC wave model charts
    fnmoc_urls = [
        ("https://www.fnmoc.navy.mil/wxmap_cgi/cgi-bin/wxmap_single.cgi?area=npac_swh&dtg=current&type=gift", "fnmoc_npac_wave_height.gif"),
        ("https://www.fnmoc.navy.mil/wxmap_cgi/cgi-bin/wxmap_single.cgi?area=npac_wind&dtg=current&type=gift", "fnmoc_npac_wind.gif"),
        ("https://www.fnmoc.navy.mil/wxmap_cgi/cgi-bin/wxmap_single.cgi?area=npac_mslp&dtg=current&type=gift", "fnmoc_npac_pressure.gif"),
    ]
    
    for url, filename in fnmoc_urls:
        try:
            data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
            if data and fn:
                out.append({
                    "source": "NorthPacific",
                    "type": "chart",
                    "subtype": Path(filename).stem,
                    "filename": fn,
                    "url": url,
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "north_facing": True
                })
        except Exception as e:
            log.warning(f"Failed to fetch FNMOC data from {url}: {e}")
    
    # NOAA OPC North Pacific Sea State Analysis - ENHANCED with forecast hours
    opc_urls = [
        # Current analysis
        ("https://ocean.weather.gov/grids/images/neast_latest.gif", "opc_npac_seastate_latest.gif", "latest"),
        # Forecast hours with better naming
        ("https://ocean.weather.gov/grids/images/neast_024.gif", "opc_npac_seastate_24h.gif", "24h"),
        ("https://ocean.weather.gov/grids/images/neast_048.gif", "opc_npac_seastate_48h.gif", "48h"),
        ("https://ocean.weather.gov/grids/images/neast_072.gif", "opc_npac_seastate_72h.gif", "72h"),
        ("https://ocean.weather.gov/grids/images/neast_096.gif", "opc_npac_seastate_96h.gif", "96h"),  # Added 96hr
    ]
    
    for url, filename, hours in opc_urls:
        try:
            data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
            if data and fn:
                out.append({
                    "source": "NorthPacific",
                    "type": "seastate",
                    "subtype": hours,
                    "filename": fn,
                    "url": url,
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "north_facing": True,
                    "forecast_hours": hours
                })
        except Exception as e:
            log.warning(f"Failed to fetch OPC data from {url}: {e}")
    
    # OPC Surface Analysis charts - Critical for storm tracking
    opc_surface_urls = [
        ("https://ocean.weather.gov/unified_analysis/P_sfc_full_ocean.png", "opc_P_sfc_full_ocean_color.png", "current"),
        ("https://ocean.weather.gov/unified_analysis/noaa_p_w_sfc_color.png", "opc_P_w_sfc_color.png", "west"),
        ("https://ocean.weather.gov/unified_analysis/noaa_p_e_sfc_color.png", "opc_P_e_sfc_color.png", "east"),
    ]
    
    for url, filename, region in opc_surface_urls:
        try:
            data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
            if data and fn:
                out.append({
                    "source": "NorthPacific",
                    "type": "surface_analysis",
                    "subtype": region,
                    "filename": fn,
                    "url": url,
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "north_facing": True
                })
        except Exception as e:
            log.warning(f"Failed to fetch OPC surface data from {url}: {e}")
    
    # OPC Wind/Wave analysis - critical for understanding fetch
    opc_ww_urls = [
        ("https://ocean.weather.gov/P_00hrww.gif", "opc_P_00hrww.gif", 0),
        ("https://ocean.weather.gov/P_24hrww.gif", "opc_P_24hrww.gif", 24),
        ("https://ocean.weather.gov/P_48hrww.gif", "opc_P_48hrww.gif", 48),
        ("https://ocean.weather.gov/P_72hrww.gif", "opc_P_72hrww.gif", 72),
    ]
    
    for url, filename, hours in opc_ww_urls:
        try:
            data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
            if data and fn:
                out.append({
                    "source": "NorthPacific",
                    "type": "wind_wave",
                    "subtype": f"{hours}hr",
                    "filename": fn,
                    "url": url,
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "north_facing": True,
                    "forecast_hours": hours
                })
        except Exception as e:
            log.warning(f"Failed to fetch OPC wind/wave data from {url}: {e}")
    
    # OPC Wave Period Forecasts - critical for swell quality
    opc_period_urls = [
        ("https://ocean.weather.gov/P_24hrwper.gif", "opc_P_24hrwper.gif", 24),
        ("https://ocean.weather.gov/P_48hrwper.gif", "opc_P_48hrwper.gif", 48),
        ("https://ocean.weather.gov/P_72hrwper.gif", "opc_P_72hrwper.gif", 72),
        ("https://ocean.weather.gov/P_96hrwper.gif", "opc_P_96hrwper.gif", 96)
    ]
    
    for url, filename, hours in opc_period_urls:
        try:
            data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
            if data and fn:
                out.append({
                    "source": "NorthPacific",
                    "type": "wave_period",
                    "subtype": f"{hours}hr",
                    "filename": fn,
                    "url": url,
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "north_facing": True,
                    "forecast_hours": hours
                })
        except Exception as e:
            log.warning(f"Failed to fetch OPC period data from {url}: {e}")
    
    # Also fetch preview thumbnails for quick analysis
    thumbnail_urls = [
        ("https://ocean.weather.gov/P_024hrwper_thumb.gif", "opc_P_024hrwper_thumb.gif", 24),
        ("https://ocean.weather.gov/P_048hrwper_thumb.gif", "opc_P_048hrwper_thumb.gif", 48),
        ("https://ocean.weather.gov/P_072hrwper_thumb.gif", "opc_P_072hrwper_thumb.gif", 72),
        ("https://ocean.weather.gov/P_096hrwper_thumb.gif", "opc_P_096hrwper_thumb.gif", 96)
    ]
    
    for url, filename, hours in thumbnail_urls:
        try:
            data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
            if data and fn:
                out.append({
                    "source": "NorthPacific",
                    "type": "thumbnail",
                    "subtype": f"{hours}hr",
                    "filename": fn,
                    "url": url,
                    "priority": 2,
                    "timestamp": utils.utcnow(),
                    "north_facing": True,
                    "forecast_hours": hours
                })
        except Exception as e:
            log.warning(f"Failed to fetch OPC thumbnail from {url}: {e}")
    
    # Enhanced Regional analysis
    region_urls = [
        ("https://ocean.weather.gov/P_reg_00hrww.gif", "opc_P_reg_00hrww.gif", "regional")
    ]
    
    for url, filename, subtype in region_urls:
        try:
            data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
            if data and fn:
                out.append({
                    "source": "NorthPacific",
                    "type": "regional",
                    "subtype": subtype,
                    "filename": fn,
                    "url": url,
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "north_facing": True
                })
        except Exception as e:
            log.warning(f"Failed to fetch regional data from {url}: {e}")
    
    # IR Satellite for tracking storm systems
    sat_urls = [
        ("https://ocean.weather.gov/nPacOff_IR_15min_latest.gif", "opc_nPacOff_IR_15min_latest.gif", "ir_satellite")
    ]
    
    for url, filename, subtype in sat_urls:
        try:
            data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
            if data and fn:
                out.append({
                    "source": "NorthPacific",
                    "type": "satellite",
                    "subtype": subtype,
                    "filename": fn,
                    "url": url,
                    "priority": 1,
                    "timestamp": utils.utcnow(),
                    "north_facing": True
                })
        except Exception as e:
            log.warning(f"Failed to fetch satellite data from {url}: {e}")
    
    # NWS overland analysis - useful for tracking systems as they approach Alaska
    nws_urls = [
        ("https://ocean.weather.gov/UA/overland_alaska_f012.png", "opc_overland_alaska_f012.png", 12),
        ("https://ocean.weather.gov/UA/overland_alaska_f024.png", "opc_overland_alaska_f024.png", 24),
        ("https://ocean.weather.gov/UA/overland_alaska_f036.png", "opc_overland_alaska_f036.png", 36)
    ]
    
    for url, filename, hours in nws_urls:
        try:
            data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
            if data and fn:
                out.append({
                    "source": "NorthPacific",
                    "type": "alaska",
                    "subtype": f"{hours}hr",
                    "filename": fn,
                    "url": url,
                    "priority": 2,
                    "timestamp": utils.utcnow(),
                    "north_facing": True,
                    "forecast_hours": hours
                })
        except Exception as e:
            log.warning(f"Failed to fetch Alaska data from {url}: {e}")
    
    # NDFD Example - for reference
    try:
        url = "https://www.weather.gov/images/opc/ndfd_example.png"
        data, fn = await fetch_with_retry(ctx, session, url, filename="opc_ndfd_example.png")
        if data and fn:
            out.append({
                "source": "NorthPacific",
                "type": "reference",
                "subtype": "ndfd",
                "filename": fn,
                "url": url,
                "priority": 3,
                "timestamp": utils.utcnow(),
                "north_facing": True
            })
    except Exception as e:
        log.warning(f"Failed to fetch NDFD reference from {url}: {e}")
    
    # RadioFax map - for reference
    try:
        url = "https://www.weather.gov/images/opc/radiofax_pac.png"
        data, fn = await fetch_with_retry(ctx, session, url, filename="opc_radiofax_pac.png")
        if data and fn:
            out.append({
                "source": "NorthPacific",
                "type": "reference",
                "subtype": "radiofax",
                "filename": fn,
                "url": url,
                "priority": 3,
                "timestamp": utils.utcnow(),
                "north_facing": True
            })
    except Exception as e:
        log.warning(f"Failed to fetch RadioFax reference from {url}: {e}")
    
    # Historical analog data - enhanced with more detailed information
    historical_data = {
        "analogs": [
            {
                "date": "2020-12-02", 
                "storm_location": "Off Kurils", 
                "fetch_direction": 310,
                "central_pressure": 968,
                "fetch_width_nm": 500,
                "fetch_duration_hours": 36,
                "max_winds_kts": 50,
                "max_seas_ft": 45,
                "hawaii_impact": "12-15 ft faces North Shore",
                "travel_time_days": 3.5,
                "similar_events": ["2018-01-15", "2016-12-20"]
            },
            {
                "date": "2019-01-15", 
                "storm_location": "Date Line", 
                "fetch_direction": 320,
                "central_pressure": 972,
                "fetch_width_nm": 600,
                "fetch_duration_hours": 48,
                "max_winds_kts": 55,
                "max_seas_ft": 40,
                "hawaii_impact": "15-20 ft faces North Shore",
                "travel_time_days": 2.8,
                "similar_events": ["2017-01-22", "2015-01-18"]
            },
            {
                "date": "2020-05-07",
                "storm_location": "Kurils to Date Line/Aleutians",
                "fetch_direction": 315,
                "central_pressure": 980,
                "fetch_width_nm": 300,
                "fetch_duration_hours": 24,
                "max_winds_kts": 40,
                "max_seas_ft": 25,
                "hawaii_impact": "6-8 ft faces North Shore, rare May event",
                "travel_time_days": 4.0,
                "similar_events": ["2018-05-10", "2016-05-15"]
            }
        ],
        "seasonal_context": {
            "current_month": datetime.now().month,
            "typical_direction": "NW" if datetime.now().month in [11, 12, 1, 2] else "NNW" if datetime.now().month in [3, 4, 10] else "SW",
            "typical_period": 14 if datetime.now().month in [11, 12, 1, 2] else 12,
            "typical_size": "Large" if datetime.now().month in [12, 1, 2] else "Medium" if datetime.now().month in [11, 3] else "Small"
        }
    }
    
    if historical_data:
        fn = ctx.save("historical_analogs.json", json.dumps(historical_data))
        out.append({
            "source": "NorthPacific",
            "type": "historical",
            "filename": fn,
            "priority": 3,
            "timestamp": utils.utcnow(),
            "north_facing": True
        })
    
    # North Pacific WPC charts
    wpc_urls = [
        ("https://mag.ncep.noaa.gov/data/gfs/latest/gfs_namer_024_1000_500_thick.gif", "npac_ppae10.gif"),
        ("https://mag.ncep.noaa.gov/data/gfs/latest/gfs_namer_024_precip_mslp.gif", "npac_ppae11.gif")
    ]
    
    for url, filename in wpc_urls:
        try:
            data, fn = await fetch_with_retry(ctx, session, url, filename=filename)
            if data and fn:
                out.append({
                    "source": "NorthPacific",
                    "type": "wpc",
                    "subtype": Path(filename).stem,
                    "filename": fn,
                    "url": url,
                    "priority": 2,
                    "timestamp": utils.utcnow(),
                    "north_facing": True
                })
        except Exception as e:
            log.warning(f"Failed to fetch WPC data from {url}: {e}")
    
    return out