"""Wave analysis utilities for SwellForecaster."""
import json
import logging
import math
import os
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Union, Any, Tuple

logger = logging.getLogger("utils.wave_analysis")


def get_south_swell_status() -> float:
    """
    Get the current south swell seasonal status as a probability.
    
    Returns:
        Probability of south swell activity based on time of year (0.0-1.0)
    """
    # Get current date
    now = datetime.now(timezone.utc)
    month = now.month
    day = now.day
    
    # South swell season logic
    # Peaks in June-August, transitions in Apr-May and Sep-Oct, minimal in Nov-Mar
    if month in [6, 7, 8]:
        # Peak south swell season
        day_offset = day / 30  # Normalize day within month
        
        # Calculate seasonal curve - peaks in mid-July
        if month == 6:
            # June: ramping up from 0.7 to 0.9
            return 0.7 + (0.2 * day_offset)
        elif month == 7:
            # July: peak at 0.9-1.0
            return 0.9 + (0.1 * day_offset) if day < 15 else 1.0 - (0.1 * (day - 15) / 15)
        else:  # August
            # August: falling from 0.9 to 0.7
            return 0.9 - (0.2 * day_offset)
    
    elif month in [4, 5, 9, 10]:
        # Transition months
        day_offset = day / 30  # Normalize day within month
        
        if month == 4:
            # April: beginning of season, 0.3 to 0.5
            return 0.3 + (0.2 * day_offset)
        elif month == 5:
            # May: increasing from 0.5 to 0.7
            return 0.5 + (0.2 * day_offset)
        elif month == 9:
            # September: decreasing from 0.7 to 0.5
            return 0.7 - (0.2 * day_offset)
        else:  # October
            # October: end of season, 0.5 to 0.3
            return 0.5 - (0.2 * day_offset)
    
    else:
        # November through March: minimal south swell activity
        # Small sliding scale from 0.3 down to 0.1 and back up
        if month == 11:
            return 0.3 - (0.1 * day / 30)
        elif month == 12:
            return 0.2 - (0.1 * day / 31)
        elif month == 1:
            return 0.1
        elif month == 2:
            return 0.1 + (0.1 * day / 28)
        else:  # March
            return 0.2 + (0.1 * day / 31)


def get_period_multiplier(period: float) -> float:
    """
    Get multiplier for wave face height based on wave period.
    
    Args:
        period: Wave period in seconds
        
    Returns:
        Multiplier for wave face height
    """
    # Standard period-to-face height multipliers
    period_multipliers = {
        # seconds: multiplier (approximate face height = swell height * multiplier)
        8: 0.7,    # Wind swell, minimal shoaling
        9: 0.8,    # Short-period swell
        10: 0.9,   # Moderate period
        11: 1.0,   # Moderate-long period
        12: 1.1,   # Getting into ground swell territory
        13: 1.2,   # Long period ground swell
        14: 1.35,  # Long period ground swell
        15: 1.45,  # Very long period
        16: 1.55,  # Very long period, slightly more shoaling
        17: 1.65,  # Very long period ground swell
        18: 1.75,  # Extended period
        19: 1.85,  # Extended period
        20: 2.0,   # Maximum realistic multiplier for extreme periods
        21: 2.0,   # Cap at 2.0 as other factors limit further growth
        22: 2.0    # Cap at 2.0 as other factors limit further growth
    }
    
    # Round period to nearest integer for lookup
    period_int = round(period)
    
    # Cap period to valid range
    if period_int < 8:
        period_int = 8
    elif period_int > 22:
        period_int = 22
    
    # Return multiplier
    return period_multipliers.get(period_int, 1.0)


def calculate_face_height(wave_height: float, period: float) -> float:
    """
    Calculate wave face height from deep water height and period.
    
    Args:
        wave_height: Significant wave height in feet
        period: Wave period in seconds
        
    Returns:
        Estimated wave face height in feet
    """
    # Get multiplier based on period
    multiplier = get_period_multiplier(period)
    
    # Calculate face height
    face_height = wave_height * multiplier
    
    return face_height


def extract_significant_south_swells(
    metadata: Dict[str, Any],
    bundle_dir: str,
    threshold_ft: float = 1.0,
    period_threshold: float = 12.0
) -> List[Dict[str, Any]]:
    """
    Extract significant south swells from bundle data.
    
    Args:
        metadata: Bundle metadata
        bundle_dir: Path to bundle directory
        threshold_ft: Minimum wave height in feet to be considered significant
        period_threshold: Minimum period in seconds to be considered groundswell
        
    Returns:
        List of significant south swell components
    """
    significant_swells = []
    
    # Use a pattern to match south swell directions
    # South: 160-200 degrees
    south_pattern = range(160, 201)
    
    # Check buoy data
    try:
        # Find buoy files in the bundle
        import os
        buoy_files = [f for f in os.listdir(bundle_dir) if f.startswith(("ndbc_", "cdip_")) and f.endswith(".txt")]
        
        logger.info(f"Processing {len(buoy_files)} buoy files for south swell detection")
        
        for buoy_file in buoy_files:
            # Parse buoy ID
            buoy_id = buoy_file.split("_")[1].split(".")[0]
            
            # Read buoy data
            try:
                with open(os.path.join(bundle_dir, buoy_file), "r") as f:
                    content = f.read()
                
                # Simple check for significant south swell
                # Look for patterns in NDBC data
                # Example: YY MM DD hh mm WD WVHT DPD APD MWD
                import re
                
                # Scan for wave data with south direction
                for line in content.split("\n"):
                    if line.startswith("#") or not line.strip():
                        continue
                    
                    parts = line.strip().split()
                    
                    # Need at least wave height and direction
                    if len(parts) < 8:
                        continue
                    
                    try:
                        # NDBC format - check positions carefully
                        if len(parts) >= 8:
                            # Try to parse wave height and direction
                            wave_height_str = parts[5] if len(parts) > 5 else None
                            direction_str = parts[7] if len(parts) > 7 else None
                            period_str = parts[6] if len(parts) > 6 else None
                            
                            if (wave_height_str and direction_str and period_str and 
                                wave_height_str not in ["99.0", "999.0", "MM"] and
                                direction_str not in ["999", "MM"] and
                                period_str not in ["99.0", "MM"]):
                                
                                wave_height = float(wave_height_str)
                                direction = int(direction_str)
                                period = float(period_str)
                                
                                # Convert from meters to feet if needed
                                wave_height_ft = wave_height * 3.28084
                                
                                # Check if it's a significant south swell
                                if direction in south_pattern and wave_height_ft >= threshold_ft and period >= period_threshold:
                                    logger.info(f"Found significant south swell in buoy {buoy_id} data: {wave_height_ft:.1f}ft")
                                    significant_swells.append({
                                        "buoy_id": buoy_id,
                                        "wave_height_ft": wave_height_ft,
                                        "direction": direction,
                                        "period": period,
                                        "face_height_ft": calculate_face_height(wave_height_ft, period)
                                    })
                                    break  # Found a significant swell, no need to process more lines
                    except (ValueError, IndexError):
                        # Skip lines with parsing errors
                        continue
            except Exception as e:
                logger.warning(f"Error processing buoy file {buoy_file}: {e}")
    except Exception as e:
        logger.warning(f"Error extracting south swells from buoys: {e}")
    
    # Check other data sources for south swell indicators
    try:
        # Look for Surfline data
        surfline_files = [f for f in os.listdir(bundle_dir) if f.startswith("surfline_") and f.endswith(".json")]
        
        for sf_file in surfline_files:
            try:
                with open(os.path.join(bundle_dir, sf_file), "r") as f:
                    data = f.read()
                
                if "south" in sf_file.lower() or "ala_moana" in sf_file.lower():
                    # Check for explicit south swell mention
                    if '"direction": 175' in data or '"direction": 180' in data or '"direction": 190' in data:
                        height_match = re.search(r'"surf":\s*{\s*"min":\s*(\d+\.?\d*),\s*"max":\s*(\d+\.?\d*)', data)
                        if height_match:
                            min_height = float(height_match.group(1))
                            max_height = float(height_match.group(2))
                            avg_height = (min_height + max_height) / 2
                            
                            if avg_height >= threshold_ft:
                                logger.info(f"Found significant south swell in Surfline data: {avg_height:.1f}ft")
                                significant_swells.append({
                                    "source": "Surfline",
                                    "wave_height_ft": avg_height,
                                    "direction": 180,  # Approximate south direction
                                    "period": 14.0,  # Assumed period
                                    "face_height_ft": calculate_face_height(avg_height, 14.0)
                                })
            except Exception as e:
                logger.warning(f"Error processing Surfline file {sf_file}: {e}")
        
        # Check for SNN forecast
        snn_file = os.path.join(bundle_dir, "snn_forecast.html")
        if os.path.exists(snn_file):
            try:
                with open(snn_file, "r") as f:
                    content = f.read()
                
                if "south" in content.lower() and "swell" in content.lower():
                    logger.info("Found explicit south swell mention in snn_forecast.html")
                    # This is a soft indicator
                    if not significant_swells:
                        significant_swells.append({
                            "source": "SNN",
                            "wave_height_ft": 1.0,  # Minimum value
                            "direction": 180,  # South
                            "period": 14.0,  # Assumed period
                            "face_height_ft": calculate_face_height(1.0, 14.0),
                            "soft_indicator": True
                        })
            except Exception as e:
                logger.warning(f"Error processing SNN file: {e}")
        
        # Check for south swell patterns file
        patterns_file = os.path.join(bundle_dir, "south_swell_patterns.json")
        if os.path.exists(patterns_file):
            try:
                with open(patterns_file, "r") as f:
                    patterns = json.load(f)
                
                if patterns.get("active_fetch", False):
                    logger.info("Found captured fetch pattern in south_swell_patterns.json")
                    
                    # Get details if available
                    height = patterns.get("predicted_height_ft", 1.0)
                    period = patterns.get("predicted_period", 14.0)
                    
                    significant_swells.append({
                        "source": "Fetch Pattern",
                        "wave_height_ft": height,
                        "direction": 180,  # South
                        "period": period,
                        "face_height_ft": calculate_face_height(height, period)
                    })
            except Exception as e:
                logger.warning(f"Error processing patterns file: {e}")
        
        # Check for MagicSeaweed South Pacific
        msw_file = os.path.join(bundle_dir, "magicseaweed_south_pacific.html")
        if os.path.exists(msw_file):
            try:
                with open(msw_file, "r") as f:
                    content = f.read()
                
                if "south" in content.lower() and "swell" in content.lower():
                    logger.info("Found explicit south swell mention in magicseaweed_south_pacific.html")
                    # This is another soft indicator
                    if not significant_swells:
                        significant_swells.append({
                            "source": "MagicSeaweed",
                            "wave_height_ft": 1.0,  # Minimum value
                            "direction": 180,  # South
                            "period": 14.0,  # Assumed period
                            "face_height_ft": calculate_face_height(1.0, 14.0),
                            "soft_indicator": True
                        })
            except Exception as e:
                logger.warning(f"Error processing MagicSeaweed file: {e}")
    except Exception as e:
        logger.warning(f"Error checking additional south swell sources: {e}")
    
    return significant_swells


def get_buoy_data_from_bundle(bundle_dir: str, buoy_id: str) -> Dict[str, Any]:
    """
    Extract buoy data from a bundle.
    
    Args:
        bundle_dir: Path to bundle directory
        buoy_id: Buoy ID to extract data for
        
    Returns:
        Dictionary of buoy data
    """
    result = {
        "buoy_id": buoy_id,
        "readings": [],
        "latest": None
    }
    
    try:
        import os
        buoy_file = os.path.join(bundle_dir, f"ndbc_{buoy_id}.txt")
        
        if not os.path.exists(buoy_file):
            logger.warning(f"Buoy file not found: {buoy_file}")
            return result
        
        with open(buoy_file, "r") as f:
            content = f.read()
        
        # Parse NDBC data
        lines = content.strip().split("\n")
        
        # Skip header line
        data_lines = [line for line in lines if not line.startswith("#") and line.strip()]
        
        if not data_lines:
            logger.warning(f"No data found in buoy file: {buoy_file}")
            return result
        
        # Process data lines
        readings = []
        
        for line in data_lines:
            parts = line.strip().split()
            
            if len(parts) < 8:
                continue
            
            try:
                # Extract data
                year = int(parts[0])
                month = int(parts[1])
                day = int(parts[2])
                hour = int(parts[3])
                minute = int(parts[4]) if len(parts) > 4 else 0
                
                wave_height_str = parts[5] if len(parts) > 5 else None
                period_str = parts[6] if len(parts) > 6 else None
                direction_str = parts[7] if len(parts) > 7 else None
                
                # Skip missing data
                if (wave_height_str in ["99.0", "999.0", "MM"] or
                    period_str in ["99.0", "MM"] or
                    direction_str in ["999", "MM"]):
                    continue
                
                # Convert to floats
                wave_height = float(wave_height_str)
                period = float(period_str)
                direction = int(direction_str)
                
                # Create timestamp
                timestamp = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
                
                # Convert wave height to feet
                wave_height_ft = wave_height * 3.28084
                
                # Add to readings
                readings.append({
                    "timestamp": timestamp.isoformat(),
                    "wave_height_m": wave_height,
                    "wave_height_ft": wave_height_ft,
                    "period": period,
                    "direction": direction,
                    "face_height_ft": calculate_face_height(wave_height_ft, period)
                })
            except (ValueError, IndexError):
                # Skip lines with parsing errors
                continue
        
        # Sort by timestamp (newest first)
        readings.sort(key=lambda x: x["timestamp"], reverse=True)
        
        # Update result
        result["readings"] = readings
        
        # Set latest reading
        if readings:
            result["latest"] = readings[0]
        
        logger.info(f"Extracted {len(readings)} readings for buoy {buoy_id}")
    except Exception as e:
        logger.warning(f"Error extracting buoy data for {buoy_id}: {e}")
    
    return result