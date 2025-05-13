"""Buoy data models for SwellForecaster."""
import re
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union, Any, Tuple

from pydantic import BaseModel, Field, validator


class BuoySource(str, Enum):
    """Buoy data source."""
    
    NDBC = "NDBC"
    CDIP = "CDIP"
    OTHER = "OTHER"


class BuoyReading(BaseModel):
    """Individual buoy reading."""
    
    timestamp: datetime = Field(..., description="Time of measurement")
    wave_height: Optional[float] = Field(None, description="Significant wave height in meters")
    peak_period: Optional[float] = Field(None, description="Dominant wave period in seconds")
    mean_period: Optional[float] = Field(None, description="Average wave period in seconds")
    peak_direction: Optional[float] = Field(None, description="Direction of dominant waves in degrees")
    wind_speed: Optional[float] = Field(None, description="Wind speed in m/s")
    wind_direction: Optional[float] = Field(None, description="Wind direction in degrees")
    wind_gust: Optional[float] = Field(None, description="Wind gust in m/s")
    air_temperature: Optional[float] = Field(None, description="Air temperature in Celsius")
    water_temperature: Optional[float] = Field(None, description="Water temperature in Celsius")
    air_pressure: Optional[float] = Field(None, description="Air pressure in hPa")
    
    @validator("peak_direction", "wind_direction")
    def validate_direction(cls, v: Optional[float]) -> Optional[float]:
        """Validate direction values."""
        if v is not None:
            if v < 0:
                return v % 360
            if v >= 360:
                return v % 360
        return v
    
    class Config:
        """Configuration for BuoyReading."""
        
        validate_assignment = True
        arbitrary_types_allowed = True


class SpectralData(BaseModel):
    """Spectral wave data."""
    
    frequency: List[float] = Field(..., description="Wave frequencies in Hz")
    energy: List[float] = Field(..., description="Wave energy density")
    direction: Optional[List[float]] = Field(None, description="Wave direction per frequency band")
    r1: Optional[List[float]] = Field(None, description="First fourier coefficient")
    r2: Optional[List[float]] = Field(None, description="Second fourier coefficient")
    a1: Optional[List[float]] = Field(None, description="Mean wave direction per frequency band")
    b1: Optional[List[float]] = Field(None, description="Principal wave direction per frequency band")
    a2: Optional[List[float]] = Field(None, description="First moment of directional spread")
    b2: Optional[List[float]] = Field(None, description="Second moment of directional spread")
    
    class Config:
        """Configuration for SpectralData."""
        
        validate_assignment = True
        arbitrary_types_allowed = True


class BuoyData(BaseModel):
    """Complete buoy data including metadata and readings."""
    
    buoy_id: str = Field(..., description="Buoy identifier")
    source: BuoySource = Field(..., description="Data source (e.g., NDBC, CDIP)")
    name: Optional[str] = Field(None, description="Buoy name or description")
    latitude: Optional[float] = Field(None, description="Buoy latitude")
    longitude: Optional[float] = Field(None, description="Buoy longitude")
    depth: Optional[float] = Field(None, description="Water depth at buoy location in meters")
    readings: List[BuoyReading] = Field([], description="Time series of buoy readings")
    spectral: Optional[Dict[datetime, SpectralData]] = Field(None, description="Spectral wave data if available")
    metadata: Dict[str, Any] = Field({}, description="Additional metadata")
    
    class Config:
        """Configuration for BuoyData."""
        
        validate_assignment = True
        arbitrary_types_allowed = True


def parse_ndbc_data(data: str, buoy_id: str) -> BuoyData:
    """
    Parse NDBC buoy data.
    
    Args:
        data: Raw NDBC data as string
        buoy_id: Buoy identifier
        
    Returns:
        Parsed BuoyData object
    """
    lines = data.strip().split("\n")
    
    # Check if we have enough lines
    if len(lines) < 2:
        return BuoyData(buoy_id=buoy_id, source=BuoySource.NDBC)
    
    # Parse header
    header = lines[0].strip().split()
    metadata = {}
    
    # Create readings
    readings = []
    
    # Parse data lines
    for i in range(2, len(lines)):
        line = lines[i].strip()
        if not line or line.startswith("#"):
            continue
        
        parts = line.split()
        if len(parts) < 5:
            continue
        
        try:
            # Extract date components
            year = int(parts[0])
            month = int(parts[1])
            day = int(parts[2])
            hour = int(parts[3])
            minute = int(parts[4]) if len(parts) > 4 else 0
            
            # Create timestamp
            timestamp = datetime(year, month, day, hour, minute)
            
            # Create reading
            reading = BuoyReading(timestamp=timestamp)
            
            # Add available data
            if len(parts) > 5 and parts[5] != "99.00" and parts[5] != "999.0":
                reading.wave_height = float(parts[5])
            
            if len(parts) > 6 and parts[6] != "99.0" and parts[6] != "999.0":
                reading.peak_period = float(parts[6])
            
            if len(parts) > 7 and parts[7] != "999":
                reading.peak_direction = float(parts[7])
            
            # Add additional fields if available
            if len(parts) > 8 and parts[8] != "99.0" and parts[8] != "999.0":
                reading.mean_period = float(parts[8])
            
            if len(parts) > 9 and parts[9] != "999.0":
                reading.air_temperature = float(parts[9])
            
            if len(parts) > 10 and parts[10] != "9999.0":
                reading.water_temperature = float(parts[10])
            
            if len(parts) > 11 and parts[11] != "999.0":
                reading.wind_direction = float(parts[11])
            
            if len(parts) > 12 and parts[12] != "99.0":
                reading.wind_speed = float(parts[12])
            
            if len(parts) > 13 and parts[13] != "99.0":
                reading.wind_gust = float(parts[13])
            
            readings.append(reading)
        
        except (ValueError, IndexError):
            continue
    
    # Create buoy data
    buoy_data = BuoyData(
        buoy_id=buoy_id,
        source=BuoySource.NDBC,
        readings=readings,
        metadata=metadata,
    )
    
    return buoy_data


def parse_ndbc_spectral(data: str, buoy_id: str) -> Dict[datetime, SpectralData]:
    """
    Parse NDBC spectral buoy data.
    
    Args:
        data: Raw NDBC spectral data as string
        buoy_id: Buoy identifier
        
    Returns:
        Dictionary of timestamp to SpectralData
    """
    lines = data.strip().split("\n")
    
    # Check if we have enough lines
    if len(lines) < 2:
        return {}
    
    # Read the separator line to determine number of frequency bands
    separator_idx = -1
    for i, line in enumerate(lines):
        if line.startswith("#"):
            separator_idx = i
    
    if separator_idx == -1 or separator_idx >= len(lines) - 1:
        return {}
    
    # Get the frequency bands from the separator line
    separator = lines[separator_idx].strip()
    freq_match = re.search(r"#\s*Frequency\s*\(Hz\)\s*(.*)", separator)
    if not freq_match:
        return {}
    
    freq_str = freq_match.group(1).strip()
    frequencies = [float(f) for f in freq_str.split()]
    
    # Parse the data blocks
    spectral_data = {}
    current_time = None
    current_energy = []
    
    for i in range(separator_idx + 1, len(lines)):
        line = lines[i].strip()
        if not line:
            continue
        
        parts = line.split()
        
        # Check if this is a new time block
        if len(parts) >= 5 and parts[0].isdigit() and parts[1].isdigit() and parts[2].isdigit():
            # Save previous block if we have one
            if current_time and current_energy:
                spectral_data[current_time] = SpectralData(
                    frequency=frequencies,
                    energy=current_energy,
                )
            
            # Start a new block
            try:
                year = int(parts[0])
                month = int(parts[1])
                day = int(parts[2])
                hour = int(parts[3])
                minute = int(parts[4]) if len(parts) > 4 else 0
                
                current_time = datetime(year, month, day, hour, minute)
                current_energy = []
            except (ValueError, IndexError):
                current_time = None
                current_energy = []
        
        # Add energy data if this is an energy line
        elif current_time and len(parts) == len(frequencies):
            try:
                energies = [float(e) for e in parts]
                current_energy = energies
            except ValueError:
                pass
    
    # Save the last block if we have one
    if current_time and current_energy:
        spectral_data[current_time] = SpectralData(
            frequency=frequencies,
            energy=current_energy,
        )
    
    return spectral_data


def parse_cdip_data(data: str, buoy_id: str) -> BuoyData:
    """
    Parse CDIP buoy data.
    
    Args:
        data: Raw CDIP data as string
        buoy_id: Buoy identifier
        
    Returns:
        Parsed BuoyData object
    """
    lines = data.strip().split("\n")
    
    # Check if we have enough lines
    if len(lines) < 2:
        return BuoyData(buoy_id=buoy_id, source=BuoySource.CDIP)
    
    # Parse header
    metadata = {}
    header_found = False
    data_start = 0
    
    for i, line in enumerate(lines):
        if "yyyy" in line.lower() and "mm" in line.lower() and "dd" in line.lower():
            header_found = True
            data_start = i + 1
            break
    
    if not header_found:
        return BuoyData(buoy_id=buoy_id, source=BuoySource.CDIP)
    
    # Create readings
    readings = []
    
    # Parse data lines
    for i in range(data_start, len(lines)):
        line = lines[i].strip()
        if not line or line.startswith("#"):
            continue
        
        parts = line.split()
        if len(parts) < 8:
            continue
        
        try:
            # Extract date components
            date_str = parts[0]
            time_str = parts[1]
            
            # Parse date
            year, month, day = map(int, date_str.split("/"))
            hour, minute = map(int, time_str.split(":"))
            
            # Create timestamp
            timestamp = datetime(year, month, day, hour, minute)
            
            # Create reading
            reading = BuoyReading(timestamp=timestamp)
            
            # Add available data
            if len(parts) > 2 and parts[2] != "999":
                reading.wave_height = float(parts[2])
            
            if len(parts) > 3 and parts[3] != "999":
                reading.peak_period = float(parts[3])
            
            if len(parts) > 4 and parts[4] != "999":
                reading.peak_direction = float(parts[4])
            
            if len(parts) > 5 and parts[5] != "999":
                reading.mean_period = float(parts[5])
            
            readings.append(reading)
        
        except (ValueError, IndexError):
            continue
    
    # Create buoy data
    buoy_data = BuoyData(
        buoy_id=buoy_id,
        source=BuoySource.CDIP,
        readings=readings,
        metadata=metadata,
    )
    
    return buoy_data