"""Settings models for SwellForecaster."""
import configparser
import logging
import os
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Any, Union

from pydantic import BaseModel, Field, validator

logger = logging.getLogger("models.settings")


class ApiSettings(BaseModel):
    """API settings including keys and credentials."""
    
    openai_key: str = Field("", description="OpenAI API key for forecast generation")
    windy_key: str = Field("", description="Windy API key for marine data")
    ecmwf_key: str = Field("", description="ECMWF API key for European model data")
    stormglass_key: str = Field("", description="Stormglass API key for marine data")
    anthropic_key: str = Field("", description="Anthropic API key for forecast generation with Claude")
    surfline_email: str = Field("", description="Surfline account email")
    surfline_password: str = Field("", description="Surfline account password")
    
    class Config:
        """Configuration for ApiSettings."""
        
        validate_assignment = True


class GeneralSettings(BaseModel):
    """General application settings."""
    
    data_dir: str = Field("pacific_data", description="Directory to store data bundles")
    timeout: int = Field(60, description="HTTP request timeout in seconds")
    max_retries: int = Field(3, description="Maximum number of retries for failed requests")
    debug: bool = Field(False, description="Enable debug mode")
    user_agent: str = Field(
        "SwellForecaster/1.0", 
        description="User agent string for HTTP requests"
    )
    windy_throttle_seconds: float = Field(
        1.0, 
        description="Seconds to wait between Windy API requests"
    )
    
    @validator("timeout")
    def validate_timeout(cls, v: int) -> int:
        """Validate timeout."""
        if v < 10:
            logger.warning(f"Timeout is very short: {v}s, using at least 10s")
            return 10
        if v > 300:
            logger.warning(f"Timeout is very long: {v}s, capping at 300s")
            return 300
        return v
    
    class Config:
        """Configuration for GeneralSettings."""
        
        validate_assignment = True


class SourceSettings(BaseModel):
    """Settings for data sources."""
    
    # Buoys
    enable_buoys: bool = Field(True, description="Enable NDBC buoy data collection")
    enable_cdip: bool = Field(True, description="Enable CDIP buoy data collection")
    
    # API sources
    enable_windy: bool = Field(True, description="Enable Windy API data collection")
    enable_open_meteo: bool = Field(True, description="Enable Open-Meteo data collection")
    enable_stormglass: bool = Field(False, description="Enable Stormglass API data collection")
    enable_surfline: bool = Field(False, description="Enable Surfline data collection")
    
    # Charts and images
    enable_opc: bool = Field(True, description="Enable Ocean Prediction Center charts")
    enable_wpc: bool = Field(True, description="Enable Weather Prediction Center charts")
    
    # Model data
    enable_models: bool = Field(True, description="Enable model data collection")
    enable_pacioos: bool = Field(True, description="Enable PacIOOS data collection")
    enable_pacioos_swan: bool = Field(True, description="Enable PacIOOS SWAN data collection")
    enable_ecmwf: bool = Field(False, description="Enable ECMWF data collection")
    
    # Regional data
    enable_north_pacific: bool = Field(True, description="Enable North Pacific data collection")
    enable_southern_ocean: bool = Field(True, description="Enable Southern Ocean data collection")
    
    class Config:
        """Configuration for SourceSettings."""
        
        validate_assignment = True


class ForecastSettings(BaseModel):
    """Settings for forecast generation."""
    
    min_data_sources: int = Field(3, description="Minimum number of data sources required")
    model: str = Field("gpt-4-1106-preview", description="OpenAI model to use for forecast generation")
    min_words: int = Field(300, description="Minimum word count for forecasts")
    max_words: int = Field(1000, description="Maximum word count for forecasts")
    temperature: float = Field(0.7, description="Temperature parameter for text generation")
    html_output: bool = Field(True, description="Generate HTML output")
    markdown_output: bool = Field(True, description="Generate Markdown output")
    pdf_output: bool = Field(True, description="Generate PDF output")
    include_images: bool = Field(True, description="Include images in output")
    include_charts: bool = Field(True, description="Include charts in output")
    
    @validator("temperature")
    def validate_temperature(cls, v: float) -> float:
        """Validate temperature."""
        if v < 0.0:
            logger.warning(f"Temperature is too low: {v}, using 0.0")
            return 0.0
        if v > 1.0:
            logger.warning(f"Temperature is too high: {v}, using 1.0")
            return 1.0
        return v
    
    class Config:
        """Configuration for ForecastSettings."""
        
        validate_assignment = True


class SSLExceptions(BaseModel):
    """Domains with SSL verification disabled."""
    
    domains: List[str] = Field([], description="List of domains with SSL verification disabled")
    
    def get_domains(self) -> List[str]:
        """Get the list of domains."""
        return self.domains
    
    class Config:
        """Configuration for SSLExceptions."""
        
        validate_assignment = True


class FallbackSourceSettings(BaseModel):
    """Settings for fallback sources."""
    
    enable_fallbacks: bool = Field(True, description="Enable fallback sources")
    retry_count: int = Field(3, description="Number of retries before falling back")
    retry_delay: float = Field(2.0, description="Delay between retries in seconds")
    
    class Config:
        """Configuration for FallbackSourceSettings."""
        
        validate_assignment = True


class Settings(BaseModel):
    """Complete application settings."""
    
    api: ApiSettings
    general: GeneralSettings
    sources: SourceSettings
    forecast: ForecastSettings
    ssl_exceptions: SSLExceptions
    fallback_sources: FallbackSourceSettings
    
    class Config:
        """Configuration for Settings."""
        
        validate_assignment = True


def load_settings(config_path: Union[str, Path]) -> Settings:
    """
    Load settings from a configuration file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Settings object
        
    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        configparser.Error: If there's an error parsing the configuration
    """
    # Check if file exists
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    # Parse configuration
    config = configparser.ConfigParser()
    config.read(path)
    
    # Create settings objects
    api_settings = ApiSettings(
        openai_key=config.get("API", "OPENAI_KEY", fallback=""),
        windy_key=config.get("API", "WINDY_KEY", fallback=""),
        ecmwf_key=config.get("API", "ECMWF_KEY", fallback=""),
        stormglass_key=config.get("API", "STORMGLASS_KEY", fallback=""),
        anthropic_key=config.get("API", "ANTHROPIC_KEY", fallback=""),
        surfline_email=config.get("API", "SURFLINE_EMAIL", fallback=""),
        surfline_password=config.get("API", "SURFLINE_PASSWORD", fallback=""),
    )
    
    general_settings = GeneralSettings(
        data_dir=config.get("GENERAL", "DATA_DIR", fallback="pacific_data"),
        timeout=config.getint("GENERAL", "TIMEOUT", fallback=60),
        max_retries=config.getint("GENERAL", "MAX_RETRIES", fallback=3),
        debug=config.getboolean("GENERAL", "DEBUG", fallback=False),
        user_agent=config.get("GENERAL", "USER_AGENT", fallback="SwellForecaster/1.0"),
        windy_throttle_seconds=config.getfloat("GENERAL", "WINDY_THROTTLE_SECONDS", fallback=1.0),
    )
    
    source_settings = SourceSettings(
        enable_buoys=config.getboolean("SOURCES", "ENABLE_BUOYS", fallback=True),
        enable_cdip=config.getboolean("SOURCES", "ENABLE_CDIP", fallback=True),
        enable_windy=config.getboolean("SOURCES", "ENABLE_WINDY", fallback=True),
        enable_open_meteo=config.getboolean("SOURCES", "ENABLE_OPEN_METEO", fallback=True),
        enable_stormglass=config.getboolean("SOURCES", "ENABLE_STORMGLASS", fallback=False),
        enable_surfline=config.getboolean("SOURCES", "ENABLE_SURFLINE", fallback=False),
        enable_opc=config.getboolean("SOURCES", "ENABLE_OPC", fallback=True),
        enable_wpc=config.getboolean("SOURCES", "ENABLE_WPC", fallback=True),
        enable_models=config.getboolean("SOURCES", "ENABLE_MODELS", fallback=True),
        enable_pacioos=config.getboolean("SOURCES", "ENABLE_PACIOOS", fallback=True),
        enable_pacioos_swan=config.getboolean("SOURCES", "ENABLE_PACIOOS_SWAN", fallback=True),
        enable_ecmwf=config.getboolean("SOURCES", "ENABLE_ECMWF", fallback=False),
        enable_north_pacific=config.getboolean("SOURCES", "ENABLE_NORTH_PACIFIC", fallback=True),
        enable_southern_ocean=config.getboolean("SOURCES", "ENABLE_SOUTHERN_OCEAN", fallback=True),
    )
    
    forecast_settings = ForecastSettings(
        min_data_sources=config.getint("FORECAST", "MIN_DATA_SOURCES", fallback=3),
        model=config.get("FORECAST", "MODEL", fallback="gpt-4-1106-preview"),
        min_words=config.getint("FORECAST", "MIN_WORDS", fallback=300),
        max_words=config.getint("FORECAST", "MAX_WORDS", fallback=1000),
        temperature=config.getfloat("FORECAST", "TEMPERATURE", fallback=0.7),
        html_output=config.getboolean("FORECAST", "HTML_OUTPUT", fallback=True),
        markdown_output=config.getboolean("FORECAST", "MARKDOWN_OUTPUT", fallback=True),
        pdf_output=config.getboolean("FORECAST", "PDF_OUTPUT", fallback=True),
        include_images=config.getboolean("FORECAST", "INCLUDE_IMAGES", fallback=True),
        include_charts=config.getboolean("FORECAST", "INCLUDE_CHARTS", fallback=True),
    )
    
    # Parse SSL exceptions
    ssl_domains = []
    if config.has_section("SSL_EXCEPTIONS"):
        for key, value in config.items("SSL_EXCEPTIONS"):
            if value.lower() in ["true", "yes", "1"]:
                ssl_domains.append(key)
    
    ssl_exceptions = SSLExceptions(
        domains=ssl_domains,
    )
    
    fallback_source_settings = FallbackSourceSettings(
        enable_fallbacks=config.getboolean("FALLBACK_SOURCES", "ENABLE_FALLBACKS", fallback=True),
        retry_count=config.getint("FALLBACK_SOURCES", "RETRY_COUNT", fallback=3),
        retry_delay=config.getfloat("FALLBACK_SOURCES", "RETRY_DELAY", fallback=2.0),
    )
    
    # Create full settings
    settings = Settings(
        api=api_settings,
        general=general_settings,
        sources=source_settings,
        forecast=forecast_settings,
        ssl_exceptions=ssl_exceptions,
        fallback_sources=fallback_source_settings,
    )
    
    return settings