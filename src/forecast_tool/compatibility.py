"""
Compatibility layer for bridging the legacy code and the new codebase.

This module provides compatibility functions and classes that map between
the old and new APIs. It's intended to be a transitional layer while
the codebase is being migrated to the new architecture.
"""
import importlib
import json
import logging
import os
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# Set up logger
logger = logging.getLogger("forecast_tool.compatibility")

# Path to the new utils module
from forecast_tool.utils import (
    # Async versions
    file_io,
    http,
    logging_config,
    retry,
    wave_analysis,
    
    # Legacy compatibility 
    jload,
    jdump_file,
    fetch_sync,
    fetch_json_sync,
    utcnow,
    get_south_swell_status,
    get_period_multiplier,
    calculate_face_height,
    extract_significant_south_swells,
    get_buoy_data_from_bundle,
    create_placeholder_image,
)


def setup_legacy_module(module_name: str, target_module: Any) -> None:
    """
    Set up a legacy module with new functionality.
    
    This creates or modifies an existing module in sys.modules to provide
    compatibility with the new codebase.
    
    Args:
        module_name: Name of the legacy module to create/update
        target_module: The new module to expose functionality from
    """
    logger.debug(f"Setting up legacy module: {module_name}")
    
    # Create or get the module
    if module_name not in sys.modules:
        sys.modules[module_name] = types.ModuleType(module_name)
    
    legacy_module = sys.modules[module_name]
    
    # Add the module's attributes to the legacy module
    for name in dir(target_module):
        if not name.startswith('_'):  # Skip private attributes
            attr = getattr(target_module, name)
            setattr(legacy_module, name, attr)
            logger.debug(f"Added {name} to {module_name}")


def setup_utils_compatibility() -> None:
    """
    Set up compatibility for the utils module.
    
    Creates or updates the global utils module with functions from the new utilities.
    """
    # Create a new utils module if it doesn't exist
    if "utils" not in sys.modules:
        sys.modules["utils"] = types.ModuleType("utils")
    
    utils_module = sys.modules["utils"]
    
    # Add utility functions
    
    # Time and formatting utilities
    setattr(utils_module, "utcnow", utcnow)
    
    # Logging utilities - implement legacy log_init function
    def legacy_log_init(name: str, level: str = "INFO") -> logging.Logger:
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # Use similar approach to original, but updated
        log_level = getattr(logging, str(level).upper(), logging.INFO)
        
        # Setup logging with FileHandler and StreamHandler
        logging.basicConfig(
            level=log_level,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            handlers=[
                logging.FileHandler(log_dir / f"{name}_{datetime.now(timezone.utc).strftime('%Y%m%d')}.log"),
                logging.StreamHandler(sys.stdout),
            ],
            force=True,  # Force reconfiguration
        )
        
        # Return the logger for the specified name
        return logging.getLogger(name)
    
    # Set the log_init function
    setattr(utils_module, "log_init", legacy_log_init)
    
    # File I/O utilities
    # For jdump, we need to copy the original behavior from utils.py
    def legacy_jdump(obj, path=None):
        """Legacy-compatible JSON dump function."""
        json_str = json.dumps(obj, indent=2, sort_keys=True)
        if path:
            with open(path, "w", encoding="utf-8") as f:
                f.write(json_str)
        return json_str

    # Add async file writing function
    async def write_file_async(path, content):
        """Write file content asynchronously."""
        # Create directory if needed
        if isinstance(path, str):
            path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Use aiofiles if available
        try:
            import aiofiles
            async with aiofiles.open(path, "w", encoding="utf-8") as f:
                await f.write(content)
        except ImportError:
            # Fallback to run_in_executor
            import asyncio
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: path.write_text(content, encoding="utf-8"))

        return str(path)

    setattr(utils_module, "jdump", legacy_jdump)
    setattr(utils_module, "jload", jload)
    setattr(utils_module, "write_file_async", write_file_async)
    
    # Wave analysis utilities
    setattr(utils_module, "get_south_swell_status", get_south_swell_status)
    setattr(utils_module, "get_period_multiplier", get_period_multiplier)
    setattr(utils_module, "calculate_face_height", calculate_face_height)
    setattr(utils_module, "extract_significant_south_swells", extract_significant_south_swells)
    setattr(utils_module, "get_buoy_data_from_bundle", get_buoy_data_from_bundle)
    
    # Image utilities
    setattr(utils_module, "create_placeholder_image", create_placeholder_image)
    
    # HTTP utilities
    setattr(utils_module, "fetch", fetch_sync)  # Use sync version for compatibility
    setattr(utils_module, "fetch_json", fetch_json_sync)  # Add JSON fetch
    
    # Config utilities
    def getint_safe(cfg, section, key, default=0):
        """Safely get integer from config with fallback."""
        try:
            return int(cfg[section][key])
        except (KeyError, ValueError):
            return default
    
    setattr(utils_module, "getint_safe", getint_safe)
    
    # Argument parser
    def argparser(desc: str):
        import argparse
        p = argparse.ArgumentParser(description=desc)
        p.add_argument("--config", default="config.ini",
                    help="INI file to read")
        p.add_argument("--cache-days", type=int, default=7,
                    help="days to keep bundles")
        return p
    
    setattr(utils_module, "argparser", argparser)
    
    logger.info("Utils compatibility layer set up")


def setup_agents_compatibility() -> None:
    """
    Set up compatibility for the agents module.
    
    Creates or updates the global agents module with functions from the new agents.
    """
    # Create a new agents module if it doesn't exist
    if "agents" not in sys.modules:
        agents_module = types.ModuleType("agents")
        
        # Make the module callable by adding a __call__ method
        def module_call(*args, **kwargs):
            logger.warning("agents module was called as a function")
            return []
        
        agents_module.__call__ = module_call
        sys.modules["agents"] = agents_module

    agents_module = sys.modules["agents"]
    
    # Import our agent modules
    try:
        # Try the legacy import first
        try:
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
            from agents import buoy_agents, chart_agents, model_agents, api_agents, region_agents
            sys.path.pop(0)
        except ImportError:
            # Try the new imports
            from forecast_tool.agents import (
                buoy_agent as buoy_agents,
                chart_agent as chart_agents,
                model_agent as model_agents,
                api_agent as api_agents,
                region_agent as region_agents,
            )
        
        # Set up shared modules
        for module_name, agent_module in [
            ("buoy_agents", buoy_agents),
            ("chart_agents", chart_agents),
            ("model_agents", model_agents),
            ("api_agents", api_agents),
            ("region_agents", region_agents),
        ]:
            setattr(agents_module, module_name.replace("_agents", ""), agent_module)
            setattr(agents_module, module_name, agent_module)
        
        # Import legacy and third-party agents if available
        try:
            import stormglass_agent
            setattr(agents_module, "stormglass", stormglass_agent)
            logger.debug("Imported stormglass_agent")
        except ImportError:
            logger.warning("stormglass_agent not found, setting dummy implementation")
            # Create dummy stormglass module
            stormglass_module = types.ModuleType("stormglass")
            setattr(stormglass_module, "fetch_stormglass", lambda *args, **kwargs: [])
            setattr(agents_module, "stormglass", stormglass_module)
        
        # Create async dummy function
        async def dummy_agent(*args, **kwargs):
            logger.debug(f"Dummy agent called with {args}, {kwargs}")
            return []
        
        # Add all the agent functions needed by collector_revised.py
        # Chart agents
        setattr(agents_module, "opc", dummy_agent)
        setattr(agents_module, "wpc", dummy_agent)
        setattr(agents_module, "nws", dummy_agent)
        
        # Buoy agents
        setattr(agents_module, "buoys", dummy_agent)
        setattr(agents_module, "noaa_coops", dummy_agent)
        
        # Model agents
        setattr(agents_module, "pacioos", dummy_agent)
        setattr(agents_module, "pacioos_swan", dummy_agent)
        setattr(agents_module, "ecmwf_wave", dummy_agent)
        setattr(agents_module, "model_agent", dummy_agent)
        setattr(agents_module, "ww3_model_fallback", dummy_agent)
        setattr(agents_module, "ecmwf_agent", dummy_agent)
        
        # API agents
        setattr(agents_module, "windy", dummy_agent)
        setattr(agents_module, "open_meteo", dummy_agent)
        setattr(agents_module, "stormglass", dummy_agent)
        setattr(agents_module, "surfline", dummy_agent)
        
        # Regional agents
        setattr(agents_module, "southern_hemisphere", dummy_agent)
        setattr(agents_module, "north_pacific_enhanced", dummy_agent)
        
        # Import existing agents if available
        try:
            import opc_wpc_agents
            setattr(agents_module, "opc", opc_wpc_agents)
            setattr(agents_module, "wpc", opc_wpc_agents)
            logger.debug("Imported opc_wpc_agents")
        except ImportError:
            logger.warning("opc_wpc_agents not found, using dummy implementation")
        
        # Set up other agent modules as needed
        
        logger.info("Agents compatibility layer set up")
    except ImportError as e:
        logger.warning(f"Failed to set up agents compatibility: {e}")


def setup_north_pacific_compatibility() -> None:
    """
    Set up compatibility for the north_pacific_analysis module.

    Creates or updates the global north_pacific_analysis module.
    """
    # Create or get the module
    if "north_pacific_analysis" not in sys.modules:
        module = sys.modules["north_pacific_analysis"] = types.ModuleType("north_pacific_analysis")
    else:
        module = sys.modules["north_pacific_analysis"]

    # Import our local north_pacific_analysis implementation
    try:
        # Try to import from actual file
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        import north_pacific_analysis as local_module
        sys.path.pop(0)

        # Update with any missing attributes
        for name in dir(local_module):
            if not name.startswith('_'):  # Skip private attributes
                setattr(module, name, getattr(local_module, name))

        logger.info("North Pacific Analysis module imported successfully")
    except (ImportError, AttributeError) as e:
        logger.warning(f"Error importing north_pacific_analysis: {e}")

        # Create a minimal implementation if needed
        class NorthPacificAnalyzer:
            def __init__(self, bundle_dir, metadata):
                self.bundle_dir = bundle_dir
                self.metadata = metadata

            def analyze(self):
                return {
                    "storms": [],
                    "swell_forecast": {},
                    "analysis_timestamp": datetime.now(timezone.utc).isoformat()
                }

        # Add the class to the module
        setattr(module, "NorthPacificAnalyzer", NorthPacificAnalyzer)
        logger.info("Created dummy NorthPacificAnalyzer implementation")


def setup_all_compatibility_bridges() -> None:
    """Set up all compatibility bridges between legacy and new code."""
    logger.info("Setting up compatibility bridges")

    # Set up utils compatibility
    setup_utils_compatibility()

    # Set up agents compatibility
    setup_agents_compatibility()

    # Set up north_pacific_analysis compatibility
    setup_north_pacific_compatibility()

    # Add other modules as needed

    logger.info("All compatibility bridges set up")


# Run the setup when imported
if __name__ != "__main__":
    logger.debug("Compatibility module loaded")