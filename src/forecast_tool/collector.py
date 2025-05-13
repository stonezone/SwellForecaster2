"""Data collection module for SwellForecaster."""
import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union, Any

from .agents.buoy_agent import BuoyAgent
from .agents.chart_agent import ChartAgent
from .agents.model_agent import ModelAgent
from .agents.api_agent import ApiAgent
from .agents.region_agent import RegionAgent
from .data.collection_context import CollectionContext
from .models.settings import Settings, load_settings
from .utils.logging_config import setup_logging
from .utils.http import setup_http_client, shutdown_http_client

logger = logging.getLogger("collector")


class DataCollector:
    """
    Main data collector for SwellForecaster.
    
    Orchestrates multiple agents to collect data from various sources.
    """
    
    def __init__(self, settings: Settings):
        """
        Initialize the data collector.
        
        Args:
            settings: Application settings
        """
        self.settings = settings
        self.agents = self._create_agents()
    
    def _create_agents(self) -> List[Any]:
        """
        Create agent instances.

        Returns:
            List of agent instances
        """
        agents = []

        # Add buoy agent if enabled
        if self.settings.sources.enable_buoys:
            agents.append(BuoyAgent(self.settings))

        # Add chart agent if any chart sources are enabled
        if (
            self.settings.sources.enable_opc or
            self.settings.sources.enable_wpc or
            self.settings.sources.enable_models
        ):
            agents.append(ChartAgent(self.settings))

        # Add model agent if any model sources are enabled
        if (
            self.settings.sources.enable_models or
            self.settings.sources.enable_pacioos or
            self.settings.sources.enable_pacioos_swan or
            self.settings.sources.enable_ecmwf
        ):
            agents.append(ModelAgent(self.settings))

        # Add API agent if any API sources are enabled
        if (
            self.settings.sources.enable_windy or
            self.settings.sources.enable_open_meteo or
            self.settings.sources.enable_stormglass or
            self.settings.sources.enable_surfline
        ):
            agents.append(ApiAgent(self.settings))

        # Add region agent if any region sources are enabled
        if (
            self.settings.sources.enable_north_pacific or
            self.settings.sources.enable_southern_ocean
        ):
            agents.append(RegionAgent(self.settings))

        return agents
    
    async def collect(self) -> Optional[Path]:
        """
        Collect data from all enabled agents.
        
        Returns:
            Path to the data bundle directory, or None if collection failed
        """
        logger.info("Starting data collection")
        
        # Set up HTTP client
        await setup_http_client(
            timeout=self.settings.general.timeout,
            ssl_exceptions=self.settings.ssl_exceptions.get_domains(),
            user_agent=self.settings.general.user_agent,
            rate_limits={"windy.com": self.settings.general.windy_throttle_seconds}
        )
        
        # Create collection context
        try:
            async with CollectionContext(self.settings) as ctx:
                # Collect data from each agent in parallel
                agent_tasks = []
                for agent in self.agents:
                    if agent.enabled:
                        logger.info(f"Starting agent: {agent.name}")
                        agent_tasks.append(agent.collect(ctx))
                    else:
                        logger.info(f"Skipping disabled agent: {agent.name}")
                
                # Wait for all agents to complete
                agent_results = await asyncio.gather(*agent_tasks, return_exceptions=True)
                
                # Process results
                for i, result in enumerate(agent_results):
                    if isinstance(result, Exception):
                        logger.error(f"Agent {self.agents[i].name} failed: {result}")
                    elif result:
                        # Add results to context
                        ctx.add_results(result)
                        logger.info(f"Agent {self.agents[i].name} completed with {len(result)} results")
                
                # Return bundle directory
                return ctx.bundle_dir
        finally:
            # Shut down HTTP client
            await shutdown_http_client()
        
        return None


async def collect_data(config_path: str = "config.ini") -> Optional[Path]:
    """
    Collect data using settings from a configuration file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Path to the data bundle directory, or None if collection failed
    """
    # Load settings
    settings = load_settings(config_path)
    
    # Set up logging
    setup_logging(log_dir=Path(settings.general.data_dir).parent / "logs")
    
    # Create collector and collect data
    collector = DataCollector(settings)
    return await collector.collect()


async def main_async() -> int:
    """
    Main async function for data collection.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="SwellForecaster Data Collector")
    parser.add_argument("--config", default="config.ini", help="Path to configuration file")
    parser.add_argument("--cache-days", type=int, default=7, help="Number of days to keep cached data")
    args = parser.parse_args()
    
    try:
        # Collect data
        bundle_dir = await collect_data(args.config)
        
        if bundle_dir:
            print(f"Data collection complete: {bundle_dir}")
            return 0
        else:
            print("Data collection failed")
            return 1
    
    except Exception as e:
        print(f"Error during data collection: {e}")
        return 1


def main() -> int:
    """
    Main entry point for data collection.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\nData collection interrupted by user")
        return 130


if __name__ == "__main__":
    sys.exit(main())