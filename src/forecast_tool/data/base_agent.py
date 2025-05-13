"""Base agent class for data collection in SwellForecaster."""
import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import aiohttp

from ..models.settings import Settings
from ..utils import http_retry
from .collection_context import CollectionContext, CollectionResult

logger = logging.getLogger("data.base_agent")


class BaseAgent(ABC):
    """
    Base class for all data collection agents.
    
    Provides common functionality for data collection, error handling, and result formatting.
    """
    
    def __init__(self, settings: Settings):
        """
        Initialize the agent.
        
        Args:
            settings: Application settings
        """
        self.settings = settings
        self.logger = logging.getLogger(f"agent.{self.__class__.__name__.lower()}")
    
    @property
    @abstractmethod
    def name(self) -> str:
        """
        Get the agent's name.
        
        Returns:
            Agent name
        """
        pass
    
    @property
    def enabled(self) -> bool:
        """
        Check if the agent is enabled in settings.
        
        Returns:
            True if enabled, False otherwise
        """
        # Default implementation - override if needed
        return True
    
    @abstractmethod
    async def collect(self, ctx: CollectionContext) -> List[CollectionResult]:
        """
        Collect data and return results.
        
        Args:
            ctx: Collection context
            
        Returns:
            List of collection results
        """
        pass
    
    async def create_result(
        self,
        ctx: CollectionContext,
        source: str,
        filename: str,
        data: Union[str, bytes],
        type_name: str,
        *,
        subtype: Optional[str] = None,
        region: Optional[str] = None,
        priority: int = 1,
        url: Optional[str] = None,
        error: Optional[bool] = None,
        north_facing: Optional[bool] = None,
        south_facing: Optional[bool] = None,
    ) -> CollectionResult:
        """
        Create a collection result after saving data.
        
        Args:
            ctx: Collection context
            source: Source name
            filename: Filename to save as
            data: Data to save
            type_name: Type of data
            subtype: Optional subtype
            region: Optional region
            priority: Priority (lower is higher)
            url: Optional source URL
            error: Whether there was an error
            north_facing: Whether relevant for north-facing shores
            south_facing: Whether relevant for south-facing shores
            
        Returns:
            Collection result
        """
        # Save the data
        saved_filename = await ctx.save(filename, data)
        
        # Create result
        result = CollectionResult(
            source=source,
            filename=saved_filename,
            type=type_name,
            subtype=subtype,
            region=region,
            priority=priority,
            timestamp=datetime.now(timezone.utc).isoformat(),
            url=url,
            error=error,
            north_facing=north_facing,
            south_facing=south_facing
        )
        
        # Add path field
        result.path = ctx.bundle_dir / saved_filename
        
        return result
    
    @http_retry
    async def fetch(
        self,
        ctx: CollectionContext,
        url: str,
        **kwargs: Any
    ) -> bytes:
        """
        Fetch data from a URL with automatic retries.

        Args:
            ctx: Collection context
            url: URL to fetch
            **kwargs: Additional keyword arguments for the HTTP client

        Returns:
            Response data as bytes
        """
        # Check if we have the context's http_client
        if hasattr(ctx, 'http_client') and ctx.http_client is not None:
            # Use the context's http_client directly
            method = kwargs.pop('method', 'GET').upper()
            headers = kwargs.pop('headers', None)
            params = kwargs.pop('params', None)
            verify_ssl = kwargs.pop('verify_ssl', None)
            timeout = kwargs.pop('timeout', None)
            json_body = kwargs.pop('json', None)

            if method == 'GET':
                response = await ctx.http_client.get(
                    url,
                    headers=headers,
                    params=params,
                    ssl=None if verify_ssl else False,
                    timeout=aiohttp.ClientTimeout(total=timeout) if timeout else None,
                    raise_for_status=False,
                    **kwargs
                )
            elif method == 'POST':
                response = await ctx.http_client.post(
                    url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    ssl=None if verify_ssl else False,
                    timeout=aiohttp.ClientTimeout(total=timeout) if timeout else None,
                    raise_for_status=False,
                    **kwargs
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            # Check status
            if response.status < 200 or response.status >= 300:
                raise ValueError(f"HTTP error {response.status}: {url}")

            # Get data
            data = await response.read()
            return data
        else:
            # Fall back to the global fetch utility
            from ..utils import fetch
            return await fetch(url, **kwargs)
    
    async def fetch_and_save(
        self,
        ctx: CollectionContext,
        url: str,
        filename: str,
        *,
        type_name: str,
        subtype: Optional[str] = None,
        region: Optional[str] = None,
        priority: int = 1,
        north_facing: Optional[bool] = None,
        south_facing: Optional[bool] = None,
        **kwargs: Any
    ) -> Optional[CollectionResult]:
        """
        Fetch data from a URL, save it, and create a result.
        
        Args:
            ctx: Collection context
            url: URL to fetch
            filename: Filename to save as
            type_name: Type of data
            subtype: Optional subtype
            region: Optional region
            priority: Priority (lower is higher)
            north_facing: Whether relevant for north-facing shores
            south_facing: Whether relevant for south-facing shores
            **kwargs: Additional keyword arguments for the HTTP client
            
        Returns:
            Collection result or None if fetch failed
        """
        try:
            data = await self.fetch(ctx, url, **kwargs)
            
            # Create result
            result = await self.create_result(
                ctx=ctx,
                source=self.name,
                filename=filename,
                data=data,
                type_name=type_name,
                subtype=subtype,
                region=region,
                priority=priority,
                url=url,
                north_facing=north_facing,
                south_facing=south_facing
            )
            
            self.logger.info(f"Successfully fetched and saved: {filename}")
            return result
        
        except Exception as e:
            self.logger.error(f"Failed to fetch {url}: {str(e)}")
            return None