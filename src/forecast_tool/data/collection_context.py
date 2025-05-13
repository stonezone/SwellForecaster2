"""Context manager for data collection operations in SwellForecaster."""
import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import aiohttp
from pydantic import BaseModel, Field

from ..models.settings import Settings
from ..utils.file_io import (
    ensure_dir,
    read_file,
    write_file,
    read_json,
    write_json,
    list_files,
    create_timestamped_dir
)
from ..utils.http import setup_http_client, shutdown_http_client

logger = logging.getLogger("collection_context")


class CollectionResult(BaseModel):
    """Represents a single collected data item."""
    
    source: str = Field(..., description="Source of the data (e.g., NDBC, ECMWF)")
    filename: str = Field(..., description="Filename where the data is stored")
    type: str = Field(..., description="Type of data (e.g., buoy, model, chart)")
    subtype: Optional[str] = Field(None, description="Subtype for more specific categorization")
    region: Optional[str] = Field(None, description="Geographic region (e.g., hawaii, north_pacific)")
    priority: int = Field(1, description="Priority of the data (lower is higher priority)")
    timestamp: str = Field(..., description="ISO 8601 timestamp of collection")
    url: Optional[str] = Field(None, description="Source URL if applicable")
    error: Optional[bool] = Field(None, description="Whether there was an error collecting this data")
    north_facing: Optional[bool] = Field(None, description="Whether this data is relevant for north-facing shores")
    south_facing: Optional[bool] = Field(None, description="Whether this data is relevant for south-facing shores")
    
    # Computed field for full path
    path: Optional[Path] = Field(None, exclude=True)


class CollectionMetadata(BaseModel):
    """Metadata for a data collection run."""
    
    run_id: str = Field(..., description="Unique identifier for the collection run")
    timestamp: str = Field(..., description="ISO 8601 timestamp of collection")
    results: List[CollectionResult] = Field(default_factory=list, description="Results of the collection")


class CollectionContext:
    """
    Context manager for data collection operations.

    Handles directory management, file saving, and metadata tracking.
    """

    def __init__(self, settings: Settings):
        """
        Initialize the collection context.

        Args:
            settings: Application settings
        """
        self.settings = settings
        self.run_id = f"{uuid.uuid4().hex}_{int(time.time())}"
        self.base_dir = Path(settings.general.data_dir)
        self.bundle_dir = self.base_dir / self.run_id
        self.headers = {"User-Agent": settings.general.user_agent}
        self.timeout = settings.general.timeout
        self.max_retries = settings.general.max_retries
        self.rate_limits = {"windy.com": settings.general.windy_throttle_seconds}
        self.ssl_exceptions = settings.ssl_exceptions.get_domains()
        self.results: List[CollectionResult] = []
        self.http_client: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self) -> "CollectionContext":
        """
        Set up the collection context.

        Creates necessary directories and initializes resources.

        Returns:
            The collection context instance
        """
        # Create bundle directory
        self.bundle_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created bundle directory: {self.bundle_dir}")

        # Set up HTTP client
        await setup_http_client(
            timeout=self.settings.general.timeout,
            ssl_exceptions=self.settings.ssl_exceptions.get_domains(),
            user_agent=self.settings.general.user_agent,
            rate_limits=self.rate_limits
        )
        # Create a new session with the same headers and timeout
        timeout_obj = aiohttp.ClientTimeout(total=self.timeout)
        self.http_client = aiohttp.ClientSession(headers=self.headers, timeout=timeout_obj)

        # Prune old bundles if necessary
        await self._prune_old_bundles(days=7)  # Default to 7 days retention

        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Clean up the collection context.

        Writes metadata and performs any necessary cleanup.
        """
        try:
            if exc_type:
                logger.error(f"Error during collection: {exc_val}")
                # Write partial metadata even on error
                await self._write_metadata()
                return False

            # Write metadata and update latest bundle pointer
            await self._write_metadata()
            return True
        finally:
            # Clean up HTTP client
            if self.http_client:
                await self.http_client.close()
                self.http_client = None
            await shutdown_http_client()
    
    async def _write_metadata(self):
        """Write metadata for the collection run."""
        # Create metadata
        metadata = CollectionMetadata(
            run_id=self.run_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            results=self.results
        )
        
        # Write metadata file
        await write_json(self.bundle_dir / "metadata.json", metadata.model_dump())
        
        # Update latest bundle pointer
        await write_file(self.base_dir / "latest_bundle.txt", self.run_id)
        
        logger.info(f"Bundle {self.run_id} complete ({len(self.results)} files)")
    
    async def _prune_old_bundles(self, days: int = 7):
        """
        Remove bundles older than the specified number of days.
        
        Args:
            days: Number of days to keep bundles
        """
        # Calculate cutoff date
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Get list of directories
        try:
            directories = await list_files(self.base_dir)
        except Exception as e:
            logger.error(f"Failed to list bundles directory: {e}")
            return
        
        # Identify directories to prune
        for directory in directories:
            if not directory.is_dir():
                continue
            
            try:
                # Check directory modification time
                mtime = datetime.fromtimestamp(directory.stat().st_mtime, timezone.utc)
                if mtime < cutoff:
                    logger.info(f"Pruning old bundle: {directory}")
                    
                    # Get list of files
                    try:
                        files = await list_files(directory)
                        
                        # Delete files
                        for file in files:
                            if file.is_file():
                                try:
                                    file.unlink()
                                except Exception as e:
                                    logger.warning(f"Failed to delete file {file}: {e}")
                        
                        # Remove directory
                        try:
                            directory.rmdir()
                            logger.info(f"Pruned bundle: {directory}")
                        except Exception as e:
                            logger.warning(f"Failed to remove directory {directory}: {e}")
                    
                    except Exception as e:
                        logger.warning(f"Failed to list files in {directory}: {e}")
            
            except Exception as e:
                logger.warning(f"Failed to check age of {directory}: {e}")
    
    async def save(self, filename: str, data: Union[str, bytes]) -> str:
        """
        Save data to the bundle directory.
        
        Args:
            filename: Name to save the file as
            data: Data to save (string or bytes)
            
        Returns:
            The filename (not the full path)
        """
        path = self.bundle_dir / filename
        await write_file(path, data)
        return filename
    
    def add_result(self, result: Union[CollectionResult, Dict[str, Any]]) -> None:
        """
        Add a collection result.
        
        Args:
            result: Collection result to add (either a CollectionResult instance or a dictionary)
        """
        if isinstance(result, dict):
            # Convert dictionary to CollectionResult
            if "timestamp" not in result:
                result["timestamp"] = datetime.now(timezone.utc).isoformat()
            
            # Create CollectionResult
            result_obj = CollectionResult(**result)
        else:
            result_obj = result
        
        # Add path field
        result_obj.path = self.bundle_dir / result_obj.filename
        
        # Add to results list
        self.results.append(result_obj)
    
    def add_results(self, results: List[Union[CollectionResult, Dict[str, Any]]]) -> None:
        """
        Add multiple collection results.
        
        Args:
            results: Collection results to add
        """
        for result in results:
            self.add_result(result)
    
    async def get_latest_bundle_id(self) -> str:
        """
        Get the latest bundle ID.

        Returns:
            The latest bundle ID
        """
        latest_bundle_path = self.base_dir / "latest_bundle.txt"
        from ..utils.file_io import file_exists
        if await file_exists(latest_bundle_path):
            content = await read_file(latest_bundle_path)
            return content.strip()
        else:
            return ""