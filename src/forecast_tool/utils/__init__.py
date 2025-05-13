"""Utility functions for SwellForecaster."""
from .file_io import (
    ensure_dir,
    read_file,
    read_binary_file,
    write_file,
    write_binary_file,
    read_json,
    write_json,
    save_to_file,
    list_files,
    create_timestamped_dir,
    copy_file,
    # Sync versions for compatibility
    read_json_sync,
    write_json_sync,
    jload,
    jdump as jdump_file,
)
from .http import (
    setup_http_client,
    shutdown_http_client,
    http_get,
    http_post,
    fetch,
    fetch_json,
    http_retry,
    # Sync versions for compatibility
    fetch_sync,
    fetch_json_sync,
)
from .image_utils import (
    create_placeholder_image,
)
from .logging_config import (
    setup_logging,
    get_logging_config,
    get_logger,
)
from .retry import (
    retry_async,
    retry_sync,
)
from .wave_analysis import (
    get_south_swell_status,
    get_period_multiplier,
    calculate_face_height,
    extract_significant_south_swells,
    get_buoy_data_from_bundle,
)

# Additional utility functions
import datetime
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


def utcnow() -> str:
    """RFC‑3339 UTC timestamp."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def jdump(obj: Any, path: Optional[Union[str, Path]] = None) -> str:
    """
    Pretty-print JSON with consistent formatting and optionally write to file.

    Args:
        obj: The object to serialize to JSON
        path: Optional file path to write the JSON to

    Returns:
        JSON string representation of the object
    """
    json_str = json.dumps(obj, indent=2, sort_keys=True)

    # Write to file if path is provided
    if path is not None:
        path_obj = Path(path)
        # Ensure the parent directory exists
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        # Write to file
        with open(path_obj, "w", encoding="utf-8") as f:
            f.write(json_str)

    return json_str


__all__ = [
    # File I/O
    "ensure_dir",
    "read_file",
    "read_binary_file",
    "write_file",
    "write_binary_file",
    "read_json",
    "write_json",
    "save_to_file",
    "list_files",
    "create_timestamped_dir",
    "copy_file",
    "read_json_sync",
    "write_json_sync",
    "jload",
    "jdump_file",

    # HTTP
    "setup_http_client",
    "shutdown_http_client",
    "http_get",
    "http_post",
    "fetch",
    "fetch_json",
    "http_retry",
    "fetch_sync",
    "fetch_json_sync",

    # Image utilities
    "create_placeholder_image",

    # Logging
    "setup_logging",
    "get_logging_config",
    "get_logger",

    # Retry
    "retry_async",
    "retry_sync",

    # Wave Analysis
    "get_south_swell_status",
    "get_period_multiplier",
    "calculate_face_height",
    "extract_significant_south_swells",
    "get_buoy_data_from_bundle",

    # Miscellaneous
    "utcnow",
    "jdump",
]