#!/usr/bin/env python3
# utils.py – enhanced helper toolkit
from __future__ import annotations
import json, logging, sys, asyncio, re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, Callable, BinaryIO
from io import BytesIO
import functools

# Try to import aiofiles for async file operations
try:
    import aiofiles
    AIOFILES_AVAILABLE = True
except ImportError:
    AIOFILES_AVAILABLE = False
    import logging
    logging.getLogger("utils").warning("aiofiles not available, falling back to synchronous I/O with asyncio.to_thread")
    logging.getLogger("utils").warning("Consider installing aiofiles with 'pip install aiofiles' for better performance")
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

def utcnow() -> str:
    """RFC‑3339 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()

def jdump(obj) -> str:
    """Pretty-print JSON with consistent formatting."""
    return json.dumps(obj, indent=2, sort_keys=True)

def log_init(name: str, level: str | int = "INFO") -> logging.Logger:
    """Initialize logging with file and console output.
    
    Args:
        name: Logger name (used for log file)
        level: Logging level (INFO, DEBUG, etc.)
        
    Returns:
        Configured logger instance
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper())
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)
    
    # File handler
    try:
        # Ensure logs directory exists
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # Use ISO 8601 date format
        date_str = datetime.now().strftime("%Y%m%d")
        file_path = log_dir / f"{name}_{date_str}.log"
        
        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        logger.warning(f"Could not set up log file: {e}")
    
    return logger

def getint_safe(cfg, section, key, default=0):
    """
    Safely get an integer from ConfigParser with fallback.
    
    Args:
        cfg: ConfigParser object
        section: Section name
        key: Key to look up
        default: Default value if key not found or not an integer
        
    Returns:
        Integer value or default
    """
    try:
        return int(cfg[section][key])
    except (KeyError, ValueError, TypeError):
        return default

def getfloat_safe(cfg, section, key, default=0.0):
    """
    Safely get a float from ConfigParser with fallback.
    
    Args:
        cfg: ConfigParser object
        section: Section name
        key: Key to look up
        default: Default value if key not found or not a float
        
    Returns:
        Float value or default
    """
    try:
        return float(cfg[section][key])
    except (KeyError, ValueError, TypeError):
        return default

def getbool_safe(cfg, section, key, default=False):
    """
    Safely get a boolean from ConfigParser with fallback.
    
    Args:
        cfg: ConfigParser object
        section: Section name
        key: Key to look up
        default: Default value if key not found or not a boolean
        
    Returns:
        Boolean value or default
    """
    try:
        return cfg[section].getboolean(key, default)
    except (KeyError, ValueError, TypeError):
        return default

def clean_buoy_value(value: Any) -> Optional[float]:
    """
    Clean buoy data values that may contain operators or invalid markers.
    Enhanced to handle more edge cases and properly process comparison operators.
    
    Args:
        value: Raw value from buoy data
        
    Returns:
        Cleaned float value or None if invalid
    """
    if value is None:
        return None

    # Handle numeric values directly
    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        # Strip whitespace first
        value = value.strip()

        # Handle special case: empty string
        if value == '':
            return None

        # Handle missing value indicators
        if value.upper() in ['MM', 'NA', 'MISSING', 'N/A', 'NULL']:
            return None

        # Extract numeric part from strings with comparison operators
        if any(c in value for c in "<>="):
            # Keep only digits, decimal points, and negative signs
            numeric_str = ''.join(c for c in value if c.isdigit() or c == '.' or c == '-')
            try:
                if numeric_str:
                    return float(numeric_str)
                else:
                    return None
            except ValueError:
                return None

        # Handle cardinal directions - they're not numeric values
        if value.upper() in ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                            'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']:
            # For MWD (wave direction), convert cardinal to degrees
            return cardinal_to_degrees(value.upper())

        # Standard numeric string case
        try:
            return float(value)
        except ValueError:
            # One last attempt to extract numeric portion
            numeric_match = re.search(r'[-+]?\d*\.?\d+', value)
            if numeric_match:
                try:
                    return float(numeric_match.group(0))
                except ValueError:
                    return None

    return None

def cardinal_to_degrees(direction):
    """Convert cardinal direction to degrees."""
    direction_map = {
        "N": 0, "NNE": 22.5, "NE": 45, "ENE": 67.5,
        "E": 90, "ESE": 112.5, "SE": 135, "SSE": 157.5,
        "S": 180, "SSW": 202.5, "SW": 225, "WSW": 247.5,
        "W": 270, "WNW": 292.5, "NW": 315, "NNW": 337.5
    }
    return direction_map.get(direction.upper())

def create_placeholder_image(message: str, width: int = 600, height: int = 400, 
                           filename: Optional[str] = None) -> bytes:
    """
    Create a simple placeholder image with text when a real image is unavailable.
    
    Args:
        message: Text message to display on the image
        width: Image width
        height: Image height
        filename: Optional name to include in the image
        
    Returns:
        Image data as bytes
    """
    if not PIL_AVAILABLE:
        # If PIL is not available, return a minimal SVG
        svg = f"""<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">
            <rect width="100%" height="100%" fill="#f0f0f0"/>
            <text x="50%" y="50%" font-family="Arial" font-size="14" text-anchor="middle">{message}</text>
        </svg>"""
        return svg.encode('utf-8')
    
    # Create a blank image with a light gray background
    img = Image.new('RGB', (width, height), color=(240, 240, 240))
    draw = ImageDraw.Draw(img)
    
    # Try to use a nice font, fall back to default if not available
    try:
        # Different options for different systems
        font_options = [
            ('Arial', '/Library/Fonts/Arial.ttf'),
            ('DejaVuSans', '/usr/share/fonts/TTF/DejaVuSans.ttf'),
            None  # Default
        ]
        
        font = None
        for font_name, font_path in font_options:
            try:
                font = ImageFont.truetype(font_path, 14)
                break
            except (OSError, IOError):
                continue
        
        if font is None:
            font = ImageFont.load_default()
            
    except Exception:
        # If all font attempts fail, use the default
        font = ImageFont.load_default()
    
    # Draw the error message centered on the image
    text_width, text_height = draw.textsize(message, font=font)
    position = ((width - text_width) // 2, (height - text_height) // 2)
    draw.text(position, message, fill=(0, 0, 0), font=font)
    
    # If a filename was provided, add it at the bottom
    if filename:
        filename_text = f"Placeholder for: {filename}"
        text_width, text_height = draw.textsize(filename_text, font=font)
        position = ((width - text_width) // 2, height - text_height - 20)
        draw.text(position, filename_text, fill=(100, 100, 100), font=font)
    
    # Add timestamp
    timestamp = f"Generated: {utcnow()}"
    text_width, text_height = draw.textsize(timestamp, font=font)
    position = ((width - text_width) // 2, height - text_height - 5)
    draw.text(position, timestamp, fill=(150, 150, 150), font=font)
    
    # Convert to bytes
    buffer = BytesIO()
    img.save(buffer, format='PNG')
    return buffer.getvalue()

def retry_async(max_retries=3, backoff_base=2):
    """
    Decorator for async functions to add retry logic with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        backoff_base: Base for exponential backoff calculation (2 means wait 2^attempt seconds)
        
    Returns:
        Decorated function with retry logic
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            logger = logging.getLogger(f"utils.retry.{func.__name__}")
            
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.warning(f"Failed after {max_retries} attempts: {e}")
                        # Re-raise the original exception to preserve traceback
                        raise
                    
                    wait_time = backoff_base ** attempt
                    logger.info(f"Retry {attempt+1}/{max_retries} after error: {e}, waiting {wait_time}s")
                    await asyncio.sleep(wait_time)
            
            # This should never be reached but just in case
            raise RuntimeError("Unexpected code path in retry_async")
            
        return wrapper
    return decorator

async def fetch_with_retry(ctx, session, url: str, max_retries: int = 3,
                          filename: Optional[str] = None, **kwargs) -> Tuple[Optional[bytes], Optional[str]]:
    """
    Fetch data from a URL with retry logic and proper error handling.

    Args:
        ctx: Context object with fetch and save methods
        session: aiohttp ClientSession object
        url: URL to fetch
        max_retries: Maximum number of retry attempts
        filename: Optional filename to save the data
        **kwargs: Additional keyword arguments to pass to ctx.fetch

    Returns:
        Tuple of (data, filename) where data is the fetched binary data and
        filename is the saved filename (if any)
    """
    logger = logging.getLogger("utils.fetch")

    @retry_async(max_retries=max_retries)
    async def _fetch():
        # Use the context's fetch method which already has SSL handling
        return await ctx.fetch(session, url, **kwargs)

    try:
        data = await _fetch()

        if data and filename:
            # Try to use async save if available, otherwise fall back to sync version
            try:
                if hasattr(ctx, 'save_async'):
                    fn = await ctx.save_async(filename, data)
                else:
                    fn = ctx.save(filename, data)
            except Exception as e:
                logger.warning(f"Error saving file {filename}: {e}, falling back to sync save")
                fn = ctx.save(filename, data)
            return data, fn
        return data, None
    except Exception as e:
        logger.warning(f"Failed to fetch: {url} - {e}")
        return None, None

async def read_file_async(path: Union[str, Path]) -> str:
    """
    Read a file asynchronously.

    Args:
        path: Path to the file (string or Path object)

    Returns:
        File contents as a string
    """
    path = Path(path)

    if AIOFILES_AVAILABLE:
        async with aiofiles.open(path, mode='r') as f:
            return await f.read()
    else:
        # Fallback to using asyncio.to_thread for Python 3.9+
        try:
            return await asyncio.to_thread(path.read_text)
        except (AttributeError, NotImplementedError):
            # For older Python versions, use run_in_executor as fallback
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, path.read_text)

async def read_binary_async(path: Union[str, Path]) -> bytes:
    """
    Read a binary file asynchronously.

    Args:
        path: Path to the file (string or Path object)

    Returns:
        File contents as bytes
    """
    path = Path(path)

    if AIOFILES_AVAILABLE:
        async with aiofiles.open(path, mode='rb') as f:
            return await f.read()
    else:
        # Fallback to using asyncio.to_thread for Python 3.9+
        try:
            return await asyncio.to_thread(path.read_bytes)
        except (AttributeError, NotImplementedError):
            # For older Python versions, use run_in_executor as fallback
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, path.read_bytes)

async def write_file_async(path: Union[str, Path], content: Union[str, bytes]) -> None:
    """
    Write to a file asynchronously.

    Args:
        path: Path to the file (string or Path object)
        content: Content to write (string or bytes)
    """
    path = Path(path)
    mode = 'wb' if isinstance(content, bytes) else 'w'

    # Create parent directories if they don't exist
    path.parent.mkdir(parents=True, exist_ok=True)

    if AIOFILES_AVAILABLE:
        async with aiofiles.open(path, mode=mode) as f:
            await f.write(content)
    else:
        # Fallback to using asyncio.to_thread for Python 3.9+
        try:
            write_func = path.write_bytes if isinstance(content, bytes) else path.write_text
            await asyncio.to_thread(write_func, content)
        except (AttributeError, NotImplementedError):
            # For older Python versions, use run_in_executor as fallback
            loop = asyncio.get_event_loop()
            write_func = path.write_bytes if isinstance(content, bytes) else path.write_text
            await loop.run_in_executor(None, write_func, content)

async def json_load_async(path: Union[str, Path]) -> Dict:
    """
    Load JSON from a file asynchronously.

    Args:
        path: Path to the JSON file

    Returns:
        Parsed JSON as a dictionary
    """
    content = await read_file_async(path)
    return json.loads(content)

async def json_save_async(path: Union[str, Path], data: Any, indent: int = 2) -> None:
    """
    Save data as JSON to a file asynchronously.

    Args:
        path: Path to save the JSON file
        data: Data to serialize as JSON
        indent: JSON indentation (default: 2)
    """
    json_str = json.dumps(data, indent=indent, sort_keys=True)
    await write_file_async(path, json_str)