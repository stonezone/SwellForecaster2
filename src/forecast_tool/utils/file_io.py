"""File I/O utilities for SwellForecaster."""
import asyncio
import json
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

# Try to import aiofiles for async file I/O
try:
    import aiofiles
    HAS_AIOFILES = True
except ImportError:
    HAS_AIOFILES = False

logger = logging.getLogger("utils.file_io")


def ensure_dir(path: Union[str, Path]) -> Path:
    """
    Ensure that a directory exists, creating it if necessary.
    
    Args:
        path: Directory path
        
    Returns:
        Path object for the directory
    """
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=True)
    return path_obj


async def read_file(path: Union[str, Path]) -> str:
    """
    Read a file asynchronously.
    
    Args:
        path: Path to the file
        
    Returns:
        File contents as a string
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If there's an error reading the file
    """
    path_obj = Path(path)
    
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found: {path}")
    
    if HAS_AIOFILES:
        async with aiofiles.open(path_obj, "r", encoding="utf-8") as f:
            return await f.read()
    else:
        # Fallback to asyncio.to_thread on Python 3.9+
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _read_file_sync, path_obj)


def _read_file_sync(path: Path) -> str:
    """
    Read a file synchronously.
    
    Args:
        path: Path to the file
        
    Returns:
        File contents as a string
    """
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


async def read_binary_file(path: Union[str, Path]) -> bytes:
    """
    Read a binary file asynchronously.
    
    Args:
        path: Path to the file
        
    Returns:
        File contents as bytes
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If there's an error reading the file
    """
    path_obj = Path(path)
    
    if not path_obj.exists():
        raise FileNotFoundError(f"File not found: {path}")
    
    if HAS_AIOFILES:
        async with aiofiles.open(path_obj, "rb") as f:
            return await f.read()
    else:
        # Fallback to asyncio.to_thread on Python 3.9+
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _read_binary_file_sync, path_obj)


def _read_binary_file_sync(path: Path) -> bytes:
    """
    Read a binary file synchronously.
    
    Args:
        path: Path to the file
        
    Returns:
        File contents as bytes
    """
    with open(path, "rb") as f:
        return f.read()


async def write_file(path: Union[str, Path], content: str) -> None:
    """
    Write a file asynchronously.

    Args:
        path: Path to the file
        content: File contents

    Raises:
        IOError: If there's an error writing the file
    """
    path_obj = Path(path)

    # Ensure the parent directory exists
    ensure_dir(path_obj.parent)

    if HAS_AIOFILES:
        async with aiofiles.open(path_obj, "w", encoding="utf-8") as f:
            await f.write(content)
    else:
        # Fallback to asyncio.to_thread on Python 3.9+
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _write_file_sync, path_obj, content)


async def write_binary(path: Union[str, Path], content: bytes) -> None:
    """
    Write binary data to a file asynchronously.

    Args:
        path: Path to the file
        content: Binary file contents

    Raises:
        IOError: If there's an error writing the file
    """
    path_obj = Path(path)

    # Ensure the parent directory exists
    ensure_dir(path_obj.parent)

    if HAS_AIOFILES:
        async with aiofiles.open(path_obj, "wb") as f:
            await f.write(content)
    else:
        # Fallback to asyncio.to_thread on Python 3.9+
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _write_binary_sync, path_obj, content)


def _write_file_sync(path: Path, content: str) -> None:
    """
    Write a file synchronously.

    Args:
        path: Path to the file
        content: File contents
    """
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _write_binary_sync(path: Path, content: bytes) -> None:
    """
    Write binary data to a file synchronously.

    Args:
        path: Path to the file
        content: Binary file contents
    """
    with open(path, "wb") as f:
        f.write(content)


def _write_file_sync(path: Path, content: str) -> None:
    """
    Write a file synchronously.

    Args:
        path: Path to the file
        content: File contents
    """
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


async def write_binary_file(path: Union[str, Path], content: bytes) -> None:
    """
    Write a binary file asynchronously.
    
    Args:
        path: Path to the file
        content: File contents
        
    Raises:
        IOError: If there's an error writing the file
    """
    path_obj = Path(path)
    
    # Ensure the parent directory exists
    ensure_dir(path_obj.parent)
    
    if HAS_AIOFILES:
        async with aiofiles.open(path_obj, "wb") as f:
            await f.write(content)
    else:
        # Fallback to asyncio.to_thread on Python 3.9+
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _write_binary_file_sync, path_obj, content)


def _write_binary_file_sync(path: Path, content: bytes) -> None:
    """
    Write a binary file synchronously.
    
    Args:
        path: Path to the file
        content: File contents
    """
    with open(path, "wb") as f:
        f.write(content)


async def read_json(path: Union[str, Path]) -> Dict[str, Any]:
    """
    Read a JSON file asynchronously.
    
    Args:
        path: Path to the file
        
    Returns:
        Parsed JSON data
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If there's an error reading the file
        json.JSONDecodeError: If the JSON is invalid
    """
    content = await read_file(path)
    return json.loads(content)


async def write_json(path: Union[str, Path], data: Dict[str, Any], indent: int = 2) -> None:
    """
    Write a JSON file asynchronously.
    
    Args:
        path: Path to the file
        data: Data to write
        indent: Indentation level for pretty-printing
        
    Raises:
        IOError: If there's an error writing the file
    """
    content = json.dumps(data, indent=indent)
    await write_file(path, content)


async def save_to_file(
    data: Union[str, bytes],
    filename: str,
    base_dir: Union[str, Path],
    *,
    makedirs: bool = True
) -> Path:
    """
    Save data to a file.
    
    Args:
        data: Data to save
        filename: Name of the file
        base_dir: Base directory
        makedirs: Whether to create directories if they don't exist
        
    Returns:
        Path to the saved file
        
    Raises:
        IOError: If there's an error writing the file
    """
    base_path = Path(base_dir)
    
    if makedirs:
        ensure_dir(base_path)
    
    file_path = base_path / filename
    
    if isinstance(data, str):
        await write_file(file_path, data)
    else:
        await write_binary_file(file_path, data)
    
    logger.debug(f"Saved file: {file_path}")
    return file_path


def list_files(
    path: Union[str, Path],
    *,
    pattern: str = "*",
    recursive: bool = False,
    sort_by_mtime: bool = False
) -> List[Path]:
    """
    List files in a directory.
    
    Args:
        path: Directory path
        pattern: Glob pattern for filtering files
        recursive: Whether to search recursively
        sort_by_mtime: Whether to sort by modification time (newest first)
        
    Returns:
        List of matching file paths
    """
    path_obj = Path(path)
    
    if not path_obj.exists():
        raise FileNotFoundError(f"Directory not found: {path}")
    
    if not path_obj.is_dir():
        raise NotADirectoryError(f"Not a directory: {path}")
    
    if recursive:
        files = list(path_obj.rglob(pattern))
    else:
        files = list(path_obj.glob(pattern))
    
    # Filter out directories
    files = [f for f in files if f.is_file()]
    
    # Sort by modification time if requested
    if sort_by_mtime:
        files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    
    return files


def create_timestamped_dir(base_dir: Union[str, Path], prefix: str = "") -> Path:
    """
    Create a timestamped directory.
    
    Args:
        base_dir: Base directory
        prefix: Optional prefix for the directory name
        
    Returns:
        Path to the created directory
    """
    # Create base directory if it doesn't exist
    base_path = Path(base_dir)
    ensure_dir(base_path)
    
    # Create timestamped directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if prefix:
        dir_name = f"{prefix}_{timestamp}"
    else:
        dir_name = timestamp
    
    dir_path = base_path / dir_name
    ensure_dir(dir_path)
    
    logger.debug(f"Created timestamped directory: {dir_path}")
    return dir_path


def copy_file(src: Union[str, Path], dst: Union[str, Path]) -> Path:
    """
    Copy a file.

    Args:
        src: Source file path
        dst: Destination file path

    Returns:
        Path to the destination file

    Raises:
        FileNotFoundError: If the source file doesn't exist
        IOError: If there's an error copying the file
    """
    src_path = Path(src)
    dst_path = Path(dst)

    if not src_path.exists():
        raise FileNotFoundError(f"Source file not found: {src}")

    # Ensure the parent directory exists
    ensure_dir(dst_path.parent)

    shutil.copy2(src_path, dst_path)

    logger.debug(f"Copied file: {src} -> {dst}")
    return dst_path


# Synchronous versions of file operations for compatibility with legacy code

def read_json_sync(path: Union[str, Path]) -> Dict[str, Any]:
    """
    Read a JSON file synchronously.

    Args:
        path: Path to the file

    Returns:
        Parsed JSON data

    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If there's an error reading the file
        json.JSONDecodeError: If the JSON is invalid
    """
    path_obj = Path(path)

    if not path_obj.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with open(path_obj, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json_sync(path: Union[str, Path], data: Dict[str, Any], indent: int = 2) -> None:
    """
    Write a JSON file synchronously.

    Args:
        path: Path to the file
        data: Data to write
        indent: Indentation level for pretty-printing

    Raises:
        IOError: If there's an error writing the file
    """
    path_obj = Path(path)

    # Ensure the parent directory exists
    ensure_dir(path_obj.parent)

    with open(path_obj, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent)


def jload(path: Union[str, Path]) -> Dict[str, Any]:
    """
    Legacy-compatible function to read a JSON file.

    Args:
        path: Path to the file

    Returns:
        Parsed JSON data
    """
    return read_json_sync(path)


def jdump(path: Union[str, Path], data: Dict[str, Any], indent: int = 2) -> None:
    """
    Legacy-compatible function to write a JSON file.

    Args:
        path: Path to the file
        data: Data to write
        indent: Indentation level for pretty-printing
    """
    write_json_sync(path, data, indent)