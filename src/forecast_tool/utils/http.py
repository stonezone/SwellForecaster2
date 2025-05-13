"""HTTP client utilities for SwellForecaster."""
import asyncio
import json
import logging
import ssl
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union, cast
from urllib.parse import urlparse

import aiohttp
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

# Global HTTP session
_SESSION: Optional[aiohttp.ClientSession] = None
_SSL_CONTEXT: Dict[str, ssl.SSLContext] = {}
_RATE_LIMITERS: Dict[str, asyncio.Semaphore] = {}
_DOMAIN_SEMAPHORES: Dict[str, asyncio.Semaphore] = {}

logger = logging.getLogger("utils.http")


async def setup_http_client(
    *,
    timeout: int = 30,
    ssl_exceptions: List[str] = None,
    user_agent: str = "SwellForecaster/1.0",
    rate_limits: Dict[str, float] = None,
    concurrent_requests: int = 5,
) -> None:
    """
    Set up HTTP client with proper SSL handling and rate limiting.
    
    Args:
        timeout: Request timeout in seconds
        ssl_exceptions: List of domains to skip SSL verification
        user_agent: User agent string
        rate_limits: Domain-specific rate limits in seconds
        concurrent_requests: Maximum number of concurrent requests per domain
    """
    global _SESSION, _SSL_CONTEXT, _RATE_LIMITERS, _DOMAIN_SEMAPHORES
    
    if _SESSION is not None:
        await shutdown_http_client()
    
    # Create headers
    headers = {
        "User-Agent": user_agent
    }
    
    # Create timeout
    timeout_obj = aiohttp.ClientTimeout(total=timeout)
    
    # Create session
    _SESSION = aiohttp.ClientSession(headers=headers, timeout=timeout_obj)
    
    # Create SSL contexts for domains with SSL exceptions
    if ssl_exceptions:
        for domain in ssl_exceptions:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            _SSL_CONTEXT[domain] = ctx
    
    # Create rate limiters
    if rate_limits:
        for domain, rate in rate_limits.items():
            _RATE_LIMITERS[domain] = asyncio.Semaphore(1)
    
    # Create domain semaphores for concurrent request limiting
    # This prevents overwhelming a single domain with too many requests
    for domain in set(ssl_exceptions or []) | set(rate_limits or {}):
        _DOMAIN_SEMAPHORES[domain] = asyncio.Semaphore(concurrent_requests)
    
    logger.info(
        f"HTTP client initialized with timeout={timeout}s, "
        f"ssl_exceptions={len(_SSL_CONTEXT) if ssl_exceptions else 0}, "
        f"rate_limits={len(_RATE_LIMITERS) if rate_limits else 0}"
    )


async def shutdown_http_client() -> None:
    """Shut down HTTP client and clean up resources."""
    global _SESSION, _SSL_CONTEXT, _RATE_LIMITERS, _DOMAIN_SEMAPHORES
    
    if _SESSION is not None:
        await _SESSION.close()
        _SESSION = None
    
    _SSL_CONTEXT = {}
    _RATE_LIMITERS = {}
    _DOMAIN_SEMAPHORES = {}
    
    logger.info("HTTP client shutdown")


def _get_domain(url: str) -> str:
    """
    Get domain from URL.
    
    Args:
        url: URL string
        
    Returns:
        Domain part of the URL
    """
    parsed = urlparse(url)
    return parsed.netloc


def _get_ssl_context(url: str) -> Optional[ssl.SSLContext]:
    """
    Get SSL context for URL.
    
    Args:
        url: URL string
        
    Returns:
        SSL context for the domain, or None if not needed
    """
    domain = _get_domain(url)
    return _SSL_CONTEXT.get(domain)


async def _wait_for_rate_limit(url: str) -> None:
    """
    Wait for rate limit to be satisfied.
    
    Args:
        url: URL string
    """
    domain = _get_domain(url)
    if domain in _RATE_LIMITERS:
        rate_limiter = _RATE_LIMITERS[domain]
        await rate_limiter.acquire()
        
        # Release after predefined delay
        rate_delay = 0
        loop = asyncio.get_event_loop()
        loop.call_later(rate_delay, rate_limiter.release)


async def _with_domain_semaphore(url: str, coro: Callable) -> Any:
    """
    Execute coroutine with domain semaphore.
    
    Args:
        url: URL string
        coro: Coroutine to execute
        
    Returns:
        Result of the coroutine
    """
    domain = _get_domain(url)
    if domain in _DOMAIN_SEMAPHORES:
        async with _DOMAIN_SEMAPHORES[domain]:
            return await coro()
    else:
        # Default semaphore for unknown domains
        # This prevents overwhelming the event loop with too many connections
        # to a single domain that doesn't have explicit limits
        default_semaphore = asyncio.Semaphore(10)
        async with default_semaphore:
            return await coro()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
async def http_get(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
    verify_ssl: Optional[bool] = None,
) -> aiohttp.ClientResponse:
    """
    Make HTTP GET request with proper SSL handling and retries.
    
    Args:
        url: URL to fetch
        headers: Optional headers to include
        params: Optional query parameters
        timeout: Optional timeout override
        verify_ssl: Whether to verify SSL certificates
        
    Returns:
        Response object
    
    Raises:
        aiohttp.ClientError: If request fails
    """
    global _SESSION
    
    if _SESSION is None:
        raise RuntimeError("HTTP client not initialized. Call setup_http_client() first.")
    
    # Get SSL context
    ssl_context = _get_ssl_context(url)
    
    # Override SSL verification if requested
    if verify_ssl is not None:
        if not verify_ssl:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            ssl_context = ctx
    
    # Wait for rate limit
    await _wait_for_rate_limit(url)
    
    # Create timeout if specified
    timeout_obj = None
    if timeout is not None:
        timeout_obj = aiohttp.ClientTimeout(total=timeout)
    
    # Make request with domain semaphore
    async def do_request():
        return await _SESSION.get(
            url,
            ssl=ssl_context,
            headers=headers,
            params=params,
            timeout=timeout_obj,
            raise_for_status=False,
        )
    
    return await _with_domain_semaphore(url, do_request)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
async def http_post(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    json: Optional[Dict[str, Any]] = None,
    data: Optional[Union[Dict[str, Any], str, bytes]] = None,
    timeout: Optional[int] = None,
    verify_ssl: Optional[bool] = None,
) -> aiohttp.ClientResponse:
    """
    Make HTTP POST request with proper SSL handling and retries.
    
    Args:
        url: URL to fetch
        headers: Optional headers to include
        params: Optional query parameters
        json: Optional JSON body
        data: Optional form data
        timeout: Optional timeout override
        verify_ssl: Whether to verify SSL certificates
        
    Returns:
        Response object
    
    Raises:
        aiohttp.ClientError: If request fails
    """
    global _SESSION
    
    if _SESSION is None:
        raise RuntimeError("HTTP client not initialized. Call setup_http_client() first.")
    
    # Get SSL context
    ssl_context = _get_ssl_context(url)
    
    # Override SSL verification if requested
    if verify_ssl is not None:
        if not verify_ssl:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            ssl_context = ctx
    
    # Wait for rate limit
    await _wait_for_rate_limit(url)
    
    # Create timeout if specified
    timeout_obj = None
    if timeout is not None:
        timeout_obj = aiohttp.ClientTimeout(total=timeout)
    
    # Make request with domain semaphore
    async def do_request():
        return await _SESSION.post(
            url,
            ssl=ssl_context,
            headers=headers,
            params=params,
            json=json,
            data=data,
            timeout=timeout_obj,
            raise_for_status=False,
        )
    
    return await _with_domain_semaphore(url, do_request)


async def fetch(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
    verify_ssl: Optional[bool] = None,
    method: str = "GET",
    json_body: Optional[Dict[str, Any]] = None,
) -> bytes:
    """
    Fetch data from URL.
    
    Args:
        url: URL to fetch
        headers: Optional headers to include
        params: Optional query parameters
        timeout: Optional timeout override
        verify_ssl: Whether to verify SSL certificates
        method: HTTP method (GET or POST)
        json_body: Optional JSON body for POST requests
        
    Returns:
        Response data as bytes
    
    Raises:
        aiohttp.ClientError: If request fails
        ValueError: If HTTP status is not in 2xx range
    """
    if method.upper() == "GET":
        response = await http_get(
            url,
            headers=headers,
            params=params,
            timeout=timeout,
            verify_ssl=verify_ssl,
        )
    elif method.upper() == "POST":
        response = await http_post(
            url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=timeout,
            verify_ssl=verify_ssl,
        )
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    # Check status
    if response.status < 200 or response.status >= 300:
        raise ValueError(f"HTTP error {response.status}: {url}")
    
    # Get data
    data = await response.read()
    
    return data


async def fetch_json(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
    verify_ssl: Optional[bool] = None,
    method: str = "GET",
    json_body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Fetch JSON data from URL.
    
    Args:
        url: URL to fetch
        headers: Optional headers to include
        params: Optional query parameters
        timeout: Optional timeout override
        verify_ssl: Whether to verify SSL certificates
        method: HTTP method (GET or POST)
        json_body: Optional JSON body for POST requests
        
    Returns:
        Response data as JSON object
    
    Raises:
        aiohttp.ClientError: If request fails
        ValueError: If HTTP status is not in 2xx range
        json.JSONDecodeError: If response is not valid JSON
    """
    if method.upper() == "GET":
        response = await http_get(
            url,
            headers=headers,
            params=params,
            timeout=timeout,
            verify_ssl=verify_ssl,
        )
    elif method.upper() == "POST":
        response = await http_post(
            url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=timeout,
            verify_ssl=verify_ssl,
        )
    else:
        raise ValueError(f"Unsupported HTTP method: {method}")
    
    # Check status
    if response.status < 200 or response.status >= 300:
        raise ValueError(f"HTTP error {response.status}: {url}")
    
    # Get JSON data
    return await response.json()


# Create a retry decorator for HTTP operations
def http_retry(func=None, *, max_attempts=3, min_wait_seconds=1.0, max_wait_seconds=60.0):
    """
    Create a retry decorator for HTTP operations.

    Can be used as @http_retry or with parameters @http_retry(max_attempts=5)

    Args:
        func: The function to wrap (used when decorator is called without parameters)
        max_attempts: Maximum number of attempts
        min_wait_seconds: Minimum wait time between attempts
        max_wait_seconds: Maximum wait time between attempts

    Returns:
        Decorated function or decorator
    """
    # Create the actual decorator
    def decorator(f):
        # Apply tenacity retry
        retry_decorator = retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=min_wait_seconds, max=max_wait_seconds),
            retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, ValueError)),
            before_sleep=before_sleep_log(logger, logging.WARNING)
        )

        # Apply the retry decorator
        return retry_decorator(f)

    # If called without parameters, apply the decorator immediately
    if func is not None:
        return decorator(func)

    # If called with parameters, return the decorator
    return decorator


# Synchronous versions of HTTP functions for backward compatibility
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    retry=retry_if_exception_type((requests.RequestException, ssl.SSLError, ConnectionError)),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def fetch_sync(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
    verify_ssl: Optional[bool] = None,
    method: str = "GET",
    json_body: Optional[Dict[str, Any]] = None,
) -> bytes:
    """
    Fetch data from URL synchronously with retry logic.

    Args:
        url: URL to fetch
        headers: Optional HTTP headers
        params: Optional query parameters
        timeout: Request timeout in seconds
        verify_ssl: Whether to verify SSL certificates
        method: HTTP method (GET, POST, etc.)
        json_body: Optional JSON body for POST/PUT requests

    Returns:
        Response content as bytes

    Raises:
        requests.exceptions.RequestException: If the request fails after retries
    """
    # Determine SSL verification from domain if not specified
    if verify_ssl is None:
        domain = urlparse(url).netloc
        if domain in _SSL_CONTEXT:
            verify_ssl = False
        else:
            verify_ssl = True

    # Set up request kwargs
    kwargs = {
        "headers": headers or {},
        "params": params or {},
        "timeout": timeout or 30,
        "verify": verify_ssl,
    }

    # Add JSON body if provided and method supports it
    if json_body and method in ["POST", "PUT", "PATCH"]:
        kwargs["json"] = json_body

    # Log the request
    logger.debug(f"{method} {url} (sync)")

    # Make the request
    start_time = time.time()
    response = requests.request(method, url, **kwargs)
    elapsed = time.time() - start_time

    # Check for success
    response.raise_for_status()

    # Log success
    logger.debug(f"{method} {url} - {response.status_code} ({elapsed:.2f}s)")

    return response.content


def fetch_json_sync(
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
    verify_ssl: Optional[bool] = None,
    method: str = "GET",
    json_body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Fetch JSON data from URL synchronously.

    Args:
        url: URL to fetch
        headers: Optional HTTP headers
        params: Optional query parameters
        timeout: Request timeout in seconds
        verify_ssl: Whether to verify SSL certificates
        method: HTTP method (GET, POST, etc.)
        json_body: Optional JSON body for POST/PUT requests

    Returns:
        Parsed JSON data

    Raises:
        requests.exceptions.RequestException: If the request fails
        json.JSONDecodeError: If the response is not valid JSON
    """
    content = fetch_sync(
        url,
        headers=headers,
        params=params,
        timeout=timeout,
        verify_ssl=verify_ssl,
        method=method,
        json_body=json_body,
    )
    return json.loads(content)