"""Retry decorators for SwellForecaster."""
import asyncio
import functools
import logging
from typing import Any, Callable, Optional, Type, TypeVar, Union, cast

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
    RetryError,
)

F = TypeVar('F', bound=Callable[..., Any])

logger = logging.getLogger("utils.retry")


def retry_async(
    max_attempts: int = 3,
    min_wait_seconds: float = 1.0,
    max_wait_seconds: float = 60.0,
    exception_types: Optional[Union[Type[Exception], tuple[Type[Exception], ...]]] = None,
    logger_name: Optional[str] = None,
) -> Callable[[F], F]:
    """
    Decorator for adding retry logic to async functions.
    
    Args:
        max_attempts: Maximum number of attempts
        min_wait_seconds: Minimum wait time between attempts
        max_wait_seconds: Maximum wait time between attempts
        exception_types: Exception types to retry on (default: Exception)
        logger_name: Logger name for logging retries
        
    Returns:
        Decorated function
    """
    # Set default exception types if not specified
    if exception_types is None:
        exception_types = (Exception,)
    
    # Get logger
    log = logging.getLogger(logger_name or "utils.retry")
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Create retry decorator
            retry_decorator = retry(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(
                    multiplier=1,
                    min=min_wait_seconds,
                    max=max_wait_seconds,
                ),
                retry=retry_if_exception_type(exception_types),
                before_sleep=before_sleep_log(log, logging.WARNING),
            )
            
            # Apply retry decorator to function
            retry_func = retry_decorator(func)
            
            try:
                # Call function with retry logic
                return await retry_func(*args, **kwargs)
            except RetryError as e:
                # Log retry error with original exception
                if e.last_attempt.exception() is not None:
                    original_exception = e.last_attempt.exception()
                    log.error(
                        f"Failed after {max_attempts} attempts: {original_exception.__class__.__name__}: {original_exception}"
                    )
                else:
                    log.error(f"Failed after {max_attempts} attempts")
                
                # Re-raise original exception
                raise e.last_attempt.exception() or e
        
        return cast(F, wrapper)
    
    return decorator


def retry_sync(
    max_attempts: int = 3,
    min_wait_seconds: float = 1.0,
    max_wait_seconds: float = 60.0,
    exception_types: Optional[Union[Type[Exception], tuple[Type[Exception], ...]]] = None,
    logger_name: Optional[str] = None,
) -> Callable[[F], F]:
    """
    Decorator for adding retry logic to sync functions.
    
    Args:
        max_attempts: Maximum number of attempts
        min_wait_seconds: Minimum wait time between attempts
        max_wait_seconds: Maximum wait time between attempts
        exception_types: Exception types to retry on (default: Exception)
        logger_name: Logger name for logging retries
        
    Returns:
        Decorated function
    """
    # Set default exception types if not specified
    if exception_types is None:
        exception_types = (Exception,)
    
    # Get logger
    log = logging.getLogger(logger_name or "utils.retry")
    
    def decorator(func: F) -> F:
        # Create retry decorator
        retry_decorator = retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(
                multiplier=1,
                min=min_wait_seconds,
                max=max_wait_seconds,
            ),
            retry=retry_if_exception_type(exception_types),
            before_sleep=before_sleep_log(log, logging.WARNING),
        )
        
        # Apply retry decorator to function
        retry_func = retry_decorator(func)
        
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                # Call function with retry logic
                return retry_func(*args, **kwargs)
            except RetryError as e:
                # Log retry error with original exception
                if e.last_attempt.exception() is not None:
                    original_exception = e.last_attempt.exception()
                    log.error(
                        f"Failed after {max_attempts} attempts: {original_exception.__class__.__name__}: {original_exception}"
                    )
                else:
                    log.error(f"Failed after {max_attempts} attempts")
                
                # Re-raise original exception
                raise e.last_attempt.exception() or e
        
        return cast(F, wrapper)
    
    return decorator