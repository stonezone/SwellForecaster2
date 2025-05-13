"""Logging configuration for SwellForecaster."""
import logging
import logging.config
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


def get_logging_config(log_dir: str = "logs") -> Dict[str, Any]:
    """
    Get logging configuration dictionary.
    
    Args:
        log_dir: Directory to store log files
        
    Returns:
        Logging configuration dictionary
    """
    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Get current date for log filenames
    date_str = datetime.now().strftime("%Y%m%d")
    
    # Create configuration
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "simple": {
                "format": "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "simple",
                "stream": "ext://sys.stdout",
            },
            "collector_file": {
                "class": "logging.FileHandler",
                "level": "DEBUG",
                "formatter": "detailed",
                "filename": os.path.join(log_dir, f"collector_{date_str}.log"),
                "encoding": "utf8",
            },
            "analyzer_file": {
                "class": "logging.FileHandler",
                "level": "DEBUG",
                "formatter": "detailed",
                "filename": os.path.join(log_dir, f"analyzer_{date_str}.log"),
                "encoding": "utf8",
            },
            "app_file": {
                "class": "logging.FileHandler",
                "level": "DEBUG",
                "formatter": "detailed",
                "filename": os.path.join(log_dir, f"swellforecaster_{date_str}.log"),
                "encoding": "utf8",
            },
        },
        "loggers": {
            "collector": {
                "level": "DEBUG",
                "handlers": ["console", "collector_file", "app_file"],
                "propagate": False,
            },
            "agent": {
                "level": "DEBUG",
                "handlers": ["console", "collector_file", "app_file"],
                "propagate": False,
            },
            "agents": {
                "level": "DEBUG",
                "handlers": ["console", "collector_file", "app_file"],
                "propagate": False,
            },
            "analyzer": {
                "level": "DEBUG",
                "handlers": ["console", "analyzer_file", "app_file"],
                "propagate": False,
            },
            "utils": {
                "level": "DEBUG",
                "handlers": ["console", "app_file"],
                "propagate": False,
            },
            "data": {
                "level": "DEBUG",
                "handlers": ["console", "app_file"],
                "propagate": False,
            },
            "models": {
                "level": "DEBUG",
                "handlers": ["console", "app_file"],
                "propagate": False,
            },
        },
        "root": {
            "level": "INFO",
            "handlers": ["console", "app_file"],
        },
    }
    
    return config


def setup_logging(
    config_dict: Optional[Dict[str, Any]] = None,
    log_dir: str = "logs",
    log_level: str = "INFO"
) -> None:
    """
    Configure the logging system using a dictionary config.

    Args:
        config_dict: Optional configuration dictionary
        log_dir: Directory to store log files
        log_level: Logging level (default: INFO)
    """
    if config_dict is None:
        config_dict = get_logging_config(log_dir)

    # Create log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)

    # Update logging level
    if log_level:
        # Set console handler level
        if "handlers" in config_dict and "console" in config_dict["handlers"]:
            config_dict["handlers"]["console"]["level"] = log_level

        # Set root logger level
        if "root" in config_dict:
            config_dict["root"]["level"] = log_level

        # Set specific logger levels if needed (keep specialized loggers at their defined levels)
        for logger_name in ["collector", "agent", "agents", "analyzer", "utils", "data", "models"]:
            if logger_name in config_dict.get("loggers", {}):
                config_dict["loggers"][logger_name]["level"] = log_level

    # Apply configuration
    logging.config.dictConfig(config_dict)

    # Log initialization
    logger = logging.getLogger("utils.logging")
    logger.info(f"Logging system initialized with level {log_level}")
    logger.debug(f"Log files will be stored in: {os.path.abspath(log_dir)}")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the given name.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)