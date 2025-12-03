"""
Centralized logging configuration for the project.

This module provides a standardized way to set up logging.
Library modules should NOT call logging.basicConfig() - that's the
application's responsibility. Instead, they should just use:

    import logging
    logger = logging.getLogger(__name__)

Only entry-point scripts (CLI tools, main scripts) should call
setup_logging() to configure the logging system.
"""

import logging
import sys
from pathlib import Path
from typing import Optional


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[Path] = None,
    format_string: Optional[str] = None
) -> None:
    """
    Configure logging for the application.

    This should only be called from entry-point scripts, not library modules.

    Args:
        level: Logging level (default: INFO)
        log_file: Optional path to write logs to file
        format_string: Optional custom format string
    """
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    handlers = [logging.StreamHandler(sys.stdout)]

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=handlers,
        force=True  # Override any existing configuration
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for the given module name.

    This is a convenience wrapper around logging.getLogger().

    Args:
        name: Module name (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)
