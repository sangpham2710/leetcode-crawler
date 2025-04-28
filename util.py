"""
Core utilities for the LeetCode crawler system.

This module provides shared functionality including:
- Request management with connection pooling
- Advanced logging configuration
- Browser cookie extraction
- Graceful shutdown handling
- Error tracking
"""

import logging
import sys
import threading
import time
import signal
import os
from datetime import datetime
from contextlib import contextmanager
from functools import wraps
from typing import Dict, Optional, Union, Callable, Any
import random

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util import Retry


try:
    import browser_cookie3
    from rich.logging import RichHandler
    from rich.console import Console
    from rich.traceback import install as install_rich_traceback

    # Install better traceback display
    install_rich_traceback(show_locals=True)

    # Create console for direct rich output
    console = Console()
except ImportError:
    browser_cookie3 = None
    tqdm = None
    console = None

# Thread-local storage for per-thread HTTP sessions
tls = threading.local()

# Global shutdown flag for graceful termination
_shutdown_requested = False
_active_threads = set()
_thread_lock = threading.RLock()


def register_shutdown_handler():
    """Register signal handlers for graceful shutdown."""

    def handler(signum, frame):
        global _shutdown_requested
        if not _shutdown_requested:
            logger = logging.getLogger(__name__)
            logger.warning(
                f"Shutdown signal received ({signum}). Starting graceful shutdown..."
            )
            _shutdown_requested = True

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def shutdown_requested():
    """Check if shutdown has been requested."""
    return _shutdown_requested


def register_thread(thread=None):
    """Register a thread as active for shutdown tracking."""
    with _thread_lock:
        if thread is None:
            thread = threading.current_thread()
        _active_threads.add(thread)


def unregister_thread(thread=None):
    """Unregister a thread when it completes."""
    with _thread_lock:
        if thread is None:
            thread = threading.current_thread()
        if thread in _active_threads:
            _active_threads.remove(thread)


def get_active_thread_count():
    """Get count of currently active worker threads."""
    with _thread_lock:
        return len(_active_threads)


def get_session() -> requests.Session:
    """
    Return a thread-local requests.Session with connection pooling and retry logic.

    The session includes:
    - Connection pooling
    - Configurable retries with exponential backoff
    - Custom User-Agent and default headers

    Returns:
        requests.Session: A configured session object
    """
    if not hasattr(tls, "session"):
        session = requests.Session()

        # Configure retry strategy - 429s, 500s, connection errors
        retry_strategy = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "HEAD"],
            respect_retry_after_header=True,
        )

        # Add adapter with retry strategy
        adapter = HTTPAdapter(
            max_retries=retry_strategy, pool_connections=10, pool_maxsize=20
        )

        # Mount for both http and https
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        # Set default headers
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
                "Accept": "application/json, text/html, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
            }
        )

        tls.session = session

    return tls.session


def setup_logging(level=logging.INFO, log_file=None, rich_output=True):
    """
    Configure structured logging with optional file output and rich console formatting.

    Args:
        level: The minimum log level to display (default: INFO)
        log_file: Optional path to write logs (default: None)
        rich_output: Whether to use Rich for console output (default: True)
    """
    # Create base logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Clear any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Format with ISO timestamps, log level, and thread name
    log_format = "%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    if rich_output and "rich" in sys.modules:
        # Rich console handler
        console_handler = RichHandler(
            rich_tracebacks=True,
            tracebacks_show_locals=True,
            show_time=False,
            omit_repeated_times=False,
        )
        console_handler.setFormatter(logging.Formatter("%(message)s"))
        root_logger.addHandler(console_handler)
    else:
        # Standard console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))
        root_logger.addHandler(console_handler)

    # Add file handler if specified
    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))
        root_logger.addHandler(file_handler)

    # Create a logger instance for this module
    logger = logging.getLogger(__name__)
    logger.debug("Logging initialized")

    return root_logger


def load_cookies_from_browser(browser="chrome", domain="leetcode.com"):
    """
    Load cookies from the user's browser for a specified domain.

    Args:
        browser: Browser to extract cookies from ('chrome' or 'firefox')
        domain: Domain to filter cookies by

    Returns:
        str: Cookie header string in format "name1=value1; name2=value2"

    Raises:
        ValueError: If unsupported browser is specified
    """
    # Check for browser_cookie3 at the top level, error will be raised there if missing
    # If the module was imported successfully, proceed.
    logger = logging.getLogger(__name__)

    # If the module wasn't imported at the top level, browser_cookie3 will be None
    if not browser_cookie3:
        logger.error(
            "browser-cookie3 module is not available. Please install it (`pip install browser-cookie3`)."
        )
        # Return empty string to indicate failure
        return ""

    logger.info(f"Loading cookies from {browser} for {domain}")

    # Attempt to load cookies
    try:
        if browser.lower() == "chrome":
            jar = browser_cookie3.chrome(domain_name=domain)
        elif browser.lower() == "firefox":
            jar = browser_cookie3.firefox(domain_name=domain)
        else:
            raise ValueError(
                f"Unsupported browser: {browser}. Use 'chrome' or 'firefox'"
            )
    except Exception as e:
        logger.error(f"Failed to load cookies from {browser}: {e}")
        return ""  # Return empty on error

    # Convert cookiejar to header string
    cookie_str = "; ".join([f"{cookie.name}={cookie.value}" for cookie in jar])

    # Log cookie presence (not values for security)
    cookie_names = [cookie.name for cookie in jar]
    logger.debug(f"Found {len(cookie_names)} cookies: {', '.join(cookie_names)}")

    return cookie_str


@contextmanager
def operation_timer(operation_name):
    """
    Context manager to time and log the duration of operations.

    Example:
        with operation_timer("fetch_problems"):
            fetch_problems()

    Args:
        operation_name: Name of the operation being timed
    """
    logger = logging.getLogger(__name__)
    start_time = time.time()
    logger.debug(f"Starting: {operation_name}")

    try:
        yield
    finally:
        duration = time.time() - start_time
        logger.info(f"Completed: {operation_name} - Duration: {duration:.2f}s")


def retry_with_backoff(max_retries=5, initial_delay=1, max_delay=60, jitter=True):
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay between retries
        jitter: Whether to add random jitter to the delay

    Returns:
        Decorated function with retry logic
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            retry_count = 0
            delay = initial_delay

            while True:
                try:
                    return func(*args, **kwargs)
                except (RequestException, ConnectionError) as e:
                    retry_count += 1
                    if retry_count > max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) exceeded for {func.__name__}: {e}"
                        )
                        raise

                    # Calculate backoff with optional jitter
                    jitter_value = (0.5 + random.random()) if jitter else 1.0
                    sleep_time = min(
                        delay * (2 ** (retry_count - 1)) * jitter_value, max_delay
                    )

                    logger.warning(
                        f"Retry {retry_count}/{max_retries} for {func.__name__} after {sleep_time:.2f}s: {e}"
                    )
                    time.sleep(sleep_time)

        return wrapper

    return decorator


# Initialize module when imported
register_shutdown_handler()
