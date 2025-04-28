"""
LeetCode Crawler Configuration Module

This module defines all configurable parameters for the LeetCode crawler system.
Values can be overridden using environment variables.
"""

import os
import logging
import platform
from typing import Dict, Any

# ---------------------------------------------------------------------------
# Core Paths and Files
# ---------------------------------------------------------------------------

# Base directory for all data
BASE_DIR = os.getenv("LEETCODE_BASE_DIR", os.path.dirname(os.path.abspath(__file__)))

# File paths with defaults
COMPANIES_FILE = os.getenv(
    "LEETCODE_COMPANIES_FILE", os.path.join(BASE_DIR, "companies.json")
)
DB_FILE = os.getenv("LEETCODE_DB_FILE", os.path.join(BASE_DIR, "leetcode_data.sqlite"))
LOG_DIR = os.getenv("LEETCODE_LOG_DIR", os.path.join(BASE_DIR, "logs"))
LOG_FILE = os.path.join(
    LOG_DIR, f"leetcode_crawler_{os.getenv('LEETCODE_LOG_SUFFIX', '')}.log"
)

# Create directories if they don't exist
os.makedirs(LOG_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Network Configuration
# ---------------------------------------------------------------------------

# LeetCode API endpoint
API_URL = "https://leetcode.com/graphql/"

# User agent based on platform
DEFAULT_USER_AGENT = (
    f"Mozilla/5.0 ({platform.system()}; {platform.machine()}) LeetCode-Crawler/1.0"
)
USER_AGENT = os.getenv("LEETCODE_USER_AGENT", DEFAULT_USER_AGENT)

# Request timing
REQUEST_DELAY_SECONDS = float(os.getenv("LEETCODE_REQUEST_DELAY_SECONDS", "0.5"))
MAX_RETRIES = int(os.getenv("LEETCODE_MAX_RETRIES", "5"))
RETRY_DELAY_SECONDS = float(os.getenv("LEETCODE_RETRY_DELAY_SECONDS", "3"))
TIMEOUT_SECONDS = float(os.getenv("LEETCODE_TIMEOUT_SECONDS", "30"))

# ---------------------------------------------------------------------------
# Concurrency & Performance Settings
# ---------------------------------------------------------------------------

# Number of workers
NUM_WORKERS = int(os.getenv("LEETCODE_NUM_WORKERS", "30"))
DB_WRITE_CONCURRENCY = int(os.getenv("LEETCODE_DB_WRITE_CONCURRENCY", "10"))

# Editorial-specific settings
EDITORIAL_NUM_WORKERS = int(
    os.getenv("LEETCODE_EDITORIAL_NUM_WORKERS", str(NUM_WORKERS))
)
EDITORIAL_DB_WRITE_CONCURRENCY = int(
    os.getenv("LEETCODE_EDITORIAL_DB_WRITE_CONCURRENCY", str(DB_WRITE_CONCURRENCY))
)
BATCH_SIZE = int(os.getenv("LEETCODE_BATCH_SIZE", "100"))

# SQLite Performance
SQLITE_CACHE_SIZE = int(
    os.getenv("LEETCODE_SQLITE_CACHE_SIZE", "100000")
)  # 100MB cache
SQLITE_SYNCHRONOUS = os.getenv(
    "LEETCODE_SQLITE_SYNCHRONOUS", "OFF"
)  # OFF, NORMAL, FULL
SQLITE_JOURNAL_MODE = os.getenv(
    "LEETCODE_SQLITE_JOURNAL_MODE", "WAL"
)  # WAL, MEMORY, DELETE
SQLITE_BUSY_TIMEOUT = int(
    os.getenv("LEETCODE_SQLITE_BUSY_TIMEOUT", "30000")
)  # 30 seconds
SQLITE_MMAP_SIZE = int(os.getenv("LEETCODE_SQLITE_MMAP_SIZE", "67108864"))  # 64MB

# ---------------------------------------------------------------------------
# Data Collection Settings
# ---------------------------------------------------------------------------

# Age of data to consider for refresh
FETCH_OLDER_THAN_DAYS = int(os.getenv("LEETCODE_FETCH_OLDER_THAN_DAYS", "7"))
FORCE_REFETCH_ALL = os.getenv("LEETCODE_FORCE_REFETCH_ALL", "False").lower() in (
    "true",
    "1",
    "yes",
)

# Logging level
LOG_LEVEL = getattr(logging, os.getenv("LEETCODE_LOG_LEVEL", "INFO").upper())
ENABLE_RICH_LOGGING = os.getenv("LEETCODE_RICH_LOGGING", "True").lower() in (
    "true",
    "1",
    "yes",
)

# ---------------------------------------------------------------------------
# Authentication Settings
# ---------------------------------------------------------------------------

# LeetCode authentication
FULL_COOKIE = os.getenv("LEETCODE_COOKIE", "")
X_CSRF_TOKEN = os.getenv("LEETCODE_CSRF_TOKEN", "")
UUUSERID = os.getenv("LEETCODE_UUUSERID", "")

# Extract from cookie if not provided directly
if not X_CSRF_TOKEN and FULL_COOKIE:
    X_CSRF_TOKEN = next(
        (
            t.split("=")[1]
            for t in FULL_COOKIE.split("; ")
            if t.startswith("csrftoken=")
        ),
        "",
    )

if not UUUSERID and FULL_COOKIE:
    UUUSERID = next(
        (t.split("=")[1] for t in FULL_COOKIE.split("; ") if t.startswith("uuuserid=")),
        "",
    )

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def get_sqlite_pragmas() -> Dict[str, Any]:
    """
    Get SQLite PRAGMA settings as a dictionary.

    Returns:
        Dict[str, Any]: Dictionary of SQLite PRAGMA key-value pairs
    """
    return {
        "journal_mode": SQLITE_JOURNAL_MODE,
        "synchronous": SQLITE_SYNCHRONOUS,
        "cache_size": SQLITE_CACHE_SIZE,
        "busy_timeout": SQLITE_BUSY_TIMEOUT,
        "temp_store": "MEMORY",
        "mmap_size": SQLITE_MMAP_SIZE,
        "page_size": 8192,
        "locking_mode": "NORMAL",
        "wal_autocheckpoint": 2000,
    }


def print_config_summary() -> None:
    """Print a summary of the current configuration."""
    from rich.table import Table
    from rich.console import Console

    try:
        console = Console()
        table = Table(title="LeetCode Crawler Configuration")

        table.add_column("Setting", style="cyan")
        table.add_column("Value", style="green")

        # Add rows for key settings
        table.add_row("Database", DB_FILE)
        table.add_row("Workers", str(NUM_WORKERS))
        table.add_row("DB Concurrency", str(DB_WRITE_CONCURRENCY))
        table.add_row("Batch Size", str(BATCH_SIZE))
        table.add_row("SQLite Journal", SQLITE_JOURNAL_MODE)
        table.add_row("SQLite Synchronous", SQLITE_SYNCHRONOUS)
        table.add_row("Force Refetch", str(FORCE_REFETCH_ALL))
        table.add_row("Data Age Limit", f"{FETCH_OLDER_THAN_DAYS} days")
        table.add_row("Auth Status", "Configured" if FULL_COOKIE else "Missing")

        console.print(table)
    except ImportError:
        # Fall back to basic printing if rich is not available
        print("\n=== LeetCode Crawler Configuration ===")
        print(f"Database: {DB_FILE}")
        print(f"Workers: {NUM_WORKERS}")
        print(f"DB Concurrency: {DB_WRITE_CONCURRENCY}")
        print(f"SQLite Journal: {SQLITE_JOURNAL_MODE}")
        print(f"Force Refetch: {FORCE_REFETCH_ALL}")
        print(f"Data Age Limit: {FETCH_OLDER_THAN_DAYS} days")
        print(f"Auth Status: {'Configured' if FULL_COOKIE else 'Missing'}")
        print("=======================================\n")
