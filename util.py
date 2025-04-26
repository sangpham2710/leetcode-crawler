import logging
import sys
import threading
import requests
from requests.adapters import HTTPAdapter

try:
    import browser_cookie3
except ImportError:
    browser_cookie3 = None

# Thread-local storage for per-thread HTTP sessions
tls = threading.local()


def get_session():
    """Return a thread-local requests.Session with minimal connection pooling."""
    if not hasattr(tls, "session"):
        session = requests.Session()
        # Mount a single-connection pool for LeetCode domain
        session.mount(
            "https://leetcode.com", HTTPAdapter(pool_connections=1, pool_maxsize=1)
        )
        tls.session = session
    return tls.session


def setup_logging(level=logging.INFO):
    """Configure root logger to output to stdout with timestamps."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def load_cookies_from_browser(browser="chrome", domain="leetcode.com"):
    """Load LeetCode cookies from the user's browser via browser_cookie3."""
    if not browser_cookie3:
        raise ImportError(
            "browser-cookie3 is required for --cookies-from-browser. Install with `pip install browser-cookie3`"
        )
    if browser == "chrome":
        jar = browser_cookie3.chrome(domain_name=domain)
    elif browser == "firefox":
        jar = browser_cookie3.firefox(domain_name=domain)
    else:
        raise ValueError(f"Unsupported browser: {browser}")
    # Convert cookiejar to header string
    return "; ".join([f"{cookie.name}={cookie.value}" for cookie in jar])
