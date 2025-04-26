# settings.py
import os

# File paths
COMPANIES_FILE = os.getenv("LEETCODE_COMPANIES_FILE", "companies.json")
# SQLite database file path
DB_FILE = os.getenv("LEETCODE_DB_FILE", "leetcode_data.sqlite")

# HTTP / GraphQL settings
URL = "https://leetcode.com/graphql/"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
REQUEST_DELAY_SECONDS = float(os.getenv("LEETCODE_REQUEST_DELAY_SECONDS", "0.5"))
MAX_RETRIES = int(os.getenv("LEETCODE_MAX_RETRIES", "5"))
RETRY_DELAY_SECONDS = float(os.getenv("LEETCODE_RETRY_DELAY_SECONDS", "3"))

# Concurrency & batching
NUM_WORKERS = int(os.getenv("LEETCODE_NUM_WORKERS", "30"))
DB_WRITE_CONCURRENCY = int(os.getenv("LEETCODE_DB_WRITE_CONCURRENCY", "1"))

# Editorial fetch settings
EDITORIAL_NUM_WORKERS = int(
    os.getenv("LEETCODE_EDITORIAL_NUM_WORKERS", str(NUM_WORKERS))
)
EDITORIAL_DB_WRITE_CONCURRENCY = int(
    os.getenv("LEETCODE_EDITORIAL_DB_WRITE_CONCURRENCY", str(DB_WRITE_CONCURRENCY))
)
BATCH_SIZE = int(os.getenv("LEETCODE_BATCH_SIZE", "100"))
FETCH_OLDER_THAN_DAYS = int(os.getenv("LEETCODE_FETCH_OLDER_THAN_DAYS", "1"))
FORCE_REFETCH_ALL = os.getenv("LEETCODE_FORCE_REFETCH_ALL", "True").lower() == "true"

# --- Auth / Cookie Settings ---
# Full LeetCode session cookie (override with LEETCODE_COOKIE env var)
FULL_COOKIE = os.getenv(
    "LEETCODE_COOKIE",
    "gr_user_id=845f8890-da65-4d47-930d-8ee069a9eaa9; 87b5a3c3f1a55520_gr_last_sent_cs1=sanglythesis; __stripe_mid=f2208bfd-fb64-4949-bfe0-e95de794b3e42a34d1; cf_clearance=uN96HjGbxcPYcxYsezMlw6etWe7EHcBJZftvpA7H8yg-1744469424-1.2.1.1-QoGQJX4sn47epc7yeckg2vQ.gQB8ZzLsi_T5B_tIN6_XIGmA_QLE9hfjFl_QBqiLLyKG2jv5q6Yz.3zRuadMb9TjGxyat7X1IVzFUj4UohyehGha8rEAwaqx7MJd74O_lDNyFIIk2156X1haXBIXDkt5gl1O_RhWdaWhkDjdGr__nnlRdzHss7J4_3_dBy8qX1OJl2D30m0VlH7qLRxfRGFU5CPXbHed2LI91_UHN5sr41MhSCJovmPKss8yeIMYvP2Wb4iaYre4OXKo1BVdlPmTxX6N7JlBEd3rIH2WoUuY4oEH0i9sGqDtVPZs_fF5omqEyAa.BKo4RBuRdZGevJXCdipMqD88Ly2LywN0ji5vN2Tk.0sufSLcW7VDoe07; csrftoken=qCV7HFIWihz6NOKyrxL5gIFLR23zelsxvetapxDdJEnNZ1j56qLRH6LOJDn83kU0",
)
# Extract CSRF token from cookie if not provided via env
X_CSRF_TOKEN = os.getenv("LEETCODE_CSRF_TOKEN") or next(
    (t.split("=")[1] for t in FULL_COOKIE.split("; ") if t.startswith("csrftoken=")), ""
)
# Extract UUUSERID from cookie if not provided via env
UUUSERID = os.getenv("LEETCODE_UUUSERID") or next(
    (t.split("=")[1] for t in FULL_COOKIE.split("; ") if t.startswith("uuuserid=")), ""
)
