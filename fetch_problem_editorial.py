#!/usr/bin/env python3

import db_utils
import sys
import time
import datetime
import random
import threading
import queue
import settings
from settings import FULL_COOKIE, X_CSRF_TOKEN, UUUSERID
import util
import argparse

# --- CONFIG ---
# Authentication cookie and tokens
FULL_COOKIE = settings.FULL_COOKIE
X_CSRF_TOKEN = settings.X_CSRF_TOKEN
UUUSERID = settings.UUUSERID

# HTTP / GraphQL settings
USER_AGENT = settings.USER_AGENT
URL = settings.API_URL

# Retry and delay configuration
REQUEST_DELAY_SECONDS = settings.REQUEST_DELAY_SECONDS
MAX_RETRIES = settings.MAX_RETRIES
RETRY_DELAY_SECONDS = settings.RETRY_DELAY_SECONDS

# Editorial fetch-specific concurrency & batching
NUM_WORKERS = settings.EDITORIAL_NUM_WORKERS
DB_WRITE_CONCURRENCY = settings.EDITORIAL_DB_WRITE_CONCURRENCY
BATCH_SIZE = settings.BATCH_SIZE
FETCH_OLDER_THAN_DAYS = settings.FETCH_OLDER_THAN_DAYS
FORCE_REFETCH_ALL = settings.FORCE_REFETCH_ALL
# ---------------

# --- Global semaphore for database write operations ---
db_write_semaphore = threading.Semaphore(
    DB_WRITE_CONCURRENCY
)  # Limit concurrent writes to database

# --- Global counters for tracking progress ---
processed_count = 0  # Total problems processed
detail_error_count = 0  # Problems with detail fetch errors
editorial_error_count = 0  # Problems with editorial fetch errors
update_success_count = 0  # Successfully updated problems
counter_lock = threading.Lock()  # Lock to prevent race conditions

# --- Retry Queue for Failed Updates ---
retry_queue = queue.Queue()  # Queue to hold items that failed to update


# Process retry items at the end
def process_retry_queue(db_name):
    """Process any items that failed and were placed in the retry queue"""
    if retry_queue.empty():
        _log("No items in retry queue.")
        return

    _log(f"Processing {retry_queue.qsize()} items from retry queue...")
    with db_utils.connect_db(db_name) as conn:
        # Process retry queue in batches
        batch_size = 50
        batch_updates = []
        retried = 0
        succeeded = 0

        while not retry_queue.empty():
            try:
                item = retry_queue.get_nowait()
                batch_updates.append(item)
                retried += 1

                # Process in batches
                if len(batch_updates) >= batch_size or retry_queue.empty():
                    try:
                        # Deduplicate by slug (keep newest updates)
                        deduplicated = {}
                        for update in batch_updates:
                            slug = update["slug"]
                            deduplicated[slug] = update["details"]

                        # Convert to format for batch update
                        problems_data = [
                            {**details, "title_slug": slug}
                            for slug, details in deduplicated.items()
                        ]

                        # Update in a single transaction
                        updated = db_utils.batch_update_problem_details(
                            conn, problems_data
                        )
                        succeeded += updated
                        _log(
                            f"Retry batch: Updated {updated}/{len(problems_data)} items."
                        )

                        # Clear batch
                        batch_updates = []
                    except Exception as e:
                        _log(f"Error processing retry batch: {e}", level="ERROR")
            except queue.Empty:
                break

        _log(
            f"Retry queue processing complete: {succeeded}/{retried} items updated successfully."
        )


# --- Database Schema Initialization ---
# Define init_db_schema function that was missing
def init_db_schema(conn):
    """Initialize the database schema with tables needed for this script"""
    cursor = conn.cursor()

    # Create Problems table if not exists
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS Problems (
        problem_id INTEGER PRIMARY KEY AUTOINCREMENT,
        question_frontend_id TEXT UNIQUE,
        title_slug TEXT UNIQUE NOT NULL,
        title TEXT,
        difficulty TEXT CHECK(difficulty IN ('Easy', 'Medium', 'Hard')),
        content TEXT,
        editorial_content TEXT,
        editorial_uuid TEXT UNIQUE,
        ac_rate REAL,
        is_paid_only INTEGER DEFAULT 0,
        last_fetched_detail DATETIME,
        last_fetched_company DATETIME,
        last_fetched_editorial DATETIME
    );
    """
    )

    # Create indices for better performance
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_problems_slug ON Problems (title_slug);"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_problems_frontend_id ON Problems (question_frontend_id);"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_problems_editorial_uuid ON Problems (editorial_uuid);"
    )

    conn.commit()
    _log("Database schema initialized successfully")


# --- Global Headers ---
# Use headers based on the curl commands provided
BASE_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "authorization": ";",
    "content-type": "application/json",
    "cookie": FULL_COOKIE,
    "origin": "https://leetcode.com",
    "priority": "u=1, i",
    # 'random-uuid': '...', # Consider generating dynamically: import uuid; str(uuid.uuid4())
    "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": USER_AGENT,
    "uuuserid": UUUSERID,
    "x-csrftoken": X_CSRF_TOKEN,
}
# ----------------------

# --- GraphQL Queries ---
QUESTION_DETAIL_QUERY = """
query questionDetail($titleSlug: String!) {
  question(titleSlug: $titleSlug) {
    questionId
    questionFrontendId
    title
    titleSlug
    content
    difficulty
    isPaidOnly
  }
}
"""

# GraphQL query for Editorial Content (restored to match original curl, including unused fragment fields)
EDITORIAL_QUERY = """
    query ugcArticleOfficialSolutionArticle($questionSlug: String!) {
      ugcArticleOfficialSolutionArticle(questionSlug: $questionSlug) {
        ...ugcSolutionArticleFragment
        content
        isSerialized # restored
        isAuthorArticleReviewer # restored
        scoreInfo { # restored
          scoreCoefficient # restored
        }
      }
    }

    fragment ugcSolutionArticleFragment on SolutionArticleNode {
      uuid
      title
      slug
      summary # restored
      author { # restored
        realName
        userAvatar
        userSlug
        userName
        nameColor
        certificationLevel
        activeBadge {
          icon
          displayName
        }
      }
      articleType # restored
      thumbnail # restored
      # summary # duplicate removed
      createdAt # restored
      updatedAt # restored
      status # restored
      isLeetcode # restored
      canSee # restored
      canEdit # restored
      isMyFavorite # restored
      chargeType # restored
      myReactionType # restored
      topicId # restored
      hitCount # restored
      hasVideoArticle # restored
      reactions { # restored
        count
        reactionType
      }
      # title # duplicate removed
      # slug # duplicate removed
      tags { # restored
        name
        slug
        tagType
      }
      topic { # restored
        id
        topLevelCommentCount
      }
    }
"""


# --- GraphQL Request Function ---
def make_graphql_request(title_slug, query, data_key, query_type):
    """Make a GraphQL request to LeetCode API"""
    thread_name = threading.current_thread().name

    # Use the correct parameter name based on the query type
    if query_type == "editorial":
        variables = {"questionSlug": title_slug}
    else:
        variables = {"titleSlug": title_slug}

    payload = {"query": query, "variables": variables, "operationName": data_key}

    headers = {
        "User-Agent": USER_AGENT,
        "Content-Type": "application/json",
        "Origin": "https://leetcode.com",
        "Referer": f"https://leetcode.com/problems/{title_slug}/",
        "Cookie": FULL_COOKIE,
        "x-csrftoken": X_CSRF_TOKEN,
        "uuuserid": UUUSERID,
    }

    retries = 0
    while retries < MAX_RETRIES:
        try:
            session = util.get_session()
            response = session.post(URL, json=payload, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if "data" in data and data["data"] is not None:
                    return data["data"]
                else:
                    _log(
                        f"GraphQL {query_type} query for '{title_slug}' returned no data or errors: {data}",
                        level="WARN",
                        thread_name=thread_name,
                    )
                    return None
            else:
                _log(
                    f"GraphQL {query_type} query for '{title_slug}' failed with status {response.status_code}",
                    level="ERROR",
                    thread_name=thread_name,
                )

        except Exception as e:
            _log(
                f"Error making GraphQL {query_type} request for '{title_slug}': {e}",
                level="ERROR",
                thread_name=thread_name,
            )

        retries += 1
        if retries < MAX_RETRIES:
            delay = RETRY_DELAY_SECONDS * (2 ** (retries - 1))
            _log(
                f"Retrying {query_type} query for '{title_slug}' in {delay}s (attempt {retries+1}/{MAX_RETRIES})",
                level="WARN",
                thread_name=thread_name,
            )
            time.sleep(delay)

    return None  # Failed after MAX_RETRIES


# --- Logging Helper ---
def _log(message, level="INFO", stream=sys.stdout, thread_name=None):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[
        :-3
    ]  # Add milliseconds
    thread_prefix = f"[{thread_name}] " if thread_name else ""
    print(f"{timestamp} [{level}] {thread_prefix}{message}", file=stream)


# --- Setup Logging Function ---
def setup_logging():
    """Configure logging via util"""
    util.setup_logging()


# --- Optimized Main ---
def main():
    # Declare globals before using them in defaults
    global NUM_WORKERS, BATCH_SIZE, FETCH_OLDER_THAN_DAYS, FORCE_REFETCH_ALL, db_write_semaphore, FULL_COOKIE, X_CSRF_TOKEN, UUUSERID
    # CLI options
    parser = argparse.ArgumentParser(description="Fetch LeetCode Editorial & Details")
    parser.add_argument(
        "--cookie", default=settings.FULL_COOKIE, help="LeetCode session cookie string"
    )
    parser.add_argument(
        "--csrftoken", default=settings.X_CSRF_TOKEN, help="X-CSRFToken header value"
    )
    parser.add_argument(
        "--uuuserid", default=settings.UUUSERID, help="uuuserid header value"
    )
    parser.add_argument(
        "--cookies-from-browser",
        choices=["chrome", "firefox"],
        default=None,
        help="Load cookies directly from your browser via browser_cookie3",
    )
    parser.add_argument("--db-name", default=settings.DB_FILE)
    parser.add_argument("--workers", type=int, default=settings.EDITORIAL_NUM_WORKERS)
    parser.add_argument(
        "--db-concurrency",
        type=int,
        default=settings.EDITORIAL_DB_WRITE_CONCURRENCY,
    )
    parser.add_argument("--batch-size", type=int, default=settings.BATCH_SIZE)
    parser.add_argument(
        "--fetch-days",
        type=int,
        default=settings.FETCH_OLDER_THAN_DAYS,
    )
    parser.add_argument(
        "--force-refetch",
        action="store_true",
        default=settings.FORCE_REFETCH_ALL,
    )
    args = parser.parse_args()
    # Override settings from CLI
    db_name = args.db_name
    # Cookies: browser first, then manual override
    if args.cookies_from_browser:
        FULL_COOKIE = util.load_cookies_from_browser(
            browser=args.cookies_from_browser, domain="leetcode.com"
        )
    elif args.cookie:
        FULL_COOKIE = args.cookie
    # CSRF token override or extract from loaded cookie
    if args.csrftoken:
        X_CSRF_TOKEN = args.csrftoken
    else:
        match = next(
            (t for t in FULL_COOKIE.split("; ") if t.startswith("csrftoken=")), None
        )
        if match:
            X_CSRF_TOKEN = match.split("=")[1]
    # UUUSERID override or extract
    if args.uuuserid:
        UUUSERID = args.uuuserid
    else:
        match = next(
            (t for t in FULL_COOKIE.split("; ") if t.startswith("uuuserid=")), None
        )
        if match:
            UUUSERID = match.split("=")[1]
    # Update headers with new cookie and tokens
    BASE_HEADERS["cookie"] = FULL_COOKIE
    BASE_HEADERS["x-csrftoken"] = X_CSRF_TOKEN
    BASE_HEADERS["uuuserid"] = UUUSERID
    # Other overrides
    NUM_WORKERS = args.workers
    BATCH_SIZE = args.batch_size
    FETCH_OLDER_THAN_DAYS = args.fetch_days
    FORCE_REFETCH_ALL = args.force_refetch
    db_write_semaphore = threading.Semaphore(args.db_concurrency)
    _log("Starting LeetCode Editorial & Detail Fetcher...")
    setup_logging()

    # Connect to database
    _log(f"Connecting to database: {db_name}")

    # Use a with statement for database connection to ensure proper cleanup
    with db_utils.connect_db(db_name) as conn:
        # Initialize database schema (creates tables if they don't exist)
        _log("Initializing/Verifying Database Schema...")
        init_db_schema(conn)

        # Get list of slugs to process from the database
        slugs_to_process = get_slugs_to_update(conn)

        if not slugs_to_process:
            _log("No slugs to process. Exiting.")
            return 0

        # Create a thread-safe queue and start the worker threads
        slug_queue = queue.Queue()
        for slug in slugs_to_process:
            slug_queue.put(slug)

        # Start the worker threads
        _log(
            f"Starting {NUM_WORKERS} worker threads to process {len(slugs_to_process)} slugs..."
        )
        threads = []
        for i in range(NUM_WORKERS):
            thread = threading.Thread(
                target=process_slug_batch_worker,
                args=(
                    slug_queue,
                    db_name,
                    BATCH_SIZE,
                    len(slugs_to_process),
                ),  # Pass db_name instead of connection
                name=f"Worker-{i+1}",
            )
            thread.daemon = (
                True  # Allow the program to exit even if threads are running
            )
            threads.append(thread)
            thread.start()

        # Wait for all tasks to complete
        slug_queue.join()

        # Process any retries
        if not retry_queue.empty():
            _log(f"Processing retry queue with {retry_queue.qsize()} items")
            process_retry_queue(db_name)

        # Print summary
        with counter_lock:  # Access counters safely
            _log("=" * 60)
            _log(f"📊 SUMMARY:")
            _log(f"  Total problems processed: {processed_count}")
            _log(f"  Detail fetch errors: {detail_error_count}")
            _log(f"  Editorial fetch errors: {editorial_error_count}")
            _log(f"  Successfully updated problems: {update_success_count}")
            _log("=" * 60)

    _log("✅ Process completed!")
    return 0


# --- New Optimized Worker for Batch Processing ---
def process_slug_batch_worker(
    slug_queue, db_name, batch_size=BATCH_SIZE, total_slugs=0
):
    """Worker thread function to process slugs from the queue with batch DB operations."""
    thread_name = threading.current_thread().name
    _log(f"Started worker thread", thread_name=thread_name)

    # Create a thread-local database connection
    with db_utils.connect_db(db_name) as conn:
        # Local counters for this worker thread
        processed_local = 0
        detail_error_local = 0
        editorial_error_local = 0
        updates_successful_local = 0

        # Process slugs in batches for improved performance
        while not slug_queue.empty():
            # Skip if the queue is almost empty (< batch_size/2)
            if slug_queue.qsize() < batch_size / 2 and processed_local > 0:
                _log(
                    f"Queue size below threshold. Letting other workers handle remaining items.",
                    thread_name=thread_name,
                )
                break

            # Prepare batch updates
            batch_updates = []
            slugs_processed = 0

            # Try to process up to batch_size slugs
            while slugs_processed < batch_size:
                # Try to get a slug, exit loop if queue is empty
                try:
                    slug = slug_queue.get_nowait()  # Get slug without blocking
                except queue.Empty:
                    break  # Queue is empty, process remaining batch if any and exit loop

                # Add a small random delay to prevent API rate limits
                time.sleep(REQUEST_DELAY_SECONDS + random.uniform(0, 0.2))

                current_detail_error = False
                current_editorial_error = False
                problem_data = None
                editorial_data = None
                details_to_update = {}

                # 1. Fetch Problem Details
                detail_response_data = make_graphql_request(
                    slug, QUESTION_DETAIL_QUERY, "questionDetail", "description"
                )
                if detail_response_data and detail_response_data.get("question"):
                    problem_data = detail_response_data["question"]
                    # Prepare details for update
                    details_to_update.update(
                        {
                            "content": problem_data.get("content"),
                            "question_frontend_id": problem_data.get(
                                "questionFrontendId"
                            ),
                            "title": problem_data.get("title"),
                            "difficulty": problem_data.get("difficulty"),
                            "is_paid_only": problem_data.get("isPaidOnly"),
                            "last_fetched_detail": datetime.datetime.now().isoformat(
                                sep=" ", timespec="seconds"
                            ),
                        }
                    )
                else:
                    current_detail_error = True
                    detail_error_local += 1

                # 2. Fetch Editorial Content
                editorial_response_data = make_graphql_request(
                    slug,
                    EDITORIAL_QUERY,
                    "ugcArticleOfficialSolutionArticle",
                    "editorial",
                )
                # Only update editorial if non-empty content is returned
                if editorial_response_data and editorial_response_data.get(
                    "ugcArticleOfficialSolutionArticle"
                ):
                    node = editorial_response_data["ugcArticleOfficialSolutionArticle"]
                    content = node.get("content") or ""
                    if content.strip():
                        # Add editorial details to the update only if content is non-empty
                        details_to_update.update(
                            {
                                "editorial_content": content,
                                "editorial_uuid": node.get("uuid"),
                                "last_fetched_editorial": datetime.datetime.now().isoformat(
                                    sep=" ", timespec="seconds"
                                ),
                            }
                        )
                    else:
                        # Empty editorial content; schedule for retry later
                        current_editorial_error = True
                        editorial_error_local += 1
                else:
                    # GraphQL error or missing field
                    current_editorial_error = True
                    editorial_error_local += 1

                # Filter out None values
                filtered_details = {
                    k: v for k, v in details_to_update.items() if v is not None
                }

                # Add to batch if there are details to update
                if filtered_details:
                    batch_updates.append({"slug": slug, "details": filtered_details})

                # Update local processed counter
                processed_local += 1

                # Mark task as done
                slug_queue.task_done()
                slugs_processed += 1

            # Process batch updates if we have any
            if batch_updates:
                try:
                    with db_write_semaphore:
                        _log(
                            f"Processing batch of {len(batch_updates)} updates",
                            thread_name=thread_name,
                        )

                        # Use batch update helper
                        problems_data = [
                            {**u["details"], "title_slug": u["slug"]}
                            for u in batch_updates
                        ]
                        updated = db_utils.batch_update_problem_details(
                            conn, problems_data
                        )
                        updates_successful_local += updated
                        _log(
                            f"Successfully processed batch of {updated} updates",
                            thread_name=thread_name,
                        )
                except Exception as e:
                    _log(
                        f"Error during batch update: {e}",
                        level="ERROR",
                        thread_name=thread_name,
                    )
                    # Add failed items to retry queue
                    for update in batch_updates:
                        retry_queue.put(update)
                    _log(
                        f"Added {len(batch_updates)} items to retry queue",
                        thread_name=thread_name,
                    )

            # Update global counters with our local counters
            with counter_lock:
                global processed_count, detail_error_count, editorial_error_count, update_success_count
                processed_count += processed_local
                detail_error_count += detail_error_local
                editorial_error_count += editorial_error_local
                update_success_count += updates_successful_local

                # Reset local counters
                processed_local = 0
                detail_error_local = 0
                editorial_error_local = 0
                updates_successful_local = 0

                # Print progress occasionally
                if processed_count % 100 == 0:
                    _log(f"Progress: {processed_count}/{total_slugs} slugs processed")

    _log(f"Worker thread finished", thread_name=thread_name)


# --- Get Slugs to Update ---
def get_slugs_to_update(conn):
    """Get a list of problem slugs that need to be updated."""
    cursor = conn.cursor()

    if FORCE_REFETCH_ALL:
        # Fetch all slugs if forced refetch is enabled
        cursor.execute("SELECT title_slug FROM Problems ORDER BY problem_id")
        all_slugs = [row["title_slug"] for row in cursor.fetchall()]
        return all_slugs

    # Calculate cutoff date
    cutoff_date = datetime.datetime.now() - datetime.timedelta(
        days=FETCH_OLDER_THAN_DAYS
    )
    cutoff_str = cutoff_date.isoformat(sep=" ", timespec="seconds")

    # Get slugs that haven't been updated recently
    cursor.execute(
        """
        SELECT title_slug FROM Problems
        WHERE last_fetched_detail IS NULL
           OR last_fetched_detail < ?
           OR last_fetched_editorial IS NULL
           OR last_fetched_editorial < ?
        ORDER BY RANDOM()  -- Randomize to reduce contention
        """,
        (cutoff_str, cutoff_str),
    )

    slugs = [row["title_slug"] for row in cursor.fetchall()]
    _log(
        f"Found {len(slugs)} slugs to update (older than {FETCH_OLDER_THAN_DAYS} days or never fetched)"
    )
    return slugs


# --- Entry Point ---
if __name__ == "__main__":
    sys.exit(main())
