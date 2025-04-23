#!/usr/bin/env python3

import requests
import json

# import csv # No longer needed
import sys
import time
import datetime
import random

# import os # No longer needed for config import
from pathlib import Path
import sqlite3  # Needed for DB interaction
import db_utils  # Import the database utility module
import threading
import queue
from concurrent.futures import ThreadPoolExecutor

# --- CONFIG ---
# Configuration now primarily relies on direct values or db_utils
# Using the cookie from the latest curl command provided
FULL_COOKIE = 'gr_user_id=845f8890-da65-4d47-930d-8ee069a9eaa9; 87b5a3c3f1a55520_gr_last_sent_cs1=sanglythesis; __stripe_mid=f2208bfd-fb64-4949-bfe0-e95de794b3e42a34d1; cf_clearance=uN96HjGbxcPYcxYsezMlw6etWe7EHcBJZftvpA7H8yg-1744469424-1.2.1.1-QoGQJX4sn47epc7yeckg2vQ.gQB8ZzLsi_T5B_tIN6_XIGmA_QLE9hfjFl_QBqiLLyKG2jv5q6Yz.3zRuadMb9TjGxyat7X1IVzFUj4UohyehGha8rEAwaqx7MJd74O_lDNyFIIk2156X1haXBIXDkt5gl1O_RhWdaWhkDjdGr__nnlRdzHss7J4_3_dBy8qX1OJl2D30m0VlH7qLRxfRGFU5CPXbHed2LI91_UHN5sr41MhSCJovmPKss8yeIMYvP2Wb4iaYre4OXKo1BVdlPmTxX6N7JlBEd3rIH2WoUuY4oEH0i9sGqDtVPZs_fF5omqEyAa.BKo4RBuRdZGevJXCdipMqD88Ly2LywN0ji5vN2Tk.0sufSLcW7VDoe07; csrftoken=qCV7HFIWihz6NOKyrxL5gIFLR23zelsxvetapxDdJEnNZ1j56qLRH6LOJDn83kU0; _gid=GA1.2.1476346995.1745157752; INGRESSCOOKIE=8c327cf6cf4f169c9d94b0394e86bc92|8e0876c7c1464cc0ac96bc2edceabd27; ip_check=(false, "101.53.53.147"); _gcl_au=1.1.905053711.1745321970; _ga_DKXQ03QCVK=GS1.1.1745337312.2.0.1745337312.60.0.0; 87b5a3c3f1a55520_gr_session_id=dfca6aeb-94ad-4b3a-a0c0-3045b14650cb; 87b5a3c3f1a55520_gr_last_sent_sid_with_cs1=dfca6aeb-94ad-4b3a-a0c0-3045b14650cb; 87b5a3c3f1a55520_gr_session_id_sent_vst=dfca6aeb-94ad-4b3a-a0c0-3045b14650cb; _ga=GA1.1.322647468.1743747905; LEETCODE_SESSION=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJfYXV0aF91c2VyX2lkIjoiMTcxOTYxNTIiLCJfYXV0aF91c2VyX2JhY2tlbmQiOiJkamFuZ28uY29udHJpYi5hdXRoLmJhY2tlbmRzLk1vZGVsQmFja2VuZCIsIl9hdXRoX3VzZXJfaGFzaCI6ImQ4ZWI3OWM0MWUwMDE0MzNjZjA1ZmU3YTkwNjI3ZGFiY2NiMTlhYjg5YTBlM2ExMzlhYWNkMTc2ODE4ZjNlMGMiLCJzZXNzaW9uX3V1aWQiOiJlY2RkM2RiZiIsImlkIjoxNzE5NjE1MiwiZW1haWwiOiJzYW5nbHl0aGVzaXNAZ21haWwuY29tIiwidXNlcm5hbWUiOiJzYW5nbHl0aGVzaXMiLCJ1c2VyX3NsdWciOiJzYW5nbHl0aGVzaXMiLCJhdmF0YXIiOiJodHRwczovL2Fzc2V0cy5sZWV0Y29kZS5jb20vdXNlcnMvZGVmYXVsdF9hdmF0YXIuanBnIiwicmVmcmVzaGVkX2F0IjoxNzQ1MzM3MzEzLCJpcCI6IjEwMS41My41My4xNDciLCJpZGVudGl0eSI6ImQ2ZGNjMGE2ZGVmNTU4MmY4ZDNhOWY3ZjJhZGRiODhiIiwiZGV2aWNlX3dpdGhfaXAiOlsiMzI4NDg4NmI5ZjVkY2NiMDZiYmVmNmEwNzM5ZTJjNTQiLCIxMDEuNTMuNTMuMTQ3Il0sIl9zZXNzaW9uX2V4cGlyeSI6MTIwOTYwMH0.hTHjinYdgs_ViwEHdtFo2ws9J4aycAMrEtP2LGDG87w; 87b5a3c3f1a55520_gr_cs1=sanglythesis; _ga_CDRWKZTDEX=GS1.1.1745388961.34.1.1745393390.60.0.0'
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
URL = "https://leetcode.com/graphql/"

# Extract CSRF token and UUUserID from cookie/headers using latest curl info
X_CSRF_TOKEN = "qCV7HFIWihz6NOKyrxL5gIFLR23zelsxvetapxDdJEnNZ1j56qLRH6LOJDn83kU0"  # From curl -H x-csrftoken
UUUSERID = "3284886b9f5dccb06bbef6a0739e2c54"  # From curl -H uuuserid

# INFILE = "leetcode_company_problems.csv" # No longer needed
# OUTFILE = "leetcode_editorial_content.csv" # No longer needed
REQUEST_DELAY_SECONDS = 0.5  # Delay between requests per worker
NUM_WORKERS = 12  # Reduced from 8 to prevent database contention
FETCH_OLDER_THAN_DAYS = 1  # Fetch problems not updated in this many days
MAX_RETRIES = 3  # Max retries for failed requests
RETRY_DELAY_SECONDS = 2  # Delay between retries
FORCE_REFETCH_ALL = True  # Set to True to ignore timestamps and fetch all slugs
BATCH_SIZE = 50  # Size of batches for DB operations
# ---------------

# --- Global semaphore for database write operations ---
db_write_semaphore = threading.Semaphore(3)  # Limit concurrent writes to database

# --- Global counters for tracking progress ---
processed_count = 0  # Total problems processed
detail_error_count = 0  # Problems with detail fetch errors
editorial_error_count = 0  # Problems with editorial fetch errors
update_success_count = 0  # Successfully updated problems
counter_lock = threading.Lock()  # Lock to prevent race conditions


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
            response = requests.post(URL, json=payload, headers=headers)
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
    """Configure the Python logging module with proper formatting"""
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# --- Optimized Main ---
def main():
    _log("Starting LeetCode Editorial & Detail Fetcher...")
    setup_logging()

    # Connect to database
    db_name = "leetcode_data.sqlite"
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
                if editorial_response_data and editorial_response_data.get(
                    "ugcArticleOfficialSolutionArticle"
                ):
                    editorial_content_node = editorial_response_data[
                        "ugcArticleOfficialSolutionArticle"
                    ]
                    if (
                        editorial_content_node
                        and editorial_content_node.get("content") is not None
                    ):
                        editorial_data = editorial_content_node
                        # Add editorial details to the update
                        details_to_update.update(
                            {
                                "editorial_content": editorial_data.get("content"),
                                "editorial_uuid": editorial_data.get("uuid"),
                                "last_fetched_editorial": datetime.datetime.now().isoformat(
                                    sep=" ", timespec="seconds"
                                ),
                            }
                        )
                else:
                    # Still mark as processed even if no editorial content is found
                    # We don't want to keep retrying if the problem doesn't have an editorial
                    details_to_update.update(
                        {
                            "last_fetched_editorial": datetime.datetime.now().isoformat(
                                sep=" ", timespec="seconds"
                            )
                        }
                    )

                    # Only count as error if the response was None (API failure)
                    if editorial_response_data is None:
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
                        # Acquire a write lock on the database to prevent concurrent writes
                        _log(
                            f"Processing batch of {len(batch_updates)} updates",
                            thread_name=thread_name,
                        )

                        # Start a transaction for the batch
                        cursor = conn.cursor()
                        cursor.execute("BEGIN TRANSACTION")

                        try:
                            # Process each item in the batch
                            for update in batch_updates:
                                slug = update["slug"]
                                details = update["details"]

                                # Use the improved update_problem_details function
                                result = db_utils.update_problem_details(
                                    conn, slug, details
                                )

                                # The function now returns True/False directly instead of a cursor
                                if result:
                                    updates_successful_local += 1

                            # Commit the transaction
                            conn.commit()
                            _log(
                                f"Successfully committed batch of {len(batch_updates)} updates",
                                thread_name=thread_name,
                            )

                        except Exception as e:
                            # Rollback on error
                            conn.rollback()
                            _log(
                                f"Error processing batch updates: {e}",
                                level="ERROR",
                                thread_name=thread_name,
                            )

                except Exception as e:
                    _log(
                        f"Error acquiring database semaphore: {e}",
                        level="ERROR",
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
