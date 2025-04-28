#!/usr/bin/env python3
"""
LeetCode Problem and Company Data Fetcher

This module fetches problem data from LeetCode, including:
- Company to problem relationships
- Problem metadata (difficulty, title, tags)
- Problem frequency data

It uses concurrent workers to efficiently process multiple companies and
implements batch database operations to minimize lock contention.

The module is designed to be resilient to network issues and database locks,
with graceful error handling and retries where appropriate.

Usage:
    python fetch_leetcode.py [options]

Author: [Your Name]
"""

import db_utils
import requests
import json
import os
import sys
import time
import datetime
import random
import logging
import threading
import queue
import argparse

import util
import settings

# Import settings with explicit namespace to make it clear where they come from
from settings import (
    NUM_WORKERS,
    DB_WRITE_CONCURRENCY,
    COMPANIES_FILE,
    API_URL as URL,
    USER_AGENT,
    FULL_COOKIE,
    X_CSRF_TOKEN,
    UUUSERID,
    REQUEST_DELAY_SECONDS,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
    TIMEOUT_SECONDS,
)

# --- ANSI Color Codes ---
RED = "\033[91m"
RESET = "\033[0m"
YELLOW = "\033[93m"


# --- Setup Logging Function ---
def setup_logging():
    """Configure the Python logging module with proper formatting"""
    util.setup_logging()


# --- Logging Helper (with color and thread name) ---
def _log(message, level="INFO", stream=sys.stdout, thread_name=None):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    thread_prefix = f"[{thread_name}] " if thread_name else ""
    log_prefix = f"{timestamp} [{level}] {thread_prefix}"

    if level in ["ERROR", "DB ERROR"] and stream is sys.stderr:
        print(f"{log_prefix}{RED}{message}{RESET}", file=stream)
    elif level in ["WARN", "DB WARN"] and stream is sys.stderr:
        print(f"{log_prefix}{YELLOW}{message}{RESET}", file=stream)
    else:
        print(f"{log_prefix}{message}", file=stream)


# --- Global variables for tracking ---
total_problems_processed = 0  # Total problems processed
total_categories_processed = 0  # Total categories processed
counter_lock = threading.Lock()  # Lock to prevent race conditions in counters
successful_companies = set()  # Track successfully processed companies
failed_companies = set()  # Track failed companies

# Initialize the DB write semaphore for limiting concurrent DB access
db_write_semaphore = threading.Semaphore(DB_WRITE_CONCURRENCY)


def make_request(payload, referer, retries=MAX_RETRIES):
    """Make a request to the LeetCode GraphQL endpoint with retries."""
    # This simple function gets replaced with the decorator version
    return _make_request_impl(payload, referer)


@util.retry_with_backoff(max_retries=MAX_RETRIES, initial_delay=RETRY_DELAY_SECONDS)
def _make_request_impl(payload, referer):
    """
    Implementation of the GraphQL request with proper headers.

    This is wrapped by the retry decorator for automatic retries.

    Args:
        payload: The GraphQL query payload
        referer: The referer URL for the request

    Returns:
        dict: Parsed JSON response

    Raises:
        requests.RequestException: If the request fails after retries
    """
    logger = logging.getLogger(__name__)

    # Build headers with dynamic referer
    headers = {
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
        "Referer": referer,
        "Origin": "https://leetcode.com",
        "x-csrftoken": X_CSRF_TOKEN,
        "Cookie": FULL_COOKIE,
        "uuuserid": UUUSERID,
    }

    # Add small random delay to prevent rate limiting
    time.sleep(random.uniform(0.2, REQUEST_DELAY_SECONDS))

    # Get the session from our utility that has retry logic built in
    session = util.get_session()
    response = session.post(URL, json=payload, headers=headers, timeout=TIMEOUT_SECONDS)

    # Raise exceptions for HTTP errors
    response.raise_for_status()

    # Parse and return JSON
    return response.json()


def process_company_worker(company_queue):
    """
    Worker function to process companies from a queue.

    Processes each company entry from the queue until empty, tracking
    success/failure and ensuring graceful thread cleanup.

    Args:
        company_queue: Queue containing company slugs to process
    """
    logger = logging.getLogger(__name__)
    thread_name = threading.current_thread().name

    # Register this thread for shutdown tracking
    util.register_thread()

    try:
        # Create a thread-local database connection
        with db_utils.connect_db(thread_name=thread_name) as conn:
            logger.info(f"Worker starting")

            # Process companies from the queue until empty or shutdown requested
            while not util.shutdown_requested():
                try:
                    # Get a company from the queue with a 1-second timeout
                    # This allows checking the shutdown flag periodically
                    company_slug = company_queue.get(timeout=1.0)
                except queue.Empty:
                    break  # Queue is empty, exit the loop
                except Exception as e:
                    logger.warning(f"Error getting item from queue: {e}")
                    continue  # Skip this iteration if queue get fails

                # Process this company
                try:
                    with util.operation_timer(f"process_company_{company_slug}"):
                        logger.info(f"Processing company: {company_slug}")

                        # Fetch company_id (possibly create entry)
                        company_id = db_utils.get_or_create_id(
                            conn,
                            "Companies",
                            "slug",
                            company_slug,
                            {"name": company_slug},
                        )

                        if company_id is None:
                            logger.error(
                                f"Failed to get or create company ID for {company_slug}. Skipping."
                            )
                            with counter_lock:
                                failed_companies.add(company_slug)
                            company_queue.task_done()
                            continue

                        # Process the company data
                        success = process_company_data(
                            conn, company_slug, company_id, thread_name
                        )

                        # Track result
                        if success:
                            with counter_lock:
                                successful_companies.add(company_slug)
                        else:
                            with counter_lock:
                                failed_companies.add(company_slug)

                except Exception as e:
                    logger.exception(f"Error processing company {company_slug}: {e}")
                    with counter_lock:
                        failed_companies.add(company_slug)

                finally:
                    # Always mark task as done
                    company_queue.task_done()

            if util.shutdown_requested():
                logger.info("Shutdown requested - worker exiting")

    except Exception as e:
        logger.exception(f"Fatal worker error: {e}")

    finally:
        # Always unregister the thread when done
        util.unregister_thread()
        logger.info("Worker finished")


def process_company_data(conn, company_slug, company_id, thread_name=None):
    """Fetches categories and questions for a company and stores them in the DB."""
    global total_problems_processed, total_categories_processed

    _log(f"Processing company: {company_slug}", thread_name=thread_name)
    current_company_success = True  # Track success within this company

    # API calls don't need semaphore as they don't access the database
    # 1. Fetch Categories for the company using the new favoriteDetailV2ForCompany query
    _log(
        f"Fetching categories for {company_slug} using favoriteDetailV2ForCompany",
        level="DEBUG",
        thread_name=thread_name,
    )
    # Use the exact query from the latest working curl
    category_query_string = """
        query favoriteDetailV2ForCompany($favoriteSlug: String!) {
          favoriteDetailV2(favoriteSlug: $favoriteSlug) {
            coverUrl
            coverEmoji
            coverBackgroundColor
            description
            favoriteLockStatus
            creator {
              realName
              userAvatar
              userSlug
            }
            hasCurrentQuestion
            isPublicFavorite
            lastQuestionAddedAt
            name
            questionNumber
            slug
            isDefaultList
            collectCount
            companyVerified
            generateFromFavoriteSlug
            favoriteType
            lastModified: lastQuestionAddedAt
            industryDisplay
            scaleDisplay
            financingStageDisplay
            website
            companyLegalName
            favoriteLockStatus
            languageTagSlug
            positionRoleTags {
              name
              nameTranslated
              slug
            }
            generatedFavoritesInfo {
              defaultFavoriteSlug
              categoriesToSlugs {
                categoryName # Use this or displayName if available
                favoriteSlug
                displayName # Prefer this for the name?
              }
            }
          }
        }
    """
    category_payload = {
        "operationName": "favoriteDetailV2ForCompany",  # Use the new operation name
        "query": category_query_string,
        "variables": {"favoriteSlug": company_slug},  # Variable name matches the query
    }
    referer_cat = (
        f"https://leetcode.com/company/{company_slug}/"  # Referer for the company page
    )
    cat_response = make_request(category_payload, referer_cat)

    # --- Adjust response parsing logic for favoriteDetailV2ForCompany ---
    if (
        not cat_response
        or not cat_response.get("data")
        or not cat_response["data"].get("favoriteDetailV2")
        or not cat_response["data"]["favoriteDetailV2"].get("generatedFavoritesInfo")
    ):
        _log(
            f"Could not fetch or parse expected category data structure (favoriteDetailV2.generatedFavoritesInfo) for {company_slug}. Skipping.",
            level="WARN",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        if cat_response and cat_response.get("data"):
            _log(
                f"Received data structure: {json.dumps(cat_response['data'], indent=2)}",
                level="DEBUG",
                thread_name=thread_name,
            )
        if cat_response and cat_response.get("errors"):
            _log(
                f"Received GraphQL errors: {json.dumps(cat_response['errors'], indent=2)}",
                level="DEBUG",
                thread_name=thread_name,
            )
        # Add to the dead-letter queue for failed companies
        dlq.put(
            {
                "type": "company_api",
                "company": company_slug,
                "error": "API request failed or returned no data",
                "worker": thread_name,
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        return False  # Indicate failure

    # Extract categories from the correct path
    categories_info = cat_response["data"]["favoriteDetailV2"].get(
        "generatedFavoritesInfo", {}
    )
    raw_categories = categories_info.get("categoriesToSlugs", [])

    # Process the extracted categories, preferring displayName
    categories_to_process = []
    for item in raw_categories:
        if item and "favoriteSlug" in item:
            # Use displayName if available and non-empty, otherwise fallback to categoryName or favoriteSlug
            name = (
                item.get("displayName")
                or item.get("categoryName")
                or item["favoriteSlug"]
            )
            categories_to_process.append({"slug": item["favoriteSlug"], "name": name})

    # --- End response parsing adjustment ---

    if not categories_to_process:
        _log(
            f"No categories found for {company_slug} in the response.",
            thread_name=thread_name,
        )
        if cat_response and cat_response.get("data"):
            _log(
                f"Received data structure: {json.dumps(cat_response['data'], indent=2)}",
                level="DEBUG",
                thread_name=thread_name,
            )
        return True  # Not necessarily an error, could be company has no lists

    # Fetch questions for all categories before batch database operations
    all_problems_data = []
    all_tags = []  # Collect all unique tags for batch processing
    categories_with_problems = {}

    for category_info in categories_to_process:
        category_slug = category_info["slug"]
        _log(
            f"Processing category: {category_slug}",
            level="DEBUG",
            thread_name=thread_name,
        )

        limit = 2000
        skip = 0
        has_more = True
        total_fetched_for_category = 0
        category_problems = []

        while has_more:
            _log(
                f"Fetching questions (skip={skip}, limit={limit})",
                level="DEBUG",
                thread_name=thread_name,
            )
            # Keep original favoriteQuestionList query and variables from previous successful run
            query_string = """
                query favoriteQuestionList(
                    $favoriteSlug: String!,
                    $skip: Int!,
                    $limit: Int!,
                    $filter: FavoriteQuestionFilterInput,
                    $filtersV2: QuestionFilterInput,
                    $sortBy: QuestionSortByInput,
                    $searchKeyword: String,
                    $version: String = "v2"
                ) {
                    favoriteQuestionList(
                        favoriteSlug: $favoriteSlug,
                        filter: $filter,
                        filtersV2: $filtersV2,
                        sortBy: $sortBy,
                        limit: $limit,
                        skip: $skip,
                        searchKeyword: $searchKeyword,
                        version: $version
                    ) {
                        questions {
                            difficulty
                            id
                            paidOnly
                            questionFrontendId
                            status
                            title
                            titleSlug
                            translatedTitle
                            isInMyFavorites
                            frequency
                            acRate
                            topicTags {
                                name
                                nameTranslated
                                slug
                            }
                        }
                        totalLength
                        hasMore
                    }
                }
            """
            questions_payload = {
                "operationName": "favoriteQuestionList",
                "query": query_string,
                "variables": {
                    "favoriteSlug": category_slug,
                    "skip": skip,
                    "limit": limit,
                    "filter": None,
                    "filtersV2": {
                        "filterCombineType": "ALL",
                        "statusFilter": {"questionStatuses": [], "operator": "IS"},
                        "difficultyFilter": {"difficulties": [], "operator": "IS"},
                        "languageFilter": {"languageSlugs": [], "operator": "IS"},
                        "topicFilter": {"topicSlugs": [], "operator": "IS"},
                        "acceptanceFilter": {},
                        "frequencyFilter": {},
                        "lastSubmittedFilter": {},
                        "publishedFilter": {},
                        "companyFilter": {"companySlugs": [], "operator": "IS"},
                        "positionFilter": {"positionSlugs": [], "operator": "IS"},
                        "premiumFilter": {"premiumStatus": [], "operator": "IS"},
                    },
                    "searchKeyword": "",
                    "sortBy": {"sortField": "AC_RATE", "sortOrder": "DESCENDING"},
                    "version": "v2",
                },
            }
            referer_q = f"https://leetcode.com/company/{company_slug}/?favoriteSlug={category_slug}"
            q_response = make_request(questions_payload, referer_q)
            if (
                not q_response
                or "data" not in q_response
                or "favoriteQuestionList" not in q_response["data"]
            ):
                _log(
                    f"Failed fetch batch (skip={skip}) for {category_slug}. Skipping rest.",
                    level="ERROR",
                    stream=sys.stderr,
                    thread_name=thread_name,
                )
                has_more = False
                time.sleep(1)
                continue
            question_list_data = q_response["data"]["favoriteQuestionList"]
            questions = question_list_data.get("questions", [])
            count = len(questions)
            has_more = question_list_data.get("hasMore", False)
            total_fetched_for_category += count
            _log(
                f"Received count={count} for {category_slug} (skip={skip}). HasMore={has_more}",
                level="DEBUG",
                thread_name=thread_name,
            )

            # Process and collect problems data
            for item in questions:
                slug = item.get("titleSlug")
                if not slug:
                    _log(
                        f"Skipping item missing titleSlug: {item.get('title')}",
                        level="WARN",
                        stream=sys.stderr,
                        thread_name=thread_name,
                    )
                    continue

                # Normalize difficulty
                difficulty = item.get("difficulty")
                normalized_difficulty = None
                if isinstance(difficulty, str):
                    normalized_difficulty = difficulty.strip().title()
                    if normalized_difficulty not in ("Easy", "Medium", "Hard"):
                        _log(
                            f"Unexpected difficulty value '{difficulty}' for problem '{slug}'. Setting difficulty to NULL.",
                            level="WARN",
                            stream=sys.stderr,
                            thread_name=thread_name,
                        )
                        normalized_difficulty = None
                elif difficulty is not None and not isinstance(difficulty, str):
                    _log(
                        f"Unexpected type for difficulty value ({type(difficulty)}: {difficulty}) for problem '{slug}'. Setting difficulty to NULL.",
                        level="WARN",
                        stream=sys.stderr,
                        thread_name=thread_name,
                    )

                # Collect tags for batch processing
                tags = item.get("topicTags", [])
                for tag in tags:
                    if tag and "slug" in tag:
                        all_tags.append(
                            {
                                "slug": tag.get("slug"),
                                "name": tag.get("name"),
                                "translated_name": tag.get("nameTranslated"),
                            }
                        )

                # Add to problems collection
                problem_data = {
                    "title_slug": slug,
                    "title": item.get("title"),
                    "difficulty": normalized_difficulty,
                    "ac_rate": item.get("acRate"),
                    "is_paid_only": item.get("paidOnly", False),
                    "question_frontend_id": item.get("questionFrontendId"),
                    "frequency": item.get("frequency"),
                    "category_slug": category_slug,  # Store category slug for later mapping
                    "tags": tags,
                }

                category_problems.append(problem_data)
                all_problems_data.append(problem_data)

            if has_more:
                skip += limit
                time.sleep(0.5)

        _log(
            f"Fetched total {total_fetched_for_category} questions for category {category_slug}.",
            thread_name=thread_name,
        )
        categories_with_problems[category_slug] = total_fetched_for_category

    # Add random delay between 0.5 and 1.5 seconds to stagger database operations
    time.sleep(random.uniform(0.5, 1.5))

    # If there are no problems to process, we're done
    if not all_problems_data:
        _log(f"No problems found for company {company_slug}.", thread_name=thread_name)
        return True

    # Now perform all database operations with the semaphore to limit concurrent access
    _log(
        f"Acquiring DB write semaphore for company {company_slug}...",
        thread_name=thread_name,
    )
    with db_write_semaphore:
        _log(
            f"Starting combined transaction for company {company_slug} ({len(all_problems_data)} problems, {len(categories_to_process)} categories)",
            thread_name=thread_name,
        )
        conn.execute("BEGIN")

        category_id_map = db_utils.batch_get_or_create_categories(
            conn, categories_to_process
        )
        all_unique_tags = {t["slug"]: t for t in all_tags if "slug" in t}
        tag_id_map = db_utils.batch_get_or_create_tags(
            conn, list(all_unique_tags.values())
        )
        problem_id_map = db_utils.batch_get_or_create_problems(conn, all_problems_data)

        # Prepare junctions and updates
        problem_company_junctions = []
        problem_category_junctions = []
        problem_tag_junctions = []
        problem_ids_for_timestamps = []
        frequency_updates = []
        for problem in all_problems_data:
            slug = problem["title_slug"]
            pid = problem_id_map.get(slug)
            if not pid:
                continue
            problem_ids_for_timestamps.append(pid)
            problem_company_junctions.append(
                {"problem_id": pid, "company_id": company_id}
            )
            freq = problem.get("frequency")
            if freq is not None:
                frequency_updates.append(
                    {"problem_id": pid, "company_id": company_id, "frequency": freq}
                )
            cid = problem.get("category_id")
            if cid:
                problem_category_junctions.append(
                    {"problem_id": pid, "category_id": cid, "company_id": company_id}
                )
            for tag in problem.get("tags", []):
                ts = tag.get("slug")
                if ts and ts in tag_id_map:
                    problem_tag_junctions.append(
                        {"problem_id": pid, "tag_id": tag_id_map[ts]}
                    )

        # Insert junction tables under lock
        if problem_company_junctions:
            db_utils.batch_insert_junctions(
                conn, "ProblemCompanies", problem_company_junctions
            )
        if frequency_updates:
            db_utils.batch_update_problem_company_frequencies(conn, frequency_updates)
            if problem_category_junctions:
                db_utils.batch_insert_junctions(
                    conn, "ProblemCategories", problem_category_junctions
                )
            if problem_tag_junctions:
                db_utils.batch_insert_junctions(
                    conn, "ProblemTags", problem_tag_junctions
                )
            if problem_ids_for_timestamps:
                db_utils.batch_update_timestamps(
                    conn,
                    "Problems",
                    "problem_id",
                    problem_ids_for_timestamps,
                    "last_fetched_company",
                )

        conn.commit()
        logger.info(f"Committed combined transaction for company {company_slug}")

        local_problem_count = len(problem_ids_for_timestamps)

    # Update global counters
    with counter_lock:
        total_categories_processed += len(category_id_map)
        total_problems_processed += local_problem_count

    _log(f"Completed processing company {company_slug}", thread_name=thread_name)
    return current_company_success  # Return True only if NO critical errors occurred for this company


def process_problem_details_worker(problem_queue):
    """Worker function to process problems and update their details"""
    conn = None
    try:
        conn = db_utils.connect_db(thread_name=threading.current_thread().name)

        # Process problems until the queue is empty
        while True:
            try:
                problem = problem_queue.get_nowait()
                if not problem:
                    break

                detail_data = fetch_problem_detail(problem["title_slug"])
                if detail_data:
                    db_utils.update_problem_details(
                        conn, problem["title_slug"], detail_data
                    )

            except queue.Empty:
                break
            except Exception as e:
                logging.error(
                    f"Error processing problem detail for: {problem.get('title_slug', 'unknown')}: {e}"
                )
            finally:
                problem_queue.task_done()
    except Exception as e:
        logging.error(f"Worker error: {e}")
    finally:
        if conn:
            db_utils.close_connection(conn)


def fetch_problem_detail(title_slug):
    """Fetch detailed information about a specific problem"""
    if not title_slug:
        return None

    # GraphQL query for problem details
    query_string = """
    query questionData($titleSlug: String!) {
      question(titleSlug: $titleSlug) {
        questionId
        questionFrontendId
        title
        titleSlug
        content
        difficulty
        likes
        dislikes
        isPaidOnly
        codeSnippets {
          lang
          langSlug
          code
        }
        topicTags {
          name
          slug
        }
        companyTagStats {
          companyName
          tagNum
        }
        stats
        hints
        solution {
          id
          content
        }
        status
        sampleTestCase
        metaData
        judgerAvailable
        judgeType
        mysqlSchemas
        enableRunCode
        enableTestMode
        enableDebugger
        envInfo
        libraryUrl
        adminUrl
        challengeQuestion {
          id
          date
          incompleteChallengeCount
          streakCount
          type
        }
      }
    }
    """

    payload = {
        "operationName": "questionData",
        "variables": {"titleSlug": title_slug},
        "query": query_string,
    }

    referer = f"https://leetcode.com/problems/{title_slug}/"
    response = make_request(payload, referer)

    if not response or "data" not in response or "question" not in response["data"]:
        logging.warning(f"Failed to fetch problem details for {title_slug}")
        return None

    question_data = response["data"]["question"]

    # Extract only the fields we need
    result = {
        "title_slug": title_slug,
        "title": question_data.get("title"),
        "content": question_data.get("content"),
        "difficulty": question_data.get("difficulty"),
        "question_frontend_id": question_data.get("questionFrontendId"),
        "ac_rate": parse_ac_rate(question_data.get("stats")),
        "is_paid_only": question_data.get("isPaidOnly", False),
    }

    # Add solution content if available
    if question_data.get("solution") and question_data["solution"].get("content"):
        result["editorial_content"] = question_data["solution"]["content"]
        result["editorial_uuid"] = question_data["solution"].get("id")

    return result


def parse_ac_rate(stats_json):
    """Parse acceptance rate from stats JSON string"""
    if not stats_json:
        return None

    try:
        stats = json.loads(stats_json)
        if "acRate" in stats:
            ac_rate_str = stats["acRate"]
            return float(ac_rate_str.rstrip("%")) / 100
    except (json.JSONDecodeError, ValueError, KeyError):
        pass

    return None


def main():
    """
    Main entry point for the LeetCode problem and company data fetcher.

    Handles command-line arguments, initializes the environment, creates
    worker threads, and processes the company queue with a graceful
    shutdown mechanism.

    Returns:
        int: 0 for success, 1 for failure
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Fetch LeetCode data for companies")
    parser.add_argument(
        "--companies-file",
        default=COMPANIES_FILE,
        help="Path to JSON file with company data",
    )
    parser.add_argument(
        "--workers", type=int, default=NUM_WORKERS, help="Number of worker threads"
    )
    parser.add_argument(
        "--db-concurrency",
        type=int,
        default=DB_WRITE_CONCURRENCY,
        help="Max concurrent DB operations",
    )
    parser.add_argument(
        "--log-file", default=settings.LOG_FILE, help="Log file to write to"
    )
    parser.add_argument(
        "--no-rich", action="store_true", help="Disable rich console logging"
    )
    parser.add_argument(
        "--sync-mode",
        choices=["OFF", "NORMAL", "FULL"],
        default=settings.SQLITE_SYNCHRONOUS,
        help="SQLite synchronous mode",
    )
    parser.add_argument(
        "--cookies-from-browser",
        choices=["chrome", "firefox"],
        default=None,
        help="Load cookies directly from your browser via browser_cookie3",
    )
    parser.add_argument("--filter", help="Only process companies matching this filter")
    args = parser.parse_args()

    # Set up logging with our enhanced utility
    util.setup_logging(
        level=settings.LOG_LEVEL, log_file=args.log_file, rich_output=not args.no_rich
    )

    logger = logging.getLogger(__name__)
    logger.info("LeetCode Company Data Fetcher starting")

    # Update settings based on args
    if args.sync_mode != settings.SQLITE_SYNCHRONOUS:
        settings.SQLITE_SYNCHRONOUS = args.sync_mode
        logger.info(f"Overriding SQLite synchronous mode to: {args.sync_mode}")

    # Optionally load cookies from browser
    if args.cookies_from_browser:
        try:
            logger.info(
                f"Attempting to load cookies from {args.cookies_from_browser}..."
            )
            loaded_cookie = util.load_cookies_from_browser(
                browser=args.cookies_from_browser, domain="leetcode.com"
            )

            # Only proceed if cookies were actually loaded
            if loaded_cookie:
                settings.FULL_COOKIE = loaded_cookie

                # Re-extract CSRF and UUUSERID from loaded cookies
                settings.X_CSRF_TOKEN = next(
                    (
                        t.split("=")[1]
                        for t in settings.FULL_COOKIE.split("; ")
                        if t.startswith("csrftoken=")
                    ),
                    "",
                )
                settings.UUUSERID = next(
                    (
                        t.split("=")[1]
                        for t in settings.FULL_COOKIE.split("; ")
                        if t.startswith("uuuserid=")
                    ),
                    "",
                )
                logger.info("Successfully loaded and configured cookies from browser.")
            else:
                logger.warning(
                    "Failed to load cookies from browser or no cookies found."
                )
                # Continue without browser cookies, rely on env var or empty string

        except Exception as e:
            # Catch any unexpected error during the loading process itself
            logger.error(
                f"Unexpected error during cookie loading from {args.cookies_from_browser}: {e}"
            )
            # Potentially return 1 here if cookies are critical
            # return 1

    # Use custom global for DB concurrency to avoid import order issues
    global db_write_semaphore
    if args.db_concurrency != DB_WRITE_CONCURRENCY:
        db_write_semaphore = threading.Semaphore(args.db_concurrency)

    # Display configuration summary
    settings.print_config_summary()

    # Load company data
    try:
        with open(args.companies_file, "r") as f:
            all_companies_data = json.load(f)
            logger.info(
                f"Loaded {len(all_companies_data)} companies from {args.companies_file}"
            )
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logger.error(f"Failed to load companies file: {e}")
        return 1

    # Filter companies if requested
    if args.filter:
        all_companies_data = [
            c for c in all_companies_data if args.filter.lower() in c["slug"].lower()
        ]
        logger.info(
            f"Filtered to {len(all_companies_data)} companies matching '{args.filter}'"
        )

    if not all_companies_data:
        logger.error("No companies to process. Exiting.")
        return 1

    # Create a queue of companies to process
    company_queue = queue.Queue()
    for company_slug in all_companies_data:  # Iterate directly over the list of slugs
        company_queue.put(company_slug)

    # Create and start worker threads
    threads = []
    for i in range(min(args.workers, len(all_companies_data))):
        thread = threading.Thread(
            target=process_company_worker,
            args=(company_queue,),
            name=f"Worker-{i+1}",
            daemon=True,  # Allow program to exit if these threads are still running
        )
        threads.append(thread)
        thread.start()

    # Wait for all work to be completed or for graceful shutdown
    try:
        # Wait for the queue to be empty
        while not company_queue.empty():
            logger.info(
                f"Progress: {len(successful_companies) + len(failed_companies)}/{len(all_companies_data)} companies processed"
            )

            # Check if all workers have died unexpectedly
            active_workers = sum(1 for t in threads if t.is_alive())
            if active_workers == 0:
                logger.error("All worker threads have died. Exiting.")
                return 1

            # Sleep briefly before checking again
            time.sleep(5)

            # Exit early if shutdown requested
            if util.shutdown_requested():
                logger.warning(
                    "Shutdown requested. Waiting for workers to finish current tasks..."
                )
                break

        # If we're still running (not shutting down), wait for all tasks to complete
        if not util.shutdown_requested():
            logger.info(
                "All companies added to queue. Waiting for processing to complete..."
            )
            company_queue.join()

    except KeyboardInterrupt:
        logger.warning("Received keyboard interrupt. Starting graceful shutdown...")
        # The shutdown handler registered in util will set the shutdown flag

    # Wait for active threads to finish (with timeout)
    wait_start = time.time()
    while util.get_active_thread_count() > 0:
        if time.time() - wait_start > 30:  # 30 second max wait
            logger.warning(
                f"Timeout waiting for {util.get_active_thread_count()} threads to exit"
            )
            break
        time.sleep(0.5)

    # Print summary
    success_count = len(successful_companies)
    fail_count = len(failed_companies)

    logger.info("=" * 60)
    logger.info("📊 SUMMARY:")
    logger.info(f"  Total companies: {len(all_companies_data)}")
    logger.info(f"  Successfully processed: {success_count}")
    logger.info(f"  Failed: {fail_count}")

    if failed_companies:
        logger.warning(f"  Failed companies: {', '.join(sorted(failed_companies))}")

    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        logging.exception(f"Unhandled exception in main: {e}")
        sys.exit(1)
