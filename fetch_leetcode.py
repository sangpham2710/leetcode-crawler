#!/usr/bin/env python3

import requests
from requests.adapters import HTTPAdapter
import json
import os
import sys
import time
import datetime  # Add datetime import
import threading
import queue
import sqlite3  # Import sqlite3 to handle exceptions directly
import random
import logging
import argparse  # For command-line options
import settings  # Central configuration
import util  # Common utilities (session, logging)

# from collections import defaultdict # No longer needed directly for output
import db_utils  # Import the database utility module
from db_utils import DB_FILE
from settings import (
    FULL_COOKIE,
    X_CSRF_TOKEN,
    UUUSERID,
)  # Central auth and cookie configuration

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


# --- CONFIG ---
# HTTP / GraphQL settings from config
URL = settings.URL
USER_AGENT = settings.USER_AGENT
REQUEST_DELAY_SECONDS = settings.REQUEST_DELAY_SECONDS
# Thread configuration
NUM_WORKERS = settings.NUM_WORKERS  # Number of worker threads
# Retry settings
MAX_RETRIES = settings.MAX_RETRIES  # Max retries for failed requests
RETRY_DELAY_SECONDS = settings.RETRY_DELAY_SECONDS

# --- Global Headers ---
# Replicate headers from the latest curl command (favoriteDetailV2ForCompany)
BASE_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "authorization": ";",  # Note: Authorization header seems empty in curl?
    "baggage": "sentry-environment=production,sentry-release=9c5cb314,sentry-transaction=%2Fcompany%2F%5Bslug%5D,sentry-public_key=2a051f9838e2450fbdd5a77eb62cc83c,sentry-trace_id=e699472efb0c4cf081e6b2f7d1639986,sentry-sample_rate=0.03",
    "content-type": "application/json",
    "cookie": FULL_COOKIE,
    "origin": "https://leetcode.com",
    "priority": "u=1, i",
    "random-uuid": "2ab45d54-ea07-e56c-6815-45c5f73fd8e6",
    # Referer is set dynamically per request
    "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sentry-trace": "e699472efb0c4cf081e6b2f7d1639986-869de5c8e5306279-0",  # Updated sentry trace from curl
    "user-agent": USER_AGENT,
    "uuuserid": UUUSERID,
    "x-csrftoken": X_CSRF_TOKEN,  # Use extracted or hardcoded CSRF token
}
# ----------------------

# --- Global Counters & Lock ---
processed_companies = 0
failed_companies_api = 0  # Companies failed due to API errors
failed_companies_db = 0  # Companies failed due to critical DB errors after retries
total_problems_processed = 0
total_categories_processed = 0
counter_lock = threading.Lock()

# --- Database Coordination ---
# Semaphore to limit concurrent database write operations
db_write_semaphore = threading.Semaphore(settings.DB_WRITE_CONCURRENCY)

# --- Dead-Letter Queue ---
dlq = queue.Queue()


def make_request(payload, referer, retries=MAX_RETRIES):
    """Makes a POST request to the LeetCode GraphQL endpoint with retry logic."""
    headers = BASE_HEADERS.copy()
    headers["Referer"] = referer

    for attempt in range(retries):
        response = None  # Initialize response here to handle potential assignment errors before exception
        try:
            session = util.get_session()
            response = session.post(URL, headers=headers, json=payload, timeout=45)

            # Check for server errors which might be temporary
            if 500 <= response.status_code < 600:
                _log(
                    f"Server error ({response.status_code}) for referer '{referer}'. Retrying in {RETRY_DELAY_SECONDS}s...",
                    level="WARN",
                    stream=sys.stderr,
                    thread_name=threading.current_thread().name,
                )
                # Go to the next iteration to retry after delay
                # Add delay *before* continuing the loop
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            # Raise exceptions for other bad statuses (e.g., 4xx client errors)
            response.raise_for_status()

            # Decode JSON response - moved inside try as it can fail
            data = response.json()

            # Check for GraphQL errors within the JSON response
            if "errors" in data:
                _log(
                    f"GraphQL error: {json.dumps(data['errors'])}",
                    level="ERROR",
                    stream=sys.stderr,
                    thread_name=threading.current_thread().name,
                )
                # Log payload and referer for easier debugging of GraphQL errors
                _log(
                    f"   Request Payload: {json.dumps(payload)}",
                    level="ERROR",
                    stream=sys.stderr,
                    thread_name=threading.current_thread().name,
                )
                _log(
                    f"   Referer: {referer}",
                    level="ERROR",
                    stream=sys.stderr,
                    thread_name=threading.current_thread().name,
                )
                return None  # GraphQL errors are usually not retryable

            # If no errors (HTTP, JSON, GraphQL), return the data successfully
            return data

        except requests.exceptions.Timeout:
            _log(
                f"Request timed out for referer: {referer}. Attempt {attempt + 1}/{retries}",
                level="WARN",  # Log timeout as WARN, retry might fix it
                stream=sys.stderr,
                thread_name=threading.current_thread().name,
            )
            # Wait before retrying the loop
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY_SECONDS)
            # else: loop will naturally end after this attempt

        except requests.exceptions.RequestException as e:
            # Includes connection errors, non-5xx HTTP errors after raise_for_status()
            _log(
                f"Request failed for referer {referer}: {e}",
                level="ERROR",
                stream=sys.stderr,
                thread_name=threading.current_thread().name,
            )
            # These are generally not retryable, so exit the function immediately
            return None

        except json.JSONDecodeError as e:
            _log(
                f"Failed to decode JSON response for referer {referer}: {e}",
                level="ERROR",
                stream=sys.stderr,
                thread_name=threading.current_thread().name,
            )
            # Log raw response text if possible
            raw_text = "<Response object not available>"
            if response is not None and hasattr(response, "text"):
                raw_text = response.text[:500] + "..."
            _log(
                f"Raw response text: {raw_text}",
                level="ERROR",
                stream=sys.stderr,
                thread_name=threading.current_thread().name,
            )

            # JSON decode errors are not retryable
            return None

        # If we reached here within the loop without returning/continuing,
        # it implies an error occured where we need to retry (e.g., Timeout)
        # The delay is handled within the except block for Timeout.

    # If the loop completes without returning data, all retries failed
    _log(
        f"All {retries} retries failed for referer: {referer} (likely due to timeouts or repeated server errors).",
        level="ERROR",
        stream=sys.stderr,
        thread_name=threading.current_thread().name,
    )
    return None


def process_company_worker(company_queue):
    """Worker thread function to process companies from the queue."""
    global processed_companies, failed_companies_api, failed_companies_db, total_problems_processed, total_categories_processed

    thread_name = threading.current_thread().name
    worker_conn = None

    try:
        # Configure SQLite connection for this worker with a longer timeout
        worker_conn = db_utils.connect_db(timeout=20.0, thread_name=thread_name)
        if not worker_conn:
            _log(
                f"Failed to get DB connection. Exiting worker.",
                level="DB ERROR",
                stream=sys.stderr,
                thread_name=thread_name,
            )
            return

        while True:
            company_slug = None  # Initialize for finally block
            try:
                company_slug = company_queue.get_nowait()
            except queue.Empty:
                break  # Queue is empty

            company_processed_successfully = False
            company_db_failure = False
            company_api_failure = False

            try:
                # 1. Get or create company ID (Critical DB step)
                # Acquire semaphore for this short database operation
                with db_write_semaphore:
                    company_id = db_utils.get_or_create_id(
                        worker_conn, "Companies", "slug", company_slug
                    )

                if company_id is None:
                    # This means get_or_create_id failed even after retries
                    error_msg = f"Failed to get/create DB entry for company '{company_slug}' after retries."
                    _log(
                        error_msg,
                        level="DB ERROR",
                        stream=sys.stderr,
                        thread_name=thread_name,
                    )
                    dlq.put(
                        {
                            "type": "company_id",
                            "slug": company_slug,
                            "error": "DB lock/Integrity Error",
                            "worker": thread_name,
                            "timestamp": datetime.datetime.now().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            ),
                        }
                    )
                    company_db_failure = True
                    # Skip processing this company further
                    continue

                # 2. Process company data (API calls + further DB ops)
                success = process_company_data(
                    worker_conn, company_slug, company_id, thread_name
                )

                if success:
                    # Commit transaction for this company if all ops succeeded
                    try:
                        with db_write_semaphore:
                            worker_conn.commit()
                        company_processed_successfully = True
                    except sqlite3.Error as commit_e:
                        _log(
                            f"Commit failed for company '{company_slug}': {commit_e}",
                            level="DB ERROR",
                            stream=sys.stderr,
                            thread_name=thread_name,
                        )
                        dlq.put(
                            {
                                "type": "company_commit",
                                "slug": company_slug,
                                "error": str(commit_e),
                                "worker": thread_name,
                                "timestamp": datetime.datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                            }
                        )
                        company_db_failure = (
                            True  # Treat commit failure as DB failure for this company
                        )
                        try:
                            worker_conn.rollback()  # Attempt rollback
                        except:
                            pass
                else:
                    # process_company_data returned False, indicating API or internal DB error
                    # Errors within process_company_data should have logged specifics and potentially added to DLQ
                    # Rollback any partial changes for this company
                    _log(
                        f"Rolling back changes for company '{company_slug}' due to internal errors.",
                        level="WARN",
                        stream=sys.stderr,
                        thread_name=thread_name,
                    )
                    company_api_failure = (
                        True  # Assume API failure if not explicitly DB
                    )
                    try:
                        with db_write_semaphore:
                            worker_conn.rollback()
                    except sqlite3.Error as rb_e:
                        _log(
                            f"Rollback failed for company '{company_slug}': {rb_e}",
                            level="DB WARN",
                            stream=sys.stderr,
                            thread_name=thread_name,
                        )

            except Exception as inner_e:
                # Catch unexpected errors during processing a single company
                _log(
                    f"Unexpected error processing company '{company_slug}': {inner_e}",
                    level="ERROR",
                    stream=sys.stderr,
                    thread_name=thread_name,
                )
                company_api_failure = (
                    True  # Treat unexpected as potential API/logic error
                )
                try:
                    with db_write_semaphore:
                        worker_conn.rollback()
                except:
                    pass
            finally:
                # Update counters based on outcome for this company
                with counter_lock:
                    if company_processed_successfully:
                        processed_companies += 1
                    elif company_db_failure:
                        failed_companies_db += 1
                    else:  # Includes API failures and other unexpected errors
                        failed_companies_api += 1

                if company_slug:
                    company_queue.task_done()  # Signal task completion only if a slug was dequeued

            # Variable delay between companies - shorter if we didn't need to access database a lot
            if company_processed_successfully:
                time.sleep(REQUEST_DELAY_SECONDS)  # Normal delay
            else:
                # Longer delay after failures to allow system to recover
                time.sleep(REQUEST_DELAY_SECONDS * 2)

    except Exception as outer_e:
        # Catch errors during worker setup or loop logic
        _log(
            f"Worker encountered major error: {outer_e}",
            level="ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        # Note: If this happens, tasks might remain in the queue.
    finally:
        if worker_conn:
            db_utils.close_connection(worker_conn)


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
            f"Starting database operations for company {company_slug} ({len(all_problems_data)} problems, {len(categories_to_process)} categories)",
            thread_name=thread_name,
        )

        # 1. Batch process all categories
        _log(
            f"Batch processing {len(categories_to_process)} categories",
            thread_name=thread_name,
        )
        category_id_map = db_utils.batch_get_or_create_categories(
            conn, categories_to_process
        )

        if not category_id_map:
            _log(
                f"Failed to process categories for {company_slug}",
                level="ERROR",
                stream=sys.stderr,
                thread_name=thread_name,
            )
            return False

        local_category_count = len(category_id_map)

        # 2. Batch process all tags (after deduplication)
        all_unique_tags = {}
        for tag in all_tags:
            if "slug" in tag:
                slug = tag["slug"]
                if slug not in all_unique_tags:
                    all_unique_tags[slug] = tag

        _log(
            f"Batch processing {len(all_unique_tags)} unique tags",
            thread_name=thread_name,
        )
        tag_id_map = db_utils.batch_get_or_create_tags(
            conn, list(all_unique_tags.values())
        )

        # 3. Process all collected problems
        _log(
            f"Batch processing {len(all_problems_data)} problems for company {company_slug}",
            thread_name=thread_name,
        )

        # Update problem data with category IDs
        for problem in all_problems_data:
            category_slug = problem.pop(
                "category_slug", None
            )  # Remove and get category_slug
            if category_slug and category_slug in category_id_map:
                problem["category_id"] = category_id_map[category_slug]

        try:
            # Batch get or create all problems
            problem_id_map = db_utils.batch_get_or_create_problems(
                conn, all_problems_data
            )

            # Prepare for batch operations
            problem_company_junctions = []
            problem_category_junctions = []
            problem_tag_junctions = []
            problem_ids_for_timestamps = []

            # Process relationships and other operations
            for problem_data in all_problems_data:
                slug = problem_data["title_slug"]
                problem_id = problem_id_map.get(slug)

                if not problem_id:
                    _log(
                        f"Missing problem_id for slug '{slug}' after batch processing",
                        level="WARN",
                        stream=sys.stderr,
                        thread_name=thread_name,
                    )
                    continue

                # Add problem_id to timestamp update batch
                problem_ids_for_timestamps.append(problem_id)

                # Add to problem-company junction batch
                problem_company_junctions.append(
                    {"problem_id": problem_id, "company_id": company_id}
                )

                # Add to frequency updates if available
                frequency = problem_data.get("frequency")
                if frequency is not None:
                    db_utils.update_problem_company_frequency(
                        conn, problem_id, company_id, frequency
                    )

                # Add to problem-category junction batch
                category_id = problem_data.get("category_id")
                if category_id:
                    problem_category_junctions.append(
                        {
                            "problem_id": problem_id,
                            "category_id": category_id,
                            "company_id": company_id,
                        }
                    )

                # Add to problem-tag junction batch
                for tag_data in problem_data.get("tags", []):
                    tag_slug = tag_data.get("slug")
                    if not tag_slug or tag_slug not in tag_id_map:
                        continue

                    tag_id = tag_id_map[tag_slug]
                    problem_tag_junctions.append(
                        {"problem_id": problem_id, "tag_id": tag_id}
                    )

            # Execute all batch operations
            if problem_company_junctions:
                _log(
                    f"Inserting {len(problem_company_junctions)} problem-company relationships",
                    thread_name=thread_name,
                )
                db_utils.batch_insert_junctions(
                    conn, "ProblemCompanies", problem_company_junctions
                )

            if problem_category_junctions:
                _log(
                    f"Inserting {len(problem_category_junctions)} problem-category relationships",
                    thread_name=thread_name,
                )
                db_utils.batch_insert_junctions(
                    conn, "ProblemCategories", problem_category_junctions
                )

            if problem_tag_junctions:
                _log(
                    f"Inserting {len(problem_tag_junctions)} problem-tag relationships",
                    thread_name=thread_name,
                )
                db_utils.batch_insert_junctions(
                    conn, "ProblemTags", problem_tag_junctions
                )

            # Update all timestamps in one batch
            if problem_ids_for_timestamps:
                db_utils.batch_update_timestamps(
                    conn,
                    "Problems",
                    "problem_id",
                    problem_ids_for_timestamps,
                    "last_fetched_company",
                )

            _log(
                f"Successfully processed {len(problem_ids_for_timestamps)} problems for company {company_slug}",
                thread_name=thread_name,
            )
            local_problem_count = len(problem_ids_for_timestamps)

        except Exception as batch_e:
            _log(
                f"Batch processing failed for company {company_slug}: {batch_e}",
                level="ERROR",
                stream=sys.stderr,
                thread_name=thread_name,
            )
            current_company_success = False

    # Update global counters
    with counter_lock:
        total_categories_processed += local_category_count
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
    """Main function to execute the script"""
    # Parse command-line options
    parser = argparse.ArgumentParser(description="Fetch LeetCode data for companies")
    parser.add_argument("--companies-file", default=settings.COMPANIES_FILE)
    parser.add_argument("--workers", type=int, default=settings.NUM_WORKERS)
    parser.add_argument(
        "--db-concurrency", type=int, default=settings.DB_WRITE_CONCURRENCY
    )
    parser.add_argument(
        "--cookies-from-browser",
        choices=["chrome", "firefox"],
        default=None,
        help="Load cookies directly from your browser via browser_cookie3",
    )
    args = parser.parse_args()
    # Override defaults
    global COMPANIES, NUM_WORKERS, db_write_semaphore, FULL_COOKIE, X_CSRF_TOKEN, UUUSERID
    COMPANIES = json.load(open(args.companies_file))
    NUM_WORKERS = args.workers
    db_write_semaphore = threading.Semaphore(args.db_concurrency)
    # Optionally load cookies from browser
    if args.cookies_from_browser:
        # Pull cookie jar directly from browser
        full_cookie = util.load_cookies_from_browser(
            browser=args.cookies_from_browser, domain="leetcode.com"
        )
        FULL_COOKIE = full_cookie
        # Re-extract CSRF and UUUSERID from loaded cookies
        tokens = [t for t in FULL_COOKIE.split("; ") if t.startswith("csrftoken=")]
        if tokens:
            X_CSRF_TOKEN = tokens[0].split("=")[1]
        tokens = [t for t in FULL_COOKIE.split("; ") if t.startswith("uuuserid=")]
        if tokens:
            UUUSERID = tokens[0].split("=")[1]
        # Update headers map
        BASE_HEADERS["cookie"] = FULL_COOKIE
        BASE_HEADERS["x-csrftoken"] = X_CSRF_TOKEN
        BASE_HEADERS["uuuserid"] = UUUSERID
    start_time = time.time()

    # Setup logging
    setup_logging()
    logging.info("Starting LeetCode data fetch...")

    # Initialize database connection
    logging.info("Initializing Database...")

    # Check if database is locked before proceeding
    try:
        import subprocess

        result = subprocess.run(["lsof", DB_FILE], capture_output=True, text=True)
        if result.stdout.strip():
            logging.error(
                f"Database is locked by another process! Output of lsof:\n{result.stdout}"
            )
            logging.info(
                "Please terminate the processes locking the database and try again."
            )
            return False
    except Exception as e:
        logging.warning(f"Failed to check if database is locked: {e}")

    conn = None
    try:
        conn = db_utils.connect_db()
        global processed_companies, failed_companies_api, failed_companies_db, total_problems_processed, total_categories_processed

        # Reset counters
        processed_companies = 0
        failed_companies_api = 0
        failed_companies_db = 0
        total_problems_processed = 0
        total_categories_processed = 0
        # Clear DLQ if running multiple times in one session ( unlikely here)
        while not dlq.empty():
            try:
                dlq.get_nowait()
            except queue.Empty:
                break

        # Create queue of companies to process
        company_queue = queue.Queue()
        for company_slug in COMPANIES:
            company_queue.put(company_slug)

        total_companies = len(COMPANIES)
        _log(
            f"Starting {NUM_WORKERS} worker threads to process {total_companies} companies..."
        )

        # Start worker threads
        threads = []
        for i in range(
            min(NUM_WORKERS, total_companies)
        ):  # Don't create more threads than companies
            thread = threading.Thread(
                target=process_company_worker,
                args=(company_queue,),
                name=f"Worker-{i+1}",
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)

        # Wait for all tasks to be processed
        company_queue.join()
        _log("All companies processed by workers.")

    except Exception as e:
        _log(
            f"An unexpected error occurred in main loop: {e}",
            level="ERROR",
            stream=sys.stderr,
        )

    finally:
        end_time = time.time()
        duration = end_time - start_time

        _log("-" * 20)
        _log(f"✅ Finished processing.")
        _log(f"   Successfully processed data for: {processed_companies} companies.")
        _log(f"   Total categories processed: {total_categories_processed}")
        _log(f"   Total problems processed: {total_problems_processed}")

        if failed_companies_api > 0:
            _log(
                f"   ⚠️ API/Logic Failures: {failed_companies_api} companies encountered non-DB errors (check logs above).",
                level="WARN",
            )
        if failed_companies_db > 0:
            _log(
                f"   🔥 Critical DB Failures: {failed_companies_db} companies had critical DB errors after retries (see DLQ).",
                level="ERROR",
            )

        # Check if there were any errors in the DLQ and print them
        if not dlq.empty():
            _log("\n" + "=" * 50, level="ERROR", stream=sys.stderr)
            _log(
                "ERRORS ENCOUNTERED DURING EXECUTION:", level="ERROR", stream=sys.stderr
            )
            _log("=" * 50, level="ERROR", stream=sys.stderr)
            error_count = 0

            # Group errors by type for better readability
            error_types = {}

            while not dlq.empty():
                error = dlq.get()
                error_type = error.get("type", "unknown")

                if error_type not in error_types:
                    error_types[error_type] = []

                error_types[error_type].append(error)
                error_count += 1

            # Print errors by type with detailed information
            for error_type, errors in error_types.items():
                _log(
                    f"\n{error_type.upper()} ERRORS ({len(errors)}):",
                    level="ERROR",
                    stream=sys.stderr,
                )
                _log("-" * 50, level="ERROR", stream=sys.stderr)

                for i, error in enumerate(errors, 1):
                    timestamp = error.get("timestamp", "unknown time")
                    worker = error.get("worker", "unknown worker")
                    error_msg = error.get("error", "No error message")

                    # Build detailed entry based on error type
                    if error_type == "company_api":
                        company = error.get("company", "unknown")
                        _log(
                            f"{i}. [TIME: {timestamp}] [WORKER: {worker}] Company: {company}",
                            level="ERROR",
                            stream=sys.stderr,
                        )
                        _log(f"   Error: {error_msg}", level="ERROR", stream=sys.stderr)

                    elif error_type == "company_id" or error_type == "company_commit":
                        company = error.get("slug", "unknown")
                        _log(
                            f"{i}. [TIME: {timestamp}] [WORKER: {worker}] Company: {company}",
                            level="ERROR",
                            stream=sys.stderr,
                        )
                        _log(f"   Error: {error_msg}", level="ERROR", stream=sys.stderr)

                    elif error_type == "category_id":
                        company = error.get("company", "unknown")
                        category = error.get("category", "unknown")
                        _log(
                            f"{i}. [TIME: {timestamp}] [WORKER: {worker}] Company: {company}, Category: {category}",
                            level="ERROR",
                            stream=sys.stderr,
                        )
                        _log(f"   Error: {error_msg}", level="ERROR", stream=sys.stderr)

                    elif error_type == "problem_id":
                        company = error.get("company", "unknown")
                        category = error.get("category", "unknown")
                        problem = error.get("problem", "unknown")
                        _log(
                            f"{i}. [TIME: {timestamp}] [WORKER: {worker}] Company: {company}, Category: {category}, Problem: {problem}",
                            level="ERROR",
                            stream=sys.stderr,
                        )
                        _log(f"   Error: {error_msg}", level="ERROR", stream=sys.stderr)

                    else:
                        # Generic fallback for unknown error types
                        _log(
                            f"{i}. [TIME: {timestamp}] [WORKER: {worker}]",
                            level="ERROR",
                            stream=sys.stderr,
                        )
                        _log(
                            f"   Error details: {error}",
                            level="ERROR",
                            stream=sys.stderr,
                        )

            _log("\n" + "=" * 50, level="ERROR", stream=sys.stderr)
            _log(f"TOTAL ERRORS: {error_count}", level="ERROR", stream=sys.stderr)
            _log("=" * 50 + "\n", level="ERROR", stream=sys.stderr)
        elif failed_companies_db == 0 and failed_companies_api == 0:
            _log("   ✨ No companies failed.")

        _log(f"   Total time: {duration:.2f} seconds")
        _log(f"   Workers: {NUM_WORKERS}")

    # At the end of main:
    if conn:
        db_utils.close_connection(conn)
        _log("Database connection closed")

    return True


if __name__ == "__main__":
    # Check if companies.json exists
    try:
        with open("companies.json", "r") as f:
            COMPANIES = json.load(f)
        if not isinstance(COMPANIES, list) or not all(
            isinstance(c, str) for c in COMPANIES
        ):
            _log(
                "Error: companies.json should contain a JSON list of strings.",
                level="ERROR",
                stream=sys.stderr,
            )
            sys.exit(1)
    except FileNotFoundError:
        _log(
            "Error: companies.json not found in the current directory.",
            level="ERROR",
            stream=sys.stderr,
        )
        sys.exit(1)
    except json.JSONDecodeError:
        _log(
            "Error: Could not decode JSON from companies.json.",
            level="ERROR",
            stream=sys.stderr,
        )
        sys.exit(1)

    main()
