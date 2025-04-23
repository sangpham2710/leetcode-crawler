#!/usr/bin/env python3

import sqlite3
import sys
from pathlib import Path
import datetime
import json
import time
import random
import os

# --- CONFIG ---
DB_FILE = "leetcode_data.sqlite"
DB_LOCK_MAX_RETRIES = 5  # Max attempts to retry a locked database operation
DB_LOCK_RETRY_DELAY_MIN = 0.1  # Min seconds to wait before retry
DB_LOCK_RETRY_DELAY_MAX = 0.5  # Max seconds to wait before retry
DB_BUSY_TIMEOUT_MS = 10000  # Increased from 5000ms to 10000ms to allow more time for SQLite to resolve locks
# --- END CONFIG ---

# --- ANSI Color Codes ---
RED = "\033[91m"
RESET = "\033[0m"
YELLOW = "\033[93m"


# --- Logging Helper (with color) ---
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


# --- Define get_db_connection FIRST ---
def get_db_connection():
    """Establishes a connection to the SQLite database with busy timeout."""
    thread_name = None
    try:
        import threading

        thread_name = threading.current_thread().name
    except:
        pass

    try:
        conn = sqlite3.connect(
            DB_FILE, timeout=DB_BUSY_TIMEOUT_MS / 1000.0
        )  # timeout is in seconds
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")

        # Enable WAL mode for better concurrency
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")

        # conn.execute(f'PRAGMA busy_timeout = {DB_BUSY_TIMEOUT_MS}') # Alternative way to set timeout
        return conn
    except sqlite3.Error as e:
        _log(
            f"Could not connect to database {DB_FILE}:\n"
            f"Database file: {DB_FILE}\n"
            f"Timeout: {DB_BUSY_TIMEOUT_MS/1000.0}s\n"
            f"Current directory: {Path.cwd()}\n"
            f"Error: {e}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        # Exit if initial connection fails
        sys.exit(1)


# --- DB Operation Decorator for Retries ---
def retry_on_lock(func):
    """Decorator to retry a function call if a SQLite OperationalError (database is locked) occurs."""

    def wrapper(*args, **kwargs):
        retries = 0
        # Get thread name if threading is being used
        thread_name = None
        try:
            import threading

            thread_name = threading.current_thread().name
        except:
            pass

        # Extract SQL and params if this is a DB function
        sql = None
        params = None
        if func.__name__ in ["_execute_write", "_execute_read_one"] and len(args) >= 2:
            sql = args[1]
            params = args[2] if len(args) > 2 else None

        while retries < DB_LOCK_MAX_RETRIES:
            try:
                return func(*args, **kwargs)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    retries += 1
                    if retries >= DB_LOCK_MAX_RETRIES:
                        query_info = f"\nSQL: {sql}\nParams: {params}" if sql else ""
                        _log(
                            f"Operation '{func.__name__}' failed after {retries} retries due to DB lock: {e}{query_info}",
                            level="DB ERROR",
                            stream=sys.stderr,
                            thread_name=thread_name,
                        )
                        raise  # Re-raise the exception after max retries
                    wait_time = random.uniform(
                        DB_LOCK_RETRY_DELAY_MIN, DB_LOCK_RETRY_DELAY_MAX
                    )
                    query_info = f"\nSQL: {sql}\nParams: {params}" if sql else ""
                    _log(
                        f"Database locked during '{func.__name__}'. Retrying ({retries}/{DB_LOCK_MAX_RETRIES}) in {wait_time:.3f}s...{query_info}",
                        level="DB WARN",
                        stream=sys.stderr,
                        thread_name=thread_name,
                    )
                    time.sleep(wait_time)
                else:
                    # Re-raise other OperationalErrors immediately
                    query_info = f"\nSQL: {sql}\nParams: {params}" if sql else ""
                    _log(
                        f"Operational error during '{func.__name__}': {e}{query_info}",
                        level="DB ERROR",
                        stream=sys.stderr,
                        thread_name=thread_name,
                    )
                    raise
            except sqlite3.Error as e:
                # Log and re-raise other SQLite errors
                query_info = f"\nSQL: {sql}\nParams: {params}" if sql else ""
                _log(
                    f"SQLite error during '{func.__name__}': {e}{query_info}",
                    level="DB ERROR",
                    stream=sys.stderr,
                    thread_name=thread_name,
                )
                raise
        return None  # Should not be reached if raise is working correctly

    return wrapper


# --- Define init_db AFTER get_db_connection ---
def init_db():
    """Initializes the database by creating tables if they don't exist and adding new columns if needed."""
    schema_statements = [
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
        """,
        """
        CREATE TABLE IF NOT EXISTS Companies (
            company_id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            name TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS ProblemCompanies (
            problem_id INTEGER NOT NULL,
            company_id INTEGER NOT NULL,
            frequency REAL,
            FOREIGN KEY (problem_id) REFERENCES Problems(problem_id) ON DELETE CASCADE,
            FOREIGN KEY (company_id) REFERENCES Companies(company_id) ON DELETE CASCADE,
            PRIMARY KEY (problem_id, company_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Tags (
            tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            name TEXT,
            translated_name TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS ProblemTags (
            problem_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            FOREIGN KEY (problem_id) REFERENCES Problems(problem_id) ON DELETE CASCADE,
            FOREIGN KEY (tag_id) REFERENCES Tags(tag_id) ON DELETE CASCADE,
            PRIMARY KEY (problem_id, tag_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Categories (
            category_id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            name TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS ProblemCategories (
            problem_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL,
            company_id INTEGER NOT NULL,
            FOREIGN KEY (problem_id) REFERENCES Problems(problem_id) ON DELETE CASCADE,
            FOREIGN KEY (category_id) REFERENCES Categories(category_id) ON DELETE CASCADE,
            FOREIGN KEY (company_id) REFERENCES Companies(company_id) ON DELETE CASCADE,
            PRIMARY KEY (problem_id, category_id, company_id)
        );
        """,
        # --- Indices --- #
        "CREATE INDEX IF NOT EXISTS idx_problems_slug ON Problems (title_slug);",
        "CREATE INDEX IF NOT EXISTS idx_problems_frontend_id ON Problems (question_frontend_id);",
        "CREATE INDEX IF NOT EXISTS idx_companies_slug ON Companies (slug);",
        "CREATE INDEX IF NOT EXISTS idx_tags_slug ON Tags (slug);",
        "CREATE INDEX IF NOT EXISTS idx_categories_slug ON Categories (slug);",
        "CREATE INDEX IF NOT EXISTS idx_pc_problem ON ProblemCompanies (problem_id);",
        "CREATE INDEX IF NOT EXISTS idx_pc_company ON ProblemCompanies (company_id);",
        "CREATE INDEX IF NOT EXISTS idx_pt_problem ON ProblemTags (problem_id);",
        "CREATE INDEX IF NOT EXISTS idx_pt_tag ON ProblemTags (tag_id);",
        "CREATE INDEX IF NOT EXISTS idx_pcat_problem ON ProblemCategories (problem_id);",
        "CREATE INDEX IF NOT EXISTS idx_pcat_category ON ProblemCategories (category_id);",
        "CREATE INDEX IF NOT EXISTS idx_pcat_company ON ProblemCategories (company_id);",
    ]

    alter_statements = [
        "ALTER TABLE Problems ADD COLUMN editorial_content TEXT;",
        "ALTER TABLE Problems ADD COLUMN editorial_uuid TEXT;",
        "ALTER TABLE Problems ADD COLUMN last_fetched_editorial DATETIME;",
    ]

    conn = None
    try:
        # Use a temporary connection for schema init/check, applying timeout
        conn = get_db_connection()
        cursor = conn.cursor()
        _log(f"Initializing database schema in {DB_FILE}...", level="DB INFO")
        for statement in schema_statements:
            if statement and not statement.isspace():
                # Wrap DDL execution with retry logic, though locks are less common here
                @retry_on_lock
                def execute_ddl(cur, stmt):
                    cur.execute(stmt)

                execute_ddl(cursor, statement)
        conn.commit()
        _log("Base schema ensured.", level="DB INFO")

        # Check and add new columns to Problems if they don't exist
        _log("Checking for necessary schema alterations...", level="DB INFO")
        cursor.execute("PRAGMA table_info(Problems);")
        existing_columns = [column["name"] for column in cursor.fetchall()]
        added_cols = False
        for alter_stmt in alter_statements:
            col_name = alter_stmt.split("ADD COLUMN")[1].strip().split()[0]
            if col_name not in existing_columns:
                try:
                    _log(f"    Applying: {alter_stmt}")

                    # Wrap ALTER execution with retry logic
                    @retry_on_lock
                    def execute_alter(cur, stmt):
                        cur.execute(stmt)

                    execute_alter(cursor, alter_stmt)
                    added_cols = True
                except sqlite3.OperationalError as e:
                    if "duplicate column name" in str(e):
                        _log(
                            f"      Column {col_name} likely already exists, skipping."
                        )
                    # Lock errors are handled by the decorator, re-raise others
                    elif "database is locked" not in str(e):
                        _log(
                            f"Operational error during ALTER: {e}\n"
                            f"SQL: {alter_stmt}\n"
                            f"Column: {col_name}",
                            level="DB ERROR",
                            stream=sys.stderr,
                        )
                        raise e
                    # If it *was* a lock error but retries failed, the decorator raises it
                except sqlite3.Error as e:
                    _log(
                        f"SQLite error during ALTER: {e}\n"
                        f"SQL: {alter_stmt}\n"
                        f"Column: {col_name}",
                        level="DB ERROR",
                        stream=sys.stderr,
                    )
                    raise e

        if added_cols:
            conn.commit()
            _log("Schema alterations applied successfully.", level="DB INFO")
        else:
            _log("No schema alterations needed.", level="DB INFO")

        # Create index for editorial_uuid *after* ensuring the column exists
        _log("Ensuring editorial_uuid index exists...", level="DB INFO")
        try:

            @retry_on_lock
            def execute_index(cur, stmt):
                cur.execute(stmt)

            index_stmt = "CREATE INDEX IF NOT EXISTS idx_problems_editorial_uuid ON Problems (editorial_uuid);"
            execute_index(cursor, index_stmt)
            conn.commit()
            _log("Index idx_problems_editorial_uuid ensured.", level="DB INFO")
        except sqlite3.Error as e:
            # Errors after retries will be raised by the decorator
            _log(
                f"Could not ensure index idx_problems_editorial_uuid after retries:\n"
                f"SQL: {index_stmt}\n"
                f"Error: {e}",
                level="DB WARN",
                stream=sys.stderr,
            )

    except sqlite3.Error as e:
        _log(
            f"Error initializing/altering database schema:\n"
            f"Database file: {DB_FILE}\n"
            f"Error: {e}\n"
            f"Current state: {'Added columns' if 'added_cols' in locals() and added_cols else 'Schema creation'}",
            level="DB ERROR",
            stream=sys.stderr,
        )
        if conn:
            # Attempt rollback, though it might also fail if locked
            try:
                conn.rollback()
            except sqlite3.Error as rb_e:
                _log(
                    f"Rollback failed during init error:\n"
                    f"Original error: {e}\n"
                    f"Rollback error: {rb_e}",
                    level="DB WARN",
                    stream=sys.stderr,
                )
        sys.exit(1)
    finally:
        if conn:
            conn.close()


# --- Helper Functions with Retry Decorator ---
# Removing retry_on_lock decorator as we've integrated retry logic directly
def _execute_write(
    conn, sql, params=None, max_retries=5, commit=True, thread_name=None
):
    """Execute write operations with retry logic for locked database"""
    if params is None:
        params = []

    retries = 0
    while retries < max_retries:
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            if commit:
                conn.commit()

            # Return an integer representing affected rows (either lastrowid or rowcount)
            # IMPORTANT: Do not attempt to access cursor attributes after returning from this function
            result = cursor.lastrowid if cursor.lastrowid else cursor.rowcount
            cursor.close()
            return result

        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                retries += 1
                if retries >= max_retries:
                    _log(
                        f"Failed to execute after {max_retries} retries due to database lock: {sql}",
                        level="DB ERROR",
                        stream=sys.stderr,
                        thread_name=thread_name,
                    )
                    raise

                # Exponential backoff with jitter
                delay = min(0.1 * (2**retries) + random.uniform(0, 0.1), 5.0)
                _log(
                    f"Database locked during '_execute_write'. Retrying ({retries}/{max_retries}) in {delay:.3f}s...\nSQL: {sql}",
                    level="DB WARN" if retries <= 2 else "DB ERROR",
                    thread_name=thread_name,
                )
                time.sleep(delay)
            else:
                _log(
                    f"SQLite error during execution: {e}\nSQL: {sql}",
                    level="DB ERROR",
                    stream=sys.stderr,
                    thread_name=thread_name,
                )
                raise

        except Exception as e:
            _log(
                f"Error during execution: {e}\nSQL: {sql}",
                level="DB ERROR",
                stream=sys.stderr,
                thread_name=thread_name,
            )
            raise


@retry_on_lock
def _execute_read_one(conn, sql, params=()):
    """Executes a read operation expecting one row, with retry."""
    cursor = conn.cursor()
    cursor.execute(sql, params)
    return cursor.fetchone()


# --- Main DB Operations ---


def get_or_create_id(conn, table, unique_column, value, other_columns=None):
    """Gets the ID of a row based on a unique value, or creates the row if it doesn't exist. Handles DB locks with retries."""
    # Determine primary key column name
    thread_name = None
    try:
        import threading

        thread_name = threading.current_thread().name
    except:
        pass

    if table == "Companies":
        primary_key_col = "company_id"
    elif table == "Categories":
        primary_key_col = "category_id"
    elif table == "Tags":
        primary_key_col = "tag_id"
    elif table == "Problems":
        primary_key_col = "problem_id"
    else:
        _log(
            f"Unknown table '{table}' in get_or_create_id",
            level="ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        return None

    try:
        # Attempt to fetch existing row
        row = _execute_read_one(
            conn,
            f"SELECT {primary_key_col} FROM {table} WHERE {unique_column} = ?",
            (value,),
        )
        if row:
            return row[primary_key_col]
        else:
            # Row doesn't exist, attempt to insert
            columns = [unique_column]
            insert_values = [value]
            placeholders = ["?"]
            if other_columns:
                valid_other_columns = {
                    k: v for k, v in other_columns.items()  # Allow None values
                }
                columns.extend(valid_other_columns.keys())
                insert_values.extend(valid_other_columns.values())
                placeholders.extend(["?"] * len(valid_other_columns))

            sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

            # Use direct cursor execution instead of _execute_write to safely access lastrowid
            cursor = conn.cursor()
            cursor.execute(sql, tuple(insert_values))
            new_id = cursor.lastrowid
            return new_id

    except sqlite3.IntegrityError as e:
        # This usually means another thread inserted between our SELECT and INSERT (race condition)
        # Or a UNIQUE constraint failed (e.g., editorial_uuid)
        select_sql = f"SELECT {primary_key_col} FROM {table} WHERE {unique_column} = ?"
        select_params = (value,)

        _log(
            f"Integrity error during get/create for {table} ({unique_column}={value}). Retrying fetch.\n"
            f"Table: {table}\n"
            f"Column: {unique_column}\n"
            f"Value: {value}\n"
            f"Other columns: {other_columns}\n"
            f"Error: {e}",
            level="DB WARN",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        # Retry the fetch just in case the insert failed due to race condition
        try:
            row = _execute_read_one(
                conn,
                select_sql,
                select_params,
            )
            if row:
                return row[primary_key_col]
            else:
                # If fetch still fails, the integrity error was likely due to something else (e.g. bad data)
                _log(
                    f"Fetch failed after integrity error for {table} ({unique_column}={value}). Constraint Violated?\n"
                    f"Table: {table}\n"
                    f"Column: {unique_column}\n"
                    f"Value: {value}\n"
                    f"Other columns: {other_columns}\n"
                    f"Error: {e}\n"
                    f"SQL: {select_sql}\n"
                    f"Params: {select_params}",
                    level="DB ERROR",
                    stream=sys.stderr,
                    thread_name=thread_name,
                )
                return None  # Indicate failure
        except sqlite3.Error as fetch_e:  # Includes potential lock errors on retry
            _log(
                f"Fetch attempt after integrity error failed for {table} ({unique_column}={value}):\n"
                f"Table: {table}\n"
                f"Column: {unique_column}\n"
                f"Value: {value}\n"
                f"Original error: {e}\n"
                f"Fetch error: {fetch_e}\n"
                f"SQL: {select_sql}\n"
                f"Params: {select_params}",
                level="DB ERROR",
                stream=sys.stderr,
                thread_name=thread_name,
            )
            return None

    except sqlite3.Error as e:
        # This catches errors from _execute_read_one or _execute_write that survived retries
        operation_details = ""
        if "Row doesn't exist" in locals():
            # We were in the INSERT part
            columns_info = (
                f"Columns: {', '.join(columns)}"
                if "columns" in locals()
                else "Columns: unknown"
            )
            values_info = (
                f"Values: {insert_values}"
                if "insert_values" in locals()
                else "Values: unknown"
            )
            sql_info = f"SQL: {sql}" if "sql" in locals() else "SQL: unknown"
            operation_details = f"\n{columns_info}\n{values_info}\n{sql_info}"

        _log(
            f"Failed operation in get_or_create_id for {table} ({unique_column}={value}) after retries:\n"
            f"Table: {table}\n"
            f"Column: {unique_column}\n"
            f"Value: {value}\n"
            f"Other columns: {other_columns}\n"
            f"Error: {e}{operation_details}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        return None  # Indicate failure


def insert_or_ignore_junction(conn, table, column_map):
    """Inserts a row into a junction table, ignoring if it already exists. Handles DB locks."""
    thread_name = None
    try:
        import threading

        thread_name = threading.current_thread().name
    except:
        pass

    columns = list(column_map.keys())
    values = tuple(column_map.values())
    placeholders = ",".join(["?"] * len(columns))
    sql = (
        f"INSERT OR IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    )
    try:
        _execute_write(conn, sql, values)
        # conn.commit() # Let worker commit
        return True
    except sqlite3.Error as e:
        _log(
            f"Failed INSERT OR IGNORE into junction table {table} for {column_map} after retries:\n"
            f"Table: {table}\n"
            f"Column map: {column_map}\n"
            f"SQL: {sql}\n"
            f"Values: {values}\n"
            f"Error: {e}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        return False  # Indicate failure


def update_problem_company_frequency(conn, problem_id, company_id, frequency):
    """Updates the frequency for a specific problem-company pair. Handles DB locks."""
    thread_name = None
    try:
        import threading

        thread_name = threading.current_thread().name
    except:
        pass

    sql = "UPDATE ProblemCompanies SET frequency = ? WHERE problem_id = ? AND company_id = ?"
    params = (frequency, problem_id, company_id)
    try:
        _execute_write(conn, sql, params)
        # conn.commit() # Let worker commit
        return True
    except sqlite3.Error as e:
        _log(
            f"Failed UPDATE frequency for p:{problem_id}, c:{company_id} after retries:\n"
            f"Problem ID: {problem_id}\n"
            f"Company ID: {company_id}\n"
            f"Frequency: {frequency}\n"
            f"SQL: {sql}\n"
            f"Params: {params}\n"
            f"Error: {e}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        return False  # Indicate failure


def update_problem_details(conn, title_slug, details):
    """Updates the Problems table. Handles DB locks."""
    thread_name = None
    try:
        import threading

        thread_name = threading.current_thread().name
    except:
        pass

    set_clauses = []
    values = []
    allowed_keys = [
        "content",
        "editorial_content",
        "editorial_uuid",
        "title",
        "difficulty",
        "is_paid_only",
        "question_frontend_id",
        "ac_rate",
        "last_fetched_detail",
        "last_fetched_editorial",
    ]

    has_editorial_update = False
    for key, value in details.items():
        if key in allowed_keys:
            set_clauses.append(f"{key} = ?")
            values.append(1 if key == "is_paid_only" else value)
            if key in ["editorial_content", "editorial_uuid", "last_fetched_editorial"]:
                has_editorial_update = True

    if not set_clauses:
        # _log(f"No valid fields provided to update for '{title_slug}'.", level="DB INFO") # Too noisy
        return True  # Nothing to do is not an error

    # Add timestamp update logic (only if not explicitly provided)
    now = datetime.datetime.now()
    if "content" in details and "last_fetched_detail" not in details:
        set_clauses.append("last_fetched_detail = ?")
        values.append(now)
    if has_editorial_update and "last_fetched_editorial" not in details:
        set_clauses.append("last_fetched_editorial = ?")
        values.append(now)

    values.append(title_slug)  # For the WHERE clause
    sql = f"UPDATE Problems SET {', '.join(set_clauses)} WHERE title_slug = ?"

    try:
        cursor = _execute_write(conn, sql, tuple(values))
        # conn.commit() # Let worker commit
        if isinstance(cursor, int) and cursor == 0:
            _log(
                f"Problem '{title_slug}' not found during UPDATE details (maybe deleted?).\n"
                f"Title slug: {title_slug}\n"
                f"Details: {details}\n"
                f"SQL: {sql}\n"
                f"Values: {values}",
                level="DB WARN",  # Warning, not necessarily error
                stream=sys.stderr,
                thread_name=thread_name,
            )
            # Still return True as the operation didn't fail, just didn't match
        return True
    except sqlite3.IntegrityError as e:
        # Catch UNIQUE constraint failure (e.g., editorial_uuid) after retries
        _log(
            f"Integrity error updating problem details for '{title_slug}' after retries:\n"
            f"Title slug: {title_slug}\n"
            f"Details: {details}\n"
            f"SQL: {sql}\n"
            f"Values: {values}\n"
            f"Error: {e}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        return False  # Indicate failure
    except sqlite3.Error as e:
        _log(
            f"Failed UPDATE problem details for '{title_slug}' after retries:\n"
            f"Title slug: {title_slug}\n"
            f"Details: {details}\n"
            f"SQL: {sql}\n"
            f"Values: {values}\n"
            f"Error: {e}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        return False  # Indicate failure


def update_problem_company_timestamp(conn, problem_id):
    """Updates the last_fetched_company timestamp for a given problem_id. Handles DB locks."""
    thread_name = None
    try:
        import threading

        thread_name = threading.current_thread().name
    except:
        pass

    sql = "UPDATE Problems SET last_fetched_company = ? WHERE problem_id = ?"
    current_time = datetime.datetime.now()
    params = (current_time, problem_id)

    try:
        result = _execute_write(conn, sql, params)
        # Note: result is an integer (either lastrowid or rowcount)
        if result == 0:
            _log(
                f"Problem ID {problem_id} not found during UPDATE company timestamp.\n"
                f"Problem ID: {problem_id}\n"
                f"Timestamp: {current_time}\n"
                f"SQL: {sql}\n"
                f"Params: {params}",
                level="DB WARN",
                stream=sys.stderr,
                thread_name=thread_name,
            )
        return True
    except sqlite3.Error as e:
        _log(
            f"Failed UPDATE company timestamp for problem_id {problem_id} after retries:\n"
            f"Problem ID: {problem_id}\n"
            f"Timestamp: {current_time}\n"
            f"SQL: {sql}\n"
            f"Params: {params}\n"
            f"Error: {e}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        return False  # Indicate failure


def batch_get_or_create_problems(conn, problems_data):
    """Batch version of get_or_create_id for multiple problems to reduce individual transactions.

    Args:
        conn: Database connection
        problems_data: List of dictionaries containing problem data with at least 'title_slug' key

    Returns:
        Dictionary mapping title_slug to problem_id
    """
    thread_name = None
    try:
        import threading

        thread_name = threading.current_thread().name
    except:
        pass

    if not problems_data:
        return {}

    result_map = {}

    # First, try to fetch all existing problems in one query
    slugs = [p["title_slug"] for p in problems_data if "title_slug" in p]
    slug_list = ",".join(["?"] * len(slugs))

    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT problem_id, title_slug FROM Problems WHERE title_slug IN ({slug_list})",
            tuple(slugs),
        )
        existing = {row["title_slug"]: row["problem_id"] for row in cursor.fetchall()}

        # Add existing problems to result map
        result_map.update(existing)

        # Create list of problems that need to be inserted
        to_insert = [p for p in problems_data if p["title_slug"] not in existing]

        if to_insert:
            # Prepare batch insert
            for problem in to_insert:
                slug = problem["title_slug"]

                # Extract other columns
                title = problem.get("title")
                difficulty = problem.get("difficulty")
                ac_rate = problem.get("ac_rate")
                is_paid_only = 1 if problem.get("is_paid_only") else 0
                question_frontend_id = problem.get("question_frontend_id")

                # Insert the problem - use direct cursor execution for access to lastrowid
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO Problems (title_slug, title, difficulty, ac_rate, is_paid_only, question_frontend_id) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        slug,
                        title,
                        difficulty,
                        ac_rate,
                        is_paid_only,
                        question_frontend_id,
                    ),
                )
                problem_id = cursor.lastrowid
                result_map[slug] = problem_id

    except sqlite3.Error as e:
        _log(
            f"Batch problem operation failed:\n"
            f"Error: {e}\n"
            f"Number of problems: {len(problems_data)}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        # Fall back to individual operations if batch fails
        for problem in problems_data:
            if "title_slug" in problem:
                slug = problem["title_slug"]
                if slug not in result_map:  # Only process if not already in result_map
                    problem_id = get_or_create_id(
                        conn,
                        "Problems",
                        "title_slug",
                        slug,
                        {k: v for k, v in problem.items() if k != "title_slug"},
                    )
                    if problem_id:
                        result_map[slug] = problem_id

    return result_map


def batch_update_problem_details(conn, problems_data):
    """Update problem details for multiple problems in a single transaction"""
    if not problems_data:
        return 0

    # Define allowed columns for update
    allowed_columns = [
        "content",
        "editorial_content",
        "editorial_uuid",
        "title",
        "difficulty",
        "is_paid_only",
        "question_frontend_id",
        "ac_rate",
        "last_fetched_detail",
        "last_fetched_editorial",
    ]

    try:
        # Start a transaction
        conn.execute("BEGIN TRANSACTION")

        updated = 0
        for problem in problems_data:
            slug = problem.get("title_slug")
            if not slug:
                continue

            # Extract details to update
            details = {k: v for k, v in problem.items() if k != "title_slug"}
            if not details:
                continue

            # Build the SQL statement
            set_clauses = []
            params = []
            for key, value in details.items():
                if key in allowed_columns:
                    set_clauses.append(f"{key} = ?")
                    params.append(value)

            if not set_clauses:
                continue

            # Add the WHERE clause parameter
            params.append(slug)
            sql = f"UPDATE Problems SET {', '.join(set_clauses)} WHERE title_slug = ?"

            # Execute the update
            cursor = conn.execute(sql, params)
            if isinstance(cursor, int):
                # If cursor is an integer (from _execute_write), check value
                if cursor > 0:
                    updated += 1
            else:
                # If it's a cursor object, check rowcount
                if cursor.rowcount > 0:
                    updated += 1

        # Commit all updates in one go
        conn.commit()
        return updated

    except Exception as e:
        # Rollback on error
        try:
            conn.rollback()
        except:
            pass

        thread_name = None
        try:
            import threading

            thread_name = threading.current_thread().name
        except:
            pass

        _log(
            f"Error updating problem details in batch: {e}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        return 0


def close_connection(conn):
    """Safely close a database connection, ensuring WAL checkpoints are completed."""
    if conn:
        try:
            # Run a WAL checkpoint to ensure data is properly committed
            conn.execute("PRAGMA wal_checkpoint(FULL);")
            conn.commit()
            conn.close()
            return True
        except sqlite3.Error as e:
            thread_name = None
            try:
                import threading

                thread_name = threading.current_thread().name
            except:
                pass
            _log(
                f"Error closing database connection: {e}",
                level="DB ERROR",
                stream=sys.stderr,
                thread_name=thread_name,
            )
            return False
    return False


def connect_db(db_file=DB_FILE, timeout=10.0, thread_name=None):
    """Connect to SQLite database with custom timeout"""
    try:
        conn = sqlite3.connect(db_file, timeout=timeout)
        conn.row_factory = sqlite3.Row

        # Set pragmas for better performance
        set_sqlite_pragmas(conn)

        return conn
    except sqlite3.Error as e:
        _log(
            f"Could not connect to database {db_file}:\nDatabase file: {db_file}\nTimeout: {timeout}s\nCurrent directory: {os.getcwd()}\nError: {e}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        raise


def set_sqlite_pragmas(conn):
    """Set SQLite PRAGMAs for better performance and concurrency"""
    try:
        # Enable WAL mode for better concurrency
        conn.execute("PRAGMA journal_mode = WAL")

        # Set an even longer busy_timeout (30 seconds)
        conn.execute("PRAGMA busy_timeout = 30000")

        # Other performance optimizations
        conn.execute(
            "PRAGMA synchronous = NORMAL"
        )  # Safer than OFF but faster than FULL
        conn.execute("PRAGMA cache_size = 20000")  # Increase cache size further
        conn.execute("PRAGMA temp_store = MEMORY")  # Store temp tables in memory
        conn.execute(
            "PRAGMA mmap_size = 67108864"
        )  # Use 64MB memory mapping for faster access

        # Increase page size for better performance
        conn.execute("PRAGMA page_size = 8192")  # 8KB pages (from default 4KB)

        # Locking mode settings
        conn.execute("PRAGMA locking_mode = NORMAL")  # Allow concurrent reads

        # WAL autocheckpoint (default is 1000)
        conn.execute(
            "PRAGMA wal_autocheckpoint = 2000"
        )  # More pages between checkpoints

        return True
    except sqlite3.Error as e:
        _log(f"Error setting SQLite pragmas: {e}", level="DB ERROR", stream=sys.stderr)
        return False


def batch_get_or_create_categories(conn, categories_data):
    """Batch version of get_or_create_id for multiple categories to reduce individual transactions.

    Args:
        conn: Database connection
        categories_data: List of dictionaries containing category data with 'slug' and 'name' keys

    Returns:
        Dictionary mapping category slug to category_id
    """
    thread_name = None
    try:
        import threading

        thread_name = threading.current_thread().name
    except:
        pass

    if not categories_data:
        return {}

    result_map = {}

    # First, try to fetch all existing categories in one query
    slugs = [c["slug"] for c in categories_data if "slug" in c]
    if not slugs:
        return {}

    slug_list = ",".join(["?"] * len(slugs))

    try:
        # Start a transaction
        conn.execute("BEGIN TRANSACTION")

        cursor = conn.cursor()
        cursor.execute(
            f"SELECT category_id, slug FROM Categories WHERE slug IN ({slug_list})",
            tuple(slugs),
        )
        existing = {row["slug"]: row["category_id"] for row in cursor.fetchall()}

        # Add existing categories to result map
        result_map.update(existing)

        # Create list of categories that need to be inserted
        to_insert = [c for c in categories_data if c["slug"] not in existing]

        if to_insert:
            # Insert new categories
            for category in to_insert:
                slug = category["slug"]
                name = category.get("name", slug)

                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO Categories (slug, name) VALUES (?, ?)", (slug, name)
                )
                category_id = cursor.lastrowid
                result_map[slug] = category_id

        # Commit the transaction
        conn.commit()
        return result_map

    except sqlite3.Error as e:
        # Rollback on error
        try:
            conn.rollback()
        except:
            pass

        _log(
            f"Batch category operation failed:\n"
            f"Error: {e}\n"
            f"Number of categories: {len(categories_data)}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )

        # Fall back to individual operations if batch fails
        result_map = {}
        for category in categories_data:
            if "slug" in category:
                slug = category["slug"]
                name = category.get("name", slug)
                category_id = get_or_create_id(
                    conn, "Categories", "slug", slug, {"name": name}
                )
                if category_id:
                    result_map[slug] = category_id

        return result_map


def batch_insert_junctions(conn, table, items):
    """Batch insert multiple items into a junction table with a single transaction.

    Args:
        conn: Database connection
        table: Target junction table name (e.g., 'ProblemCompanies', 'ProblemTags', 'ProblemCategories')
        items: List of dictionaries, each containing column-value pairs for one row

    Returns:
        Number of items successfully inserted
    """
    thread_name = None
    try:
        import threading

        thread_name = threading.current_thread().name
    except:
        pass

    if not items:
        return 0

    # Determine columns from the first item
    if not items[0]:
        return 0

    columns = list(items[0].keys())
    if not columns:
        return 0

    # Start transaction
    try:
        conn.execute("BEGIN TRANSACTION")

        # Prepare SQL
        placeholders = ",".join(["?"] * len(columns))
        column_names = ",".join(columns)
        sql = f"INSERT OR IGNORE INTO {table} ({column_names}) VALUES ({placeholders})"

        # Execute batch
        cursor = conn.cursor()
        count = 0

        for item in items:
            values = [item.get(col) for col in columns]
            cursor.execute(sql, values)
            count += 1

        conn.commit()
        return count

    except sqlite3.Error as e:
        try:
            conn.rollback()
        except:
            pass

        _log(
            f"Batch insert into {table} failed: {e}\n"
            f"Number of items: {len(items)}\n"
            f"Columns: {columns}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )

        # Fall back to individual inserts if batch fails
        count = 0
        for item in items:
            try:
                if insert_or_ignore_junction(conn, table, item):
                    count += 1
            except:
                pass

        return count


def batch_get_or_create_tags(conn, tags_data):
    """Batch version of get_or_create_id for multiple tags to reduce individual transactions.

    Args:
        conn: Database connection
        tags_data: List of dictionaries containing tag data with 'slug', 'name', and 'translated_name' keys

    Returns:
        Dictionary mapping tag slug to tag_id
    """
    thread_name = None
    try:
        import threading

        thread_name = threading.current_thread().name
    except:
        pass

    if not tags_data:
        return {}

    result_map = {}

    # First, try to fetch all existing tags in one query
    slugs = [t["slug"] for t in tags_data if "slug" in t]
    if not slugs:
        return {}

    slug_list = ",".join(["?"] * len(slugs))

    try:
        # Start a transaction
        conn.execute("BEGIN TRANSACTION")

        cursor = conn.cursor()
        cursor.execute(
            f"SELECT tag_id, slug FROM Tags WHERE slug IN ({slug_list})",
            tuple(slugs),
        )
        existing = {row["slug"]: row["tag_id"] for row in cursor.fetchall()}

        # Add existing tags to result map
        result_map.update(existing)

        # Create list of tags that need to be inserted
        to_insert = [t for t in tags_data if t["slug"] not in existing]

        if to_insert:
            # Insert new tags
            for tag in to_insert:
                slug = tag["slug"]
                name = tag.get("name", slug)
                translated_name = tag.get("translated_name")

                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO Tags (slug, name, translated_name) VALUES (?, ?, ?)",
                    (slug, name, translated_name),
                )
                tag_id = cursor.lastrowid
                result_map[slug] = tag_id

        # Commit the transaction
        conn.commit()
        return result_map

    except sqlite3.Error as e:
        # Rollback on error
        try:
            conn.rollback()
        except:
            pass

        _log(
            f"Batch tag operation failed:\n"
            f"Error: {e}\n"
            f"Number of tags: {len(tags_data)}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )

        # Fall back to individual operations if batch fails
        result_map = {}
        for tag in tags_data:
            if "slug" in tag:
                slug = tag["slug"]
                tag_id = get_or_create_id(
                    conn,
                    "Tags",
                    "slug",
                    slug,
                    {
                        "name": tag.get("name", slug),
                        "translated_name": tag.get("translated_name"),
                    },
                )
                if tag_id:
                    result_map[slug] = tag_id

        return result_map


def batch_update_timestamps(
    conn, table, id_column, ids, timestamp_column="last_updated"
):
    """Batch update timestamps for multiple rows in a table.

    Args:
        conn: Database connection
        table: Target table name
        id_column: Primary key column name
        ids: List of primary key values to update
        timestamp_column: Column name for timestamp (default: "last_updated")

    Returns:
        Number of rows updated
    """
    thread_name = None
    try:
        import threading

        thread_name = threading.current_thread().name
    except:
        pass

    if not ids:
        return 0

    # Use current timestamp for all updates
    current_time = datetime.datetime.now()

    try:
        # Batch the ids in groups of 100 to avoid excessively large queries
        batch_size = 100
        total_updated = 0

        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i : i + batch_size]
            id_list = ",".join(["?"] * len(batch_ids))

            sql = f"UPDATE {table} SET {timestamp_column} = ? WHERE {id_column} IN ({id_list})"
            params = [current_time] + batch_ids

            cursor = conn.cursor()
            cursor.execute(sql, params)

            # Get the number of updated rows
            if isinstance(cursor, int):
                total_updated += cursor
            else:
                total_updated += cursor.rowcount

        return total_updated

    except sqlite3.Error as e:
        _log(
            f"Batch timestamp update failed:\n"
            f"Error: {e}\n"
            f"Table: {table}\n"
            f"Number of ids: {len(ids)}",
            level="DB ERROR",
            stream=sys.stderr,
            thread_name=thread_name,
        )
        return 0


# --- Main execution --- (for standalone testing/init)
if __name__ == "__main__":
    _log("Running DB Initialization / Schema Update...")
    init_db()
    _log("DB Initialization / Schema Update complete.")
