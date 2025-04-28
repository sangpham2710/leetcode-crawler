# LeetCode Crawler and Organizer

A high-performance Python utility for collecting, organizing, and accessing LeetCode problem data with a focus on company-tagged questions and their frequency of appearance in technical interviews.

## Purpose

LeetCode Crawler and Organizer efficiently fetches and stores problem data, company affiliations, and editorial content from LeetCode into a local SQLite database, enabling offline access and advanced querying capabilities for interview preparation.

## Key Features

- **Company-specific problem tracking**: Maps problems to companies where they've been asked in interviews
- **Problem metadata collection**: Difficulty, acceptance rates, tags, and categories
- **Editorial content**: Stores official solution explanations when available
- **Multi-threaded crawling**: Concurrent workers for efficient data collection
- **Robust SQLite database**: Optimized schema for fast querying and data integrity
- **Browser cookie integration**: Optional direct authentication from Chrome/Firefox
- **Configurable performance settings**: Adjustable concurrency, caching, and sync modes

## Technical Design

### Database Optimization

**Batched Write Operations**
- Groups multiple SQL operations into single transactions
- Minimizes the number of write transactions to reduce lock contention
- Prevents database throttling when multiple threads attempt simultaneous writes
- Implementation: `batch_get_or_create_problems`, `batch_update_problem_details` in `db_utils.py`

**In-Memory Journaling and WAL Mode**
- Write-Ahead Logging (WAL) dramatically improves concurrent read/write performance
- Configurable synchronous modes (OFF, NORMAL, FULL) allow trading durability for speed
- Memory-mapped I/O reduces disk operations and improves cache efficiency
- Implementation: `SQLITE_JOURNAL_MODE`, `SQLITE_SYNCHRONOUS` in `settings.py`

### Architecture Design

**Module Separation**
- Parsing, network, and database logic cleanly separated
- Network layer (`util.py`) handles retries and connection pooling
- Database layer (`db_utils.py`) manages transactions and concurrency control
- Business logic (`fetch_*.py`) focuses on data organization
- Result: Thread safety through clear boundaries of responsibility

**Schema Normalization**
- Junction tables for many-to-many relationships (problems-companies, problems-tags)
- Prevents data redundancy and update anomalies
- Enables efficient queries across related entities
- Timestamp tracking for data freshness management

**Graceful Error Handling**
- Exponential backoff with jitter for network retries
- Dead-letter queue for tracking failed crawling attempts
- Database locks handled with retries and timeouts
- Session-level HTTP connection pooling with automatic reconnection
- Implementation: `retry_with_backoff` decorator, `retry_on_lock` in database operations

### Trade-offs

**Thread-based vs. Async I/O**
- Uses Python's threading model instead of asyncio
- Simpler to reason about for mixed I/O and CPU-bound operations
- Avoids callback complexity and maintains linear code flow
- SQLite works better with threads than async due to connection limitations

**Complete Refresh vs. Incremental Updates**
- Favors complete refreshes of data on configurable intervals
- Simplifies consistency checking and error recovery
- Trade-off: Higher bandwidth usage but fewer edge cases to handle

## Extensibility

The codebase is designed for easy extension:

1. **New Data Sources**: Add new fetch modules by following the pattern in `fetch_problem_editorial.py`
2. **Custom Queries**: The normalized schema supports adding new query patterns
3. **Configuration**: All parameters can be overridden via environment variables
4. **Authentication Methods**: Support for multiple cookie sources can be extended
5. **Output Formats**: Database abstraction allows for adding exporters (JSON, CSV, etc.)

## Setup and Usage

### Requirements

- Python 3.8+
- Required packages listed in `requirements.txt`

### Installation

```bash
git clone https://github.com/yourusername/leetcode-crawler.git
cd leetcode-crawler
pip install -r requirements.txt
```

### Running

Basic usage:

```bash
python fetch_leetcode.py
python fetch_problem_editorial.py
```

With browser cookies:

```bash
python fetch_leetcode.py --cookies-from-browser chrome
```

Performance tuning:

```bash
python fetch_leetcode.py --workers 20 --db-concurrency 5 --sync-mode OFF
```

## Legal Notice

**No redistribution of copyrighted LeetCode premium content. Project intended for personal learning and study support only.**

This tool is meant for personal use to organize your LeetCode study materials. Using this tool to distribute premium content is against LeetCode's terms of service. Please respect copyright and use responsibly.

## License

[MIT License](LICENSE)
