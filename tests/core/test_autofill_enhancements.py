"""Tests for autofill enhancement helpers.

Tests for:
- search_engines helper module
- deleted_form_history helper module
- autofill_profile_tokens helper module
"""

import sqlite3
from typing import Any, Dict, List

import pytest

from core.database import (
    # Search engines
    insert_search_engine,
    insert_search_engines,
    get_search_engines,
    delete_search_engines_by_run,
    # Deleted form history
    insert_deleted_form_history,
    insert_deleted_form_history_entries,
    get_deleted_form_history,
    delete_deleted_form_history_by_run,
    # Profile tokens
    CHROMIUM_TOKEN_TYPES,
    get_token_type_name,
    insert_autofill_profile_token,
    insert_autofill_profile_tokens,
    get_autofill_profile_tokens,
    delete_autofill_profile_tokens_by_run,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def evidence_db() -> sqlite3.Connection:
    """In-memory evidence database with autofill enhancement tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create search_engines table (full schema from definitions.py)
    conn.execute("""
        CREATE TABLE search_engines (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            browser TEXT NOT NULL,
            profile TEXT,
            short_name TEXT,
            keyword TEXT,
            url TEXT,
            favicon_url TEXT,
            suggest_url TEXT,
            prepopulate_id INTEGER,
            usage_count INTEGER DEFAULT 0,
            date_created_utc TEXT,
            last_modified_utc TEXT,
            last_visited_utc TEXT,
            is_default INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            new_tab_url TEXT,
            image_url TEXT,
            search_url_post_params TEXT,
            suggest_url_post_params TEXT,
            token_mappings TEXT,
            run_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            discovered_by TEXT,
            partition_index INTEGER,
            fs_type TEXT,
            logical_path TEXT,
            forensic_path TEXT,
            tags TEXT,
            notes TEXT,
            created_at_utc TEXT
        )
    """)

    # Create deleted_form_history table (full schema from definitions.py)
    conn.execute("""
        CREATE TABLE deleted_form_history (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            browser TEXT NOT NULL,
            profile TEXT,
            guid TEXT,
            time_deleted_utc TEXT,
            original_fieldname TEXT,
            original_value TEXT,
            run_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            discovered_by TEXT,
            partition_index INTEGER,
            fs_type TEXT,
            logical_path TEXT,
            forensic_path TEXT,
            tags TEXT,
            notes TEXT,
            created_at_utc TEXT
        )
    """)

    # Create autofill_profile_tokens table (full schema from definitions.py)
    # Note: uses 'guid', 'token_value', and 'source_table' column names
    conn.execute("""
        CREATE TABLE autofill_profile_tokens (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            browser TEXT NOT NULL,
            profile TEXT,
            guid TEXT NOT NULL,
            token_type INTEGER NOT NULL,
            token_type_name TEXT,
            token_value TEXT,
            source_table TEXT,
            parent_table TEXT,
            parent_use_count INTEGER,
            parent_use_date_utc TEXT,
            parent_date_modified_utc TEXT,
            run_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            discovered_by TEXT,
            created_at_utc TEXT
        )
    """)

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Search Engines Tests
# ---------------------------------------------------------------------------


def test_insert_search_engine_single(evidence_db: sqlite3.Connection) -> None:
    """Test inserting a single search engine."""
    insert_search_engine(
        evidence_db, 1, "chrome",
        short_name="Google",
        keyword="google.com",
        url="https://www.google.com/search?q={searchTerms}",
        profile="Default",
        is_default=True,
        run_id="test_run_1",
        source_path="/path/to/Web Data",
    )

    # Verify insertion
    cursor = evidence_db.execute("SELECT * FROM search_engines WHERE evidence_id = 1")
    row = cursor.fetchone()
    assert row["short_name"] == "Google"
    assert row["keyword"] == "google.com"
    assert row["is_default"] == 1
    assert row["browser"] == "chrome"


def test_insert_search_engines_batch(evidence_db: sqlite3.Connection) -> None:
    """Test batch inserting multiple search engines."""
    records = [
        {
            "short_name": "Google",
            "keyword": "google.com",
            "url": "https://www.google.com/search?q={searchTerms}",
            "browser": "chrome",
            "profile": "Default",
            "is_default": True,
            "run_id": "test_run_1",
            "source_path": "/path/to/Web Data",
        },
        {
            "short_name": "YouTube",
            "keyword": "yt",
            "url": "https://www.youtube.com/results?search_query={searchTerms}",
            "browser": "chrome",
            "profile": "Default",
            "is_default": False,
            "run_id": "test_run_1",
            "source_path": "/path/to/Web Data",
        },
        {
            "short_name": "DuckDuckGo",
            "keyword": "ddg",
            "url": "https://duckduckgo.com/?q={searchTerms}",
            "browser": "chrome",
            "profile": "Default",
            "is_default": False,
            "run_id": "test_run_1",
            "source_path": "/path/to/Web Data",
        },
    ]

    result = insert_search_engines(evidence_db, 1, records)
    assert result == 3

    # Verify
    rows = get_search_engines(evidence_db, 1)
    assert len(rows) == 3
    assert rows[0]["short_name"] in ["Google", "YouTube", "DuckDuckGo"]


def test_get_search_engines_with_filter(evidence_db: sqlite3.Connection) -> None:
    """Test filtering search engines by browser."""
    records = [
        {"short_name": "Chrome Search", "browser": "chrome", "run_id": "run1", "source_path": "/test"},
        {"short_name": "Edge Search", "browser": "edge", "run_id": "run1", "source_path": "/test"},
        {"short_name": "Chrome Search 2", "browser": "chrome", "run_id": "run1", "source_path": "/test"},
    ]
    insert_search_engines(evidence_db, 1, records)

    # Filter by browser
    chrome_results = get_search_engines(evidence_db, 1, browser="chrome")
    assert len(chrome_results) == 2
    for r in chrome_results:
        assert r["browser"] == "chrome"

    edge_results = get_search_engines(evidence_db, 1, browser="edge")
    assert len(edge_results) == 1


def test_delete_search_engines_by_run(evidence_db: sqlite3.Connection) -> None:
    """Test deleting search engines by run_id."""
    records = [
        {"short_name": "SE1", "browser": "chrome", "run_id": "run_a", "source_path": "/test"},
        {"short_name": "SE2", "browser": "chrome", "run_id": "run_a", "source_path": "/test"},
        {"short_name": "SE3", "browser": "chrome", "run_id": "run_b", "source_path": "/test"},
    ]
    insert_search_engines(evidence_db, 1, records)

    # Delete run_a
    deleted = delete_search_engines_by_run(evidence_db, 1, "run_a")
    assert deleted == 2

    # Verify only run_b remains
    remaining = get_search_engines(evidence_db, 1)
    assert len(remaining) == 1
    assert remaining[0]["short_name"] == "SE3"


# ---------------------------------------------------------------------------
# Deleted Form History Tests
# ---------------------------------------------------------------------------


def test_insert_deleted_form_history_single(evidence_db: sqlite3.Connection) -> None:
    """Test inserting a single deleted form history entry."""
    # Use insert_deleted_form_history with kwargs
    insert_deleted_form_history(
        evidence_db, 1, "firefox",
        profile="default",
        guid="abc123-def456",
        time_deleted_utc="2024-01-15T10:30:00Z",
        run_id="test_run_1",
        source_path="/path/to/formhistory.sqlite",
    )

    # Verify
    cursor = evidence_db.execute("SELECT * FROM deleted_form_history WHERE evidence_id = 1")
    row = cursor.fetchone()
    assert row["guid"] == "abc123-def456"
    assert row["browser"] == "firefox"


def test_insert_deleted_form_history_batch(evidence_db: sqlite3.Connection) -> None:
    """Test batch inserting deleted form history entries."""
    records = [
        {"guid": "guid1", "time_deleted_utc": "2024-01-01T00:00:00Z", "browser": "firefox", "run_id": "run1", "source_path": "/test"},
        {"guid": "guid2", "time_deleted_utc": "2024-01-02T00:00:00Z", "browser": "firefox", "run_id": "run1", "source_path": "/test"},
        {"guid": "guid3", "time_deleted_utc": "2024-01-03T00:00:00Z", "browser": "tor", "run_id": "run1", "source_path": "/test"},
    ]

    result = insert_deleted_form_history_entries(evidence_db, 1, records)
    assert result == 3


def test_get_deleted_form_history(evidence_db: sqlite3.Connection) -> None:
    """Test retrieving deleted form history with filters."""
    records = [
        {"guid": "g1", "browser": "firefox", "run_id": "run1", "source_path": "/test"},
        {"guid": "g2", "browser": "tor", "run_id": "run1", "source_path": "/test"},
    ]
    insert_deleted_form_history_entries(evidence_db, 1, records)

    # Get all
    all_results = get_deleted_form_history(evidence_db, 1)
    assert len(all_results) == 2

    # Filter by browser
    firefox_results = get_deleted_form_history(evidence_db, 1, browser="firefox")
    assert len(firefox_results) == 1
    assert firefox_results[0]["guid"] == "g1"


def test_delete_deleted_form_history_by_run(evidence_db: sqlite3.Connection) -> None:
    """Test deleting form history by run_id."""
    records = [
        {"guid": "g1", "browser": "firefox", "run_id": "run_a", "source_path": "/test"},
        {"guid": "g2", "browser": "firefox", "run_id": "run_b", "source_path": "/test"},
    ]
    insert_deleted_form_history_entries(evidence_db, 1, records)

    deleted = delete_deleted_form_history_by_run(evidence_db, 1, "run_a")
    assert deleted == 1

    remaining = get_deleted_form_history(evidence_db, 1)
    assert len(remaining) == 1


# ---------------------------------------------------------------------------
# Autofill Profile Tokens Tests
# ---------------------------------------------------------------------------


def test_chromium_token_types_mapping() -> None:
    """Test that token type constants are properly defined."""
    assert CHROMIUM_TOKEN_TYPES[0] == "UNKNOWN_TYPE"
    assert CHROMIUM_TOKEN_TYPES[1] == "NAME_FULL"
    assert CHROMIUM_TOKEN_TYPES[9] == "EMAIL_ADDRESS"
    assert CHROMIUM_TOKEN_TYPES[34] == "ADDRESS_HOME_CITY"
    assert CHROMIUM_TOKEN_TYPES[14] == "PHONE_HOME_WHOLE_NUMBER"
    assert CHROMIUM_TOKEN_TYPES[77] == "COMPANY_NAME"


def test_get_token_type_name() -> None:
    """Test decoding token type codes to names."""
    assert get_token_type_name(0) == "UNKNOWN_TYPE"
    assert get_token_type_name(1) == "NAME_FULL"
    assert get_token_type_name(9) == "EMAIL_ADDRESS"
    assert get_token_type_name(999) == "UNKNOWN_999"  # Unknown code returns UNKNOWN_{code}


def test_insert_autofill_profile_token_single(evidence_db: sqlite3.Connection) -> None:
    """Test inserting a single profile token."""
    # Use correct function signature: insert_autofill_profile_token(conn, evidence_id, browser, guid, token_type, token_value, **kwargs)
    insert_autofill_profile_token(
        evidence_db, 1, "chrome",
        guid="profile-guid-123",
        token_type=1,  # NAME_FULL
        token_value="John Doe",
        profile="Default",
        run_id="test_run_1",
        source_path="/path/to/Web Data",
    )

    # Verify
    cursor = evidence_db.execute("SELECT * FROM autofill_profile_tokens WHERE evidence_id = 1")
    row = cursor.fetchone()
    assert row["guid"] == "profile-guid-123"
    assert row["token_type"] == 1
    assert row["token_value"] == "John Doe"
    assert row["token_type_name"] == "NAME_FULL"


def test_insert_autofill_profile_tokens_batch(evidence_db: sqlite3.Connection) -> None:
    """Test batch inserting profile tokens."""
    # Batch insert uses dict with schema column names: guid, token_type, token_value
    records = [
        {"guid": "guid1", "token_type": 1, "token_value": "John Doe", "browser": "chrome", "run_id": "run1", "source_path": "/test"},
        {"guid": "guid1", "token_type": 9, "token_value": "john@example.com", "browser": "chrome", "run_id": "run1", "source_path": "/test"},
        {"guid": "guid1", "token_type": 14, "token_value": "+1-555-1234", "browser": "chrome", "run_id": "run1", "source_path": "/test"},
    ]

    result = insert_autofill_profile_tokens(evidence_db, 1, records)
    assert result == 3


def test_get_autofill_profile_tokens_with_filter(evidence_db: sqlite3.Connection) -> None:
    """Test filtering profile tokens by browser."""
    records = [
        {"guid": "g1", "token_type": 1, "token_value": "Val1", "browser": "chrome", "run_id": "run1", "source_path": "/test"},
        {"guid": "g2", "token_type": 1, "token_value": "Val2", "browser": "edge", "run_id": "run1", "source_path": "/test"},
    ]
    insert_autofill_profile_tokens(evidence_db, 1, records)

    chrome_results = get_autofill_profile_tokens(evidence_db, 1, browser="chrome")
    assert len(chrome_results) == 1
    assert chrome_results[0]["token_value"] == "Val1"


def test_delete_autofill_profile_tokens_by_run(evidence_db: sqlite3.Connection) -> None:
    """Test deleting profile tokens by run_id."""
    records = [
        {"guid": "g1", "token_type": 1, "token_value": "V1", "browser": "chrome", "run_id": "run_a", "source_path": "/test"},
        {"guid": "g2", "token_type": 1, "token_value": "V2", "browser": "chrome", "run_id": "run_b", "source_path": "/test"},
    ]
    insert_autofill_profile_tokens(evidence_db, 1, records)

    deleted = delete_autofill_profile_tokens_by_run(evidence_db, 1, "run_a")
    assert deleted == 1

    remaining = get_autofill_profile_tokens(evidence_db, 1)
    assert len(remaining) == 1
    assert remaining[0]["token_value"] == "V2"
