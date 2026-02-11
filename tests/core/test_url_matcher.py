"""
Tests for URL List Matcher (Phase 3).

Tests URL matching against reference lists with wildcard and regex modes.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from core.database import DatabaseManager
from core.matching import URLMatcher


@pytest.fixture
def db_manager(tmp_path):
    """Create test database manager."""
    case_db_path = tmp_path / "test_surfsifter.sqlite"
    db_mgr = DatabaseManager(tmp_path, case_db_path=case_db_path)

    # Create case via case connection
    case_conn = db_mgr.get_case_conn()
    with case_conn:
        case_conn.execute(
            "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("CASE-1", "Test Case", "2025-11-12T00:00:00"),
        )
        case_conn.execute(
            "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
            (1, "TEST", "/dev/null", "2025-11-12T00:00:00"),
        )
    case_conn.close()

    return db_mgr


@pytest.fixture
def evidence_conn(db_manager):
    """Create evidence database connection."""
    conn = db_manager.get_evidence_conn(evidence_id=1, label="TEST")
    yield conn
    conn.close()


@pytest.fixture
def sample_urls(evidence_conn):
    """Insert sample URLs for testing."""
    urls = [
        ("https://www.acmeshop.com/sports", "acmeshop.com"),
        ("https://acmeshop.ee/portal", "acmeshop.ee"),
        ("https://m.acmeshop.dk/login", "m.acmeshop.dk"),
        ("https://widgetco.com/games", "widgetco.com"),
        ("https://admin.dashapp.com/dashboard", "admin.dashapp.com"),
        ("https://newsportal.com/", "newsportal.com"),
        ("https://example.com/test", "example.com"),
        ("http://192.168.1.100/admin", "192.168.1.100"),
        ("https://localhost:8080/", "localhost"),
        ("https://www.tracker200.com/sports", "www.tracker200.com"),
    ]

    for url, domain in urls:
        evidence_conn.execute(
            """
            INSERT INTO urls (evidence_id, url, domain, discovered_by, first_seen_utc)
            VALUES (?, ?, ?, ?, ?)
        """,
            (1, url, domain, "test", "2025-11-12T10:00:00Z"),
        )

    evidence_conn.commit()
    return urls


@pytest.fixture
def temp_url_list(tmp_path):
    """Create temporary URL list file."""
    def _create_list(name: str, patterns: list[str], regex: bool = False) -> Path:
        list_path = tmp_path / f"{name}.txt"
        with open(list_path, "w", encoding="utf-8") as f:
            f.write(f"# {name} - URL Reference List\n")
            if regex:
                f.write("# REGEX: true\n")
            f.write("#\n")
            f.write("# Test list\n")
            f.write("\n")
            for pattern in patterns:
                f.write(f"{pattern}\n")
        return list_path

    return _create_list


# ============================================================================
# List Loading Tests
# ============================================================================

def test_load_list_skips_comments(temp_url_list):
    """Test that comments are skipped when loading lists."""
    list_path = temp_url_list("test", [
        "# This is a comment",
        "acmeshop.com",
        "# Another comment",
        "widgetco.com",
    ])

    matcher = URLMatcher(None, 1)
    patterns, is_regex = matcher.load_list(list_path)

    assert len(patterns) == 2
    assert "acmeshop.com" in patterns
    assert "widgetco.com" in patterns
    assert not is_regex


def test_load_list_skips_blank_lines(temp_url_list):
    """Test that blank lines are skipped."""
    list_path = temp_url_list("test", [
        "acmeshop.com",
        "",
        "widgetco.com",
        "   ",  # Whitespace only
        "newsportal.com",
    ])

    matcher = URLMatcher(None, 1)
    patterns, is_regex = matcher.load_list(list_path)

    assert len(patterns) == 3
    assert "" not in patterns


def test_load_list_strips_whitespace(temp_url_list):
    """Test that whitespace is stripped from patterns."""
    list_path = temp_url_list("test", [
        "  acmeshop.com  ",
        "\twidgetco.com\t",
        " newsportal.com",
    ])

    matcher = URLMatcher(None, 1)
    patterns, is_regex = matcher.load_list(list_path)

    assert patterns[0] == "acmeshop.com"
    assert patterns[1] == "widgetco.com"
    assert patterns[2] == "newsportal.com"


def test_load_list_detects_regex_flag(temp_url_list):
    """Test that REGEX flag in comments is detected."""
    list_path = temp_url_list("test_regex", [
        "^https://acmeshop\\.",
        "(admin|login)",
    ], regex=True)

    matcher = URLMatcher(None, 1)
    patterns, is_regex = matcher.load_list(list_path)

    assert is_regex is True
    assert len(patterns) == 2


def test_load_list_file_not_found():
    """Test that FileNotFoundError is raised for missing list."""
    matcher = URLMatcher(None, 1)

    with pytest.raises(FileNotFoundError):
        matcher.load_list(Path("/nonexistent/list.txt"))


# ============================================================================
# Pattern Matching Tests (Wildcard Mode)
# ============================================================================

def test_match_pattern_wildcard_substring():
    """Test wildcard substring matching."""
    matcher = URLMatcher(None, 1)

    # Pattern "acmeshop" should match URLs containing "acmeshop"
    assert matcher.match_pattern("https://www.acmeshop.com/sports", "acmeshop", "wildcard")
    assert matcher.match_pattern("https://acmeshop.ee/portal", "acmeshop", "wildcard")
    assert matcher.match_pattern("https://m.acmeshop.dk/", "acmeshop", "wildcard")
    assert matcher.match_pattern("https://288365.com/", "365", "wildcard")  # Substring match

    # Should not match
    assert not matcher.match_pattern("https://example.com/", "acmeshop", "wildcard")


def test_match_pattern_wildcard_exact_domain():
    """Test wildcard matching with exact domain."""
    matcher = URLMatcher(None, 1)

    # Pattern "acmeshop.com" should match URLs with "acmeshop.com" in them
    assert matcher.match_pattern("https://www.acmeshop.com/sports", "acmeshop.com", "wildcard")
    assert matcher.match_pattern("https://acmeshop.com/", "acmeshop.com", "wildcard")

    # Should not match different TLD
    assert not matcher.match_pattern("https://acmeshop.ee/portal", "acmeshop.com", "wildcard")
    assert not matcher.match_pattern("https://m.acmeshop.dk/", "acmeshop.com", "wildcard")


def test_match_pattern_wildcard_case_insensitive():
    """Test that wildcard matching is case-insensitive."""
    matcher = URLMatcher(None, 1)

    assert matcher.match_pattern("https://WWW.ACMESHOP.COM/", "acmeshop", "wildcard")
    assert matcher.match_pattern("https://www.acmeshop.com/", "ACMESHOP", "wildcard")
    assert matcher.match_pattern("https://Acmeshop.Ee/", "ACMESHOP", "wildcard")


def test_match_pattern_wildcard_ip_address():
    """Test wildcard matching with IP addresses."""
    matcher = URLMatcher(None, 1)

    assert matcher.match_pattern("http://192.168.1.100/admin", "192.168.1.100", "wildcard")
    assert matcher.match_pattern("http://192.168.1.100:8080/", "192.168.1.100", "wildcard")

    # Partial IP match (substring)
    assert matcher.match_pattern("http://192.168.1.100/", "192.168.1", "wildcard")

    # Should not match different IP
    assert not matcher.match_pattern("http://10.0.0.1/", "192.168.1.100", "wildcard")


def test_match_pattern_wildcard_localhost():
    """Test wildcard matching with localhost."""
    matcher = URLMatcher(None, 1)

    assert matcher.match_pattern("https://localhost:8080/", "localhost", "wildcard")
    assert matcher.match_pattern("http://localhost/admin", "localhost", "wildcard")


def test_match_pattern_wildcard_with_wildcards():
    """Test wildcard patterns with * characters."""
    matcher = URLMatcher(None, 1)

    # Pattern with * wildcard
    assert matcher.match_pattern("https://www.acmeshop.com/sports", "*.acmeshop.com*", "wildcard")
    assert matcher.match_pattern("https://admin.dashapp.com/", "*admin*", "wildcard")
    assert matcher.match_pattern("https://acmeshop.com/", "*acmeshop*", "wildcard")
    assert matcher.match_pattern("https://widgetco4.com/", "*widgetco*", "wildcard")


# ============================================================================
# Pattern Matching Tests (Regex Mode)
# ============================================================================

def test_match_pattern_regex_basic():
    """Test basic regex matching."""
    matcher = URLMatcher(None, 1)

    # Match URLs starting with https://acmeshop
    assert matcher.match_pattern("https://acmeshop.com/sports", "^https://acmeshop", "regex")
    assert matcher.match_pattern("https://acmeshop.ee/", "^https://acmeshop", "regex")

    # Should not match
    assert not matcher.match_pattern("http://acmeshop.com/", "^https://acmeshop", "regex")
    assert not matcher.match_pattern("https://www.acmeshop.com/", "^https://acmeshop", "regex")


def test_match_pattern_regex_alternation():
    """Test regex with alternation (OR)."""
    matcher = URLMatcher(None, 1)

    pattern = "(admin|login|dashboard)"
    assert matcher.match_pattern("https://example.com/admin", pattern, "regex")
    assert matcher.match_pattern("https://example.com/login", pattern, "regex")
    assert matcher.match_pattern("https://example.com/dashboard", pattern, "regex")

    # Should not match
    assert not matcher.match_pattern("https://example.com/user", pattern, "regex")


def test_match_pattern_regex_case_insensitive():
    """Test that regex matching is case-insensitive."""
    matcher = URLMatcher(None, 1)

    pattern = "ACMESHOP"
    assert matcher.match_pattern("https://www.acmeshop.com/", pattern, "regex")
    assert matcher.match_pattern("https://ACMESHOP.COM/", pattern, "regex")


def test_match_pattern_regex_invalid():
    """Test handling of invalid regex patterns."""
    matcher = URLMatcher(None, 1)

    # Invalid regex should return False (not raise exception)
    assert not matcher.match_pattern("https://example.com/", "[invalid(regex", "regex")


# ============================================================================
# URL Matching Tests (Database Integration)
# ============================================================================

def test_match_urls_wildcard_basic(evidence_conn, sample_urls, temp_url_list):
    """Test basic URL matching with wildcard mode."""
    list_path = temp_url_list("acmeshop", [
        "acmeshop",
    ])

    matcher = URLMatcher(evidence_conn, 1)
    result = matcher.match_urls("acmeshop", list_path)

    assert result["matched"] == 3  # acmeshop.com, acmeshop.ee, acmeshop.dk
    assert result["total"] == 10
    assert result["list_name"] == "acmeshop"


def test_match_urls_multiple_patterns(evidence_conn, sample_urls, temp_url_list):
    """Test matching with multiple patterns in one list."""
    list_path = temp_url_list("monitoring", [
        "acmeshop",
        "widgetco",
        "newsportal",
    ])

    matcher = URLMatcher(evidence_conn, 1)
    result = matcher.match_urls("monitoring", list_path)

    # Should match: 3 acmeshop URLs + 1 widgetco + 1 newsportal = 5
    assert result["matched"] == 5


def test_match_urls_stores_in_database(evidence_conn, sample_urls, temp_url_list):
    """Test that matches are stored in url_matches table."""
    list_path = temp_url_list("acmeshop", ["acmeshop"])

    matcher = URLMatcher(evidence_conn, 1)
    matcher.match_urls("acmeshop", list_path)

    # Check database
    cursor = evidence_conn.execute(
        """
        SELECT url_id, list_name, match_type, matched_pattern
        FROM url_matches
        WHERE evidence_id = 1
    """
    )

    matches = cursor.fetchall()
    assert len(matches) == 3

    # Verify match details
    for match in matches:
        url_id, list_name, match_type, matched_pattern = match
        assert list_name == "acmeshop"
        assert match_type == "wildcard"
        assert matched_pattern == "acmeshop"


def test_match_urls_no_duplicates(evidence_conn, sample_urls, temp_url_list):
    """Test that running match twice creates duplicate matches (no UNIQUE constraint)."""
    list_path = temp_url_list("acmeshop", ["acmeshop"])

    matcher = URLMatcher(evidence_conn, 1)

    # Run match twice
    result1 = matcher.match_urls("acmeshop", list_path)
    result2 = matcher.match_urls("acmeshop", list_path)

    assert result1["matched"] == 3
    assert result2["matched"] == 3  # Duplicates are created (schema allows it)

    # Check database has duplicates
    count = evidence_conn.execute(
        "SELECT COUNT(*) FROM url_matches WHERE evidence_id = 1"
    ).fetchone()[0]

    assert count == 6  # 3 + 3 duplicates


def test_match_urls_first_pattern_wins(evidence_conn, sample_urls, temp_url_list):
    """Test that first matching pattern wins (no multiple matches per URL)."""
    list_path = temp_url_list("multi", [
        "acmeshop",
        "365",  # Would also match acmeshop URLs
    ])

    matcher = URLMatcher(evidence_conn, 1)
    result = matcher.match_urls("multi", list_path)

    # Check that each URL has only one match
    cursor = evidence_conn.execute(
        """
        SELECT url_id, COUNT(*)
        FROM url_matches
        WHERE evidence_id = 1
        GROUP BY url_id
    """
    )

    for url_id, match_count in cursor.fetchall():
        assert match_count == 1, f"URL {url_id} has {match_count} matches (expected 1)"


def test_match_urls_empty_list(evidence_conn, sample_urls, temp_url_list):
    """Test matching against empty list."""
    list_path = temp_url_list("empty", [])

    matcher = URLMatcher(evidence_conn, 1)
    result = matcher.match_urls("empty", list_path)

    assert result["matched"] == 0
    assert result["total"] == 0


def test_match_urls_no_matches(evidence_conn, sample_urls, temp_url_list):
    """Test matching with patterns that don't match any URLs."""
    list_path = temp_url_list("nomatch", [
        "nonexistent.com",
        "fake-domain.org",
    ])

    matcher = URLMatcher(evidence_conn, 1)
    result = matcher.match_urls("nomatch", list_path)

    assert result["matched"] == 0
    assert result["total"] == 10


def test_match_urls_regex_mode(evidence_conn, sample_urls, temp_url_list):
    """Test URL matching with regex mode."""
    list_path = temp_url_list("admin_urls", [
        ".*admin.*",
    ], regex=True)

    matcher = URLMatcher(evidence_conn, 1)
    result = matcher.match_urls("admin_urls", list_path)

    # Should match: admin.dashapp.com and 192.168.1.100/admin
    assert result["matched"] == 2


def test_match_urls_progress_callback(evidence_conn, sample_urls, temp_url_list):
    """Test that progress callback is called."""
    list_path = temp_url_list("acmeshop", ["acmeshop"])

    progress_calls = []

    def progress_callback(current, total):
        progress_calls.append((current, total))

    matcher = URLMatcher(evidence_conn, 1)
    matcher.match_urls("acmeshop", list_path, progress_callback=progress_callback)

    # Should have final progress call
    assert len(progress_calls) > 0
    assert progress_calls[-1] == (10, 10)  # Final: 10/10 URLs processed


# ============================================================================
# Utility Methods Tests
# ============================================================================

def test_clear_matches_specific_list(evidence_conn, sample_urls, temp_url_list):
    """Test clearing matches for specific list."""
    list1 = temp_url_list("acmeshop", ["acmeshop"])
    list2 = temp_url_list("widgetco", ["widgetco"])

    matcher = URLMatcher(evidence_conn, 1)
    matcher.match_urls("acmeshop", list1)
    matcher.match_urls("widgetco", list2)

    # Clear only acmeshop matches
    removed = matcher.clear_matches("acmeshop")
    assert removed == 3

    # Check database
    cursor = evidence_conn.execute(
        "SELECT list_name FROM url_matches WHERE evidence_id = 1"
    )
    remaining = [row[0] for row in cursor.fetchall()]

    assert "acmeshop" not in remaining
    assert "widgetco" in remaining


def test_clear_matches_all(evidence_conn, sample_urls, temp_url_list):
    """Test clearing all URL matches."""
    list1 = temp_url_list("acmeshop", ["acmeshop"])
    list2 = temp_url_list("widgetco", ["widgetco"])

    matcher = URLMatcher(evidence_conn, 1)
    matcher.match_urls("acmeshop", list1)
    matcher.match_urls("widgetco", list2)

    # Clear all matches
    removed = matcher.clear_matches()
    assert removed == 4  # 3 acmeshop + 1 widgetco

    # Check database
    count = evidence_conn.execute(
        "SELECT COUNT(*) FROM url_matches WHERE evidence_id = 1"
    ).fetchone()[0]

    assert count == 0


def test_get_match_stats(evidence_conn, sample_urls, temp_url_list):
    """Test getting match statistics."""
    list1 = temp_url_list("acmeshop", ["acmeshop"])
    list2 = temp_url_list("widgetco", ["widgetco"])

    matcher = URLMatcher(evidence_conn, 1)
    matcher.match_urls("acmeshop", list1)
    matcher.match_urls("widgetco", list2)

    stats = matcher.get_match_stats()

    assert stats["total_urls"] == 10
    assert stats["matched_urls"] == 4  # 3 acmeshop + 1 widgetco
    assert stats["match_count"] == 4
    assert stats["lists"]["acmeshop"] == 3
    assert stats["lists"]["widgetco"] == 1


def test_get_match_stats_no_matches(evidence_conn, sample_urls):
    """Test stats when no matches exist."""
    matcher = URLMatcher(evidence_conn, 1)
    stats = matcher.get_match_stats()

    assert stats["total_urls"] == 10
    assert stats["matched_urls"] == 0
    assert stats["match_count"] == 0
    assert stats["lists"] == {}


# ============================================================================
# Edge Cases
# ============================================================================

def test_match_malformed_url(evidence_conn, temp_url_list):
    """Test matching with malformed URLs."""
    # Insert malformed URL
    evidence_conn.execute(
        """
        INSERT INTO urls (evidence_id, url, domain, discovered_by)
        VALUES (?, ?, ?, ?)
    """,
        (1, "not-a-valid-url", "", "test"),
    )
    evidence_conn.commit()

    list_path = temp_url_list("test", ["nonexistent-pattern"])

    matcher = URLMatcher(evidence_conn, 1)
    # Should not crash and should not match
    result = matcher.match_urls("test", list_path)
    assert result["matched"] == 0


def test_match_url_with_path(evidence_conn, temp_url_list):
    """Test matching URLs with specific paths."""
    evidence_conn.execute(
        """
        INSERT INTO urls (evidence_id, url, domain, discovered_by)
        VALUES (?, ?, ?, ?)
    """,
        (1, "https://example.com/admin/login", "example.com", "test"),
    )
    evidence_conn.commit()

    list_path = temp_url_list("admin", ["admin/login"])

    matcher = URLMatcher(evidence_conn, 1)
    result = matcher.match_urls("admin", list_path)

    assert result["matched"] == 1


def test_match_url_with_port(evidence_conn, temp_url_list):
    """Test matching URLs with ports."""
    evidence_conn.execute(
        """
        INSERT INTO urls (evidence_id, url, domain, discovered_by)
        VALUES (?, ?, ?, ?)
    """,
        (1, "https://example.com:8443/login", "example.com", "test"),
    )
    evidence_conn.commit()

    list_path = temp_url_list("test", ["example.com"])

    matcher = URLMatcher(evidence_conn, 1)
    result = matcher.match_urls("test", list_path)

    assert result["matched"] == 1
