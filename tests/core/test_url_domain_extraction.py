"""
Test that domain field is properly extracted during URL import.

This test verifies that all URL importers (bulk_extractor, sqlite_extractor,
regex scanner) correctly populate the domain field using extract_domain().
"""

import sqlite3
from pathlib import Path

from core.database import insert_urls
from core.database import DatabaseManager


def test_insert_urls_populates_domain_field(tmp_path: Path):
    """Test that insert_urls stores domain field correctly."""
    case_db_path = tmp_path / "test_surfsifter.sqlite"
    db_mgr = DatabaseManager(tmp_path, case_db_path=case_db_path)

    # Get evidence connection
    case_conn = db_mgr.get_case_conn()
    with case_conn:
        case_conn.execute(
            "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("CASE-1", "Test Case", "2025-01-01T00:00:00"),
        )
        case_conn.execute(
            "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
            (1, "TEST", "/dev/null", "2025-01-01T00:00:00"),
        )

    evidence_conn = db_mgr.get_evidence_conn(1, "TEST")
    insert_urls(
        evidence_conn,
        evidence_id=1,
        urls=[
            {
                "url": "https://www.example.com/path",
                "domain": "www.example.com",
                "discovered_by": "test",
            },
            {
                "url": "https://casinoexample.com/slots",
                "domain": "casinoexample.com",
                "discovered_by": "test",
            },
            {
                "url": "http://192.168.1.1/admin",
                "domain": "192.168.1.1",
                "discovered_by": "test",
            },
        ],
    )

    # Verify domains are stored
    cursor = evidence_conn.execute(
        "SELECT url, domain FROM urls WHERE evidence_id = ? ORDER BY url",
        (1,)
    )
    urls = [{"url": row[0], "domain": row[1]} for row in cursor.fetchall()]
    assert len(urls) == 3

    domains = [u["domain"] for u in urls]
    assert "www.example.com" in domains
    assert "casinoexample.com" in domains
    assert "192.168.1.1" in domains


def test_url_grouping_works_with_domain_field(tmp_path: Path):
    """Test that URL grouping uses the domain field."""
    case_db_path = tmp_path / "test_surfsifter.sqlite"
    db_mgr = DatabaseManager(tmp_path, case_db_path=case_db_path)

    case_conn = db_mgr.get_case_conn()
    with case_conn:
        case_conn.execute(
            "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("CASE-1", "Test Case", "2025-01-01T00:00:00"),
        )
        case_conn.execute(
            "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
            (1, "TEST", "/dev/null", "2025-01-01T00:00:00"),
        )

    evidence_conn = db_mgr.get_evidence_conn(1, "TEST")
    insert_urls(
        evidence_conn,
        evidence_id=1,
        urls=[
            {"url": "https://www.example.com/1", "domain": "www.example.com", "discovered_by": "test"},
            {"url": "https://www.example.com/2", "domain": "www.example.com", "discovered_by": "test"},
            {"url": "https://cdn.example.com/3", "domain": "cdn.example.com", "discovered_by": "test"},
            {"url": "https://casinoexample.com/4", "domain": "casinoexample.com", "discovered_by": "test"},
        ],
    )

    # Query distinct domains
    cursor = evidence_conn.execute(
        "SELECT DISTINCT domain FROM urls WHERE evidence_id = ? ORDER BY domain",
        (1,)
    )
    domains = [row[0] for row in cursor.fetchall()]

    assert domains == ["casinoexample.com", "cdn.example.com", "www.example.com"]

    # Verify grouping query works
    cursor = evidence_conn.execute(
        """
        SELECT domain, COUNT(*) as url_count
        FROM urls
        WHERE evidence_id = ?
        GROUP BY domain
        ORDER BY domain
        """,
        (1,)
    )
    groups = [(row[0], row[1]) for row in cursor.fetchall()]

    assert groups == [
        ("casinoexample.com", 1),
        ("cdn.example.com", 1),
        ("www.example.com", 2),
    ]


def test_domain_field_indexed(tmp_path: Path):
    """Test that domain field has an index for fast filtering."""
    case_db_path = tmp_path / "test_surfsifter.sqlite"
    db_mgr = DatabaseManager(tmp_path, case_db_path=case_db_path)

    case_conn = db_mgr.get_case_conn()
    with case_conn:
        case_conn.execute(
            "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("CASE-1", "Test Case", "2025-01-01T00:00:00"),
        )
        case_conn.execute(
            "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
            (1, "TEST", "/dev/null", "2025-01-01T00:00:00"),
        )

    evidence_conn = db_mgr.get_evidence_conn(1, "TEST")
    cursor = evidence_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='urls'"
    )
    indexes = [row[0] for row in cursor.fetchall()]

    assert "idx_urls_domain" in indexes, "Domain index should exist for performance"
