from pathlib import Path

from core.database import (
    insert_urls,
    insert_browser_history,
    insert_images,
    insert_os_indicators,
    create_process_log,
    finalize_process_log,
    # Storage analysis functions
    insert_storage_tokens,
    get_storage_tokens,
    get_storage_token_stats,
    delete_storage_tokens_by_run,
    insert_storage_identifiers,
    get_storage_identifiers,
    get_storage_identifier_stats,
    delete_storage_identifiers_by_run,
    DatabaseManager,
)
from tests.fixtures.db import CaseContext


def test_init_db_creates_schema(case_context: CaseContext) -> None:
    """Test case DB creation and evidence DB artifact insertion."""
    case_conn = case_context.case_conn
    manager = case_context.manager

    # Verify case DB schema
    cursor = case_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='cases';"
    )
    assert cursor.fetchone() is not None
    cursor = case_conn.execute("SELECT COUNT(*) FROM schema_version;")
    assert cursor.fetchone()[0] >= 1

    # Get evidence connection and insert artifacts
    evidence_conn = manager.get_evidence_conn(case_context.evidence_id, case_context.evidence_label)
    insert_urls(evidence_conn, case_context.evidence_id, [{"url": "https://example.com", "discovered_by": "test"}])
    insert_browser_history(
        evidence_conn,
        case_context.evidence_id,
        [{"url": "https://example.com", "browser": "test"}],
    )
    insert_images(
        evidence_conn,
        case_context.evidence_id,
        [{"rel_path": "carved/file.jpg", "filename": "file.jpg", "discovered_by": "test"}],
    )
    insert_os_indicators(
        evidence_conn,
        case_context.evidence_id,
        [
            {
                "type": "registry",
                "name": "Deep Freeze",
                "value": "present",
                "hive": "SYSTEM",
                "confidence": "high",
            }
        ],
    )
    log_id = create_process_log(evidence_conn, case_context.evidence_id, "test-task", "echo test")
    finalize_process_log(evidence_conn, log_id, exit_code=0, stdout="ok", stderr="")

    # Verify artifacts are in evidence DB
    url_row = evidence_conn.execute(
        "SELECT url FROM urls WHERE evidence_id = ?",
        (case_context.evidence_id,),
    ).fetchone()
    assert url_row is not None
    assert url_row[0] == "https://example.com"

    evidence_conn.close()


# =============================================================================
# Storage Analysis Tests
# =============================================================================

class TestStorageTokens:
    """Tests for storage_tokens table operations."""

    def test_insert_and_get_storage_tokens(self, tmp_path: Path) -> None:
        """Test inserting and retrieving storage tokens."""
        case_db_path = tmp_path / "test_surfsifter.sqlite"
        manager = DatabaseManager(tmp_path, case_db_path=case_db_path)
        case_conn = manager.get_case_conn()

        try:
            # Create case and evidence
            with case_conn:
                case_conn.execute(
                    "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
                    ("CASE-1", "Test Case", "2024-01-01T00:00:00"),
                )
                case_conn.execute(
                    "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                    (1, "EWF", "path", "2024-01-01T00:00:00"),
                )

            evidence_conn = manager.get_evidence_conn(1, "EWF")

            # Insert tokens
            tokens = [
                {
                    "run_id": "run-001",
                    "browser": "firefox",
                    "profile": "default",
                    "origin": "https://onedrive.live.com",
                    "storage_type": "local_storage",
                    "storage_key": "graph_auth",
                    "token_type": "microsoft_oauth",
                    "token_value": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                    "token_hash": "abc123",
                    "associated_email": "user@example.com",
                    "expires_at_utc": "2024-12-31T23:59:59",
                    "risk_level": "high",
                    "is_expired": 0,
                },
                {
                    "run_id": "run-001",
                    "browser": "firefox",
                    "profile": "default",
                    "origin": "https://google.com",
                    "storage_type": "local_storage",
                    "storage_key": "session_token",
                    "token_type": "session",
                    "token_value": "session-abc-123",
                    "token_hash": "def456",
                    "risk_level": "medium",
                    "is_expired": 1,
                },
            ]

            count = insert_storage_tokens(evidence_conn, 1, tokens)
            assert count == 2

            # Retrieve all tokens
            results = get_storage_tokens(evidence_conn, 1)
            assert len(results) == 2

            # Check first token
            token = results[0]
            assert token["browser"] == "firefox"
            assert token["origin"] == "https://onedrive.live.com"
            assert token["token_type"] == "microsoft_oauth"
            assert token["associated_email"] == "user@example.com"

            evidence_conn.close()
        finally:
            case_conn.close()

    def test_storage_token_filters(self, tmp_path: Path) -> None:
        """Test filtering storage tokens by type, origin, and run_id."""
        case_db_path = tmp_path / "test_surfsifter.sqlite"
        manager = DatabaseManager(tmp_path, case_db_path=case_db_path)
        case_conn = manager.get_case_conn()

        try:
            with case_conn:
                case_conn.execute(
                    "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
                    ("CASE-1", "Test Case", "2024-01-01T00:00:00"),
                )
                case_conn.execute(
                    "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                    (1, "EWF", "path", "2024-01-01T00:00:00"),
                )

            evidence_conn = manager.get_evidence_conn(1, "EWF")

            tokens = [
                {
                    "run_id": "run-001",
                    "browser": "firefox",
                    "origin": "https://microsoft.com",
                    "storage_type": "local_storage",
                    "storage_key": "key1",
                    "token_type": "microsoft_oauth",
                    "token_value": "token1",
                    "token_hash": "hash1",
                },
                {
                    "run_id": "run-002",
                    "browser": "chrome",
                    "origin": "https://google.com",
                    "storage_type": "local_storage",
                    "storage_key": "key2",
                    "token_type": "jwt",
                    "token_value": "token2",
                    "token_hash": "hash2",
                },
            ]

            insert_storage_tokens(evidence_conn, 1, tokens)

            # Filter by token_type
            oauth = get_storage_tokens(evidence_conn, 1, token_type="microsoft_oauth")
            assert len(oauth) == 1
            assert oauth[0]["origin"] == "https://microsoft.com"

            # Filter by run_id
            run1 = get_storage_tokens(evidence_conn, 1, run_id="run-001")
            assert len(run1) == 1

            evidence_conn.close()
        finally:
            case_conn.close()

    def test_storage_token_stats(self, tmp_path: Path) -> None:
        """Test storage token statistics."""
        case_db_path = tmp_path / "test_surfsifter.sqlite"
        manager = DatabaseManager(tmp_path, case_db_path=case_db_path)
        case_conn = manager.get_case_conn()

        try:
            with case_conn:
                case_conn.execute(
                    "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
                    ("CASE-1", "Test Case", "2024-01-01T00:00:00"),
                )
                case_conn.execute(
                    "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                    (1, "EWF", "path", "2024-01-01T00:00:00"),
                )

            evidence_conn = manager.get_evidence_conn(1, "EWF")

            tokens = [
                {"run_id": "run-001", "browser": "firefox", "origin": "https://a.com", "storage_type": "local_storage", "storage_key": "k1", "token_type": "jwt", "token_value": "v1", "token_hash": "h1"},
                {"run_id": "run-001", "browser": "firefox", "origin": "https://b.com", "storage_type": "local_storage", "storage_key": "k2", "token_type": "jwt", "token_value": "v2", "token_hash": "h2"},
                {"run_id": "run-001", "browser": "firefox", "origin": "https://a.com", "storage_type": "local_storage", "storage_key": "k3", "token_type": "oauth", "token_value": "v3", "token_hash": "h3"},
            ]
            insert_storage_tokens(evidence_conn, 1, tokens)

            stats = get_storage_token_stats(evidence_conn, 1)
            assert stats["total"] == 3
            assert stats["by_type"]["jwt"] == 2
            assert stats["by_type"]["oauth"] == 1
            assert stats["unique_origins"] == 2

            evidence_conn.close()
        finally:
            case_conn.close()

    def test_delete_tokens_by_run(self, tmp_path: Path) -> None:
        """Test deleting tokens by run_id."""
        case_db_path = tmp_path / "test_surfsifter.sqlite"
        manager = DatabaseManager(tmp_path, case_db_path=case_db_path)
        case_conn = manager.get_case_conn()

        try:
            with case_conn:
                case_conn.execute(
                    "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
                    ("CASE-1", "Test Case", "2024-01-01T00:00:00"),
                )
                case_conn.execute(
                    "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                    (1, "EWF", "path", "2024-01-01T00:00:00"),
                )

            evidence_conn = manager.get_evidence_conn(1, "EWF")

            tokens = [
                {"run_id": "run-001", "browser": "firefox", "origin": "https://a.com", "storage_type": "local_storage", "storage_key": "k1", "token_type": "jwt", "token_value": "v1", "token_hash": "h1"},
                {"run_id": "run-002", "browser": "firefox", "origin": "https://b.com", "storage_type": "local_storage", "storage_key": "k2", "token_type": "jwt", "token_value": "v2", "token_hash": "h2"},
            ]
            insert_storage_tokens(evidence_conn, 1, tokens)

            assert len(get_storage_tokens(evidence_conn, 1)) == 2

            deleted = delete_storage_tokens_by_run(evidence_conn, 1, "run-001")
            assert deleted == 1
            assert len(get_storage_tokens(evidence_conn, 1)) == 1

            evidence_conn.close()
        finally:
            case_conn.close()


class TestStorageIdentifiers:
    """Tests for storage_identifiers table operations."""

    def test_insert_and_get_identifiers(self, tmp_path: Path) -> None:
        """Test inserting and retrieving storage identifiers."""
        case_db_path = tmp_path / "test_surfsifter.sqlite"
        manager = DatabaseManager(tmp_path, case_db_path=case_db_path)
        case_conn = manager.get_case_conn()

        try:
            with case_conn:
                case_conn.execute(
                    "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
                    ("CASE-1", "Test Case", "2024-01-01T00:00:00"),
                )
                case_conn.execute(
                    "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                    (1, "EWF", "path", "2024-01-01T00:00:00"),
                )

            evidence_conn = manager.get_evidence_conn(1, "EWF")

            identifiers = [
                {
                    "run_id": "run-001",
                    "browser": "firefox",
                    "profile": "default",
                    "origin": "https://microsoft.com",
                    "storage_type": "local_storage",
                    "storage_key": "muid",
                    "identifier_type": "device_id",
                    "identifier_name": "muid",
                    "identifier_value": "abc-123-def",
                    "first_seen_utc": "2024-01-01T00:00:00",
                    "last_seen_utc": "2024-06-01T00:00:00",
                },
                {
                    "run_id": "run-001",
                    "browser": "firefox",
                    "origin": "https://analytics.com",
                    "storage_type": "local_storage",
                    "storage_key": "_ga",
                    "identifier_type": "tracking_id",
                    "identifier_name": "_ga",
                    "identifier_value": "GA1.2.123456.7890",
                },
            ]

            count = insert_storage_identifiers(evidence_conn, 1, identifiers)
            assert count == 2

            results = get_storage_identifiers(evidence_conn, 1)
            assert len(results) == 2

            # Check first identifier
            ident = results[0]
            assert ident["browser"] == "firefox"
            assert ident["identifier_type"] == "device_id"
            assert ident["identifier_value"] == "abc-123-def"

            evidence_conn.close()
        finally:
            case_conn.close()

    def test_identifier_filters(self, tmp_path: Path) -> None:
        """Test filtering identifiers by type."""
        case_db_path = tmp_path / "test_surfsifter.sqlite"
        manager = DatabaseManager(tmp_path, case_db_path=case_db_path)
        case_conn = manager.get_case_conn()

        try:
            with case_conn:
                case_conn.execute(
                    "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
                    ("CASE-1", "Test Case", "2024-01-01T00:00:00"),
                )
                case_conn.execute(
                    "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                    (1, "EWF", "path", "2024-01-01T00:00:00"),
                )

            evidence_conn = manager.get_evidence_conn(1, "EWF")

            identifiers = [
                {"run_id": "run-001", "browser": "firefox", "origin": "https://a.com", "storage_type": "local_storage", "storage_key": "k1", "identifier_type": "email", "identifier_value": "user@example.com"},
                {"run_id": "run-001", "browser": "firefox", "origin": "https://b.com", "storage_type": "local_storage", "storage_key": "k2", "identifier_type": "tracking_id", "identifier_value": "GA123"},
            ]
            insert_storage_identifiers(evidence_conn, 1, identifiers)

            emails = get_storage_identifiers(evidence_conn, 1, identifier_type="email")
            assert len(emails) == 1
            assert emails[0]["identifier_value"] == "user@example.com"

            evidence_conn.close()
        finally:
            case_conn.close()

    def test_identifier_stats(self, tmp_path: Path) -> None:
        """Test identifier statistics."""
        case_db_path = tmp_path / "test_surfsifter.sqlite"
        manager = DatabaseManager(tmp_path, case_db_path=case_db_path)
        case_conn = manager.get_case_conn()

        try:
            with case_conn:
                case_conn.execute(
                    "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
                    ("CASE-1", "Test Case", "2024-01-01T00:00:00"),
                )
                case_conn.execute(
                    "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                    (1, "EWF", "path", "2024-01-01T00:00:00"),
                )

            evidence_conn = manager.get_evidence_conn(1, "EWF")

            identifiers = [
                {"run_id": "run-001", "browser": "firefox", "origin": "https://a.com", "storage_type": "local_storage", "storage_key": "k1", "identifier_type": "email", "identifier_value": "user@example.com"},
                {"run_id": "run-001", "browser": "firefox", "origin": "https://b.com", "storage_type": "local_storage", "storage_key": "k2", "identifier_type": "email", "identifier_value": "other@example.com"},
                {"run_id": "run-001", "browser": "firefox", "origin": "https://c.com", "storage_type": "local_storage", "storage_key": "k3", "identifier_type": "tracking_id", "identifier_value": "GA123"},
            ]
            insert_storage_identifiers(evidence_conn, 1, identifiers)

            stats = get_storage_identifier_stats(evidence_conn, 1)
            assert stats["total"] == 3
            assert stats["by_type"]["email"] == 2
            assert stats["by_type"]["tracking_id"] == 1
            assert stats["unique_origins"] == 3

            evidence_conn.close()
        finally:
            case_conn.close()

    def test_delete_identifiers_by_run(self, tmp_path: Path) -> None:
        """Test deleting identifiers by run_id."""
        case_db_path = tmp_path / "test_surfsifter.sqlite"
        manager = DatabaseManager(tmp_path, case_db_path=case_db_path)
        case_conn = manager.get_case_conn()

        try:
            with case_conn:
                case_conn.execute(
                    "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
                    ("CASE-1", "Test Case", "2024-01-01T00:00:00"),
                )
                case_conn.execute(
                    "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                    (1, "EWF", "path", "2024-01-01T00:00:00"),
                )

            evidence_conn = manager.get_evidence_conn(1, "EWF")

            identifiers = [
                {"run_id": "run-001", "browser": "firefox", "origin": "https://a.com", "storage_type": "local_storage", "storage_key": "k1", "identifier_type": "email", "identifier_value": "user@example.com"},
                {"run_id": "run-002", "browser": "firefox", "origin": "https://b.com", "storage_type": "local_storage", "storage_key": "k2", "identifier_type": "email", "identifier_value": "other@example.com"},
            ]
            insert_storage_identifiers(evidence_conn, 1, identifiers)

            assert len(get_storage_identifiers(evidence_conn, 1)) == 2

            deleted = delete_storage_identifiers_by_run(evidence_conn, 1, "run-001")
            assert deleted == 1
            assert len(get_storage_identifiers(evidence_conn, 1)) == 1

            evidence_conn.close()
        finally:
            case_conn.close()
