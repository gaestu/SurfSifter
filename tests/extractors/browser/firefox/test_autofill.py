"""
Tests for FirefoxAutofillExtractor

Tests the browser-specific Firefox autofill extractor:
- Metadata and registration
- Browser pattern matching (Firefox, Tor)
- SQLite parsing for formhistory.sqlite
- JSON parsing for logins.json
- Extraction and ingestion workflows
- Statistics collector integration
"""
import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, PropertyMock

import pytest

from extractors.browser.firefox.autofill import FirefoxAutofillExtractor
from extractors.browser.firefox.autofill._parsers import parse_moz_deleted_formhistory
from extractors.browser.firefox.autofill._schemas import classify_autofill_file
from extractors.browser.firefox._patterns import (
    FIREFOX_BROWSERS,
    FIREFOX_ARTIFACTS,
    get_artifact_patterns,
)
from extractors._shared.timestamps import prtime_to_datetime, unix_milliseconds_to_datetime


# ===========================================================================
# Test Fixtures
# ===========================================================================

@pytest.fixture
def extractor():
    """Create FirefoxAutofillExtractor instance."""
    return FirefoxAutofillExtractor()


@pytest.fixture
def mock_callbacks():
    """Create mock callbacks for testing."""
    class MockCallbacks:
        def __init__(self):
            self.progress_calls = []
            self.log_calls = []
            self.step_calls = []
            self.error_calls = []
            self._cancelled = False

        def on_progress(self, current: int, total: int, message: str = ""):
            self.progress_calls.append((current, total, message))

        def on_log(self, message: str, level: str = "info"):
            self.log_calls.append((message, level))

        def on_step(self, step: str):
            self.step_calls.append(step)

        def on_error(self, error: str, details: str = ""):
            self.error_calls.append((error, details))

        def is_cancelled(self) -> bool:
            return self._cancelled

    return MockCallbacks()


@pytest.fixture
def mock_evidence_fs():
    """Create mock EvidenceFS for extraction tests."""
    mock_fs = MagicMock()
    mock_fs.iter_paths.return_value = []
    return mock_fs


@pytest.fixture
def temp_formhistory_db(tmp_path):
    """Create a temporary formhistory.sqlite database."""
    db_path = tmp_path / "formhistory.sqlite"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Create moz_formhistory table
    cursor.execute("""
        CREATE TABLE moz_formhistory (
            id INTEGER PRIMARY KEY,
            fieldname TEXT NOT NULL,
            value TEXT NOT NULL,
            timesUsed INTEGER DEFAULT 0,
            firstUsed INTEGER DEFAULT 0,
            lastUsed INTEGER DEFAULT 0,
            guid TEXT
        )
    """)

    # Insert test data (PRTime format: microseconds since Unix epoch)
    # 1640000000000000 = 2021-12-20 approx in microseconds
    cursor.execute("""
        INSERT INTO moz_formhistory (fieldname, value, timesUsed, firstUsed, lastUsed, guid)
        VALUES ('email', 'test@example.com', 5, 1640000000000000, 1645000000000000, 'test-guid-1')
    """)
    cursor.execute("""
        INSERT INTO moz_formhistory (fieldname, value, timesUsed, firstUsed, lastUsed, guid)
        VALUES ('search', 'python tutorials', 10, 1640000000000000, 1650000000000000, 'test-guid-2')
    """)

    conn.commit()
    conn.close()

    return db_path


@pytest.fixture
def temp_logins_json(tmp_path):
    """Create a temporary logins.json file."""
    logins_path = tmp_path / "logins.json"
    logins_data = {
        "nextId": 2,
        "logins": [
            {
                "id": 1,
                "hostname": "https://example.com",
                "httpRealm": None,
                "formSubmitURL": "https://example.com/login",
                "usernameField": "username",
                "passwordField": "password",
                "encryptedUsername": "MEIEEPgAAAAAAAAAAAAAAAAAAAEwFAYIKoZIhvcNAwcECGVw",
                "encryptedPassword": "MEIEEPgAAAAAAAAAAAAAAAAAAAEwFAYIKoZIhvcNAwcECGVx",
                "guid": "{12345678-1234-1234-1234-123456789abc}",
                "encType": 1,
                "timeCreated": 1640000000000,
                "timeLastUsed": 1645000000000,
                "timePasswordChanged": 1640000000000,
                "timesUsed": 5,
                "syncCounter": 3,
                "everSynced": True
            }
        ],
        "potentiallyVulnerablePasswords": [],
        "dismissedBreachAlertsByLoginGUID": {},
        "version": 3
    }
    logins_path.write_text(json.dumps(logins_data))
    return logins_path


@pytest.fixture
def temp_signons_sqlite(tmp_path):
    """Create a temporary legacy signons.sqlite database."""
    db_path = tmp_path / "signons.sqlite"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Create moz_logins table (legacy Firefox < 32 schema)
    cursor.execute("""
        CREATE TABLE moz_logins (
            id INTEGER PRIMARY KEY,
            hostname TEXT NOT NULL,
            httpRealm TEXT,
            formSubmitURL TEXT,
            usernameField TEXT,
            passwordField TEXT,
            encryptedUsername TEXT,
            encryptedPassword TEXT NOT NULL,
            guid TEXT,
            encType INTEGER DEFAULT 1,
            timeCreated INTEGER,
            timeLastUsed INTEGER,
            timePasswordChanged INTEGER,
            timesUsed INTEGER DEFAULT 0
        )
    """)

    # Create moz_disabledHosts table
    cursor.execute("""
        CREATE TABLE moz_disabledHosts (
            id INTEGER PRIMARY KEY,
            hostname TEXT NOT NULL
        )
    """)

    # Insert test data (PRTime format: microseconds since Unix epoch)
    cursor.execute("""
        INSERT INTO moz_logins (
            hostname, httpRealm, formSubmitURL, usernameField, passwordField,
            encryptedUsername, encryptedPassword, guid, encType,
            timeCreated, timeLastUsed, timePasswordChanged, timesUsed
        ) VALUES (
            'https://legacy-site.com', NULL, 'https://legacy-site.com/login',
            'user', 'pass', 'ENCRYPTED_USER_BASE64', 'ENCRYPTED_PASS_BASE64',
            'legacy-guid-1234', 1, 1400000000000000, 1410000000000000,
            1400000000000000, 10
        )
    """)

    conn.commit()
    conn.close()

    return db_path


# ===========================================================================
# Parser Tests
# ===========================================================================


def test_parse_deleted_formhistory_with_source_correlation(tmp_path):
    """Deleted formhistory should enrich with original value and source notes."""
    db_path = tmp_path / "formhistory.sqlite"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE moz_formhistory (
            id INTEGER PRIMARY KEY,
            fieldname TEXT,
            value TEXT,
            guid TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE moz_deleted_formhistory (
            id INTEGER PRIMARY KEY,
            timeDeleted INTEGER,
            guid TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE moz_sources (
            id INTEGER PRIMARY KEY,
            source_name TEXT,
            source_url TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE moz_history_to_sources (
            history_id INTEGER,
            source_id INTEGER
        )
        """
    )

    deleted_prtime = 1645000000000000
    cursor.execute(
        "INSERT INTO moz_formhistory (id, fieldname, value, guid) VALUES (?, ?, ?, ?)",
        (10, "email", "person@example.com", "deleted-guid-1"),
    )
    cursor.execute(
        "INSERT INTO moz_deleted_formhistory (id, timeDeleted, guid) VALUES (?, ?, ?)",
        (1, deleted_prtime, "deleted-guid-1"),
    )
    cursor.execute(
        "INSERT INTO moz_sources (id, source_name, source_url) VALUES (?, ?, ?)",
        (55, "Profile Import", "https://example.test/import"),
    )
    cursor.execute(
        "INSERT INTO moz_history_to_sources (history_id, source_id) VALUES (?, ?)",
        (10, 55),
    )
    conn.commit()
    conn.close()

    with sqlite3.connect(str(db_path)) as read_conn:
        read_conn.row_factory = sqlite3.Row
        records = parse_moz_deleted_formhistory(
            read_conn,
            "firefox",
            {"profile": "default", "logical_path": "Users/test/formhistory.sqlite"},
            "run_deleted",
            "firefox_autofill:2.5.0:run_deleted",
        )

    assert len(records) == 1
    row = records[0]
    assert row["guid"] == "deleted-guid-1"
    assert row["original_fieldname"] == "email"
    assert row["original_value"] == "person@example.com"
    assert row["time_deleted_utc"] == prtime_to_datetime(deleted_prtime).isoformat()
    assert "correlated:moz_formhistory" in (row.get("notes") or "")
    assert "Profile Import" in (row.get("notes") or "")


# ===========================================================================
# Metadata Tests
# ===========================================================================

class TestFirefoxAutofillExtractorMetadata:
    """Test extractor metadata properties."""

    def test_metadata_name(self, extractor):
        """Test metadata name is correct."""
        assert extractor.metadata.name == "firefox_autofill"

    def test_metadata_display_name(self, extractor):
        """Test display name is descriptive."""
        assert "Firefox" in extractor.metadata.display_name
        assert "Autofill" in extractor.metadata.display_name or "Form" in extractor.metadata.display_name

    def test_metadata_category(self, extractor):
        """Test category is browser."""
        assert extractor.metadata.category == "browser"

    def test_metadata_version(self, extractor):
        """Test version format."""
        assert extractor.metadata.version
        assert "." in extractor.metadata.version

    def test_metadata_can_extract(self, extractor):
        """Test extraction capability."""
        assert extractor.metadata.can_extract is True

    def test_metadata_can_ingest(self, extractor):
        """Test ingestion capability."""
        assert extractor.metadata.can_ingest is True


# ===========================================================================
# Browser Pattern Tests
# ===========================================================================

class TestFirefoxAutofillPatterns:
    """Test browser-specific pattern matching."""

    def test_supported_browsers(self, extractor):
        """Test that Firefox browsers are supported."""
        supported = extractor.SUPPORTED_BROWSERS
        assert "firefox" in supported

    def test_no_chromium_support(self, extractor):
        """Test that Chromium browsers are NOT supported (use ChromiumAutofillExtractor)."""
        supported = extractor.SUPPORTED_BROWSERS
        assert "chrome" not in supported
        assert "edge" not in supported
        assert "brave" not in supported

    def test_formhistory_patterns_exist(self):
        """Test that formhistory patterns are defined."""
        # Firefox uses 'autofill' artifact which includes formhistory.sqlite
        patterns = get_artifact_patterns("firefox", "autofill")
        assert patterns, "autofill patterns should exist for Firefox"
        # Should include formhistory.sqlite pattern
        formhistory_found = any("formhistory" in str(p).lower() for p in patterns)
        assert formhistory_found, "formhistory.sqlite should be in autofill patterns"

    def test_logins_patterns_exist(self):
        """Test that logins.json patterns are defined."""
        # Firefox uses 'autofill' artifact which includes logins.json
        patterns = get_artifact_patterns("firefox", "autofill")
        assert patterns, "autofill patterns should exist"
        # Should include logins.json pattern
        logins_found = any("logins.json" in str(p) for p in patterns)
        assert logins_found, "logins.json should be in autofill patterns"


# ===========================================================================
# Extraction Tests
# ===========================================================================

class TestFirefoxAutofillExtraction:
    """Test extraction functionality."""

    def test_can_run_extraction_requires_evidence(self, extractor):
        """Test can_run_extraction returns False without evidence."""
        can_run, reason = extractor.can_run_extraction(None)
        assert can_run is False
        assert "evidence" in reason.lower() or "mount" in reason.lower()

    def test_can_run_extraction_with_evidence(self, extractor):
        """Test can_run_extraction returns True with evidence."""
        mock_fs = MagicMock()
        can_run, reason = extractor.can_run_extraction(mock_fs)
        assert can_run is True

    def test_get_output_dir(self, extractor, tmp_path):
        """Test output directory generation."""
        output_dir = extractor.get_output_dir(tmp_path, "test_evidence")
        assert "firefox_autofill" in str(output_dir)


# ===========================================================================
# Ingestion Tests
# ===========================================================================

class TestFirefoxAutofillIngestion:
    """Test ingestion functionality."""

    def test_can_run_ingestion_requires_manifest(self, extractor, tmp_path):
        """Test can_run_ingestion requires manifest."""
        output_dir = tmp_path / "firefox_autofill"
        output_dir.mkdir(parents=True)
        can_run, reason = extractor.can_run_ingestion(output_dir)
        assert can_run is False
        assert "manifest" in reason.lower()

    def test_can_run_ingestion_with_manifest(self, extractor, tmp_path):
        """Test can_run_ingestion with manifest file."""
        output_dir = tmp_path / "firefox_autofill"
        output_dir.mkdir(parents=True)
        (output_dir / "manifest.json").write_text('{"extractor": "firefox_autofill"}')

        can_run, reason = extractor.can_run_ingestion(output_dir)
        assert can_run is True


# ===========================================================================
# Timestamp Conversion Tests
# ===========================================================================

class TestFirefoxTimestamps:
    """Test Firefox-specific timestamp handling."""

    def test_prtime_conversion(self):
        """Test PRTime (microseconds) to datetime conversion."""
        # 1640000000000000 microseconds = 2021-12-20T06:13:20
        dt = prtime_to_datetime(1640000000000000)
        assert dt is not None
        assert dt.year == 2021
        assert dt.month == 12

    def test_unix_milliseconds_conversion(self):
        """Test Unix milliseconds to datetime conversion."""
        # 1640000000000 milliseconds = 2021-12-20T06:13:20
        dt = unix_milliseconds_to_datetime(1640000000000)
        assert dt is not None
        assert dt.year == 2021
        assert dt.month == 12

    def test_zero_timestamp(self):
        """Test zero timestamp handling."""
        dt = prtime_to_datetime(0)
        # Should either return None or epoch
        assert dt is None or dt.year == 1970


# ===========================================================================
# Statistics Collector Integration Tests
# ===========================================================================

class TestFirefoxAutofillStatistics:
    """Test StatisticsCollector integration."""

    def test_statistics_collector_used_on_extract(self, extractor, mock_callbacks, tmp_path):
        """Test that extraction triggers statistics collection."""
        mock_fs = MagicMock()
        mock_fs.iter_paths.return_value = []

        output_dir = tmp_path / "firefox_autofill"
        output_dir.mkdir(parents=True)

        with patch("extractors.browser.firefox.autofill.extractor.StatisticsCollector") as mock_stats:
            mock_instance = MagicMock()
            mock_stats.instance.return_value = mock_instance

            config = {"evidence_id": 1, "selected_browsers": ["firefox"]}

            # Run extraction (will find nothing but should still track)
            try:
                extractor.run_extraction(mock_fs, output_dir, config, mock_callbacks)
            except Exception:
                pass  # May fail due to mocked FS, but we just want to check stats

            # Verify statistics were recorded
            assert mock_stats.instance.called

    def test_extractor_has_statistics_imports(self):
        """Test that extractor module imports StatisticsCollector."""
        import extractors.browser.firefox.autofill.extractor as extractor_module
        assert hasattr(extractor_module, 'StatisticsCollector')


# ===========================================================================
# Cross-Family Exclusivity Tests
# ===========================================================================

class TestBrowserFamilyExclusivity:
    """Test that browser families are properly separated."""

    def test_chromium_extractor_different_from_firefox(self):
        """Test Chromium and Firefox autofill extractors are distinct."""
        from extractors.browser.chromium.autofill import ChromiumAutofillExtractor
        from extractors.browser.firefox.autofill import FirefoxAutofillExtractor

        chromium = ChromiumAutofillExtractor()
        firefox = FirefoxAutofillExtractor()

        assert chromium.metadata.name != firefox.metadata.name
        assert "chromium" in chromium.metadata.name.lower()
        assert "firefox" in firefox.metadata.name.lower()

    def test_no_browser_overlap(self):
        """Test that browser lists don't overlap."""
        from extractors.browser.chromium.autofill import ChromiumAutofillExtractor
        from extractors.browser.firefox.autofill import FirefoxAutofillExtractor

        chromium = ChromiumAutofillExtractor()
        firefox = FirefoxAutofillExtractor()

        chromium_browsers = set(chromium.SUPPORTED_BROWSERS)
        firefox_browsers = set(firefox.SUPPORTED_BROWSERS)

        # Should have no overlap
        overlap = chromium_browsers & firefox_browsers
        assert len(overlap) == 0, f"Browser overlap detected: {overlap}"


# ===========================================================================
# Legacy Support Tests (signons.sqlite, key3.db)
# ===========================================================================

class TestLegacyFirefoxSupport:
    """Test legacy Firefox artifact support."""

    def test_key3_patterns_exist(self):
        """Test that key3.db patterns are defined for legacy NSS key store."""
        patterns = get_artifact_patterns("firefox", "autofill")
        key3_found = any("key3.db" in str(p) for p in patterns)
        assert key3_found, "key3.db should be in autofill patterns for legacy support"

    def test_key4_patterns_exist(self):
        """Test that key4.db patterns are defined for modern NSS key store."""
        patterns = get_artifact_patterns("firefox", "autofill")
        key4_found = any("key4.db" in str(p) for p in patterns)
        assert key4_found, "key4.db should be in autofill patterns"

    def test_signons_patterns_exist(self):
        """Test that signons.sqlite patterns are defined for legacy credentials."""
        patterns = get_artifact_patterns("firefox", "autofill")
        signons_found = any("signons.sqlite" in str(p) for p in patterns)
        assert signons_found, "signons.sqlite should be in autofill patterns for legacy support"

    def test_classify_key3_file(self, extractor):
        """Test key3.db is classified correctly."""
        file_type = classify_autofill_file("/path/to/profile/key3.db")
        assert file_type == "key3"

    def test_classify_key4_file(self, extractor):
        """Test key4.db is classified correctly."""
        file_type = classify_autofill_file("/path/to/profile/key4.db")
        assert file_type == "key4"

    def test_classify_signons_file(self, extractor):
        """Test signons.sqlite is classified correctly."""
        file_type = classify_autofill_file("/path/to/profile/signons.sqlite")
        assert file_type == "signons"


# ===========================================================================
# Signons.sqlite Parser Tests
# ===========================================================================

class TestSignonsSqliteParser:
    """Test legacy signons.sqlite parsing."""

    def test_parse_signons_sqlite(self, extractor, temp_signons_sqlite, tmp_path):
        """Test parsing of legacy signons.sqlite database."""
        # Create mock file entry
        file_entry = {
            "browser": "firefox",
            "profile": "test_profile",
            "file_type": "signons",
            "logical_path": str(temp_signons_sqlite),
        }

        # Create mock database connection
        evidence_db_path = tmp_path / "test_evidence.sqlite"
        import sqlite3 as sqlite
        evidence_conn = sqlite.connect(str(evidence_db_path))

        # Create credentials table (includes  columns: is_insecure, is_breached, password_notes)
        evidence_conn.execute("""
            CREATE TABLE credentials (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL,
                browser TEXT NOT NULL,
                profile TEXT,
                origin_url TEXT NOT NULL,
                action_url TEXT,
                username_element TEXT,
                username_value TEXT,
                password_element TEXT,
                password_value_encrypted BLOB,
                signon_realm TEXT,
                date_created_utc TEXT,
                date_last_used_utc TEXT,
                date_password_modified_utc TEXT,
                times_used INTEGER,
                blacklisted_by_user INTEGER DEFAULT 0,
                is_insecure INTEGER DEFAULT 0,
                is_breached INTEGER DEFAULT 0,
                password_notes TEXT,
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
        evidence_conn.commit()

        # Create mock callbacks
        class MockCallbacks:
            def on_log(self, msg, level): pass
            def is_cancelled(self): return False

        # Parse the signons.sqlite
        count = extractor._parse_signons_db(
            temp_signons_sqlite,
            "firefox",
            file_entry,
            "test_run_id",
            "firefox_autofill:2.5.0:test_run_id",
            1,
            evidence_conn,
            MockCallbacks()
        )

        assert count == 1, "Should parse 1 credential from signons.sqlite"

        # Verify data was inserted
        cursor = evidence_conn.execute("SELECT * FROM credentials")
        row = cursor.fetchone()
        assert row is not None

        evidence_conn.close()

    def test_signons_sqlite_notes_include_legacy_marker(self, extractor, temp_signons_sqlite, tmp_path):
        """Test that signons.sqlite records are marked as legacy."""
        file_entry = {
            "browser": "firefox",
            "profile": "test_profile",
            "file_type": "signons",
            "logical_path": str(temp_signons_sqlite),
        }

        evidence_db_path = tmp_path / "test_evidence.sqlite"
        import sqlite3 as sqlite
        evidence_conn = sqlite.connect(str(evidence_db_path))

        evidence_conn.execute("""
            CREATE TABLE credentials (
                id INTEGER PRIMARY KEY, evidence_id INTEGER, browser TEXT,
                profile TEXT, origin_url TEXT, action_url TEXT, username_element TEXT,
                username_value TEXT, password_element TEXT, password_value_encrypted BLOB,
                signon_realm TEXT, date_created_utc TEXT, date_last_used_utc TEXT,
                date_password_modified_utc TEXT, times_used INTEGER,
                blacklisted_by_user INTEGER DEFAULT 0,
                is_insecure INTEGER DEFAULT 0, is_breached INTEGER DEFAULT 0, password_notes TEXT,
                run_id TEXT, source_path TEXT,
                discovered_by TEXT, partition_index INTEGER, fs_type TEXT,
                logical_path TEXT, forensic_path TEXT, tags TEXT, notes TEXT,
                created_at_utc TEXT
            )
        """)
        evidence_conn.commit()

        class MockCallbacks:
            def on_log(self, msg, level): pass
            def is_cancelled(self): return False

        extractor._parse_signons_db(
            temp_signons_sqlite, "firefox", file_entry, "run_id",
            "firefox_autofill:2.5.0:run_id", 1, evidence_conn, MockCallbacks()
        )

        cursor = evidence_conn.execute("SELECT notes FROM credentials")
        row = cursor.fetchone()
        assert row is not None
        assert "legacy" in row[0].lower(), "Notes should indicate legacy source"
        assert "signons.sqlite" in row[0], "Notes should mention signons.sqlite"

        evidence_conn.close()


# ===========================================================================
# Enhanced Field Extraction Tests
# ===========================================================================

class TestEnhancedFieldExtraction:
    """Test enhanced field extraction for logins.json and formhistory."""

    def test_logins_json_extracts_form_fields(self, extractor, temp_logins_json, tmp_path):
        """Test that usernameField and passwordField are extracted."""
        file_entry = {
            "browser": "firefox",
            "profile": "test_profile",
            "file_type": "logins_json",
            "logical_path": str(temp_logins_json),
        }

        evidence_db_path = tmp_path / "test_evidence.sqlite"
        import sqlite3 as sqlite
        evidence_conn = sqlite.connect(str(evidence_db_path))

        evidence_conn.execute("""
            CREATE TABLE credentials (
                id INTEGER PRIMARY KEY, evidence_id INTEGER, browser TEXT,
                profile TEXT, origin_url TEXT, action_url TEXT, username_element TEXT,
                username_value TEXT, password_element TEXT, password_value_encrypted BLOB,
                signon_realm TEXT, date_created_utc TEXT, date_last_used_utc TEXT,
                date_password_modified_utc TEXT, times_used INTEGER,
                blacklisted_by_user INTEGER DEFAULT 0,
                is_insecure INTEGER DEFAULT 0, is_breached INTEGER DEFAULT 0, password_notes TEXT,
                run_id TEXT, source_path TEXT,
                discovered_by TEXT, partition_index INTEGER, fs_type TEXT,
                logical_path TEXT, forensic_path TEXT, tags TEXT, notes TEXT,
                created_at_utc TEXT
            )
        """)
        evidence_conn.commit()

        class MockCallbacks:
            def on_log(self, msg, level): pass
            def is_cancelled(self): return False

        extractor._parse_logins_json_file(
            temp_logins_json, "firefox", file_entry, "run_id",
            "firefox_autofill:2.5.0:run_id", 1, evidence_conn, MockCallbacks()
        )

        cursor = evidence_conn.execute("SELECT username_element, password_element FROM credentials")
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == "username", "username_element should be extracted"
        assert row[1] == "password", "password_element should be extracted"

        evidence_conn.close()

    def test_logins_json_extracts_guid_and_sync_metadata(self, extractor, temp_logins_json, tmp_path):
        """Test that guid, encType, and sync metadata are in notes."""
        file_entry = {
            "browser": "firefox",
            "profile": "test_profile",
            "file_type": "logins_json",
            "logical_path": str(temp_logins_json),
        }

        evidence_db_path = tmp_path / "test_evidence.sqlite"
        import sqlite3 as sqlite
        evidence_conn = sqlite.connect(str(evidence_db_path))

        evidence_conn.execute("""
            CREATE TABLE credentials (
                id INTEGER PRIMARY KEY, evidence_id INTEGER, browser TEXT,
                profile TEXT, origin_url TEXT, action_url TEXT, username_element TEXT,
                username_value TEXT, password_element TEXT, password_value_encrypted BLOB,
                signon_realm TEXT, date_created_utc TEXT, date_last_used_utc TEXT,
                date_password_modified_utc TEXT, times_used INTEGER,
                blacklisted_by_user INTEGER DEFAULT 0,
                is_insecure INTEGER DEFAULT 0, is_breached INTEGER DEFAULT 0, password_notes TEXT,
                run_id TEXT, source_path TEXT,
                discovered_by TEXT, partition_index INTEGER, fs_type TEXT,
                logical_path TEXT, forensic_path TEXT, tags TEXT, notes TEXT,
                created_at_utc TEXT
            )
        """)
        evidence_conn.commit()

        class MockCallbacks:
            def on_log(self, msg, level): pass
            def is_cancelled(self): return False

        extractor._parse_logins_json_file(
            temp_logins_json, "firefox", file_entry, "run_id",
            "firefox_autofill:2.5.0:run_id", 1, evidence_conn, MockCallbacks()
        )

        cursor = evidence_conn.execute("SELECT notes FROM credentials")
        row = cursor.fetchone()
        assert row is not None
        notes = row[0]
        assert "guid:" in notes, "Notes should include guid"
        assert "encType:" in notes, "Notes should include encType"
        assert "syncCounter:" in notes, "Notes should include syncCounter"
        assert "everSynced:" in notes, "Notes should include everSynced"

        evidence_conn.close()

    def test_formhistory_extracts_guid(self, extractor, temp_formhistory_db, tmp_path):
        """Test that guid is extracted from formhistory.sqlite."""
        file_entry = {
            "browser": "firefox",
            "profile": "test_profile",
            "file_type": "formhistory",
            "logical_path": str(temp_formhistory_db),
        }

        evidence_db_path = tmp_path / "test_evidence.sqlite"
        import sqlite3 as sqlite
        evidence_conn = sqlite.connect(str(evidence_db_path))

        evidence_conn.execute("""
            CREATE TABLE autofill (
                id INTEGER PRIMARY KEY, evidence_id INTEGER, browser TEXT,
                profile TEXT, name TEXT, value TEXT, date_created_utc TEXT,
                date_last_used_utc TEXT, count INTEGER,
                field_id_hash TEXT, is_deleted INTEGER DEFAULT 0,
                run_id TEXT,
                source_path TEXT, discovered_by TEXT, partition_index INTEGER,
                fs_type TEXT, logical_path TEXT, forensic_path TEXT,
                tags TEXT, notes TEXT, created_at_utc TEXT
            )
        """)
        evidence_conn.commit()

        class MockCallbacks:
            def on_log(self, msg, level): pass
            def is_cancelled(self): return False

        extractor._parse_formhistory_db(
            temp_formhistory_db, "firefox", file_entry, "run_id",
            "firefox_autofill:2.5.0:run_id", 1, evidence_conn, MockCallbacks()
        )

        cursor = evidence_conn.execute("SELECT notes FROM autofill WHERE notes IS NOT NULL")
        rows = cursor.fetchall()
        assert len(rows) > 0, "Should have records with notes"

        # Check that at least one has guid
        has_guid = any("guid:" in (row[0] or "") for row in rows)
        assert has_guid, "At least one record should have guid in notes"

        evidence_conn.close()
