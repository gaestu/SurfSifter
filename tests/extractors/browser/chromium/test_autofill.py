"""
Tests for ChromiumAutofillExtractor

Tests the browser-specific Chromium autofill extractor:
- Metadata and registration
- Browser pattern matching (Chrome, Edge, Brave, Opera)
- SQLite parsing for Web Data and Login Data
- Extraction and ingestion workflows
- Statistics collector integration
"""
import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, PropertyMock

import pytest

from extractors.browser.chromium.autofill import ChromiumAutofillExtractor
from extractors.browser.chromium.autofill._parsers import parse_iban_tables, parse_all_token_tables
from extractors.browser.chromium._patterns import (
    CHROMIUM_BROWSERS,
    CHROMIUM_ARTIFACTS,
    get_artifact_patterns,
)
from extractors._shared.timestamps import webkit_to_datetime


# ===========================================================================
# Test Fixtures
# ===========================================================================

@pytest.fixture
def extractor():
    """Create ChromiumAutofillExtractor instance."""
    return ChromiumAutofillExtractor()


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
def temp_web_data_db(tmp_path):
    """Create a temporary Web Data database with autofill entries."""
    db_path = tmp_path / "Web Data"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Create autofill table
    cursor.execute("""
        CREATE TABLE autofill (
            name TEXT NOT NULL,
            value TEXT NOT NULL,
            date_created INTEGER DEFAULT 0,
            date_last_used INTEGER DEFAULT 0,
            count INTEGER DEFAULT 1
        )
    """)

    # Create autofill_profiles table
    cursor.execute("""
        CREATE TABLE autofill_profiles (
            guid TEXT NOT NULL,
            company_name TEXT DEFAULT '',
            street_address TEXT DEFAULT '',
            city TEXT DEFAULT '',
            state TEXT DEFAULT '',
            zipcode TEXT DEFAULT '',
            country_code TEXT DEFAULT '',
            date_modified INTEGER DEFAULT 0
        )
    """)

    # Insert test data
    # WebKit timestamp: 13300000000000000 = 2022-01-15 approx
    cursor.execute("""
        INSERT INTO autofill (name, value, date_created, date_last_used, count)
        VALUES ('email', 'test@example.com', 13300000000000000, 13300000000000000, 5)
    """)
    cursor.execute("""
        INSERT INTO autofill (name, value, date_created, date_last_used, count)
        VALUES ('phone', '555-1234', 13300000000000000, 13300000000000000, 3)
    """)

    cursor.execute("""
        INSERT INTO autofill_profiles (guid, company_name, street_address, city, state, zipcode, country_code, date_modified)
        VALUES ('test-guid-1', 'Test Corp', '123 Main St', 'Anytown', 'CA', '12345', 'US', 13300000000000000)
    """)

    conn.commit()
    conn.close()

    return db_path


@pytest.fixture
def temp_login_data_db(tmp_path):
    """Create a temporary Login Data database with credential entries."""
    db_path = tmp_path / "Login Data"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Create logins table
    cursor.execute("""
        CREATE TABLE logins (
            origin_url TEXT NOT NULL,
            action_url TEXT,
            username_element TEXT,
            username_value TEXT,
            password_element TEXT,
            password_value BLOB,
            signon_realm TEXT NOT NULL,
            date_created INTEGER DEFAULT 0,
            date_last_used INTEGER DEFAULT 0,
            date_password_modified INTEGER DEFAULT 0,
            times_used INTEGER DEFAULT 0,
            blacklisted_by_user INTEGER DEFAULT 0
        )
    """)

    # Insert test credential
    cursor.execute("""
        INSERT INTO logins (origin_url, action_url, username_element, username_value,
                           password_element, password_value, signon_realm,
                           date_created, date_last_used, date_password_modified, times_used)
        VALUES ('https://example.com/login', 'https://example.com/auth', 'user', 'testuser',
                'pass', X'0102030405', 'https://example.com/',
                13300000000000000, 13300000000000000, 13300000000000000, 10)
    """)

    conn.commit()
    conn.close()

    return db_path


# ===========================================================================
# Parser Tests
# ===========================================================================


def test_parse_iban_tables_local_and_masked(tmp_path):
    """Parse local_ibans + masked_ibans rows from Web Data."""
    db_path = tmp_path / "Web Data"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE local_ibans (
            guid TEXT,
            instrument_id INTEGER,
            value TEXT,
            value_encrypted BLOB,
            nickname TEXT,
            prefix TEXT,
            suffix TEXT,
            length INTEGER,
            use_count INTEGER,
            use_date INTEGER,
            date_modified INTEGER
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE masked_ibans (
            instrument_id INTEGER,
            nickname TEXT,
            prefix TEXT,
            suffix TEXT,
            length INTEGER,
            use_count INTEGER,
            use_date INTEGER,
            date_modified INTEGER
        )
        """
    )

    ts = 13300000000000000
    cursor.execute(
        """
        INSERT INTO local_ibans (
            guid, instrument_id, value, value_encrypted, nickname,
            prefix, suffix, length, use_count, use_date, date_modified
        )
        VALUES (?, ?, ?, X'0102', ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "guid-local-1",
            101,
            "DE89370400440532013000",
            "Primary",
            "DE89",
            "3000",
            22,
            7,
            ts,
            ts,
        ),
    )
    cursor.execute(
        """
        INSERT INTO masked_ibans (
            instrument_id, nickname, prefix, suffix, length, use_count, use_date, date_modified
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (202, "Masked", "GB29", "1234", 22, 3, ts, ts),
    )
    conn.commit()
    conn.close()

    with sqlite3.connect(str(db_path)) as read_conn:
        read_conn.row_factory = sqlite3.Row
        records = parse_iban_tables(
            read_conn,
            "chrome",
            {
                "profile": "Default",
                "logical_path": "Users/test/Web Data",
                "partition_index": 1,
                "fs_type": "ntfs",
                "forensic_path": "/p01/Users/test/Web Data",
            },
            "run_1",
            "test:1.0.0:run_1",
        )

    assert len(records) == 2
    sources = {r["source_table"] for r in records}
    assert sources == {"local_ibans", "masked_ibans"}

    local = next(r for r in records if r["source_table"] == "local_ibans")
    masked = next(r for r in records if r["source_table"] == "masked_ibans")
    expected_ts = webkit_to_datetime(ts).isoformat()

    assert local["browser"] == "chrome"
    assert local["guid"] == "guid-local-1"
    assert local["value"] == "DE89370400440532013000"
    assert local["use_date_utc"] == expected_ts
    assert local["date_modified_utc"] == expected_ts
    assert masked["instrument_id"] == 202
    assert masked["prefix"] == "GB29"
    assert masked["use_date_utc"] == expected_ts


def test_parse_token_tables_include_parent_metadata(tmp_path):
    """Token rows should include parent table metadata (use/date/modified)."""
    db_path = tmp_path / "Web Data"
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE addresses (
            guid TEXT,
            use_count INTEGER,
            use_date INTEGER,
            date_modified INTEGER
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE address_type_tokens (
            guid TEXT,
            type INTEGER,
            value TEXT
        )
        """
    )

    ts = 13300000000000000
    cursor.execute(
        "INSERT INTO addresses (guid, use_count, use_date, date_modified) VALUES (?, ?, ?, ?)",
        ("guid-addr-1", 11, ts, ts),
    )
    cursor.execute(
        "INSERT INTO address_type_tokens (guid, type, value) VALUES (?, ?, ?)",
        ("guid-addr-1", 34, "Seattle"),
    )
    conn.commit()
    conn.close()

    with sqlite3.connect(str(db_path)) as read_conn:
        read_conn.row_factory = sqlite3.Row
        records = parse_all_token_tables(
            read_conn,
            "chrome",
            {
                "profile": "Default",
                "logical_path": "Users/test/Web Data",
            },
            "run_tokens",
            "test:1.0.0:run_tokens",
        )

    assert len(records) == 1
    record = records[0]
    expected_ts = webkit_to_datetime(ts).isoformat()

    assert record["guid"] == "guid-addr-1"
    assert record["source_table"] == "address_type_tokens"
    assert record["parent_table"] == "addresses"
    assert record["parent_use_count"] == 11
    assert record["parent_use_date_utc"] == expected_ts
    assert record["parent_date_modified_utc"] == expected_ts


# ===========================================================================
# Metadata Tests
# ===========================================================================

class TestChromiumAutofillExtractorMetadata:
    """Test extractor metadata properties."""

    def test_metadata_name(self, extractor):
        """Test metadata name is correct."""
        assert extractor.metadata.name == "chromium_autofill"

    def test_metadata_display_name(self, extractor):
        """Test display name is descriptive."""
        assert "Chromium" in extractor.metadata.display_name
        assert "Autofill" in extractor.metadata.display_name

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

class TestChromiumAutofillPatterns:
    """Test browser-specific pattern matching."""

    def test_supported_browsers(self, extractor):
        """Test that all Chromium browsers are supported."""
        supported = extractor.SUPPORTED_BROWSERS
        assert "chrome" in supported
        assert "edge" in supported
        assert "brave" in supported
        assert "opera" in supported

    def test_no_firefox_support(self, extractor):
        """Test that Firefox is NOT supported (use FirefoxAutofillExtractor)."""
        supported = extractor.SUPPORTED_BROWSERS
        assert "firefox" not in supported

    def test_autofill_patterns_exist(self):
        """Test that autofill patterns are defined."""
        # get_artifact_patterns requires browser and artifact
        patterns = get_artifact_patterns("chrome", "autofill")
        # Should have patterns for Chrome at minimum
        assert patterns, "Autofill patterns should exist for Chrome"
        # Should include Web Data pattern
        web_data_found = any("Web Data" in str(p) for p in patterns)
        assert web_data_found, "Web Data pattern should be in autofill patterns"

    def test_login_data_in_autofill_patterns(self):
        """Test that Login Data is included in autofill patterns."""
        patterns = get_artifact_patterns("chrome", "autofill")
        # Login Data should be part of autofill artifact
        login_data_found = any("Login Data" in str(p) for p in patterns)
        assert login_data_found, "Login Data should be in autofill patterns"


# ===========================================================================
# Extraction Tests
# ===========================================================================

class TestChromiumAutofillExtraction:
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
        assert "chromium_autofill" in str(output_dir)


# ===========================================================================
# Ingestion Tests
# ===========================================================================

class TestChromiumAutofillIngestion:
    """Test ingestion functionality."""

    def test_can_run_ingestion_requires_manifest(self, extractor, tmp_path):
        """Test can_run_ingestion requires manifest."""
        output_dir = tmp_path / "chromium_autofill"
        output_dir.mkdir(parents=True)
        can_run, reason = extractor.can_run_ingestion(output_dir)
        assert can_run is False
        assert "manifest" in reason.lower()

    def test_can_run_ingestion_with_manifest(self, extractor, tmp_path):
        """Test can_run_ingestion with manifest file."""
        output_dir = tmp_path / "chromium_autofill"
        output_dir.mkdir(parents=True)
        (output_dir / "manifest.json").write_text('{"extractor": "chromium_autofill"}')

        can_run, reason = extractor.can_run_ingestion(output_dir)
        assert can_run is True


# ===========================================================================
# Statistics Collector Integration Tests
# ===========================================================================

class TestChromiumAutofillStatistics:
    """Test StatisticsCollector integration."""

    def test_statistics_collector_used_on_extract(self, extractor, mock_callbacks, tmp_path):
        """Test that extraction triggers statistics collection."""
        mock_fs = MagicMock()
        mock_fs.iter_paths.return_value = []

        output_dir = tmp_path / "chromium_autofill"
        output_dir.mkdir(parents=True)

        with patch("extractors.browser.chromium.autofill.extractor.StatisticsCollector") as mock_stats:
            mock_instance = MagicMock()
            mock_stats.instance.return_value = mock_instance

            config = {"evidence_id": 1, "selected_browsers": ["chrome"]}

            # Run extraction (will find nothing but should still track)
            try:
                extractor.run_extraction(mock_fs, output_dir, config, mock_callbacks)
            except Exception:
                pass  # May fail due to mocked FS, but we just want to check stats

            # Verify statistics were recorded
            assert mock_stats.instance.called

    def test_extractor_has_statistics_imports(self):
        """Test that extractor module imports StatisticsCollector."""
        import extractors.browser.chromium.autofill.extractor as extractor_module
        assert hasattr(extractor_module, 'StatisticsCollector')
