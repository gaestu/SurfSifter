"""Tests for the embedded_artifacts extractor — debug.log parser and ingestion."""

from __future__ import annotations

import io
import json
import sqlite3
import textwrap
from pathlib import Path
from typing import List

import pytest

from extractors.browser.chromium.embedded_artifacts._debuglog import (
    DebugLogEntry,
    _iter_raw_entries,
    extract_urls,
    parse_debuglog,
    parse_entry,
)


# ──────────────────────────────────────────────────────────────────────
# _debuglog.py — line-level parser tests
# ──────────────────────────────────────────────────────────────────────


class TestParseEntry:
    """Unit tests for parse_entry()."""

    def test_old_format_console(self):
        text = (
            '[0607/155418:INFO:CONSOLE(3)] "<-m->", '
            "source: https://example.net/games/mobile/menu.php? (3)"
        )
        entry = parse_entry(1, text)
        assert entry is not None
        assert entry.date_code == "0607"
        assert entry.time_code == "155418"
        assert entry.severity == "INFO"
        assert entry.source_location == "CONSOLE(3)"
        assert entry.is_console
        assert entry.console_source_url == "https://example.net/games/mobile/menu.php?"
        assert entry.console_source_line == 3
        assert entry.pid is None
        assert entry.tid is None
        assert entry.milliseconds is None

    def test_new_format_with_pid_tid_ms(self):
        text = "[0101/120000.456:1234:5678:WARNING:gpu_process.cc(99)] GPU init failed"
        entry = parse_entry(10, text)
        assert entry is not None
        assert entry.date_code == "0101"
        assert entry.time_code == "120000"
        assert entry.milliseconds == "456"
        assert entry.pid == 1234
        assert entry.tid == 5678
        assert entry.severity == "WARNING"
        assert entry.source_location == "gpu_process.cc(99)"
        assert not entry.is_console
        assert entry.console_source_url is None

    def test_warning_no_url(self):
        text = "[0607/155818:WARNING:audio_sync_reader.cc(112)] AudioSyncReader::Read timed out, audio glitch count=1"
        entry = parse_entry(1, text)
        assert entry is not None
        assert entry.severity == "WARNING"
        assert entry.console_source_url is None
        assert entry.message_urls == []

    def test_error_with_file_path_no_url(self):
        text = r"[0607/160713:ERROR:cache_util.cc(134)] Unable to move cache folder C:\Application\cache to C:\Application\old_cache_000"
        entry = parse_entry(1, text)
        assert entry is not None
        assert entry.severity == "ERROR"
        assert entry.message_urls == []  # Windows paths are not HTTP URLs

    def test_message_body_url_extraction(self):
        text = '[0101/120000:INFO:CONSOLE(1)] "Loading https://example.com/api/data from cache", source: https://example.com/main.js (1)'
        entry = parse_entry(1, text)
        assert entry is not None
        assert entry.console_source_url == "https://example.com/main.js"
        # The message body URL should be in message_urls (not duplicated from console_source_url)
        assert "https://example.com/api/data" in entry.message_urls
        assert "https://example.com/main.js" not in entry.message_urls

    def test_invalid_line(self):
        assert parse_entry(1, "not a log line") is None

    def test_empty_message(self):
        text = "[0101/120000:INFO:test.cc(1)]"
        entry = parse_entry(1, text)
        assert entry is not None
        assert entry.message == ""

    def test_url_cleaning_trailing_punctuation(self):
        text = '[0101/120000:INFO:CONSOLE(1)] "See https://example.com/path.", source: https://other.com/file.js (1)'
        entry = parse_entry(1, text)
        assert entry is not None
        # Trailing dot should be stripped from message URL
        assert "https://example.com/path" in entry.message_urls


# ──────────────────────────────────────────────────────────────────────
# _debuglog.py — multi-line entry accumulation
# ──────────────────────────────────────────────────────────────────────


class TestIterRawEntries:
    """Tests for multi-line entry accumulation."""

    def test_single_line_entries(self):
        lines = [
            "[0101/120000:INFO:a.cc(1)] first\n",
            "[0102/130000:WARNING:b.cc(2)] second\n",
        ]
        entries = list(_iter_raw_entries(iter(lines)))
        assert len(entries) == 2
        assert entries[0] == (1, "[0101/120000:INFO:a.cc(1)] first")
        assert entries[1] == (2, "[0102/130000:WARNING:b.cc(2)] second")

    def test_multi_line_entry(self):
        lines = [
            "[0101/120000:INFO:CONSOLE(1)] multi-line\n",
            "continuation line\n",
            "more continuation\n",
            "[0102/130000:INFO:a.cc(1)] next entry\n",
        ]
        entries = list(_iter_raw_entries(iter(lines)))
        assert len(entries) == 2
        assert "continuation line" in entries[0][1]
        assert "more continuation" in entries[0][1]
        assert entries[0][0] == 1  # line number of first entry
        assert entries[1][0] == 4  # line number of second entry

    def test_leading_text_before_first_header(self):
        lines = [
            "some garbage before logs\n",
            "[0101/120000:INFO:a.cc(1)] real entry\n",
        ]
        entries = list(_iter_raw_entries(iter(lines)))
        assert len(entries) == 1
        assert entries[0][0] == 2

    def test_empty_input(self):
        assert list(_iter_raw_entries(iter([]))) == []


# ──────────────────────────────────────────────────────────────────────
# _debuglog.py — file-level parsing
# ──────────────────────────────────────────────────────────────────────


class TestParseDebuglog:
    """Tests for parse_debuglog() with streams and files."""

    SAMPLE_LOG = textwrap.dedent("""\
        [0607/155418:INFO:CONSOLE(3)] "<-m->", source: https://example.net/games/mobile/menu.php? (3)
        [0607/155421:INFO:CONSOLE(16)] "
         %c PixiJS 4.8.1
        ", source: https://example.net/lobby/src/pixi.min.js (16)
        [0607/155818:WARNING:audio_sync_reader.cc(112)] AudioSyncReader::Read timed out
        [0607/160712:ERROR:backend_impl.cc(1037)] Critical error found -8
        [0609/143926:ERROR:cache_util.cc(134)] Unable to move cache folder
        [0610/100000:INFO:CONSOLE(1)] "fetch", source: https://example.com/api (1)
    """)

    def test_stream_parsing(self):
        stream = io.StringIO(self.SAMPLE_LOG)
        entries = parse_debuglog(stream)
        assert len(entries) == 6
        # First entry is a CONSOLE line
        assert entries[0].is_console
        assert entries[0].console_source_url == "https://example.net/games/mobile/menu.php?"
        # Second entry is a multi-line CONSOLE line
        assert entries[1].is_console
        assert entries[1].console_source_url == "https://example.net/lobby/src/pixi.min.js"
        # Third entry is a WARNING
        assert entries[2].severity == "WARNING"
        assert not entries[2].is_console

    def test_file_parsing(self, tmp_path):
        log_file = tmp_path / "debug.log"
        log_file.write_text(self.SAMPLE_LOG)
        entries = parse_debuglog(log_file)
        assert len(entries) == 6

    def test_file_parsing_str_path(self, tmp_path):
        log_file = tmp_path / "debug.log"
        log_file.write_text(self.SAMPLE_LOG)
        entries = parse_debuglog(str(log_file))
        assert len(entries) == 6


# ──────────────────────────────────────────────────────────────────────
# _debuglog.py — URL extraction
# ──────────────────────────────────────────────────────────────────────


class TestExtractUrls:
    """Tests for extract_urls() deduplication and metadata."""

    def _make_entries(self, texts: List[str]) -> List[DebugLogEntry]:
        """Helper: parse multiple raw log lines into entries."""
        entries = []
        for i, text in enumerate(texts, 1):
            e = parse_entry(i, text)
            if e is not None:
                entries.append(e)
        return entries

    def test_deduplication(self):
        entries = self._make_entries([
            '[0101/100000:INFO:CONSOLE(1)] "a", source: https://a.com/page.js (1)',
            '[0101/110000:INFO:CONSOLE(1)] "b", source: https://a.com/page.js (1)',
            '[0101/120000:INFO:CONSOLE(1)] "c", source: https://b.com/other.js (1)',
        ])
        url_records = extract_urls(entries)
        assert len(url_records) == 2
        urls = {r["url"] for r in url_records}
        assert urls == {"https://a.com/page.js", "https://b.com/other.js"}

    def test_occurrence_count(self):
        entries = self._make_entries([
            '[0101/100000:INFO:CONSOLE(1)] "a", source: https://repeat.com/x.js (1)',
            '[0101/110000:INFO:CONSOLE(2)] "b", source: https://repeat.com/x.js (2)',
            '[0101/120000:INFO:CONSOLE(3)] "c", source: https://repeat.com/x.js (3)',
        ])
        url_records = extract_urls(entries)
        assert len(url_records) == 1
        assert url_records[0]["occurrence_count"] == 3

    def test_first_last_seen(self):
        entries = self._make_entries([
            '[0601/100000:INFO:CONSOLE(1)] "a", source: https://t.com/script.js (1)',
            '[0815/200000:INFO:CONSOLE(1)] "b", source: https://t.com/script.js (1)',
        ])
        url_records = extract_urls(entries)
        assert len(url_records) == 1
        assert url_records[0]["first_seen"] == "0601/100000"
        assert url_records[0]["last_seen"] == "0815/200000"

    def test_message_body_urls(self):
        entries = self._make_entries([
            '[0101/100000:INFO:nav.cc(1)] Navigating to https://nav-target.com/page',
        ])
        url_records = extract_urls(entries)
        assert len(url_records) == 1
        assert url_records[0]["url"] == "https://nav-target.com/page"
        assert url_records[0]["source_context"] == "message_body"

    def test_no_entries_returns_empty(self):
        assert extract_urls([]) == []

    def test_console_and_body_urls_separate(self):
        entries = self._make_entries([
            '[0101/100000:INFO:CONSOLE(1)] "Loading https://cdn.example.com/lib.js", source: https://app.example.com/index.js (1)',
        ])
        url_records = extract_urls(entries)
        urls = {r["url"] for r in url_records}
        assert "https://app.example.com/index.js" in urls
        assert "https://cdn.example.com/lib.js" in urls


# ──────────────────────────────────────────────────────────────────────
# Integration — real Application debug.log (if available)
# ──────────────────────────────────────────────────────────────────────


Application_LOG = Path(__file__).resolve().parents[4] / "test_cases" / "Application" / "debug.log"


@pytest.mark.skipif(not Application_LOG.exists(), reason="Application debug.log not available")
class TestRealApplicationDebugLog:
    """Smoke tests against the real Application debug.log in test_cases/."""

    def test_parse_all_entries(self):
        entries = parse_debuglog(Application_LOG)
        # We expect ~10,000+ entries.
        assert len(entries) > 5000
        # All entries should have valid severity.
        severities = {e.severity for e in entries}
        assert severities <= {"INFO", "WARNING", "ERROR", "FATAL", "VERBOSE"}

    def test_extract_unique_urls(self):
        entries = parse_debuglog(Application_LOG)
        url_records = extract_urls(entries)
        # The Application log has 213 unique source URLs — we may also get message-body URLs.
        assert len(url_records) >= 200
        urls = {r["url"] for r in url_records}
        # Known URLs from the Application log
        assert any("example.net" in u for u in urls)

    def test_console_sources_found(self):
        entries = parse_debuglog(Application_LOG)
        console_entries = [e for e in entries if e.is_console and e.console_source_url]
        assert len(console_entries) > 1000


# ──────────────────────────────────────────────────────────────────────
# Extractor — metadata and registration
# ──────────────────────────────────────────────────────────────────────


class TestExtractorMetadata:
    """Test the ChromiumEmbeddedArtifactsExtractor metadata and basic checks."""

    def test_metadata(self):
        from extractors.browser.chromium.embedded_artifacts import ChromiumEmbeddedArtifactsExtractor

        ext = ChromiumEmbeddedArtifactsExtractor()
        meta = ext.metadata
        assert meta.name == "chromium_embedded_artifacts"
        assert meta.category == "browser"
        assert meta.can_extract is True
        assert meta.can_ingest is True

    def test_can_run_extraction_no_fs(self):
        from extractors.browser.chromium.embedded_artifacts import ChromiumEmbeddedArtifactsExtractor

        ext = ChromiumEmbeddedArtifactsExtractor()
        ok, reason = ext.can_run_extraction(None)
        assert ok is False

    def test_can_run_extraction_with_fs(self):
        from extractors.browser.chromium.embedded_artifacts import ChromiumEmbeddedArtifactsExtractor

        ext = ChromiumEmbeddedArtifactsExtractor()
        ok, _ = ext.can_run_extraction(object())  # any non-None value
        assert ok is True

    def test_can_run_ingestion_no_manifest(self, tmp_path):
        from extractors.browser.chromium.embedded_artifacts import ChromiumEmbeddedArtifactsExtractor

        ext = ChromiumEmbeddedArtifactsExtractor()
        ok, reason = ext.can_run_ingestion(tmp_path)
        assert ok is False
        assert "manifest" in reason.lower()

    def test_can_run_ingestion_with_manifest(self, tmp_path):
        from extractors.browser.chromium.embedded_artifacts import ChromiumEmbeddedArtifactsExtractor

        (tmp_path / "manifest.json").write_text("{}")
        ext = ChromiumEmbeddedArtifactsExtractor()
        ok, _ = ext.can_run_ingestion(tmp_path)
        assert ok is True

    def test_output_dir(self, tmp_path):
        from extractors.browser.chromium.embedded_artifacts import ChromiumEmbeddedArtifactsExtractor

        ext = ChromiumEmbeddedArtifactsExtractor()
        result = ext.get_output_dir(tmp_path, "my-evidence")
        assert result == tmp_path / "evidences" / "my-evidence" / "chromium_embedded_artifacts"


# ──────────────────────────────────────────────────────────────────────
# Extractor — ingestion integration test
# ──────────────────────────────────────────────────────────────────────


class _StubCallbacks:
    """Minimal ExtractorCallbacks stub for testing."""

    def __init__(self):
        self.logs: List[str] = []
        self.steps: List[str] = []

    def on_progress(self, current: int, total: int, message: str = "") -> None:
        pass

    def on_log(self, message: str, level: str = "info") -> None:
        self.logs.append(f"[{level}] {message}")

    def on_error(self, error: str, details: str = "") -> None:
        self.logs.append(f"[ERROR] {error}: {details}")

    def on_step(self, step_name: str) -> None:
        self.steps.append(step_name)

    def is_cancelled(self) -> bool:
        return False


def _make_evidence_db(tmp_path: Path) -> sqlite3.Connection:
    """Create a minimal evidence database with urls and process_log tables."""
    db_path = tmp_path / "evidence.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            url TEXT NOT NULL,
            domain TEXT,
            scheme TEXT,
            discovered_by TEXT NOT NULL,
            first_seen_utc TEXT,
            last_seen_utc TEXT,
            source_path TEXT,
            tags TEXT,
            notes TEXT,
            context TEXT,
            run_id TEXT,
            cache_key TEXT,
            cache_filename TEXT,
            response_code INTEGER,
            content_type TEXT,
            file_extension TEXT,
            file_type TEXT,
            occurrence_count INTEGER,
            partition_index INTEGER,
            fs_type TEXT,
            logical_path TEXT,
            forensic_path TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE process_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            tool_name TEXT,
            command_line TEXT,
            started_at TEXT,
            finished_at TEXT,
            exit_code INTEGER,
            output_path TEXT,
            run_id TEXT,
            extractor_version TEXT,
            record_count INTEGER,
            metadata TEXT
        )
        """
    )
    conn.commit()
    return conn


class TestIngestion:
    """Test the ingestion phase with a synthetic manifest + debug.log."""

    SAMPLE_LOG = textwrap.dedent("""\
        [0607/155418:INFO:CONSOLE(3)] "<-m->", source: https://example.net/games/mobile/menu.php? (3)
        [0607/155421:INFO:CONSOLE(16)] "loaded", source: https://example.net/lobby/src/pixi.min.js (16)
        [0607/155818:WARNING:audio_sync_reader.cc(112)] AudioSyncReader::Read timed out
        [0609/143926:ERROR:cache_util.cc(134)] Unable to move cache folder
        [0610/100000:INFO:CONSOLE(1)] "fetch https://api.example.com/data", source: https://example.com/app.js (1)
    """)

    @pytest.fixture()
    def setup_ingestion(self, tmp_path):
        """Set up manifest, extracted debug.log, and evidence DB."""
        from extractors.browser.chromium.embedded_artifacts import ChromiumEmbeddedArtifactsExtractor

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        log_file = output_dir / "debug_p0_abcd1234.log"
        log_file.write_text(self.SAMPLE_LOG)

        manifest = {
            "extractor": "chromium_embedded_artifacts",
            "version": "1.0.0",
            "run_id": "emb_artifacts_test_12345678",
            "evidence_id": 1,
            "extraction_timestamp_utc": "2024-01-01T00:00:00+00:00",
            "files": [
                {
                    "logical_path": "Application/debug.log",
                    "extracted_path": str(log_file),
                    "filename": "debug_p0_abcd1234.log",
                    "artifact_type": "debug_log",
                    "partition_index": 0,
                    "file_size_bytes": len(self.SAMPLE_LOG.encode()),
                    "md5": "fakehash",
                    "sha256": "fakehash256",
                    "copy_status": "ok",
                }
            ],
            "status": "ok",
            "notes": [],
        }
        (output_dir / "manifest.json").write_text(json.dumps(manifest))

        conn = _make_evidence_db(tmp_path)
        ext = ChromiumEmbeddedArtifactsExtractor()
        callbacks = _StubCallbacks()

        return ext, output_dir, conn, callbacks

    def test_ingestion_inserts_urls(self, setup_ingestion):
        ext, output_dir, conn, callbacks = setup_ingestion
        result = ext.run_ingestion(output_dir, conn, 1, {}, callbacks)

        assert result["urls"] > 0
        rows = conn.execute("SELECT * FROM urls WHERE evidence_id = 1").fetchall()
        assert len(rows) > 0

        urls = {row["url"] for row in rows}
        assert "https://example.net/games/mobile/menu.php?" in urls
        assert "https://example.net/lobby/src/pixi.min.js" in urls
        assert "https://example.com/app.js" in urls

    def test_ingestion_records_domain(self, setup_ingestion):
        ext, output_dir, conn, callbacks = setup_ingestion
        ext.run_ingestion(output_dir, conn, 1, {}, callbacks)

        row = conn.execute(
            "SELECT domain FROM urls WHERE url = ?",
            ("https://example.net/games/mobile/menu.php?",),
        ).fetchone()
        assert row is not None
        assert row["domain"] == "example.net"

    def test_ingestion_discovered_by_format(self, setup_ingestion):
        ext, output_dir, conn, callbacks = setup_ingestion
        ext.run_ingestion(output_dir, conn, 1, {}, callbacks)

        row = conn.execute("SELECT discovered_by FROM urls LIMIT 1").fetchone()
        assert row is not None
        assert row["discovered_by"].startswith("embedded_debuglog:")

    def test_ingestion_writes_process_log(self, setup_ingestion):
        ext, output_dir, conn, callbacks = setup_ingestion
        ext.run_ingestion(output_dir, conn, 1, {}, callbacks)

        logs = conn.execute("SELECT * FROM process_log").fetchall()
        assert len(logs) == 1
        log = logs[0]
        assert log["tool_name"] == "chromium_embedded_artifacts"
        assert log["exit_code"] == 0
        meta = json.loads(log["metadata"])
        assert "unique_urls_ingested" in meta
        assert "log_entries_parsed" in meta

    def test_idempotent_reingestion(self, setup_ingestion):
        ext, output_dir, conn, callbacks = setup_ingestion

        result1 = ext.run_ingestion(output_dir, conn, 1, {}, callbacks)
        result2 = ext.run_ingestion(output_dir, conn, 1, {}, callbacks)

        # Should get same count both times (old records deleted first)
        assert result1["urls"] == result2["urls"]

        # Only one set of URL records should exist
        count = conn.execute(
            "SELECT COUNT(*) FROM urls WHERE evidence_id = 1"
        ).fetchone()[0]
        assert count == result1["urls"]

    def test_ingestion_with_message_body_urls(self, setup_ingestion):
        ext, output_dir, conn, callbacks = setup_ingestion
        ext.run_ingestion(output_dir, conn, 1, {}, callbacks)

        # The log has "fetch https://api.example.com/data" in a console message body
        urls = {
            row["url"]
            for row in conn.execute("SELECT url FROM urls WHERE evidence_id = 1").fetchall()
        }
        assert "https://api.example.com/data" in urls

    def test_ingestion_notes_contain_context(self, setup_ingestion):
        ext, output_dir, conn, callbacks = setup_ingestion
        ext.run_ingestion(output_dir, conn, 1, {}, callbacks)

        row = conn.execute(
            "SELECT notes FROM urls WHERE url = ?",
            ("https://example.net/games/mobile/menu.php?",),
        ).fetchone()
        assert row is not None
        assert "context=console_source" in row["notes"]
        assert "severity=INFO" in row["notes"]

    def test_ingestion_missing_file_skipped(self, tmp_path):
        """Ingestion should skip files that no longer exist and not crash."""
        from extractors.browser.chromium.embedded_artifacts import ChromiumEmbeddedArtifactsExtractor

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        manifest = {
            "extractor": "chromium_embedded_artifacts",
            "version": "1.0.0",
            "run_id": "emb_artifacts_test_missing",
            "evidence_id": 1,
            "files": [
                {
                    "logical_path": "missing/debug.log",
                    "extracted_path": str(output_dir / "nonexistent.log"),
                    "copy_status": "ok",
                }
            ],
            "status": "ok",
            "notes": [],
        }
        (output_dir / "manifest.json").write_text(json.dumps(manifest))

        conn = _make_evidence_db(tmp_path)
        ext = ChromiumEmbeddedArtifactsExtractor()
        callbacks = _StubCallbacks()
        result = ext.run_ingestion(output_dir, conn, 1, {}, callbacks)
        assert result["urls"] == 0


# ──────────────────────────────────────────────────────────────────────
# Extractor registry discovery
# ──────────────────────────────────────────────────────────────────────


class TestRegistryDiscovery:
    """Verify the extractor is auto-discovered by the registry."""

    def test_registry_finds_embedded_artifacts(self):
        from extractors.extractor_registry import ExtractorRegistry

        registry = ExtractorRegistry()
        names = [ext.metadata.name for ext in registry.get_all()]
        assert "chromium_embedded_artifacts" in names
