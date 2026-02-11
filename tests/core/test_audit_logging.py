"""
Unit tests for the forensic audit logging system.

Tests cover:
- ExtractorLoggerAdapter default field handling
- EvidenceLogger file creation and rotation
- CaseLogger database writes
- AuditLogger lifecycle management
- Log format and UTC timestamps
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestExtractorLoggerAdapter:
    """Tests for ExtractorLoggerAdapter default field handling."""

    def test_default_field_provided(self):
        """Test that adapter provides default extractor field when not specified."""
        from core.audit_logging import ExtractorLoggerAdapter

        base_logger = logging.getLogger("test.adapter.default")
        base_logger.setLevel(logging.DEBUG)

        # Capture log records
        records = []
        handler = logging.Handler()
        handler.emit = lambda r: records.append(r)
        handler.setLevel(logging.DEBUG)
        base_logger.addHandler(handler)

        adapter = ExtractorLoggerAdapter(base_logger, {"extractor": "default_value"})
        adapter.info("test message")

        assert len(records) == 1
        assert records[0].extractor == "default_value"

    def test_override_default_field(self):
        """Test that extra fields can override defaults."""
        from core.audit_logging import ExtractorLoggerAdapter

        base_logger = logging.getLogger("test.adapter.override")
        base_logger.setLevel(logging.DEBUG)

        records = []
        handler = logging.Handler()
        handler.emit = lambda r: records.append(r)
        handler.setLevel(logging.DEBUG)
        base_logger.addHandler(handler)

        adapter = ExtractorLoggerAdapter(base_logger, {"extractor": "default_value"})
        adapter.info("test message", extra={"extractor": "custom_value"})

        assert len(records) == 1
        assert records[0].extractor == "custom_value"

    def test_multiple_default_fields(self):
        """Test adapter with multiple default fields."""
        from core.audit_logging import ExtractorLoggerAdapter

        base_logger = logging.getLogger("test.adapter.multi")
        base_logger.setLevel(logging.DEBUG)

        records = []
        handler = logging.Handler()
        handler.emit = lambda r: records.append(r)
        handler.setLevel(logging.DEBUG)
        base_logger.addHandler(handler)

        adapter = ExtractorLoggerAdapter(base_logger, {
            "extractor": "test_extractor",
            "evidence_id": 42
        })
        adapter.info("test message")

        assert len(records) == 1
        assert records[0].extractor == "test_extractor"
        assert records[0].evidence_id == 42


class TestEvidenceLogger:
    """Tests for EvidenceLogger file and database operations."""

    @pytest.fixture
    def temp_case(self, tmp_path):
        """Create temporary case structure with evidence database."""
        case_path = tmp_path / "test_case"
        case_path.mkdir()
        logs_dir = case_path / "logs"
        logs_dir.mkdir()

        # Create evidence database with process_log table
        evidence_db_path = case_path / "evidence_1.sqlite"
        conn = sqlite3.connect(evidence_db_path)
        conn.execute("""
            CREATE TABLE process_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER,
                task TEXT,
                command TEXT,
                started_at_utc TEXT,
                finished_at_utc TEXT,
                exit_code INTEGER,
                stdout TEXT,
                stderr TEXT,
                run_id TEXT,
                extractor_name TEXT,
                extractor_version TEXT,
                records_extracted INTEGER,
                records_ingested INTEGER,
                warnings_json TEXT,
                log_file_path TEXT
            )
        """)
        conn.commit()
        conn.close()

        return case_path, evidence_db_path

    def test_creates_log_file(self, temp_case):
        """Test that EvidenceLogger creates log file."""
        from core.audit_logging import EvidenceLogger

        case_path, evidence_db_path = temp_case
        logger = EvidenceLogger(case_path, 1, evidence_db_path)

        try:
            logger.log_message("Test message")
            # Force handler to flush
            logger._logger.logger.handlers[0].flush()

            assert logger.log_path.exists()
            content = logger.log_path.read_text()
            assert "Test message" in content
        finally:
            logger.close()

    def test_creates_logs_directory(self, tmp_path):
        """Test that EvidenceLogger creates logs directory if missing."""
        from core.audit_logging import EvidenceLogger

        case_path = tmp_path / "new_case"
        case_path.mkdir()

        # Create minimal evidence database
        evidence_db_path = case_path / "evidence_1.sqlite"
        conn = sqlite3.connect(evidence_db_path)
        conn.execute("""
            CREATE TABLE process_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER,
                task TEXT,
                command TEXT,
                started_at_utc TEXT,
                finished_at_utc TEXT,
                exit_code INTEGER,
                stdout TEXT,
                stderr TEXT,
                run_id TEXT,
                extractor_name TEXT,
                extractor_version TEXT,
                records_extracted INTEGER,
                records_ingested INTEGER,
                warnings_json TEXT,
                log_file_path TEXT
            )
        """)
        conn.commit()
        conn.close()

        # logs/ directory should not exist yet
        logs_dir = case_path / "logs"
        assert not logs_dir.exists()

        logger = EvidenceLogger(case_path, 1, evidence_db_path)
        try:
            assert logs_dir.exists()
        finally:
            logger.close()

    def test_tail_returns_last_lines(self, temp_case):
        """Test that tail() returns last N lines from log file."""
        from core.audit_logging import EvidenceLogger

        case_path, evidence_db_path = temp_case
        logger = EvidenceLogger(case_path, 1, evidence_db_path)

        try:
            # Write multiple log entries
            for i in range(10):
                logger.log_message(f"Message {i}")
            logger._logger.logger.handlers[0].flush()

            # Read last 5 lines
            lines = logger.tail(5)
            assert len(lines) == 5
            assert "Message 5" in lines[0]
            assert "Message 9" in lines[4]
        finally:
            logger.close()

    def test_tail_empty_file(self, temp_case):
        """Test that tail() returns empty list for new log file."""
        from core.audit_logging import EvidenceLogger

        case_path, evidence_db_path = temp_case
        logger = EvidenceLogger(case_path, 1, evidence_db_path)

        try:
            lines = logger.tail(100)
            assert lines == []
        finally:
            logger.close()

    def test_writes_to_process_log(self, temp_case):
        """Test that extraction_start/result writes to process_log table."""
        from core.audit_logging import EvidenceLogger

        case_path, evidence_db_path = temp_case
        logger = EvidenceLogger(case_path, 1, evidence_db_path)

        try:
            log_id = logger.log_extraction_start(
                extractor="browser_history",
                run_id="test_run_123",
                command="test command"
            )

            logger.log_extraction_result(
                extractor="browser_history",
                run_id="test_run_123",
                records=500,
                errors=0,
                elapsed_sec=5.5,
                process_log_id=log_id
            )

            # Verify database content
            conn = sqlite3.connect(evidence_db_path)
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM process_log WHERE id = ?", (log_id,)).fetchone()
            conn.close()

            assert row["run_id"] == "test_run_123"
            assert row["extractor_name"] == "browser_history"
            assert row["task"] == "extract:browser_history"
            assert row["records_extracted"] == 500
            assert row["exit_code"] == 0
        finally:
            logger.close()

    def test_propagate_false(self, temp_case):
        """Test that evidence logger has propagate=False to prevent duplicates."""
        from core.audit_logging import EvidenceLogger

        case_path, evidence_db_path = temp_case
        logger = EvidenceLogger(case_path, 1, evidence_db_path)

        try:
            # The underlying base logger should not propagate
            base_logger = logger._logger.logger
            assert base_logger.propagate is False
        finally:
            logger.close()

    def test_log_format_iso8601_utc(self, temp_case):
        """Test that log entries use ISO 8601 UTC timestamps."""
        from core.audit_logging import EvidenceLogger

        case_path, evidence_db_path = temp_case
        logger = EvidenceLogger(case_path, 1, evidence_db_path)

        try:
            logger.log_message("Test timestamp format")
            logger._logger.logger.handlers[0].flush()

            content = logger.log_path.read_text()
            # Check for ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ
            import re
            assert re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", content)
        finally:
            logger.close()


class TestCaseLogger:
    """Tests for CaseLogger file and database operations."""

    @pytest.fixture
    def temp_case_db(self, tmp_path):
        """Create temporary case folder with case database."""
        case_path = tmp_path / "test_case"
        case_path.mkdir()

        # Create case database with case_audit_log table
        case_db_path = case_path / "test_surfsifter.sqlite"
        conn = sqlite3.connect(case_db_path)
        conn.execute("""
            CREATE TABLE case_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL,
                level TEXT NOT NULL,
                category TEXT NOT NULL,
                action TEXT NOT NULL,
                target_type TEXT,
                target_id INTEGER,
                details_json TEXT,
                investigator TEXT
            )
        """)
        conn.commit()
        conn.close()

        return case_path, case_db_path

    def test_logs_evidence_added(self, temp_case_db):
        """Test that log_evidence_added writes to file and database."""
        from core.audit_logging import CaseLogger

        case_path, case_db_path = temp_case_db
        logger = CaseLogger(case_path, "TEST-001", case_db_path)

        try:
            logger.log_evidence_added(
                evidence_id=5,
                label="4Dell.E01",
                source_path="/images/4Dell.E01",
                size_bytes=4_500_000_000
            )
            logger._logger.handlers[0].flush()

            # Check file content
            content = logger.log_path.read_text()
            assert "Evidence added" in content
            assert "4Dell.E01" in content

            # Check database content
            conn = sqlite3.connect(case_db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM case_audit_log").fetchall()
            conn.close()

            assert len(rows) == 1
            assert rows[0]["category"] == "evidence"
            assert rows[0]["action"] == "added"
            assert rows[0]["target_id"] == 5

            details = json.loads(rows[0]["details_json"])
            assert details["label"] == "4Dell.E01"
        finally:
            logger.close()

    def test_logs_case_opened_closed(self, temp_case_db):
        """Test that case open/close events are logged."""
        from core.audit_logging import CaseLogger

        case_path, case_db_path = temp_case_db
        logger = CaseLogger(case_path, "TEST-001", case_db_path)

        try:
            logger.log_case_opened()
            logger.log_case_closed()
            logger._logger.handlers[0].flush()

            # Check database content
            conn = sqlite3.connect(case_db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM case_audit_log ORDER BY id").fetchall()
            conn.close()

            assert len(rows) == 2
            assert rows[0]["category"] == "case"
            assert rows[0]["action"] == "opened"
            assert rows[1]["action"] == "closed"
        finally:
            logger.close()

    def test_propagate_false(self, temp_case_db):
        """Test that case logger has propagate=False."""
        from core.audit_logging import CaseLogger

        case_path, case_db_path = temp_case_db
        logger = CaseLogger(case_path, "TEST-001", case_db_path)

        try:
            assert logger._logger.propagate is False
        finally:
            logger.close()


class TestAuditLogger:
    """Tests for AuditLogger lifecycle management."""

    @pytest.fixture
    def temp_case_structure(self, tmp_path):
        """Create complete temporary case structure."""
        case_path = tmp_path / "test_case"
        case_path.mkdir()
        logs_dir = case_path / "logs"
        logs_dir.mkdir()

        # Create case database
        case_db_path = case_path / "test_surfsifter.sqlite"
        conn = sqlite3.connect(case_db_path)
        conn.execute("""
            CREATE TABLE case_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL,
                level TEXT NOT NULL,
                category TEXT NOT NULL,
                action TEXT NOT NULL,
                target_type TEXT,
                target_id INTEGER,
                details_json TEXT,
                investigator TEXT
            )
        """)
        conn.commit()
        conn.close()

        # Create evidence database
        evidence_db_path = logs_dir / "evidence_1.sqlite"
        conn = sqlite3.connect(evidence_db_path)
        conn.execute("""
            CREATE TABLE process_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER,
                task TEXT,
                command TEXT,
                started_at_utc TEXT,
                finished_at_utc TEXT,
                exit_code INTEGER,
                stdout TEXT,
                stderr TEXT,
                run_id TEXT,
                extractor_name TEXT,
                extractor_version TEXT,
                records_extracted INTEGER,
                records_ingested INTEGER,
                warnings_json TEXT,
                log_file_path TEXT
            )
        """)
        conn.commit()
        conn.close()

        return case_path, case_db_path, evidence_db_path

    def test_set_case_creates_case_logger(self, temp_case_structure):
        """Test that set_case creates and returns CaseLogger."""
        from core.audit_logging import AuditLogger, CaseLogger

        case_path, case_db_path, evidence_db_path = temp_case_structure

        audit_logger = AuditLogger()
        try:
            case_logger = audit_logger.set_case(case_path, "TEST-001", case_db_path)

            assert isinstance(case_logger, CaseLogger)
            assert audit_logger.case_logger is case_logger
            assert audit_logger.case_path == case_path
        finally:
            audit_logger.close()

    def test_get_evidence_logger_caches(self, temp_case_structure):
        """Test that get_evidence_logger caches EvidenceLogger instances."""
        from core.audit_logging import AuditLogger

        case_path, case_db_path, evidence_db_path = temp_case_structure

        audit_logger = AuditLogger()
        try:
            audit_logger.set_case(case_path, "TEST-001", case_db_path)

            logger1 = audit_logger.get_evidence_logger(1, evidence_db_path)
            logger2 = audit_logger.get_evidence_logger(1, evidence_db_path)

            assert logger1 is logger2  # Same instance
        finally:
            audit_logger.close()

    def test_close_cleans_up_all_loggers(self, temp_case_structure):
        """Test that close() cleans up all loggers."""
        from core.audit_logging import AuditLogger

        case_path, case_db_path, evidence_db_path = temp_case_structure

        audit_logger = AuditLogger()
        audit_logger.set_case(case_path, "TEST-001", case_db_path)
        audit_logger.get_evidence_logger(1, evidence_db_path)

        audit_logger.close()

        assert audit_logger.case_logger is None
        assert len(audit_logger._evidence_loggers) == 0
        assert audit_logger.case_path is None

    def test_requires_case_for_evidence_logger(self, tmp_path):
        """Test that get_evidence_logger raises without case."""
        from core.audit_logging import AuditLogger

        audit_logger = AuditLogger()

        with pytest.raises(RuntimeError, match="Cannot create evidence logger without case"):
            audit_logger.get_evidence_logger(1, tmp_path / "test.sqlite")

    def test_config_overrides_defaults(self, temp_case_structure):
        """Test that config dict overrides default values."""
        from core.audit_logging import AuditLogger

        case_path, case_db_path, evidence_db_path = temp_case_structure

        config = {
            "case_log_max_mb": 25,
            "case_log_backup_count": 3,
            "evidence_log_max_mb": 75,
            "evidence_log_backup_count": 4,
        }

        audit_logger = AuditLogger(config)
        try:
            case_logger = audit_logger.set_case(case_path, "TEST-001", case_db_path)

            # Check that case logger uses custom config
            handler = case_logger._logger.handlers[0]
            assert handler.maxBytes == 25 * 1024 * 1024
            assert handler.backupCount == 3
        finally:
            audit_logger.close()


class TestProcessLogHelpers:
    """Tests for enhanced process_log helper functions."""

    @pytest.fixture
    def temp_db(self, tmp_path):
        """Create temporary database with process_log table."""
        db_path = tmp_path / "test.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE process_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER,
                task TEXT,
                command TEXT,
                started_at_utc TEXT,
                finished_at_utc TEXT,
                exit_code INTEGER,
                stdout TEXT,
                stderr TEXT,
                run_id TEXT,
                extractor_name TEXT,
                extractor_version TEXT,
                records_extracted INTEGER,
                records_ingested INTEGER,
                warnings_json TEXT,
                log_file_path TEXT
            )
        """)
        conn.commit()
        return conn, db_path

    def test_create_process_log_enhanced(self, temp_db):
        """Test create_process_log_enhanced with all fields."""
        from core.audit_logging import create_process_log_enhanced

        conn, db_path = temp_db

        log_id = create_process_log_enhanced(
            conn,
            evidence_id=1,
            task="extract:browser_history",
            command="python -m extractor",
            run_id="run_abc123",
            extractor_name="browser_history",
            extractor_version="0.65.0",
            log_file_path="/case/logs/evidence_1.log"
        )
        conn.commit()

        row = conn.execute("SELECT * FROM process_log WHERE id = ?", (log_id,)).fetchone()

        assert row[1] == 1  # evidence_id
        assert row[2] == "extract:browser_history"  # task
        assert row[9] == "run_abc123"  # run_id
        assert row[10] == "browser_history"  # extractor_name
        assert row[11] == "0.65.0"  # extractor_version
        assert row[15] == "/case/logs/evidence_1.log"  # log_file_path

        conn.close()

    def test_finalize_process_log_enhanced(self, temp_db):
        """Test finalize_process_log_enhanced with all fields."""
        from core.audit_logging import create_process_log_enhanced, finalize_process_log_enhanced

        conn, db_path = temp_db

        log_id = create_process_log_enhanced(
            conn,
            evidence_id=1,
            task="extract:test",
            command=None,
            run_id="run_xyz"
        )
        conn.commit()

        finalize_process_log_enhanced(
            conn,
            log_id,
            exit_code=0,
            stdout="Success output",
            stderr=None,
            records_extracted=1000,
            records_ingested=950,
            warnings_json='["warning 1"]'
        )
        conn.commit()

        row = conn.execute("SELECT * FROM process_log WHERE id = ?", (log_id,)).fetchone()

        assert row[5] is not None  # finished_at_utc
        assert row[6] == 0  # exit_code
        assert row[7] == "Success output"  # stdout
        assert row[12] == 1000  # records_extracted
        assert row[13] == 950  # records_ingested
        assert row[14] == '["warning 1"]'  # warnings_json

        conn.close()


class TestWorkerCallbacksIntegration:
    """Tests for WorkerCallbacks integration with EvidenceLogger."""

    def test_on_log_routes_through_evidence_logger(self, tmp_path):
        """Test that on_log writes to EvidenceLogger when provided."""
        from extractors.workers import WorkerCallbacks
        from core.audit_logging import EvidenceLogger

        # Create minimal case structure
        case_path = tmp_path / "case"
        case_path.mkdir()
        logs_dir = case_path / "logs"
        logs_dir.mkdir()

        evidence_db_path = case_path / "evidence_1.sqlite"
        conn = sqlite3.connect(evidence_db_path)
        conn.execute("""
            CREATE TABLE process_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER,
                task TEXT,
                command TEXT,
                started_at_utc TEXT,
                finished_at_utc TEXT,
                exit_code INTEGER,
                stdout TEXT,
                stderr TEXT,
                run_id TEXT,
                extractor_name TEXT,
                extractor_version TEXT,
                records_extracted INTEGER,
                records_ingested INTEGER,
                warnings_json TEXT,
                log_file_path TEXT
            )
        """)
        conn.commit()
        conn.close()

        evidence_logger = EvidenceLogger(case_path, 1, evidence_db_path)

        try:
            callbacks = WorkerCallbacks(
                evidence_logger=evidence_logger,
                extractor_name="test_extractor"
            )

            callbacks.on_log("Test message from worker")
            evidence_logger._logger.logger.handlers[0].flush()

            content = evidence_logger.log_path.read_text()
            assert "Test message from worker" in content
            assert "test_extractor" in content
        finally:
            evidence_logger.close()

    def test_set_extractor_name(self):
        """Test that set_extractor_name updates logging context."""
        from extractors.workers import WorkerCallbacks

        callbacks = WorkerCallbacks(extractor_name="initial")
        assert callbacks._extractor_name == "initial"

        callbacks.set_extractor_name("updated")
        assert callbacks._extractor_name == "updated"
