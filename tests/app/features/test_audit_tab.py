"""
Tests for the Audit Tab feature.

Tests for:
- AuditTab creation and structure
- ExtractionSubtab filtering
- ExtractedFilesTableModel data loading
"""
import pytest
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

from PySide6.QtCore import Qt


class TestExtractedFilesTableModel:
    """Tests for ExtractedFilesTableModel."""

    def test_model_columns(self):
        """Test model has expected columns."""
        from app.features.audit.models import ExtractedFilesTableModel

        assert len(ExtractedFilesTableModel.COLUMNS) == 9
        assert len(ExtractedFilesTableModel.HEADERS) == 9
        assert "extractor_name" in ExtractedFilesTableModel.COLUMNS
        assert "dest_filename" in ExtractedFilesTableModel.COLUMNS
        assert "status" in ExtractedFilesTableModel.COLUMNS
        assert "sha256" in ExtractedFilesTableModel.COLUMNS

    def test_model_headers(self):
        """Test model headers are human-readable."""
        from app.features.audit.models import ExtractedFilesTableModel

        assert "Extractor" in ExtractedFilesTableModel.HEADERS
        assert "Filename" in ExtractedFilesTableModel.HEADERS
        assert "Status" in ExtractedFilesTableModel.HEADERS
        assert "SHA256" in ExtractedFilesTableModel.HEADERS

    def test_format_size(self):
        """Test file size formatting."""
        from app.features.audit.models import ExtractedFilesTableModel

        assert ExtractedFilesTableModel._format_size(0) == "0 B"
        assert ExtractedFilesTableModel._format_size(512) == "512 B"
        assert ExtractedFilesTableModel._format_size(1024) == "1.0 KB"
        assert ExtractedFilesTableModel._format_size(1536) == "1.5 KB"
        assert ExtractedFilesTableModel._format_size(1048576) == "1.0 MB"
        assert ExtractedFilesTableModel._format_size(1073741824) == "1.00 GB"

    def test_model_init(self):
        """Test model initialization."""
        from app.features.audit.models import ExtractedFilesTableModel

        mock_db_manager = MagicMock()
        model = ExtractedFilesTableModel(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )

        assert model.evidence_id == 1
        assert model.evidence_label == "test_evidence"
        assert model.rowCount() == 0
        assert model.columnCount() == 9


class TestDownloadAuditTableModel:
    """Tests for DownloadAuditTableModel."""

    def test_model_columns(self):
        """Test model has expected columns."""
        from app.features.audit.models import DownloadAuditTableModel

        assert len(DownloadAuditTableModel.COLUMNS) == 11
        assert len(DownloadAuditTableModel.HEADERS) == 11
        assert "url" in DownloadAuditTableModel.COLUMNS
        assert "outcome" in DownloadAuditTableModel.COLUMNS
        assert "reason" in DownloadAuditTableModel.COLUMNS


class TestExtractionSubtab:
    """Tests for ExtractionSubtab widget."""

    def test_subtab_creation(self, qtbot):
        """Test subtab creates without error."""
        from app.features.audit.tab import ExtractionSubtab

        mock_db_manager = MagicMock()
        subtab = ExtractionSubtab(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        qtbot.addWidget(subtab)

        assert subtab is not None
        assert subtab._extractor_combo is not None
        assert subtab._status_combo is not None
        assert subtab._table is not None

    def test_subtab_status_options(self, qtbot):
        """Test status dropdown has expected options."""
        from app.features.audit.tab import ExtractionSubtab

        mock_db_manager = MagicMock()
        subtab = ExtractionSubtab(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        qtbot.addWidget(subtab)

        # Check status options
        assert subtab._status_combo.count() == 5  # All, OK, Partial, Error, Skipped

        # Verify "All Statuses" is first with empty value
        assert subtab._status_combo.itemText(0) == "All Statuses"
        assert subtab._status_combo.itemData(0) == ""

    def test_subtab_format_size(self):
        """Test subtab size formatting helper."""
        from app.features.audit.tab import ExtractionSubtab

        assert ExtractionSubtab._format_size(0) == "0 B"
        assert ExtractionSubtab._format_size(None) == "0 B"
        assert ExtractionSubtab._format_size(1024) == "1.0 KB"


class TestAuditTab:
    """Tests for main AuditTab widget."""

    def test_audit_tab_creation(self, qtbot):
        """Test AuditTab creates without error."""
        from app.features.audit import AuditTab

        mock_db_manager = MagicMock()
        tab = AuditTab(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        qtbot.addWidget(tab)

        assert tab is not None
        assert tab._tab_widget is not None
        assert tab._extraction_tab is not None

    def test_audit_tab_has_extraction_subtab(self, qtbot):
        """Test AuditTab has Extraction subtab."""
        from app.features.audit import AuditTab

        mock_db_manager = MagicMock()
        tab = AuditTab(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        qtbot.addWidget(tab)

        # Should have five tabs (Extraction + Warnings + Download Audit + Statistics + Logs)
        assert tab._tab_widget.count() == 5

        # Tab 0 should be "📦 Extraction"
        assert tab._tab_widget.tabText(0) == "📦 Extraction"

        # Tab 1 should be "⚠️ Warnings"
        assert tab._tab_widget.tabText(1) == "⚠️ Warnings"

        # Tab 2 should be "⬇️ Download Audit"
        assert tab._tab_widget.tabText(2) == "⬇️ Download Audit"

        # Tab 3 should be "📈 Statistics"
        assert tab._tab_widget.tabText(3) == "📈 Statistics"

        # Tab 4 should be "📜 Logs"
        assert tab._tab_widget.tabText(4) == "📜 Logs"

    def test_audit_tab_mark_stale(self, qtbot):
        """Test mark_stale sets flag."""
        from app.features.audit import AuditTab

        mock_db_manager = MagicMock()
        tab = AuditTab(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        qtbot.addWidget(tab)

        assert tab._data_stale is False
        tab.mark_stale()
        assert tab._data_stale is True

    def test_audit_tab_statistics_tab_property(self, qtbot):
        """Test statistics_tab property returns the StatisticsSubtab subtab."""
        from app.features.audit import AuditTab
        from app.features.audit.statistics_subtab import StatisticsSubtab

        mock_db_manager = MagicMock()
        tab = AuditTab(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        qtbot.addWidget(tab)

        # statistics_tab property should return a StatisticsSubtab
        assert isinstance(tab.statistics_tab, StatisticsSubtab)
        assert tab.statistics_tab._evidence_id == 1
        assert tab.statistics_tab._evidence_label == "test_evidence"

    def test_audit_tab_logs_tab_property(self, qtbot):
        """Test logs_tab and log_widget properties."""
        from app.features.audit import AuditTab
        from app.features.audit.tab import LogsSubtab
        from PySide6.QtWidgets import QTextEdit

        mock_db_manager = MagicMock()
        tab = AuditTab(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        qtbot.addWidget(tab)

        # logs_tab property should return a LogsSubtab
        assert isinstance(tab.logs_tab, LogsSubtab)
        assert tab.logs_tab.evidence_id == 1
        assert tab.logs_tab.evidence_label == "test_evidence"

        # log_widget property should return the underlying QTextEdit
        assert isinstance(tab.log_widget, QTextEdit)
        assert tab.log_widget is tab.logs_tab.log_widget


class TestAuditTabIntegration:
    """Integration tests for AuditTab with real database."""

    @pytest.fixture
    def temp_evidence_db(self, tmp_path):
        """Create a temporary evidence database with extracted_files table."""
        db_path = tmp_path / "evidence_test.sqlite"

        with sqlite3.connect(db_path) as conn:
            # Create extracted_files table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS extracted_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    evidence_id INTEGER NOT NULL,
                    extractor_name TEXT NOT NULL,
                    extractor_version TEXT,
                    run_id TEXT NOT NULL,
                    extracted_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
                    source_path TEXT,
                    source_inode TEXT,
                    partition_index INTEGER,
                    source_offset_bytes INTEGER,
                    source_block_size INTEGER,
                    dest_rel_path TEXT NOT NULL,
                    dest_filename TEXT NOT NULL,
                    size_bytes INTEGER,
                    file_type TEXT,
                    mime_type TEXT,
                    md5 TEXT,
                    sha256 TEXT,
                    status TEXT NOT NULL DEFAULT 'ok',
                    error_message TEXT,
                    metadata_json TEXT
                )
            """)

            # Insert test data
            conn.executemany("""
                INSERT INTO extracted_files (
                    evidence_id, extractor_name, run_id, dest_rel_path, dest_filename,
                    source_path, size_bytes, file_type, sha256, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (1, "filesystem_images", "fs_001", "images/test1.jpg", "test1.jpg",
                 "Users/John/Pictures/test1.jpg", 1024, "JPEG", "abc123", "ok"),
                (1, "filesystem_images", "fs_001", "images/test2.png", "test2.png",
                 "Users/John/Pictures/test2.png", 2048, "PNG", "def456", "ok"),
                (1, "cache_firefox", "ff_001", "cache/file1.bin", "file1.bin",
                 "Users/John/AppData/Mozilla/cache", 512, None, "ghi789", "ok"),
                (1, "filesystem_images", "fs_001", "images/corrupt.jpg", "corrupt.jpg",
                 "Users/John/Pictures/corrupt.jpg", 100, "JPEG", "jkl012", "error"),
            ])
            conn.commit()

        return db_path

    @pytest.fixture
    def temp_download_audit_db(self, tmp_path):
        """Create temporary evidence database with download_audit rows."""
        db_path = tmp_path / "evidence_download_audit.sqlite"
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS download_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    evidence_id INTEGER NOT NULL,
                    ts_utc TEXT NOT NULL,
                    url TEXT NOT NULL,
                    method TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    blocked INTEGER NOT NULL DEFAULT 0,
                    reason TEXT,
                    status_code INTEGER,
                    attempts INTEGER,
                    duration_s REAL,
                    bytes_written INTEGER,
                    content_type TEXT,
                    caller_info TEXT,
                    created_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
                )
            """)
            conn.executemany("""
                INSERT INTO download_audit (
                    evidence_id, ts_utc, url, method, outcome, blocked, reason,
                    status_code, attempts, duration_s, bytes_written, content_type,
                    caller_info
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (1, "2026-02-06T10:00:00+00:00", "https://a.test/file1.jpg", "GET", "success", 0, None, 200, 1, 0.21, 1024, "image/jpeg", "download_tab"),
                (1, "2026-02-06T10:01:00+00:00", "https://b.test/file2.bin", "GET", "blocked", 1, "Blocked content-type application/octet-stream", 200, 1, 0.01, 0, "application/octet-stream", "download_tab"),
            ])
            conn.commit()
        return db_path

    def test_model_loads_data(self, temp_evidence_db):
        """Test model loads data from database."""
        from app.features.audit.models import ExtractedFilesTableModel

        mock_db_manager = MagicMock()
        mock_db_manager.evidence_db_path.return_value = temp_evidence_db

        model = ExtractedFilesTableModel(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test",
        )

        model.load()

        assert model.rowCount() == 4
        assert model.total_count == 4

    def test_model_filter_by_extractor(self, temp_evidence_db):
        """Test model filters by extractor name."""
        from app.features.audit.models import ExtractedFilesTableModel

        mock_db_manager = MagicMock()
        mock_db_manager.evidence_db_path.return_value = temp_evidence_db

        model = ExtractedFilesTableModel(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test",
        )

        model.load(extractor_filter="cache_firefox")

        assert model.rowCount() == 1

    def test_model_filter_by_status(self, temp_evidence_db):
        """Test model filters by status."""
        from app.features.audit.models import ExtractedFilesTableModel

        mock_db_manager = MagicMock()
        mock_db_manager.evidence_db_path.return_value = temp_evidence_db

        model = ExtractedFilesTableModel(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test",
        )

        model.load(status_filter="error")

        assert model.rowCount() == 1

    def test_model_get_distinct_extractors(self, temp_evidence_db):
        """Test getting distinct extractor names."""
        from app.features.audit.models import ExtractedFilesTableModel

        mock_db_manager = MagicMock()
        mock_db_manager.evidence_db_path.return_value = temp_evidence_db

        model = ExtractedFilesTableModel(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test",
        )

        extractors = model.get_distinct_extractors()

        assert len(extractors) == 2
        assert "cache_firefox" in extractors
        assert "filesystem_images" in extractors

    def test_model_get_stats(self, temp_evidence_db):
        """Test getting extraction statistics."""
        from app.features.audit.models import ExtractedFilesTableModel

        mock_db_manager = MagicMock()
        mock_db_manager.evidence_db_path.return_value = temp_evidence_db

        model = ExtractedFilesTableModel(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test",
        )

        stats = model.get_stats()

        assert stats["total_count"] == 4

    def test_download_audit_model_loads_data(self, temp_download_audit_db):
        """Test DownloadAuditTableModel loads rows."""
        from app.features.audit.models import DownloadAuditTableModel

        mock_db_manager = MagicMock()
        mock_db_manager.evidence_db_path.return_value = temp_download_audit_db

        model = DownloadAuditTableModel(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test",
        )

        model.load()
        assert model.rowCount() == 2
        assert model.total_count == 2

    def test_download_audit_model_filter_by_outcome(self, temp_download_audit_db):
        """Test DownloadAuditTableModel outcome filtering."""
        from app.features.audit.models import DownloadAuditTableModel

        mock_db_manager = MagicMock()
        mock_db_manager.evidence_db_path.return_value = temp_download_audit_db

        model = DownloadAuditTableModel(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test",
        )

        model.load(outcome_filter="blocked")
        assert model.rowCount() == 1
