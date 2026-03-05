"""Tests for appendix download audit module."""

from __future__ import annotations

import sqlite3
from typing import Generator

import pytest

from reports.appendix import AppendixDownloadAuditModule, AppendixRegistry
from reports.appendix.base import FilterType
from reports.locales import get_translations


@pytest.fixture
def module() -> AppendixDownloadAuditModule:
    """Create module instance."""
    return AppendixDownloadAuditModule()


@pytest.fixture
def test_db() -> Generator[sqlite3.Connection, None, None]:
    """Create in-memory DB with download_audit sample data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript(
        """
        CREATE TABLE download_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            ts_utc TEXT,
            url TEXT,
            method TEXT,
            outcome TEXT,
            blocked INTEGER DEFAULT 0,
            reason TEXT,
            status_code INTEGER,
            attempts INTEGER DEFAULT 1,
            duration_s REAL,
            bytes_written INTEGER,
            content_type TEXT,
            caller_info TEXT,
            created_at_utc TEXT
        );
        """
    )

    # Insert sample audit entries
    entries = [
        (1, 1, "2024-03-15T14:22:01", "https://example.com/img/photo.jpg",
         "GET", "success", 0, None, 200, 1, 0.35, 204800,
         "image/jpeg", "net_download:fetch_url", "2024-03-15T14:22:01"),
        (2, 1, "2024-03-15T14:22:05", "https://malware.test/bad.exe",
         "GET", "blocked", 1, "URL blocked by policy", None, 0, None, 0,
         None, "net_download:fetch_url", "2024-03-15T14:22:05"),
        (3, 1, "2024-03-15T14:23:10", "https://example.com/img/banner.png",
         "GET", "success", 0, None, 200, 1, 1.2, 1048576,
         "image/png", "net_download:fetch_url", "2024-03-15T14:23:10"),
        (4, 1, "2024-03-15T14:24:00", "https://cdn.example.com/style.css",
         "GET", "failed", 0, "Connection timeout", 0, 3, 30.0, 0,
         None, "net_download:fetch_url", "2024-03-15T14:24:00"),
        (5, 1, "2024-03-15T14:25:30", "https://example.com/data.json",
         "GET", "error", 0, "SSL certificate error", 0, 1, 0.01, 0,
         None, "net_download:fetch_url", "2024-03-15T14:25:30"),
        # Entry for a different evidence_id
        (6, 2, "2024-03-16T10:00:00", "https://other.example.com/file.zip",
         "GET", "success", 0, None, 200, 1, 5.0, 5242880,
         "application/zip", "net_download:fetch_url", "2024-03-16T10:00:00"),
    ]

    for entry in entries:
        conn.execute(
            """
            INSERT INTO download_audit
                (id, evidence_id, ts_utc, url, method, outcome, blocked,
                 reason, status_code, attempts, duration_s, bytes_written,
                 content_type, caller_info, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            entry,
        )
    conn.commit()

    yield conn
    conn.close()


@pytest.fixture
def empty_db() -> Generator[sqlite3.Connection, None, None]:
    """Create in-memory DB with download_audit table but no data."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE download_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            ts_utc TEXT, url TEXT, method TEXT, outcome TEXT,
            blocked INTEGER DEFAULT 0, reason TEXT, status_code INTEGER,
            attempts INTEGER DEFAULT 1, duration_s REAL, bytes_written INTEGER,
            content_type TEXT, caller_info TEXT, created_at_utc TEXT
        );
        """
    )
    yield conn
    conn.close()


# ── Metadata ──────────────────────────────────────────────────────────


class TestMetadata:
    """Test module metadata."""

    def test_module_id(self, module: AppendixDownloadAuditModule):
        assert module.metadata.module_id == "appendix_download_audit"

    def test_module_category(self, module: AppendixDownloadAuditModule):
        assert module.metadata.category == "Appendix"

    def test_module_icon(self, module: AppendixDownloadAuditModule):
        assert module.metadata.icon == "📋"

    def test_default_title(self, module: AppendixDownloadAuditModule):
        assert module.get_default_title() == "Download Audit Log"


# ── Filter Fields ─────────────────────────────────────────────────────


class TestFilterFields:
    """Test filter field definitions."""

    def test_filter_field_count(self, module: AppendixDownloadAuditModule):
        fields = module.get_filter_fields()
        assert len(fields) == 4

    def test_outcome_filter_field(self, module: AppendixDownloadAuditModule):
        fields = {f.key: f for f in module.get_filter_fields()}
        f = fields["outcome_filter"]
        assert f.filter_type == FilterType.DROPDOWN
        assert f.default == "all"
        assert any(v[0] == "success" for v in f.options)
        assert any(v[0] == "blocked" for v in f.options)

    def test_include_reason_field(self, module: AppendixDownloadAuditModule):
        fields = {f.key: f for f in module.get_filter_fields()}
        f = fields["include_reason"]
        assert f.filter_type == FilterType.CHECKBOX
        assert f.default is True

    def test_include_caller_field(self, module: AppendixDownloadAuditModule):
        fields = {f.key: f for f in module.get_filter_fields()}
        f = fields["include_caller"]
        assert f.filter_type == FilterType.CHECKBOX
        assert f.default is False

    def test_sort_field(self, module: AppendixDownloadAuditModule):
        fields = {f.key: f for f in module.get_filter_fields()}
        f = fields["sort_by"]
        assert f.filter_type == FilterType.DROPDOWN
        assert f.default == "date_desc"


# ── Dynamic Options ───────────────────────────────────────────────────


class TestDynamicOptions:
    """Test dynamic option loading from DB."""

    def test_outcome_options_from_db(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        options = module.get_dynamic_options("outcome_filter", test_db)
        assert options is not None
        values = [o[0] for o in options]
        assert "all" in values
        assert "success" in values
        assert "blocked" in values
        assert "failed" in values
        assert "error" in values

    def test_unknown_key_returns_none(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        assert module.get_dynamic_options("unknown_key", test_db) is None

    def test_empty_db_has_only_all(
        self,
        module: AppendixDownloadAuditModule,
        empty_db: sqlite3.Connection,
    ):
        options = module.get_dynamic_options("outcome_filter", empty_db)
        assert options is not None
        assert len(options) == 1
        assert options[0][0] == "all"


# ── Render ────────────────────────────────────────────────────────────


class TestRender:
    """Test rendering of the download audit appendix."""

    def _render(
        self,
        module: AppendixDownloadAuditModule,
        db: sqlite3.Connection,
        **overrides,
    ) -> str:
        config = {
            "_locale": "en",
            "_translations": get_translations("en"),
            "_date_format": "eu",
            **overrides,
        }
        return module.render(db, evidence_id=1, config=config)

    def test_render_default(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        html = self._render(module, test_db)
        # Should contain table rows for evidence_id=1 (5 entries)
        assert '<table class="appendix-audit-table">' in html
        assert "5 entries" in html
        # Should NOT contain data from evidence_id=2
        assert "other.example.com" not in html

    def test_render_contains_urls(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        html = self._render(module, test_db)
        # URLs appear in dedicated url-row spans
        assert "example.com/img/photo.jpg" in html
        assert "malware.test/bad.exe" in html
        assert "cdn.example.com/style.css" in html

    def test_render_two_row_layout(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        html = self._render(module, test_db)
        # URL appears in a separate row with url-label
        assert '<tr class="url-row">' in html
        assert '<tr class="data-row">' in html
        assert '<span class="url-label">URL:</span>' in html

    def test_render_outcome_badges(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        html = self._render(module, test_db)
        assert "outcome-success" in html
        assert "outcome-blocked" in html
        assert "outcome-failed" in html
        assert "outcome-error" in html

    def test_render_includes_reason_column_by_default(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        html = self._render(module, test_db)
        assert "Reason" in html
        assert "URL blocked by policy" in html
        assert "Connection timeout" in html

    def test_render_excludes_reason_when_disabled(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        html = self._render(module, test_db, include_reason=False)
        # Reason header should not be present
        assert "URL blocked by policy" not in html

    def test_render_excludes_caller_by_default(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        html = self._render(module, test_db)
        # Caller column header should not appear
        assert '<th class="col-caller">' not in html

    def test_render_includes_caller_when_enabled(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        html = self._render(module, test_db, include_caller=True)
        assert "net_download:fetch_url" in html

    def test_render_no_summary(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        html = self._render(module, test_db)
        assert '<div class="appendix-audit-summary">' not in html

    def test_render_outcome_filter(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        html = self._render(module, test_db, outcome_filter="blocked")
        # Only blocked entries
        assert "malware.test/bad.exe" in html
        assert "1 entries" in html

    def test_render_sort_date_asc(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        html = self._render(module, test_db, sort_by="date_asc")
        # photo should appear before data.json
        pos_photo = html.index("photo.jpg")
        pos_data = html.index("data.json")
        assert pos_photo < pos_data

    def test_render_empty_db(
        self,
        module: AppendixDownloadAuditModule,
        empty_db: sqlite3.Connection,
    ):
        html = self._render(module, empty_db)
        assert '<p class="empty-message">' in html
        assert "No download audit entries found." in html
        assert '<table class="appendix-audit-table">' not in html

    def test_render_duration_formatting(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        html = self._render(module, test_db)
        # 0.35s → "350 ms", 1.2s → "1.2 s", 30.0s → "30.0 s"
        assert "350 ms" in html
        assert "1.2 s" in html
        assert "30.0 s" in html

    def test_render_bytes_formatting(
        self,
        module: AppendixDownloadAuditModule,
        test_db: sqlite3.Connection,
    ):
        html = self._render(module, test_db)
        # 204800 → "200.0 KB", 1048576 → "1.0 MB"
        assert "200.0 KB" in html
        assert "1.0 MB" in html


# ── Internal helpers ──────────────────────────────────────────────────


class TestHelpers:
    """Test internal helper methods."""

    def test_format_size_bytes(self, module: AppendixDownloadAuditModule):
        assert module._format_size(500) == "500 B"

    def test_format_size_kilobytes(self, module: AppendixDownloadAuditModule):
        assert module._format_size(2048) == "2.0 KB"

    def test_format_size_megabytes(self, module: AppendixDownloadAuditModule):
        assert module._format_size(5 * 1024 * 1024) == "5.0 MB"

    def test_outcome_styles_coverage(self, module: AppendixDownloadAuditModule):
        """All five standard outcomes must have styles."""
        for outcome in ("success", "failed", "blocked", "cancelled", "error"):
            assert outcome in module.OUTCOME_STYLES
            css_class, icon = module.OUTCOME_STYLES[outcome]
            assert css_class
            assert icon


# ── Registry ──────────────────────────────────────────────────────────


class TestRegistry:
    """Test appendix registry discovery."""

    def test_module_discovered_by_registry(self):
        registry = AppendixRegistry()
        modules = registry.list_modules()
        ids = [m.module_id for m in modules]
        assert "appendix_download_audit" in ids

    def test_registry_create_module(self):
        registry = AppendixRegistry()
        mod = registry.get_module("appendix_download_audit")
        assert mod is not None
        assert mod.metadata.module_id == "appendix_download_audit"


# ── Translations ──────────────────────────────────────────────────────


class TestTranslations:
    """Test translation key coverage."""

    def test_en_translations_present(self):
        t = get_translations("en")
        assert "appendix_download_audit_title" in t
        assert "appendix_download_audit_empty" in t
        assert "audit_outcome" in t
        assert "audit_method" in t
        assert "audit_total" in t
        assert "audit_entries" in t

    def test_de_translations_present(self):
        t = get_translations("de")
        assert "appendix_download_audit_title" in t
        assert "appendix_download_audit_empty" in t
        assert "audit_outcome" in t
        assert "audit_method" in t
        assert "audit_total" in t
        assert "audit_entries" in t
