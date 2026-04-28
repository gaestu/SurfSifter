"""
Tests for IE Legacy extractor bug fixes.

Bug #1: History container filter misses MSHist containers
Bug #2: is_persistent kwarg crashes every cookie insert
Bug #3: Edge Legacy DOM Storage XML parser reads attributes instead of child elements
"""

import sqlite3
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from core.database import migrate, EVIDENCE_MIGRATIONS_DIR
from core.database.manager import _ensure_dpapi_decrypt_columns


def _make_evidence_db():
    """Create an in-memory evidence DB with the full schema applied."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)
    _ensure_dpapi_decrypt_columns(conn)
    return conn


# ---------------------------------------------------------------------------
# Bug #1 – MSHist URL prefix parsing
# ---------------------------------------------------------------------------


class TestExtractUrlFromWebcacheEntry:
    """IEHistoryExtractor._extract_url_from_webcache_entry strips MSHist
    date prefixes, 'Visited: ' prefixes, and user@ portions correctly."""

    @pytest.fixture(autouse=True)
    def _extractor(self):
        """Build a minimal IEHistoryExtractor without running __init__."""
        from extractors.browser.ie_legacy.history.extractor import (
            IEHistoryExtractor,
        )

        self.ext = object.__new__(IEHistoryExtractor)

    @pytest.mark.parametrize(
        "raw_url, expected",
        [
            # MSHist date prefix + user@
            (
                ":2018071420180715: user@http://example.com/page",
                "http://example.com/page",
            ),
            (
                ":2018081020180811: admin@https://secure.example.com",
                "https://secure.example.com",
            ),
            # Visited prefix + user@
            ("Visited: user@https://example.com", "https://example.com"),
            # Plain URL – no prefix at all
            ("https://example.com/plain", "https://example.com/plain"),
            # MSHist date prefix but no user@
            (
                ":2018071420180715: http://no-user.example.com",
                "http://no-user.example.com",
            ),
            # Empty / None
            ("", None),
            (None, None),
        ],
        ids=[
            "mshist-user",
            "mshist-admin-https",
            "visited-user",
            "plain-url",
            "mshist-no-user",
            "empty-string",
            "none",
        ],
    )
    def test_url_extraction(self, raw_url, expected):
        result = self.ext._extract_url_from_webcache_entry(raw_url)
        assert result == expected


# ---------------------------------------------------------------------------
# Bug #2 – is_persistent kwarg removed from cookie insert
# ---------------------------------------------------------------------------


class TestCookieInsertNoPersistentKwarg:
    """insert_cookie_row must work *without* is_persistent and must reject it
    if someone tries to pass it (the old buggy call-site)."""

    @pytest.fixture()
    def evidence_conn(self):
        conn = _make_evidence_db()
        yield conn
        conn.close()

    def test_insert_without_is_persistent(self, evidence_conn):
        from core.database.helpers import insert_cookie_row

        # Minimal kwargs matching what IEINetCookiesExtractor now passes
        insert_cookie_row(
            evidence_conn,
            evidence_id=1,
            browser="Internet Explorer",
            profile="Default",
            name="test_cookie",
            domain=".example.com",
            value="abc123",
            path="/",
            expires_utc="2025-01-01T00:00:00",
            is_secure=0,
            is_httponly=0,
            run_id="run-001",
            source_path="/some/path",
            discovered_by="ie_inetcookies",
        )

        row = evidence_conn.execute("SELECT * FROM cookies WHERE id = 1").fetchone()
        assert row is not None

    def test_is_persistent_kwarg_raises(self, evidence_conn):
        from core.database.helpers import insert_cookie_row

        with pytest.raises(TypeError):
            insert_cookie_row(
                evidence_conn,
                evidence_id=1,
                browser="Internet Explorer",
                profile="Default",
                name="test_cookie",
                domain=".example.com",
                value="abc123",
                path="/",
                expires_utc="2025-01-01T00:00:00",
                is_secure=0,
                is_httponly=0,
                run_id="run-001",
                source_path="/some/path",
                discovered_by="ie_inetcookies",
                is_persistent=True,  # old buggy kwarg
            )


# ---------------------------------------------------------------------------
# Bug #3 – DOM Storage XML parser handles child elements
# ---------------------------------------------------------------------------

# XML with child elements (the format that the old parser couldn't read)
CHILD_ELEMENT_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="utf-8"?>
    <DomStorage>
        <Item>
            <Key>theme</Key>
            <Value>dark</Value>
        </Item>
        <Item>
            <Key>lang</Key>
            <Value>en-US</Value>
        </Item>
    </DomStorage>
""")


class TestDomStorageXmlParser:
    """IEDOMStorageExtractor._parse_edge_domstore_file correctly reads XML
    files where Key/Value are child elements (not attributes)."""

    @pytest.fixture()
    def evidence_conn(self):
        conn = _make_evidence_db()
        yield conn
        conn.close()

    @pytest.fixture()
    def xml_file(self, tmp_path):
        p = tmp_path / "domstore.xml"
        p.write_text(CHILD_ELEMENT_XML, encoding="utf-8")
        return p

    @pytest.fixture()
    def extractor(self):
        from extractors.browser.ie_legacy.dom_storage.extractor import (
            IEDOMStorageExtractor,
        )

        ext = object.__new__(IEDOMStorageExtractor)
        return ext

    @pytest.fixture()
    def callbacks(self):
        from extractors.callbacks import ExtractorCallbacks

        cb = MagicMock(spec=ExtractorCallbacks)
        return cb

    def test_child_element_format_parsed(
        self, extractor, xml_file, evidence_conn, callbacks
    ):
        file_entry = {
            "logical_path": "/Users/test/domstore.xml",
            "forensic_path": "img_Evidence/domstore.xml",
            "partition_index": 0,
            "fs_type": "NTFS",
        }

        count = extractor._parse_edge_domstore_file(
            file_path=xml_file,
            file_entry=file_entry,
            run_id="run-002",
            evidence_id=1,
            evidence_conn=evidence_conn,
            callbacks=callbacks,
        )

        assert count == 2

        rows = evidence_conn.execute(
            "SELECT key, value FROM local_storage ORDER BY key"
        ).fetchall()
        assert [(r["key"], r["value"]) for r in rows] == [
            ("lang", "en-US"),
            ("theme", "dark"),
        ]
