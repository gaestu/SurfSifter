"""
Qt model for cache entries table.

Displays URLs extracted from browser caches with HTTP metadata.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import DatabaseManager

logger = logging.getLogger(__name__)


def get_cache_entry_count(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    domain: Optional[str] = None,
    content_type: Optional[str] = None,
) -> int:
    """
    Get total count of cache entries matching filters.

    Used for pagination to determine total pages.
    """
    sql = """
        SELECT COUNT(*) FROM urls
        WHERE evidence_id = ?
          AND (discovered_by LIKE 'cache_simple%'
               OR discovered_by LIKE 'cache_firefox%'
               OR discovered_by LIKE 'cache_blockfile%')
    """
    params: List[Any] = [evidence_id]

    if domain:
        sql += " AND domain LIKE ?"
        params.append(f"%{domain}%")

    if content_type:
        sql += " AND content_type LIKE ?"
        params.append(f"%{content_type}%")

    # Browser filter requires post-filtering, so we can't count exactly
    # Return the filtered count (browser filter applied in get_cache_entries)
    return conn.execute(sql, params).fetchone()[0]


def get_cache_entries(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    domain: Optional[str] = None,
    content_type: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Retrieve cache entries (URLs from cache extractors) for an evidence.

    Cache entries are stored in the urls table with discovered_by containing
    'cache_simple', 'cache_firefox', 'cache_blockfile', etc.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter (extracts from discovered_by)
        domain: Optional domain substring filter
        content_type: Optional content_type filter
        limit: Maximum rows to return (page size)
        offset: Number of rows to skip (for pagination)

    Returns:
        List of cache entry records as dicts with parsed metadata
    """
    # Base query for cache entries
    sql = """
        SELECT
            id, evidence_id, url, domain, scheme,
            discovered_by, first_seen_utc, last_seen_utc,
            source_path, run_id,
            cache_key, cache_filename, response_code, content_type,
            tags, notes
        FROM urls
        WHERE evidence_id = ?
          AND (discovered_by LIKE 'cache_simple%'
               OR discovered_by LIKE 'cache_firefox%'
               OR discovered_by LIKE 'cache_blockfile%')
    """
    params: List[Any] = [evidence_id]

    if domain:
        sql += " AND domain LIKE ?"
        params.append(f"%{domain}%")

    if content_type:
        sql += " AND content_type LIKE ?"
        params.append(f"%{content_type}%")

    sql += " ORDER BY last_seen_utc DESC LIMIT ? OFFSET ?"
    params.append(limit)
    params.append(offset)

    rows = conn.execute(sql, params).fetchall()

    result = []
    for row in rows:
        entry = dict(row)

        # Parse browser/profile from discovered_by
        # Format: "cache_simple:version:run_id" or "cache_blockfile:version:run_id"
        discovered_by = entry.get("discovered_by", "")
        browser_name = "unknown"
        if discovered_by.startswith("cache_simple"):
            browser_name = "chromium"
        elif discovered_by.startswith("cache_blockfile"):
            browser_name = "chromium"
        elif discovered_by.startswith("cache_firefox"):
            browser_name = "firefox"

        # Try to extract browser from source_path
        source_path = entry.get("source_path", "")
        if source_path:
            path_lower = source_path.lower()
            if "chrome" in path_lower:
                browser_name = "chrome"
            elif "edge" in path_lower:
                browser_name = "edge"
            elif "opera" in path_lower:
                browser_name = "opera"
            elif "brave" in path_lower:
                browser_name = "brave"
            elif "firefox" in path_lower:
                browser_name = "firefox"

        entry["browser"] = browser_name

        # Parse additional metadata from tags JSON if present
        tags_str = entry.get("tags")
        if tags_str:
            try:
                tags_data = json.loads(tags_str)
                entry["stream0_size"] = tags_data.get("stream0_size")
                entry["stream1_size"] = tags_data.get("stream1_size")
                entry["content_encoding"] = tags_data.get("content_encoding")
                entry["last_used_time"] = tags_data.get("last_used_time")
            except (json.JSONDecodeError, TypeError):
                pass

        # Apply browser filter after parsing
        if browser and browser.lower() != browser_name.lower():
            continue

        result.append(entry)

    return result


def get_distinct_cache_browsers(conn: sqlite3.Connection, evidence_id: int) -> List[str]:
    """
    Get distinct browsers with cache entries for an evidence.

    Returns:
        List of browser names
    """
    rows = conn.execute(
        """
        SELECT DISTINCT source_path FROM urls
        WHERE evidence_id = ?
          AND (discovered_by LIKE 'cache_simple%'
               OR discovered_by LIKE 'cache_firefox%'
               OR discovered_by LIKE 'cache_blockfile%')
        """,
        (evidence_id,)
    ).fetchall()

    browsers = set()
    for row in rows:
        path = row[0] or ""
        path_lower = path.lower()
        if "chrome" in path_lower:
            browsers.add("chrome")
        elif "edge" in path_lower:
            browsers.add("edge")
        elif "opera" in path_lower:
            browsers.add("opera")
        elif "brave" in path_lower:
            browsers.add("brave")
        elif "firefox" in path_lower:
            browsers.add("firefox")

    return sorted(browsers)


def get_cache_content_types(conn: sqlite3.Connection, evidence_id: int) -> List[str]:
    """
    Get distinct content types from cache entries.

    Returns:
        List of content type strings
    """
    rows = conn.execute(
        """
        SELECT DISTINCT content_type FROM urls
        WHERE evidence_id = ?
          AND content_type IS NOT NULL
          AND (discovered_by LIKE 'cache_simple%'
               OR discovered_by LIKE 'cache_firefox%'
               OR discovered_by LIKE 'cache_blockfile%')
        ORDER BY content_type
        """,
        (evidence_id,)
    ).fetchall()

    return [row[0] for row in rows if row[0]]


def get_cache_entry_by_id(conn: sqlite3.Connection, entry_id: int) -> Optional[Dict[str, Any]]:
    """
    Get a single cache entry by ID with full metadata.

    Args:
        conn: SQLite connection to evidence database
        entry_id: URL row ID

    Returns:
        Cache entry as dict, or None if not found
    """
    row = conn.execute(
        """
        SELECT id, evidence_id, url, domain, scheme,
               discovered_by, first_seen_utc, last_seen_utc,
               source_path, run_id,
               cache_key, cache_filename, response_code, content_type,
               tags, notes, context
        FROM urls WHERE id = ?
        """,
        (entry_id,)
    ).fetchone()

    if not row:
        return None

    entry = dict(row)

    # Parse browser from source_path
    source_path = entry.get("source_path", "")
    browser_name = "unknown"
    if source_path:
        path_lower = source_path.lower()
        if "chrome" in path_lower:
            browser_name = "chrome"
        elif "edge" in path_lower:
            browser_name = "edge"
        elif "opera" in path_lower:
            browser_name = "opera"
        elif "brave" in path_lower:
            browser_name = "brave"
        elif "firefox" in path_lower:
            browser_name = "firefox"

    entry["browser"] = browser_name

    # Parse tags JSON
    tags_str = entry.get("tags")
    if tags_str:
        try:
            entry["tags_parsed"] = json.loads(tags_str)
        except (json.JSONDecodeError, TypeError):
            entry["tags_parsed"] = {}

    return entry


def get_cache_stats(conn: sqlite3.Connection, evidence_id: int) -> Dict[str, Any]:
    """
    Get cache statistics for an evidence.

    Returns:
        Dict with total_entries, by_browser, by_content_type, by_response_code
    """
    # Total count
    total = conn.execute(
        """
        SELECT COUNT(*) FROM urls
        WHERE evidence_id = ?
          AND (discovered_by LIKE 'cache_simple%'
               OR discovered_by LIKE 'cache_firefox%'
               OR discovered_by LIKE 'cache_blockfile%')
        """,
        (evidence_id,)
    ).fetchone()[0]

    # By response code
    by_status = conn.execute(
        """
        SELECT response_code, COUNT(*) as cnt FROM urls
        WHERE evidence_id = ?
          AND (discovered_by LIKE 'cache_simple%'
               OR discovered_by LIKE 'cache_firefox%'
               OR discovered_by LIKE 'cache_blockfile%')
        GROUP BY response_code
        ORDER BY cnt DESC
        """,
        (evidence_id,)
    ).fetchall()

    # By content type (top 10)
    by_type = conn.execute(
        """
        SELECT content_type, COUNT(*) as cnt FROM urls
        WHERE evidence_id = ?
          AND content_type IS NOT NULL
          AND (discovered_by LIKE 'cache_simple%'
               OR discovered_by LIKE 'cache_firefox%'
               OR discovered_by LIKE 'cache_blockfile%')
        GROUP BY content_type
        ORDER BY cnt DESC
        LIMIT 10
        """,
        (evidence_id,)
    ).fetchall()

    return {
        "total_entries": total,
        "by_response_code": {row[0]: row[1] for row in by_status},
        "by_content_type": {row[0]: row[1] for row in by_type},
    }


class CacheEntriesTableModel(QAbstractTableModel):
    """
    Qt model for cache entries table.

    Displays URLs extracted from browser caches with HTTP metadata.
    """

    # Column definitions
    COLUMNS = [
        "url",
        "domain",
        "browser",
        "response_code",
        "content_type",
        "last_seen_utc",
        "cache_filename",
        "source_path",
    ]

    HEADERS = [
        "URL",
        "Domain",
        "Browser",
        "Status",
        "Content-Type",
        "Last Used",
        "Cache File",
        "Source Path",
    ]

    # Column indexes
    COL_URL = 0
    COL_DOMAIN = 1
    COL_BROWSER = 2
    COL_STATUS = 3
    COL_CONTENT_TYPE = 4
    COL_LAST_USED = 5
    COL_CACHE_FILE = 6
    COL_SOURCE_PATH = 7

    ARTIFACT_TYPE = "cache_entry"

    # Pagination defaults
    DEFAULT_PAGE_SIZE = 500

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize cache entries model.

        Args:
            db_manager: Database manager instance
            evidence_id: Evidence ID
            evidence_label: Evidence label for database path resolution
            case_data: Optional CaseDataAccess for tagging
            parent: Parent widget
        """
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self.case_data = case_data

        # Data storage
        self._rows: List[Dict[str, Any]] = []

        # Filters
        self._browser_filter: str = ""
        self._domain_filter: str = ""
        self._content_type_filter: str = ""

        # Pagination
        self.page: int = 0
        self.page_size: int = self.DEFAULT_PAGE_SIZE
        self._total_count: int = 0

    def load(
        self,
        browser_filter: str = "",
        domain_filter: str = "",
        content_type_filter: str = "",
        reset_page: bool = True,
    ) -> None:
        """
        Load cache entries from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            domain_filter: Domain substring filter (empty = all)
            content_type_filter: Content-Type filter (empty = all)
            reset_page: If True, reset to page 0 when filters change
        """
        # Reset page when filters change
        if reset_page:
            self.page = 0

        self._browser_filter = browser_filter
        self._domain_filter = domain_filter
        self._content_type_filter = content_type_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row

                # Get total count for pagination
                self._total_count = get_cache_entry_count(
                    conn,
                    self.evidence_id,
                    domain=domain_filter or None,
                    content_type=content_type_filter or None,
                )

                # Calculate offset
                offset = self.page * self.page_size

                self._rows = get_cache_entries(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    domain=domain_filter or None,
                    content_type=content_type_filter or None,
                    limit=self.page_size,
                    offset=offset,
                )

            logger.debug(f"Loaded {len(self._rows)} cache entries (page {self.page + 1}, total {self._total_count})")

        except Exception as e:
            logger.error(f"Failed to load cache entries: {e}", exc_info=True)
            self._rows = []
            self._total_count = 0

        self.endResetModel()

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Return number of rows."""
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Return number of columns."""
        if parent.isValid():
            return 0
        return len(self.COLUMNS)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        """Return data for index and role."""
        if not index.isValid() or index.row() >= len(self._rows):
            return None

        row = self._rows[index.row()]
        col = index.column()
        col_name = self.COLUMNS[col]

        if role == Qt.DisplayRole:
            value = row.get(col_name, "")

            # Format specific columns
            if col == self.COL_URL:
                # Truncate long URLs for display
                url = str(value) if value else ""
                return url[:100] + "..." if len(url) > 100 else url

            if col == self.COL_STATUS:
                code = row.get("response_code")
                return str(code) if code else ""

            if col == self.COL_LAST_USED:
                ts = row.get("last_seen_utc") or row.get("last_used_time")
                if ts:
                    # Format timestamp, strip microseconds
                    return str(ts)[:19] if len(str(ts)) > 19 else str(ts)
                return ""

            if col == self.COL_BROWSER:
                return str(value).capitalize() if value else ""

            return str(value) if value else ""

        if role == Qt.ToolTipRole:
            if col == self.COL_URL:
                return row.get("url", "")
            if col == self.COL_SOURCE_PATH:
                return row.get("source_path", "")

        if role == Qt.UserRole:
            # Return full row data for details dialog
            return row

        if role == Qt.TextAlignmentRole:
            if col == self.COL_STATUS:
                return Qt.AlignCenter

        if role == Qt.ForegroundRole:
            # Color-code by HTTP status
            if col == self.COL_STATUS:
                code = row.get("response_code")
                if code:
                    from PySide6.QtGui import QColor
                    if 200 <= code < 300:
                        return QColor(0, 128, 0)  # Green for success
                    elif 300 <= code < 400:
                        return QColor(0, 0, 200)  # Blue for redirect
                    elif code >= 400:
                        return QColor(200, 0, 0)  # Red for errors

        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if 0 <= section < len(self.HEADERS):
                return self.HEADERS[section]
        return None

    def get_row_data(self, row: int) -> Optional[Dict[str, Any]]:
        """Get full row data by index."""
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

    def get_available_browsers(self) -> List[str]:
        """Get browsers available in current data."""
        browsers = set()
        for row in self._rows:
            browser = row.get("browser")
            if browser:
                browsers.add(browser)
        return sorted(browsers)

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for current data."""
        total = len(self._rows)
        by_status = {}
        by_type = {}

        for row in self._rows:
            code = row.get("response_code")
            if code:
                by_status[code] = by_status.get(code, 0) + 1

            ctype = row.get("content_type")
            if ctype:
                # Simplify content type (take before semicolon)
                ctype_simple = ctype.split(";")[0].strip()
                by_type[ctype_simple] = by_type.get(ctype_simple, 0) + 1

        return {
            "total": total,
            "by_status": by_status,
            "by_type": by_type,
        }

    # Pagination methods

    def total_count(self) -> int:
        """Get total count of entries matching current filters."""
        return self._total_count

    def total_pages(self) -> int:
        """Get total number of pages."""
        if self._total_count == 0:
            return 1
        return (self._total_count + self.page_size - 1) // self.page_size

    def current_page(self) -> int:
        """Get current page (0-indexed)."""
        return self.page

    def has_next_page(self) -> bool:
        """Check if there is a next page."""
        return self.page < self.total_pages() - 1

    def has_prev_page(self) -> bool:
        """Check if there is a previous page."""
        return self.page > 0

    def next_page(self) -> None:
        """Go to next page and reload."""
        if self.has_next_page():
            self.page += 1
            self.load(
                browser_filter=self._browser_filter,
                domain_filter=self._domain_filter,
                content_type_filter=self._content_type_filter,
                reset_page=False,
            )

    def prev_page(self) -> None:
        """Go to previous page and reload."""
        if self.has_prev_page():
            self.page -= 1
            self.load(
                browser_filter=self._browser_filter,
                domain_filter=self._domain_filter,
                content_type_filter=self._content_type_filter,
                reset_page=False,
            )

    def goto_page(self, page: int) -> None:
        """Go to specific page (0-indexed) and reload."""
        max_page = self.total_pages() - 1
        self.page = max(0, min(page, max_page))
        self.load(
            browser_filter=self._browser_filter,
            domain_filter=self._domain_filter,
            content_type_filter=self._content_type_filter,
            reset_page=False,
        )

    def reload(self) -> None:
        """Reload current page with current filters."""
        self.load(
            browser_filter=self._browser_filter,
            domain_filter=self._domain_filter,
            content_type_filter=self._content_type_filter,
            reset_page=False,
        )
