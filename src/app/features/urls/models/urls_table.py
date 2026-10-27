"""
URLs table model for flat URL listing.

Provides keyset-paginated, filterable URL listing. Tag/match strings come from
batch-loaded caches; display methods never query the database (issue #60).
The exact total is loaded separately and cached per evidence, filter spec and
data revision (issue #61).
"""

from __future__ import annotations

import csv
from pathlib import Path
from dataclasses import asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple, TYPE_CHECKING

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt, Signal

from app.data import UrlCursor, UrlFilterSpec, UrlPage, UrlPageQuery

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess


class UrlsTableModel(QAbstractTableModel):
    headers = [
        "URL",
        "Domain",
        "Scheme",
        "Source",
        "First Seen (UTC)",
        "Last Seen (UTC)",
        "Source Path",
        "Occurrences",  # Duplicate count after deduplication
        "Tags",
        "Match",  #
    ]

    # Only the Tags column is editable inline (issue #66).
    TAGS_COLUMN = 8

    # Emitted after tags were changed by an inline edit; tag-filtered totals
    # are then outdated (issue #66).
    tags_edited = Signal()

    # Issue #61: keyset pages; small default page, user-selectable sizes.
    PAGE_SIZE_CHOICES = (100, 500, 1000, 5000, 10000)
    PAGE_SIZE = 500
    # Rows per keyset batch when exporting (independent of the page size).
    EXPORT_BATCH_SIZE = 10000

    def __init__(self, case_data: Optional[CaseDataAccess] = None, page_size: Optional[int] = None) -> None:
        super().__init__()
        self.case_data = case_data
        self.evidence_id: Optional[int] = None
        self.page_size = page_size or self.PAGE_SIZE
        self.page = 0
        # Start cursor of every page from 0 to the current one (None = start).
        self._page_starts: List[Optional[UrlCursor]] = [None]
        self._has_more = False
        self._rows: List[Dict[str, Any]] = []
        self._matches_cache: Dict[int, str] = {}  # url_id -> matched_lists string (batch-loaded)
        self._tags_cache: Dict[int, str] = {}  # url_id -> tags string (batch-loaded)
        self._spec = UrlFilterSpec()
        # Exact totals keyed by (evidence_id, filter spec, data revision).
        self._data_revision = 0
        self._count_cache: Dict[Tuple[Any, ...], int] = {}
        # PRAGMA data_version of the last loaded page; a change means another
        # connection wrote to the evidence DB (e.g. tag merge in the Tags tab).
        self._data_version: Optional[int] = None
        # Background writes the tab started itself (URL search index build,
        # issue #62) change data_version without changing URL data. While they
        # run, a data_version change is set aside instead of hiding row
        # numbers on every page; a set-aside change is applied once, on the
        # first page after they ended, since a real unnotified write (e.g. a
        # tag merge in the Tags tab) may have happened meanwhile.
        self._own_writes_active = False
        self._own_writes_settling = False
        self._own_writes_change_seen = False
        # False after an unnotified change was detected away from page 1:
        # earlier pages may have changed, so page * page_size is no longer
        # the true row position. Exact again once back on page 1.
        self._row_numbers_exact = True

    # Qt interface -------------------------------------------------------

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        return len(self.headers)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:  # noqa: N802
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None
        row = self._rows[index.row()]
        column = index.column()
        if role in (Qt.DisplayRole, Qt.EditRole):
            if column == 0:
                return row.get("url") or ""
            if column == 1:
                return row.get("domain") or ""
            if column == 2:
                return row.get("scheme") or ""
            if column == 3:
                return row.get("discovered_by") or ""
            if column == 4:
                return row.get("first_seen_utc") or ""
            if column == 5:
                return row.get("last_seen_utc") or ""
            if column == 6:
                # Truncate long source_path lists in display
                source_path = row.get("source_path") or ""
                if source_path and ", " in source_path:
                    paths = source_path.split(", ")
                    if len(paths) > 3:
                        return f"{paths[0]}, {paths[1]}, {paths[2]}, … (+{len(paths) - 3} more)"
                return source_path
            if column == 7:  # Occurrences column
                count = row.get("occurrence_count") or 1
                return str(count) if count > 1 else ""
            if column == 8:  # Tags column (batch-loaded cache)
                return self._get_tags(row.get("id"))
            if column == 9:  # Match column (batch-loaded cache)
                return self._get_matches(row.get("id"))
        return None

    def _get_tags(self, url_id: Optional[int]) -> str:
        """Return cached tags for a URL ("" if none). Never queries the database."""
        if not url_id:
            return ""
        return self._tags_cache.get(url_id) or ""

    def _get_matches(self, url_id: Optional[int]) -> str:
        """Return cached matched list names for a URL ("—" if none). Never queries the database."""
        if not url_id:
            return "—"
        return self._matches_cache.get(url_id) or "—"

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:  # noqa: N802
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            if 0 <= section < len(self.headers):
                return self.headers[section]
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:  # noqa: N802
        base = super().flags(index)
        if index.isValid() and index.column() == self.TAGS_COLUMN:
            return base | Qt.ItemIsEditable
        return base

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.EditRole) -> bool:  # noqa: N802
        if (
            role != Qt.EditRole
            or not index.isValid()
            or index.column() != self.TAGS_COLUMN
            or not (0 <= index.row() < len(self._rows))
            or not self.case_data
            or self.evidence_id is None
        ):
            return False
        row = self._rows[index.row()]
        url_id = row.get("id")
        if not url_id:
            return False
        tags = str(value).strip()
        if tags == self._get_tags(url_id):
            return False  # Editor closed without a change
        self.case_data.update_url_tags(int(self.evidence_id), int(url_id), tags)
        # Update tags cache instead of row dict (tags no longer in iter_urls result)
        self._tags_cache[url_id] = tags
        self.dataChanged.emit(index, index, [Qt.DisplayRole, Qt.EditRole])
        self.tags_edited.emit()
        return True

    # Data loading -------------------------------------------------------

    def set_case_data(self, case_data: Optional[CaseDataAccess]) -> None:
        self.case_data = case_data
        self.evidence_id = None
        self._count_cache.clear()
        # data_version is per connection; a new case means new connections.
        self._data_version = None
        self._own_writes_settling = self._own_writes_change_seen = False
        self._reset_position()
        self._clear_rows()

    def set_evidence(self, evidence_id: Optional[int], *, reload: bool = True) -> None:
        """
        Set the evidence ID for this model.

        Args:
            evidence_id: Evidence ID to load URLs for
            reload: If True (default), immediately reload data. Set to False for
                   deferred loading where reload will be triggered later.
        """
        if evidence_id != self.evidence_id:
            # Never show rows/caches of a different evidence.
            self._clear_rows()
            self._data_version = None
            self._own_writes_settling = self._own_writes_change_seen = False
        self.evidence_id = evidence_id
        self._reset_position()
        if reload:
            self.reload()

    def _clear_rows(self) -> None:
        self.beginResetModel()
        self._rows = []
        self._has_more = False
        self._tags_cache.clear()
        self._matches_cache.clear()
        self.endResetModel()

    def clear_page(self) -> None:
        """Show no rows and return to the first page (e.g. after a load error)."""
        self._reset_position()
        self._clear_rows()

    def _reset_position(self) -> None:
        """Return to the first page; cursors of other pages are discarded."""
        self.page = 0
        self._page_starts = [None]
        self._row_numbers_exact = True

    def set_filters(
        self,
        *,
        domain: Optional[str] = None,
        domain_contains: Optional[str] = None,
        text: Optional[str] = None,
        exclude_text: Optional[str] = None,
        sources: Optional[Iterable[str]] = None,
        match_filter: Optional[str] = None,  #
        tag: Optional[str] = None,  #
        untagged: Optional[bool] = None,
        reload: bool = True,  # Allow setting filters without immediate reload
    ) -> None:
        """
        Update filters for URL listing and return to the first page.

        Args:
            domain: Exact domain, ASCII case-insensitive ("" or "*" = all)
            domain_contains: Literal domain substring, ASCII case-insensitive
                ("" = off; % and _ are not wildcards)
            text: URL text filter (substring)
            exclude_text: Hide URLs containing this literal substring ("" = off)
            sources: List of source filters
            match_filter: Match filter ("all", "matched", "unmatched", or list name)
            tag: Tag filter pattern
            untagged: Only URLs without a tag
            reload: If True (default), immediately reload data. Set to False when
                   loading in the background (see ``build_query``).
        """
        self._spec = self._spec.with_ui_filters(
            domain=domain,
            domain_contains=domain_contains,
            text=text,
            exclude_text=exclude_text,
            sources=sources,
            match_filter=match_filter,
            tag=tag,
            untagged=untagged,
        )
        self._reset_position()
        # Tag/match caches are replaced wholesale when the next page arrives;
        # clearing them here would make the still-visible rows render blank.
        if reload:
            self.reload()

    def get_filters(self) -> Dict[str, Any]:
        """Get current filter settings."""
        return asdict(self._spec)

    def filter_spec(self) -> UrlFilterSpec:
        """Current filters as used for row and count queries."""
        return self._spec

    def set_page_size(self, page_size: int) -> bool:
        """Change rows per page; returns True (and resets to page 1) if it changed."""
        page_size = max(1, int(page_size))
        if page_size == self.page_size:
            return False
        self.page_size = page_size
        self._reset_position()
        return True

    def mark_data_changed(self, *, keep_position: bool) -> None:
        """Invalidate cached totals after a data change.

        Args:
            keep_position: Keep the current page cursor (e.g. after tagging the
                visible rows); otherwise return to the first page, since
                earlier pages may have changed (ingestion, deduplication,
                list matching).
        """
        self._data_revision += 1
        self._count_cache.clear()
        # Already invalidated: the next page only sets a new baseline.
        self._data_version = None
        if not keep_position:
            self._reset_position()

    def begin_own_writes(self) -> None:
        """The tab started background writes that do not change URL data (#62)."""
        self._own_writes_active = True

    def end_own_writes(self) -> None:
        """Those writes ended; the next loaded page applies a set-aside change."""
        if self._own_writes_active:
            self._own_writes_active = False
            self._own_writes_settling = True

    def has_set_aside_change(self) -> bool:
        """True if a data_version change was set aside and not applied yet."""
        return self._own_writes_change_seen

    def _cursor_for_page(self, page: int) -> Optional[UrlCursor]:
        if 0 <= page < len(self._page_starts):
            return self._page_starts[page]
        if page == self.page + 1 and self._has_more and self._rows:
            return UrlCursor.from_row(self._rows[-1])
        raise ValueError(f"URL page {page} is not reachable from page {self.page}")

    def build_query(self, page: Optional[int] = None) -> UrlPageQuery:
        """Build the background-load request for the current filters.

        Args:
            page: Page index to request (defaults to the current page). Only
                visited pages and the page after the current one are reachable.
        """
        if self.evidence_id is None:
            raise ValueError("No evidence selected")
        page = self.page if page is None else max(0, page)
        return UrlPageQuery(
            evidence_id=int(self.evidence_id),
            filters=self._spec,
            page=page,
            page_size=self.page_size,
            after=self._cursor_for_page(page),
            data_revision=self._data_revision,
        )

    def is_current_query(self, query: UrlPageQuery) -> bool:
        """True if ``query`` still matches the model's filters, page size,
        data revision and cursor stack (a page built for it can be applied)."""
        if (
            query.evidence_id != self.evidence_id
            or query.filters != self._spec
            or query.page_size != self.page_size
            or query.data_revision != self._data_revision
        ):
            return False
        try:
            return self._cursor_for_page(query.page) == query.after
        except ValueError:
            return False

    def set_loaded_page(self, page: UrlPage) -> bool:
        """Apply a page prepared off the GUI thread (no database access).

        Returns:
            True if the page applied a write this model was not told about
            (``PRAGMA data_version`` changed), e.g. a tag merge through
            another connection. A change that was set aside while the tab's
            own writes ran is reported when it is applied after them, because
            it may include writes made elsewhere meanwhile (issue #63).
        """
        query = page.query
        # Only a page requested at the current data revision may define the total.
        fresh = query.data_revision == self._data_revision
        changed = (
            page.data_version is not None
            and self._data_version is not None
            and page.data_version != self._data_version
        )
        if changed and (self._own_writes_active or self._own_writes_settling):
            self._own_writes_change_seen = True
            changed = False
        if self._own_writes_settling and not self._own_writes_active:
            self._own_writes_settling = False
            changed = changed or self._own_writes_change_seen
            self._own_writes_change_seen = False
        if changed:
            # Written by a connection that did not notify us: cached totals
            # may be stale. Rows of this page are fresh, so keep the position
            # (unrelated writes, e.g. a running extraction, also land here),
            # but earlier pages may have changed: stop claiming row numbers.
            self.mark_data_changed(keep_position=True)
            self._row_numbers_exact = False
        if page.data_version is not None:
            self._data_version = page.data_version
        if query.page == 0:
            self._row_numbers_exact = True
        if self._shows_page_content(page):
            # Same page with identical content (e.g. the re-check when the tab
            # is shown again, issue #67): no model reset, so the view keeps
            # its selection and scroll position.
            self._page_starts = self._page_starts[: query.page + 1]
        else:
            self.beginResetModel()
            self.page = query.page
            self._page_starts = self._page_starts[: query.page] + [query.after]
            self._rows = list(page.rows)
            self._has_more = page.has_more
            self._tags_cache = dict(page.tags)
            self._matches_cache = dict(page.matches)
            self.endResetModel()
        # A first page without a next page holds the complete result.
        if (
            fresh
            and query.page == 0
            and not page.has_more
            and query.evidence_id == self.evidence_id
            and query.filters == self._spec
        ):
            self.store_total_count(self.count_key(), len(page.rows))
        return changed

    def _shows_page_content(self, page: UrlPage) -> bool:
        """True if ``page`` is the current page and has exactly the shown content."""
        query = page.query
        return (
            query.evidence_id == self.evidence_id
            and query.page == self.page
            and self._page_starts[query.page : query.page + 1] == [query.after]
            and page.has_more == self._has_more
            and page.rows == self._rows
            and page.tags == self._tags_cache
            and page.matches == self._matches_cache
        )

    def reload(self) -> None:
        """Synchronously reload the current page ON THE CALLING THREAD.

        Kept for backward compatibility and tests only. GUI code must not use
        it (nor ``reload=True`` on ``set_evidence``/``set_filters``); the URLs
        tab loads pages and counts in the background (issues #60, #61).
        """
        if not self.case_data or self.evidence_id is None:
            self._clear_rows()
            return
        key = self.count_key()
        if key not in self._count_cache:
            self.store_total_count(key, self.case_data.count_urls(int(self.evidence_id), filters=self._spec))
        self.set_loaded_page(self.case_data.load_url_page(self.build_query()))

    # Exact totals (issue #61) -------------------------------------------

    def count_key(self) -> Tuple[Any, ...]:
        """Cache key for the exact total of the current filters and data."""
        return (self.evidence_id, self._spec, self._data_revision)

    def store_total_count(self, key: Tuple[Any, ...], count: int) -> None:
        """Remember an exact total (ignored if ``key`` is outdated)."""
        if key == self.count_key():
            self._count_cache[key] = int(count)

    def total_count(self) -> Optional[int]:
        """Exact total of URLs matching the current filters, or None while unknown."""
        return self._count_cache.get(self.count_key())

    def has_next_page(self) -> bool:
        return self._has_more

    def has_previous_page(self) -> bool:
        return self.page > 0

    def current_page(self) -> int:
        return self.page

    def has_data_baseline(self) -> bool:
        """True once a page set the data_version used to detect unnotified writes."""
        return self._data_version is not None

    def row_numbers_exact(self) -> bool:
        """False if earlier pages may have changed since they were walked."""
        return self._row_numbers_exact

    def first_row_number(self) -> int:
        """1-based number of the first row on the current page."""
        return self.page * self.page_size + 1

    def export_to_csv(self, output_path: Path) -> None:
        if not self.case_data or self.evidence_id is None:
            return
        # Issue #61: keyset batches; cost per batch does not grow with depth.
        rows: List[Dict[str, Any]] = list(
            self.case_data.iter_url_rows(
                int(self.evidence_id), self._spec, batch_size=self.EXPORT_BATCH_SIZE
            )
        )

        rows_sorted = sorted(
            rows,
            key=lambda row: (
                row.get("first_seen_utc") or row.get("last_seen_utc") or "\uffff",
                row.get("url") or "",
                row.get("id") or 0,
            ),
        )

        # Issue #47: Batch load all tags and matches upfront instead of N+1 queries
        evidence_id = int(self.evidence_id)
        all_url_ids = [row["id"] for row in rows_sorted if row.get("id")]
        tags_batch = self.case_data.get_tag_strings_for_artifacts(
            evidence_id, "url", all_url_ids
        ) if all_url_ids else {}
        matches_batch = self.case_data.get_url_matches_batch(
            evidence_id, all_url_ids
        ) if all_url_ids else {}

        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
            writer.writerow(self.headers)
            for row in rows_sorted:
                url_id = row.get("id")
                # Use batch-loaded tags and matches
                tags = tags_batch.get(url_id, "") if url_id else ""
                matched_lists = matches_batch.get(url_id, "") if url_id else ""

                writer.writerow(
                    [
                        row.get("url", ""),
                        row.get("domain", ""),
                        row.get("scheme", ""),
                        row.get("discovered_by", ""),
                        row.get("first_seen_utc", ""),
                        row.get("last_seen_utc", ""),
                        row.get("source_path", ""),
                        row.get("occurrence_count") or 1,  # Occurrence count
                        tags,  # batch-loaded tags
                        matched_lists or "—",  # batch-loaded matches
                    ]
                )

    def get_row(self, row: int) -> Optional[Dict[str, Any]]:
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None
