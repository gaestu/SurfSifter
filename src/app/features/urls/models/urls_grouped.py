"""
URLs grouped model for hierarchical domain-grouped URL view.

Two-level tree: domain groups, each with its URLs as children (issue #64).
Groups cover all URLs matching the filters and are loaded in keyset pages;
children are loaded per group, on expansion, in keyset pages. Only loaded
nodes are stored, and the model never queries the database itself except in
the synchronous ``reload`` kept for backward compatibility and tests: the
URLs tab builds requests with ``build_group_query``/``build_child_query``,
loads them in the background and applies the results here.

Group order, child order and the grouping key (domain, ASCII
case-insensitive; URLs without a domain last) are defined by the data layer
(``app.data._urls``).
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING

from PySide6.QtCore import QAbstractItemModel, QModelIndex, Qt, Signal
from PySide6.QtGui import QFont, QPalette
from PySide6.QtWidgets import QApplication

from app.data import (
    UrlCursor,
    UrlDomainChildPage,
    UrlDomainChildQuery,
    UrlDomainGroupPage,
    UrlDomainGroupQuery,
    UrlFilterSpec,
    ascii_fold,
)

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess


NO_DOMAIN_LABEL = "(no domain)"

# Load state of the top level and of each group's children.
NOT_LOADED = "not_loaded"
LOADING = "loading"
LOADED = "loaded"
FAILED = "failed"


def group_key(domain: Optional[str]) -> Optional[str]:
    """Identity of a domain group: the domain with ASCII letters lower-cased,
    like ``COLLATE NOCASE`` (``None``: the group of URLs without a domain)."""
    if domain is None:
        return None
    return ascii_fold(str(domain))


def _shows_status_row(state: str, has_more: bool) -> bool:
    """A loading, error or "load more" row follows the loaded rows."""
    return state in (LOADING, FAILED) or (state == LOADED and has_more)


class _Group:
    """A loaded domain group and its loaded child URLs."""

    __slots__ = ("domain", "key", "count", "rows", "tags", "matches", "state", "has_more", "error")

    def __init__(self, domain: Optional[str], count: int) -> None:
        self.domain = domain
        self.key = group_key(domain)
        self.count = count
        self.rows: List[Dict[str, Any]] = []
        self.tags: Dict[int, str] = {}
        self.matches: Dict[int, str] = {}
        self.state = NOT_LOADED
        self.has_more = False
        self.error = ""

    def next_cursor(self) -> Optional[UrlCursor]:
        return UrlCursor.from_row(self.rows[-1]) if self.rows else None

    def has_status_row(self) -> bool:
        return _shows_status_row(self.state, self.has_more)


class UrlsGroupedModel(QAbstractItemModel):
    headers = [
        "URL",
        "Domain",
        "Scheme",
        "Source",
        "First Seen (UTC)",
        "Last Seen (UTC)",
        "Source Path",
        "Tags",
    ]

    # Only the Tags column of URL rows is editable (issue #66).
    TAGS_COLUMN = 7

    # Keyset page sizes (issue #64).
    GROUP_PAGE_SIZE = 500
    CHILD_PAGE_SIZE = 500
    # Child URL rows kept for collapsed groups; beyond this, the children of
    # the least recently collapsed groups are dropped (reloaded on expansion).
    RETAINED_COLLAPSED_ROWS = 5000

    # Emitted after an inline Tags edit wrote to the database.
    tags_edited = Signal()

    def __init__(self, case_data: Optional[CaseDataAccess] = None) -> None:
        super().__init__()
        self.case_data = case_data
        # Same filter semantics as the flat view (issue #61).
        self._spec = UrlFilterSpec()
        self.evidence_id: Optional[int] = None
        self._groups: List[_Group] = []
        self._row_by_key: Dict[Optional[str], int] = {}
        self._state = NOT_LOADED
        self._has_more = False
        self._error = ""
        # Keys of collapsed groups holding child rows, least recent first.
        self._collapsed: "OrderedDict[Optional[str], None]" = OrderedDict()

    # Configuration ------------------------------------------------------

    def set_case_data(self, case_data: Optional[CaseDataAccess]) -> None:
        if case_data is not self.case_data:
            # Never show groups of another case (evidence ids repeat).
            self.reset_tree()
        self.case_data = case_data

    def set_evidence(self, evidence_id: Optional[int], *, reload: bool = True) -> None:
        """
        Set the evidence ID for this model.

        Args:
            evidence_id: Evidence ID to group URLs for
            reload: If True (default), immediately reload synchronously (see
                ``reload``). The URLs tab passes False and loads in the background.
        """
        if evidence_id != self.evidence_id:
            # Never show groups of a different evidence.
            self.reset_tree()
        self.evidence_id = evidence_id
        if reload:
            self.reload()

    def set_filters(
        self,
        *,
        domain: Optional[str] = None,
        domain_contains: Optional[str] = None,
        url: Optional[str] = None,
        sources: Optional[Iterable[str]] = None,
        match_filter: Optional[str] = None,
        tag: Optional[str] = None,
        reload: bool = True,
    ) -> None:
        """Update filters. Loaded nodes stay until the next ``reset_tree``;
        results requested for other filters are rejected meanwhile."""
        self._spec = self._spec.with_ui_filters(
            domain=domain,
            domain_contains=domain_contains,
            text=url,
            sources=sources,
            match_filter=match_filter,
            tag=tag,
        )
        if reload:
            self.reload()

    def filter_spec(self) -> UrlFilterSpec:
        return self._spec

    def reload(self) -> None:
        """Synchronously reload the first group page ON THE CALLING THREAD.

        Kept for backward compatibility and tests only; the URLs tab loads
        groups and children in the background (issue #64).
        """
        self.reset_tree()
        if not self.case_data or self.evidence_id is None:
            return
        query = self.begin_group_load()
        if query is not None:
            self.apply_group_page(self.case_data.load_url_domain_groups(query))

    # Top level: groups --------------------------------------------------

    def reset_tree(self) -> None:
        """Drop all groups and children (nothing loaded)."""
        self.beginResetModel()
        self._groups = []
        self._row_by_key = {}
        self._state = NOT_LOADED
        self._has_more = False
        self._error = ""
        self._collapsed.clear()
        self.endResetModel()

    def _top_has_status_row(self) -> bool:
        return _shows_status_row(self._state, self._has_more)

    def _next_group_after(self) -> Optional[str]:
        return self._groups[-1].domain if self._groups else None

    def can_load_groups(self) -> bool:
        """True if a group page can be requested now (first page, more, or retry)."""
        return self.evidence_id is not None and (
            self._state in (NOT_LOADED, FAILED) or (self._state == LOADED and self._has_more)
        )

    def build_group_query(self) -> UrlDomainGroupQuery:
        """Request for the next group page of the current filters."""
        if self.evidence_id is None:
            raise ValueError("No evidence selected")
        return UrlDomainGroupQuery(
            evidence_id=int(self.evidence_id),
            filters=self._spec,
            after=self._next_group_after(),
            page_size=self.GROUP_PAGE_SIZE,
        )

    def begin_group_load(self) -> Optional[UrlDomainGroupQuery]:
        """Show the loading row and return the next group page request
        (None if no group page can be requested)."""
        if not self.can_load_groups():
            return None
        self._set_top_state(LOADING)
        return self.build_group_query()

    def is_current_group_query(self, query: UrlDomainGroupQuery) -> bool:
        return (
            self._state == LOADING
            and query.evidence_id == self.evidence_id
            and query.filters == self._spec
            and query.after == self._next_group_after()
        )

    def apply_group_page(self, page: UrlDomainGroupPage) -> bool:
        """Append a group page prepared off the GUI thread; False if outdated."""
        if not self.is_current_group_query(page.query):
            return False
        # Keyset pages never overlap; the check only guards the key index.
        groups = [_Group(g.domain, g.count) for g in page.groups if group_key(g.domain) not in self._row_by_key]
        if groups:
            first = len(self._groups)
            self.beginInsertRows(QModelIndex(), first, first + len(groups) - 1)
            for offset, group in enumerate(groups):
                self._row_by_key[group.key] = first + offset
            self._groups.extend(groups)
            self.endInsertRows()
        self._has_more = page.has_more
        self._set_top_state(LOADED)
        return True

    def fail_group_load(self, error: str) -> None:
        if self._state == LOADING:
            self._error = error
            self._set_top_state(FAILED)

    def _set_top_state(self, state: str) -> None:
        def apply() -> None:
            self._state = state

        self._change_status_row(
            QModelIndex(), len(self._groups), self._top_has_status_row(),
            _shows_status_row(state, self._has_more), apply,
        )

    def _change_status_row(self, parent: QModelIndex, position: int, had: bool, has: bool, apply) -> None:
        """Run the state change ``apply`` with the notifications for the status
        row at ``position`` under ``parent``: removed, inserted or refreshed."""
        if had and not has:
            self.beginRemoveRows(parent, position, position)
            apply()
            self.endRemoveRows()
        elif has and not had:
            self.beginInsertRows(parent, position, position)
            apply()
            self.endInsertRows()
        else:
            apply()
            if has:
                self.dataChanged.emit(
                    self.index(position, 0, parent), self.index(position, len(self.headers) - 1, parent)
                )

    def state(self) -> str:
        """Load state of the group list."""
        return self._state

    def error(self) -> str:
        return self._error

    def has_more_groups(self) -> bool:
        return self._has_more

    def group_count(self) -> int:
        """Number of loaded groups."""
        return len(self._groups)

    def group_domain(self, row: int) -> Optional[str]:
        return self._groups[row].domain

    def group_url_count(self, row: int) -> int:
        return self._groups[row].count

    def group_row(self, key: Optional[str]) -> Optional[int]:
        """Row of the loaded group with ``key`` (see ``group_key``), if any."""
        return self._row_by_key.get(key)

    # Children -----------------------------------------------------------

    def group_state(self, row: int) -> str:
        return self._groups[row].state

    def group_has_more(self, row: int) -> bool:
        return self._groups[row].has_more

    def loaded_url_count(self, row: int) -> int:
        return len(self._groups[row].rows)

    def can_load_children(self, row: int) -> bool:
        """True if a child page of group ``row`` can be requested now."""
        if not (0 <= row < len(self._groups)):
            return False
        group = self._groups[row]
        return group.state in (NOT_LOADED, FAILED) or (group.state == LOADED and group.has_more)

    def begin_child_load(self, row: int) -> bool:
        """Show the loading row of group ``row``; False if nothing can be loaded."""
        if not self.can_load_children(row):
            return False
        self._set_group_state(row, LOADING)
        return True

    def build_child_query(self, row: int) -> UrlDomainChildQuery:
        """Request for the next child page of group ``row``."""
        if self.evidence_id is None:
            raise ValueError("No evidence selected")
        group = self._groups[row]
        return UrlDomainChildQuery(
            evidence_id=int(self.evidence_id),
            domain=group.domain,
            filters=self._spec,
            after=group.next_cursor(),
            page_size=self.CHILD_PAGE_SIZE,
        )

    def is_current_child_query(self, query: UrlDomainChildQuery) -> bool:
        row = self._row_by_key.get(group_key(query.domain))
        if row is None:
            return False
        group = self._groups[row]
        return (
            group.state == LOADING
            and query.evidence_id == self.evidence_id
            and query.filters == self._spec
            and query.after == group.next_cursor()
        )

    def apply_child_page(self, page: UrlDomainChildPage) -> bool:
        """Append a child page prepared off the GUI thread; False if outdated."""
        if not self.is_current_child_query(page.query):
            return False
        row = self._row_by_key[group_key(page.query.domain)]
        group = self._groups[row]
        if page.rows:
            parent = self.index(row, 0)
            first = len(group.rows)
            # Inserted before the status row, which stays last.
            self.beginInsertRows(parent, first, first + len(page.rows) - 1)
            group.rows.extend(page.rows)
            group.tags.update(page.tags)
            group.matches.update(page.matches)
            self.endInsertRows()
        group.has_more = page.has_more
        self._set_group_state(row, LOADED)
        return True

    def fail_child_load(self, domain: Optional[str], error: str) -> None:
        row = self._row_by_key.get(group_key(domain))
        if row is not None and self._groups[row].state == LOADING:
            self._groups[row].error = error
            self._set_group_state(row, FAILED)

    def cancel_child_load(self, row: int) -> None:
        """Undo ``begin_child_load`` for a request that will not run."""
        group = self._groups[row]
        if group.state == LOADING:
            self._set_group_state(row, LOADED if group.rows else NOT_LOADED)

    def _set_group_state(self, row: int, state: str) -> None:
        group = self._groups[row]

        def apply() -> None:
            group.state = state

        self._change_status_row(
            self.index(row, 0), len(group.rows), group.has_status_row(),
            _shows_status_row(state, group.has_more), apply,
        )

    def _unload_children(self, row: int) -> None:
        group = self._groups[row]
        count = len(group.rows) + (1 if group.has_status_row() else 0)
        if count:
            self.beginRemoveRows(self.index(row, 0), 0, count - 1)
        group.rows = []
        group.tags = {}
        group.matches = {}
        group.state = NOT_LOADED
        group.has_more = False
        group.error = ""
        if count:
            self.endRemoveRows()

    # Retention of collapsed groups' children ----------------------------

    def group_expanded(self, row: int) -> None:
        if 0 <= row < len(self._groups):
            self._collapsed.pop(self._groups[row].key, None)

    def group_collapsed(self, row: int) -> None:
        """Remember a collapsed group; drop the oldest retained children over the bound."""
        if not (0 <= row < len(self._groups)):
            return
        key = self._groups[row].key
        self._collapsed.pop(key, None)
        self._collapsed[key] = None
        self.trim_collapsed()

    def trim_collapsed(self) -> None:
        """Drop children of least recently collapsed groups beyond ``RETAINED_COLLAPSED_ROWS``.

        Groups whose next page is loading keep their rows (the page is still
        applied); the group and its count always stay.
        """
        retained = 0
        rows_by_key = []
        for key in self._collapsed:
            row = self._row_by_key.get(key)
            if row is not None:
                count = len(self._groups[row].rows)
                retained += count
                rows_by_key.append((key, row, count))
        for key, row, count in rows_by_key:
            if retained <= self.RETAINED_COLLAPSED_ROWS:
                break
            if self._groups[row].state == LOADING:
                continue
            self._unload_children(row)
            del self._collapsed[key]
            retained -= count
        for key in [k for k in self._collapsed if self._row_by_key.get(k) is None]:
            del self._collapsed[key]

    def retained_collapsed_rows(self) -> int:
        """Child URL rows currently kept for collapsed groups."""
        return sum(
            len(self._groups[row].rows)
            for row in (self._row_by_key.get(key) for key in self._collapsed)
            if row is not None
        )

    # Node helpers -------------------------------------------------------

    def is_group_index(self, index: QModelIndex) -> bool:
        return index.isValid() and index.internalId() == 0 and index.row() < len(self._groups)

    def is_url_index(self, index: QModelIndex) -> bool:
        return self._url_location(index) is not None

    def is_status_index(self, index: QModelIndex) -> bool:
        """True for a loading / load more / error row (top level or in a group)."""
        if not index.isValid():
            return False
        if index.internalId() == 0:
            return index.row() == len(self._groups) and self._top_has_status_row()
        group = self._group_of_child(index)
        return group is not None and index.row() == len(group.rows) and group.has_status_row()

    def _group_of_child(self, index: QModelIndex) -> Optional[_Group]:
        group_row = index.internalId() - 1
        if 0 <= group_row < len(self._groups):
            return self._groups[group_row]
        return None

    def _url_location(self, index: QModelIndex):
        if not index.isValid() or index.internalId() == 0:
            return None
        group = self._group_of_child(index)
        if group is None or not (0 <= index.row() < len(group.rows)):
            return None
        return group, group.rows[index.row()]

    def url_row(self, index: QModelIndex) -> Optional[Dict[str, Any]]:
        """URL row dict of a child index (None for group and status rows)."""
        location = self._url_location(index)
        return location[1] if location else None

    def url_tags(self, index: QModelIndex) -> str:
        location = self._url_location(index)
        if location is None:
            return ""
        group, row = location
        return group.tags.get(row.get("id"), "") or ""

    def _status_text(self, index: QModelIndex) -> str:
        if index.internalId() == 0:
            # Loading texts must not depend on the loaded rows: rows are
            # inserted before the status row while it shows them.
            if self._state == LOADING:
                return "Loading domains…"
            if self._state == FAILED:
                return f"Could not load domains: {self._error} (double-click to retry)"
            return f"Load {self.GROUP_PAGE_SIZE:,} more domains… ({len(self._groups):,} shown)"
        group = self._group_of_child(index)
        if group.state == LOADING:
            return "Loading URLs…"
        if group.state == FAILED:
            return f"Could not load URLs: {group.error} (double-click to retry)"
        return (
            f"Load {self.CHILD_PAGE_SIZE:,} more URLs… "
            f"({len(group.rows):,} of {group.count:,} shown)"
        )

    # Qt interface -------------------------------------------------------

    def index(self, row: int, column: int, parent: QModelIndex = QModelIndex()) -> QModelIndex:  # noqa: A003
        if row < 0 or not (0 <= column < len(self.headers)):
            return QModelIndex()
        if not parent.isValid():
            if row >= len(self._groups) + (1 if self._top_has_status_row() else 0):
                return QModelIndex()
            return self.createIndex(row, column, 0)
        if not self.is_group_index(parent):
            return QModelIndex()
        group = self._groups[parent.row()]
        if row >= len(group.rows) + (1 if group.has_status_row() else 0):
            return QModelIndex()
        return self.createIndex(row, column, parent.row() + 1)

    def parent(self, index: QModelIndex = QModelIndex()) -> QModelIndex:  # type: ignore[override]
        if not index.isValid() or index.internalId() == 0:
            return QModelIndex()
        return self.createIndex(index.internalId() - 1, 0, 0)

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        if not parent.isValid():
            return len(self._groups) + (1 if self._top_has_status_row() else 0)
        if parent.column() != 0 or not self.is_group_index(parent):
            return 0
        group = self._groups[parent.row()]
        return len(group.rows) + (1 if group.has_status_row() else 0)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        return len(self.headers)

    def hasChildren(self, parent: QModelIndex = QModelIndex()) -> bool:  # noqa: N802
        if not parent.isValid():
            return self.rowCount() > 0
        if parent.column() != 0 or not self.is_group_index(parent):
            return False
        group = self._groups[parent.row()]
        # Expandable before its children are loaded.
        return group.count > 0 or bool(group.rows) or group.has_status_row()

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:  # noqa: N802
        if role == Qt.DisplayRole and orientation == Qt.Horizontal and 0 <= section < len(self.headers):
            return self.headers[section]
        return None

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        if not index.isValid():
            return None
        column = index.column()
        if self.is_group_index(index):
            if role == Qt.DisplayRole and column == 0:
                group = self._groups[index.row()]
                label = NO_DOMAIN_LABEL if group.domain is None else str(group.domain)
                return f"{label} ({group.count:,})"
            return None
        if self.is_status_index(index):
            if column != 0:
                return None
            if role in (Qt.DisplayRole, Qt.ToolTipRole):
                return self._status_text(index)
            if role == Qt.FontRole:
                font = QFont()
                font.setItalic(True)
                return font
            if role == Qt.ForegroundRole:
                app = QApplication.instance()
                if app is not None:
                    return app.palette().brush(QPalette.Disabled, QPalette.Text)
            return None
        location = self._url_location(index)
        if location is None:
            return None
        group, row = location
        if role in (Qt.DisplayRole, Qt.EditRole):
            if column == self.TAGS_COLUMN:
                return group.tags.get(row.get("id"), "") or ""
            key = (
                "url", "domain", "scheme", "discovered_by",
                "first_seen_utc", "last_seen_utc", "source_path",
            )[column]
            value = row.get(key)
            return "" if value is None else str(value)
        if role == Qt.UserRole:
            return row.get("id")
        if role == Qt.UserRole + 1 and column == 0:
            return row
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:
        if not index.isValid():
            return Qt.NoItemFlags
        flags = Qt.ItemIsEnabled | Qt.ItemIsSelectable
        if index.column() == self.TAGS_COLUMN and self.is_url_index(index):
            flags |= Qt.ItemIsEditable
        return flags

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.EditRole) -> bool:  # noqa: N802
        if (
            role != Qt.EditRole
            or index.column() != self.TAGS_COLUMN
            or not self.case_data
            or self.evidence_id is None
        ):
            return False
        location = self._url_location(index)
        if location is None:
            return False
        group, row = location
        url_id = row.get("id")
        if not url_id:
            return False
        tags = str(value).strip()
        if tags == (group.tags.get(url_id, "") or ""):
            return False  # Editor closed without a change
        self.case_data.update_url_tags(int(self.evidence_id), int(url_id), tags)
        group.tags[url_id] = tags
        self.dataChanged.emit(index, index, [Qt.DisplayRole, Qt.EditRole])
        self.tags_edited.emit()
        return True
