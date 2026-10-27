"""File list query operations for UI layer (issue #73).

This module provides the File List tab's queries:
- One shared filter builder for the row and count queries
- Literal Search / Exclude on file name or path
- Tag filters (exact name, any tag, untagged) via tag_associations
- Matches column and filter dropdown values

Moved out of ``app.features.file_list`` (the model and the refresh worker
held two copies of the filter SQL).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from core.database.helpers.sql_utils import LIKE_ESCAPE_CHAR, escape_like

from ._base import BaseDataAccess

logger = logging.getLogger(__name__)

# Row order of the File List tab. The id tie-break keeps the order (and so
# OFFSET pages) deterministic when one path exists on several partitions.
# Without planner statistics (the app never runs ANALYZE) SQLite sorts the
# evidence's matching rows for each page, as it did before the tie-break.
FILE_LIST_ORDER_BY = "fl.file_path, fl.id"

# ``FileListFilterSpec.match_list`` value selecting files matched by any list.
FILE_LIST_ANY_MATCH = "any"

_FILE_LIST_ROW_COLUMNS = (
    "fl.id, fl.file_path, fl.file_name, fl.extension, fl.size_bytes, "
    "fl.modified_ts, fl.deleted, fl.partition_index"
)

# Association to an existing tag of this evidence: a non-empty Tags column.
# CROSS JOIN keeps the per-file seek on idx_tag_assoc_artifact_evidence first
# (as for URLs, issue #72).
_HAS_TAG_SUBQUERY = """
    SELECT 1 FROM tag_associations ta
    CROSS JOIN tags t ON ta.tag_id = t.id
    WHERE ta.artifact_type = 'file_list'
    AND ta.artifact_id = fl.id
    AND ta.evidence_id = fl.evidence_id
    AND t.evidence_id = fl.evidence_id
"""

_NAME_OR_PATH_LIKE = (
    f"(fl.file_name LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}' "
    f"OR fl.file_path LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}')"
)


@dataclass(frozen=True)
class FileListFilterSpec:
    """Filters for File List rows and counts (issue #73).

    Row and count queries are built from the same spec by
    ``build_file_list_filter_predicates`` so they always agree.

    Attributes:
        extension: Exact extension.
        size_min: Minimum size in bytes (inclusive).
        size_max: Maximum size in bytes (inclusive).
        deleted: ``"all"``, ``"show_only"`` or ``"hide"``.
        match_list: Reference list name, ``FILE_LIST_ANY_MATCH`` for any
            list, or ``None`` for all files.
        tag_name: Exact tag name (``tags.name``, case-sensitive). As before
            issue #73 it does not check the evidence of the association or
            the tag.
        any_tag: Only files with a tag, i.e. whose Tags column is not empty.
        untagged: Only files without a tag, i.e. whose Tags column is empty.
            Exact complement of ``any_tag``; combined with ``any_tag``
            nothing matches. Combined with ``tag_name`` only files tagged
            with another evidence's tag of that name match; the tab never
            combines the two.
        search: Literal substring of the file name or path, ASCII
            case-insensitive; ``%``, ``_`` and ``\\`` are not wildcards.
        exclude: Literal substring neither the file name nor the path may
            contain, with the same matching as ``search``.
    """

    extension: Optional[str] = None
    size_min: Optional[int] = None
    size_max: Optional[int] = None
    deleted: str = "all"
    match_list: Optional[str] = None
    tag_name: Optional[str] = None
    any_tag: bool = False
    untagged: bool = False
    search: Optional[str] = None
    exclude: Optional[str] = None

    def __post_init__(self) -> None:
        # Normalize so equal filters compare equally.
        for name in ("extension", "match_list", "tag_name", "search", "exclude"):
            if getattr(self, name) == "":
                object.__setattr__(self, name, None)
        if self.deleted not in ("show_only", "hide"):
            object.__setattr__(self, "deleted", "all")
        object.__setattr__(self, "any_tag", bool(self.any_tag))
        object.__setattr__(self, "untagged", bool(self.untagged))

    @property
    def tag_filtered(self) -> bool:
        """True if any tag filter is active."""
        return self.tag_name is not None or self.any_tag or self.untagged


def build_file_list_filter_predicates(
    evidence_id: int, spec: FileListFilterSpec
) -> Tuple[List[str], List[Any]]:
    """Build parameterized WHERE terms (alias ``fl``) for ``spec``.

    Shared by row and count queries so both always select the same files.
    """
    params: List[Any] = [evidence_id]
    where = ["fl.evidence_id = ?"]

    if spec.extension is not None:
        where.append("fl.extension = ?")
        params.append(spec.extension)
    if spec.size_min is not None:
        where.append("fl.size_bytes >= ?")
        params.append(spec.size_min)
    if spec.size_max is not None:
        where.append("fl.size_bytes <= ?")
        params.append(spec.size_max)
    if spec.deleted == "show_only":
        where.append("fl.deleted = 1")
    elif spec.deleted == "hide":
        where.append("fl.deleted = 0")

    if spec.match_list is not None:
        if spec.match_list == FILE_LIST_ANY_MATCH:
            where.append("EXISTS (SELECT 1 FROM file_list_matches m WHERE m.file_list_id = fl.id)")
        else:
            where.append(
                "EXISTS (SELECT 1 FROM file_list_matches m "
                "WHERE m.file_list_id = fl.id AND m.reference_list_name = ?)"
            )
            params.append(spec.match_list)

    if spec.tag_name is not None:
        where.append("""
            EXISTS (
                SELECT 1 FROM tag_associations ta
                JOIN tags t ON ta.tag_id = t.id
                WHERE ta.artifact_type = 'file_list'
                AND ta.artifact_id = fl.id
                AND t.name = ?
            )
        """)
        params.append(spec.tag_name)
    if spec.any_tag:
        where.append(f"EXISTS ({_HAS_TAG_SUBQUERY})")
    if spec.untagged:
        where.append(f"NOT EXISTS ({_HAS_TAG_SUBQUERY})")

    if spec.search is not None:
        where.append(_NAME_OR_PATH_LIKE)
        pattern = f"%{escape_like(spec.search)}%"
        params.extend([pattern, pattern])
    if spec.exclude is not None:
        # file_name and file_path are NOT NULL, so NOT never drops a row
        # through NULL: Search X and Exclude X add up to all files.
        where.append(f"NOT {_NAME_OR_PATH_LIKE}")
        pattern = f"%{escape_like(spec.exclude)}%"
        params.extend([pattern, pattern])

    return where, params


def _row_to_dict(row: Any) -> Dict[str, Any]:
    return {
        "id": row[0],
        "file_path": row[1] or "",
        "file_name": row[2] or "",
        "extension": row[3] or "",
        "size_bytes": row[4] or 0,
        "modified_ts": row[5] or "",
        "deleted": bool(row[6]),
        "partition_index": row[7],
    }


class FileListQueryMixin(BaseDataAccess):
    """Mixin providing File List tab queries (issue #73).

    All methods operate on the evidence database and return empty results
    (without creating it) if the evidence database does not exist.
    """

    def list_file_list_rows(
        self,
        evidence_id: int,
        filters: Optional[FileListFilterSpec] = None,
        *,
        offset: int = 0,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        """Return one page of file list rows, ordered by path.

        Args:
            evidence_id: Evidence ID
            filters: Filters (``None`` = all files)
            offset: Rows to skip
            limit: Maximum rows to return

        Returns:
            Row dicts with keys id, file_path, file_name, extension,
            size_bytes, modified_ts, deleted, partition_index
        """
        if not self._evidence_db_exists(evidence_id):
            return []
        where, params = build_file_list_filter_predicates(evidence_id, filters or FileListFilterSpec())
        sql = f"""
            SELECT {_FILE_LIST_ROW_COLUMNS}
            FROM file_list fl
            WHERE {' AND '.join(where)}
            ORDER BY {FILE_LIST_ORDER_BY}
            LIMIT ? OFFSET ?
        """
        params.extend([int(limit), int(offset)])
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return [_row_to_dict(row) for row in conn.execute(sql, params).fetchall()]

    def count_file_list(self, evidence_id: int, filters: Optional[FileListFilterSpec] = None) -> int:
        """Count file list rows matching ``filters`` (``None`` = all files)."""
        if not self._evidence_db_exists(evidence_id):
            return 0
        where, params = build_file_list_filter_predicates(evidence_id, filters or FileListFilterSpec())
        sql = f"SELECT COUNT(*) FROM file_list fl WHERE {' AND '.join(where)}"
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return int(self._scalar(conn, sql, tuple(params)))

    def get_file_list_matches_str(self, evidence_id: int, file_id: int) -> str:
        """Return the reference lists a file matched, comma-separated ("" if none)."""
        if not self._evidence_db_exists(evidence_id):
            return ""
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                result = conn.execute(
                    "SELECT GROUP_CONCAT(reference_list_name) FROM file_list_matches WHERE file_list_id = ?",
                    (file_id,),
                ).fetchone()
        return result[0] if result and result[0] else ""

    def list_file_list_filter_values(self, evidence_id: int) -> Dict[str, List[str]]:
        """Return the File List dropdown values: extensions, matches, tags.

        Uses the pre-computed ``file_list_filter_cache`` when present, with
        DISTINCT queries as fallback.
        """
        if not self._evidence_db_exists(evidence_id):
            return {"extensions": [], "matches": [], "tags": []}
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                return self._file_list_filter_values(conn, evidence_id)

    @staticmethod
    def _file_list_filter_values(conn, evidence_id: int) -> Dict[str, List[str]]:
        def column(sql: str) -> List[str]:
            return [row[0] for row in conn.execute(sql, (evidence_id,)).fetchall()]

        distinct_extensions = """
            SELECT DISTINCT extension
            FROM file_list
            WHERE evidence_id = ? AND extension IS NOT NULL AND extension != ''
            ORDER BY extension
        """
        distinct_tags = """
            SELECT DISTINCT t.name
            FROM tags t
            JOIN tag_associations ta ON t.id = ta.tag_id
            WHERE ta.evidence_id = ? AND ta.artifact_type = 'file_list'
            ORDER BY t.name
        """
        cache_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='file_list_filter_cache'"
        ).fetchone()

        if cache_exists:
            def cached(filter_type: str) -> List[str]:
                return [
                    row[0]
                    for row in conn.execute(
                        """
                        SELECT filter_value
                        FROM file_list_filter_cache
                        WHERE evidence_id = ? AND filter_type = ?
                        ORDER BY filter_value
                        """,
                        (evidence_id, filter_type),
                    ).fetchall()
                ]

            # Fall back to DISTINCT queries if the cache is empty for a type
            extensions = cached("extension") or column(distinct_extensions)
            matches = cached("match")
            tags = cached("tag") or column(distinct_tags)
        else:
            logger.debug("Filter cache not available, using DISTINCT queries")
            extensions = column(distinct_extensions)
            matches = column("""
                SELECT DISTINCT reference_list_name
                FROM file_list_matches flm
                JOIN file_list fl ON flm.file_list_id = fl.id
                WHERE fl.evidence_id = ?
                ORDER BY reference_list_name
            """)
            tags = column(distinct_tags)

        return {"extensions": extensions, "matches": matches, "tags": tags}
