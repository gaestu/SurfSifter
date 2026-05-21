"""
Tag-summary export queries.

Provides per-evidence, tag-grouped artifact listings and reference-list
match listings used by the Reports tab "Export Tag Summary" feature.

All queries are read-only and return plain Python dicts/lists so the
result can be consumed by deterministic XLSX generation.

Presentation strings (section labels, column headers) intentionally live
in :mod:`reports.tag_summary_presentation` so the core helper layer
remains free of UI/report concerns; the structures returned here only
identify *which* canonical artifact type each section corresponds to.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Sequence, Tuple

__all__ = [
    "ARTIFACT_EXPORT_SPECS",
    "REFERENCE_LIST_SPECS",
    "get_tagged_artifact_export",
    "get_reference_list_match_export",
]


# ---------------------------------------------------------------------------
# Per-artifact-type SQL specifications
# ---------------------------------------------------------------------------
#
# Each entry maps an artifact_type (the value stored in
# tag_associations.artifact_type) to:
#   - "table":     evidence DB table to read from
#   - "columns":   ordered list of SQL expressions for the row body
#                  (the leading checkbox column is added by the writer)
#   - "ts_expr":   SQL expression used for "most recent timestamp"
#                  ordering (NULLs sort last)
#   - "tie_expr":  SQL expression used as alphabetical tiebreaker
#
# Some artifact types appear in the codebase under multiple legacy names
# (e.g. "download" / "downloads" / "browser_download").  All known
# variants are mapped to the same canonical spec via _ARTIFACT_TYPE_ALIASES.
# ---------------------------------------------------------------------------

_BROWSER_PROFILE_EXPR = (
    "TRIM(COALESCE(browser, '') || "
    "CASE WHEN profile IS NOT NULL AND profile != '' "
    "THEN ' / ' || profile ELSE '' END)"
)


def _json_value_expr(path: str) -> str:
    """Safely read a JSON field from os_indicators.extra_json in SQL."""
    return (
        "CASE WHEN json_valid(extra_json) "
        f"THEN json_extract(extra_json, '$.{path}') ELSE NULL END"
    )


_APP_PATH_EXPR = (
    f"COALESCE({_json_value_expr('decoded_path')}, value, path, name, '')"
)
_APP_LAST_RUN_EXPR = (
    f"COALESCE({_json_value_expr('last_run_utc')}, "
    f"{_json_value_expr('last_run')}, detected_at_utc)"
)
_INSTALL_NAME_EXPR = (
    f"COALESCE(value, {_json_value_expr('name')}, name, '')"
)
_INSTALL_DATE_EXPR = (
    f"COALESCE({_json_value_expr('install_date_formatted')}, "
    f"{_json_value_expr('install_date')}, detected_at_utc)"
)


ARTIFACT_EXPORT_SPECS: Dict[str, Dict[str, Any]] = {
    "url": {
        "table": "urls",
        "columns": [
            "url",
            "COALESCE(last_seen_utc, first_seen_utc, '')",
            "COALESCE(occurrence_count, '')",
            "COALESCE(discovered_by, '')",
        ],
        "ts_expr": "COALESCE(last_seen_utc, first_seen_utc)",
        "tie_expr": "url",
    },
    "browser_history": {
        "table": "browser_history",
        "columns": [
            "COALESCE(title, '')",
            "url",
            "COALESCE(ts_utc, last_visit_time_utc, '')",
            _BROWSER_PROFILE_EXPR,
            "COALESCE(visit_count, '')",
        ],
        "ts_expr": "COALESCE(ts_utc, last_visit_time_utc)",
        "tie_expr": "url",
    },
    "browser_search_term": {
        "table": "browser_search_terms",
        "columns": [
            "term",
            "COALESCE(search_time_utc, '')",
            _BROWSER_PROFILE_EXPR,
        ],
        "ts_expr": "search_time_utc",
        "tie_expr": "term",
    },
    "credential": {
        "table": "credentials",
        "columns": [
            "COALESCE(username_value, '')",
            "origin_url",
            _BROWSER_PROFILE_EXPR,
        ],
        "ts_expr": "COALESCE(date_last_used_utc, date_created_utc)",
        "tie_expr": "origin_url",
    },
    "stored_site": {
        "table": "stored_sites",
        "columns": [
            "origin",
            "COALESCE(browsers, '')",
            "COALESCE(total_keys, 0)",
        ],
        "ts_expr": "COALESCE(last_updated_utc, first_seen_utc)",
        "tie_expr": "origin",
    },
    "browser_download": {
        "table": "browser_downloads",
        "columns": [
            "COALESCE(filename, '')",
            "COALESCE(target_path, '')",
            "COALESCE(start_time_utc, '')",
            _BROWSER_PROFILE_EXPR,
        ],
        "ts_expr": "COALESCE(end_time_utc, start_time_utc)",
        "tie_expr": "COALESCE(filename, target_path, url)",
    },
    "bookmark": {
        "table": "bookmarks",
        "columns": [
            "COALESCE(title, '')",
            "url",
            _BROWSER_PROFILE_EXPR,
        ],
        "ts_expr": "COALESCE(date_modified_utc, date_added_utc)",
        "tie_expr": "COALESCE(title, url)",
    },
    "cookie": {
        "table": "cookies",
        "columns": [
            "domain",
            "name",
            _BROWSER_PROFILE_EXPR,
        ],
        "ts_expr": "COALESCE(last_access_utc, creation_utc)",
        "tie_expr": "domain || '|' || name",
    },
    "autofill": {
        "table": "autofill",
        "columns": [
            "name",
            "COALESCE(value, '')",
            _BROWSER_PROFILE_EXPR,
        ],
        "ts_expr": "COALESCE(date_last_used_utc, date_created_utc)",
        "tie_expr": "name",
    },
    "image": {
        "table": "images",
        "columns": [
            "filename",
            "COALESCE(rel_path, '')",
            "COALESCE(ts_utc, '')",
        ],
        "ts_expr": "ts_utc",
        "tie_expr": "filename",
    },
    "file_list": {
        "table": "file_list",
        "columns": [
            "file_name",
            "file_path",
            "COALESCE(modified_ts, '')",
        ],
        "ts_expr": "COALESCE(modified_ts, created_ts)",
        "tie_expr": "file_path",
    },
    "session_tab": {
        "table": "session_tabs",
        "columns": [
            "COALESCE(title, '')",
            "url",
            _BROWSER_PROFILE_EXPR,
        ],
        "ts_expr": "last_accessed_utc",
        "tie_expr": "url",
    },
    "site_permission": {
        "table": "site_permissions",
        "columns": [
            "origin",
            "permission_type",
            "permission_value",
            _BROWSER_PROFILE_EXPR,
        ],
        "ts_expr": "granted_at_utc",
        "tie_expr": "origin || '|' || permission_type",
    },
    "media_playback": {
        "table": "media_playback",
        "columns": [
            "url",
            "COALESCE(origin, '')",
            "COALESCE(last_played_utc, '')",
        ],
        "ts_expr": "last_played_utc",
        "tie_expr": "url",
    },
    "local_storage": {
        "table": "local_storage",
        "columns": [
            "origin",
            "key",
            _BROWSER_PROFILE_EXPR,
        ],
        "ts_expr": "created_at_utc",
        "tie_expr": "origin || '|' || key",
    },
    "session_storage": {
        "table": "session_storage",
        "columns": [
            "origin",
            "key",
            _BROWSER_PROFILE_EXPR,
        ],
        "ts_expr": "created_at_utc",
        "tie_expr": "origin || '|' || key",
    },
    "jump_list_entry": {
        "table": "jump_list_entries",
        "columns": [
            "COALESCE(title, '')",
            "COALESCE(target_path, '')",
            "COALESCE(browser, '')",
        ],
        "ts_expr": "COALESCE(lnk_modification_time, lnk_creation_time, lnk_access_time)",
        "tie_expr": "COALESCE(title, target_path, jumplist_path)",
    },
    "app_execution": {
        "table": "os_indicators",
        "where": "a.type IN ('execution:user_assist', 'execution:launch_services')",
        "columns": [
            _APP_PATH_EXPR,
            f"COALESCE({_APP_LAST_RUN_EXPR}, '')",
            f"COALESCE({_json_value_expr('run_count')}, '')",
            "COALESCE(provenance, hive, '')",
        ],
        "ts_expr": _APP_LAST_RUN_EXPR,
        "tie_expr": _APP_PATH_EXPR,
    },
    "installed_software": {
        "table": "os_indicators",
        "where": "a.type IN ('system:installed_software', 'system:installed_app_macos', 'system:install_receipt_macos')",
        "columns": [
            _INSTALL_NAME_EXPR,
            f"COALESCE({_json_value_expr('publisher')}, '')",
            f"COALESCE({_json_value_expr('version')}, '')",
            f"COALESCE({_INSTALL_DATE_EXPR}, '')",
        ],
        "ts_expr": _INSTALL_DATE_EXPR,
        "tie_expr": _INSTALL_NAME_EXPR,
    },
    "timeline": {
        "table": "timeline",
        "columns": [
            "kind",
            "ts_utc",
            "ref_table",
        ],
        "ts_expr": "ts_utc",
        "tie_expr": "kind || '|' || ref_table",
    },
}


# Aliases for legacy / pluralised artifact_type values seen in the codebase.
_ARTIFACT_TYPE_ALIASES: Dict[str, str] = {
    "download": "browser_download",
    "downloads": "browser_download",
    "browser_downloads": "browser_download",
    "history": "browser_history",
    "cookies": "cookie",
    "bookmarks": "bookmark",
    "jump_list": "jump_list_entry",
}


def _resolve_artifact_type(artifact_type: str) -> str:
    return _ARTIFACT_TYPE_ALIASES.get(artifact_type, artifact_type)


# ---------------------------------------------------------------------------
# Reference-list match specifications
# ---------------------------------------------------------------------------
#
# Each entry describes one of the three reference-list match tables and how
# to join it back to the underlying artifact for display columns.  Top-N
# selection and per-(list_name, artifact_id) deduplication happens in SQL
# via a window function so we never load the full match set into memory.
# ---------------------------------------------------------------------------

# Sentinel used when a reference-list row has a NULL/blank list name.
# Renderers can safely call str-only operations on this value.
_UNNAMED_LIST_NAME = "(unnamed)"


REFERENCE_LIST_SPECS: List[Dict[str, Any]] = [
    {
        "kind": "url",
        # DISTINCT collapses repeated matches against the same URL within
        # one list so the row count and "total" reflect distinct artifacts,
        # not raw match rows.
        "sql": """
            WITH distinct_matches AS (
                SELECT DISTINCT
                       COALESCE(NULLIF(TRIM(m.list_name), ''), :unnamed) AS list_name,
                       m.url_id AS artifact_id
                FROM url_matches m
                WHERE m.evidence_id = :eid
            ),
            joined AS (
                SELECT dm.list_name AS list_name,
                       u.id AS pk,
                       u.url AS col1,
                       COALESCE(u.last_seen_utc, u.first_seen_utc, '') AS col2,
                       COALESCE(u.discovered_by, '') AS col3,
                       COALESCE(u.last_seen_utc, u.first_seen_utc) AS ts,
                       u.url AS tie
                FROM distinct_matches dm
                JOIN urls u ON u.id = dm.artifact_id
            ),
            ranked AS (
                SELECT list_name, col1, col2, col3,
                       ROW_NUMBER() OVER (
                           PARTITION BY list_name
                           -- pk ASC is the deterministic tiebreaker.
                           ORDER BY (ts IS NULL) ASC, ts DESC, tie ASC, pk ASC
                       ) AS rn,
                       COUNT(*) OVER (PARTITION BY list_name) AS total
                FROM joined
            )
            SELECT list_name, col1, col2, col3, rn, total
            FROM ranked
            WHERE rn <= :top_n
            ORDER BY list_name COLLATE NOCASE, rn
        """,
    },
    {
        "kind": "image",
        "sql": """
            WITH distinct_matches AS (
                SELECT DISTINCT
                       COALESCE(NULLIF(TRIM(m.list_name), ''), :unnamed) AS list_name,
                       m.image_id AS artifact_id
                FROM hash_matches m
                WHERE m.evidence_id = :eid
            ),
            joined AS (
                SELECT dm.list_name AS list_name,
                       i.id AS pk,
                       i.filename AS col1,
                       COALESCE(i.rel_path, '') AS col2,
                       COALESCE(i.md5, '') AS col3,
                       i.ts_utc AS ts,
                       i.filename AS tie
                FROM distinct_matches dm
                JOIN images i ON i.id = dm.artifact_id
            ),
            ranked AS (
                SELECT list_name, col1, col2, col3,
                       ROW_NUMBER() OVER (
                           PARTITION BY list_name
                           -- pk ASC is the deterministic tiebreaker.
                           ORDER BY (ts IS NULL) ASC, ts DESC, tie ASC, pk ASC
                       ) AS rn,
                       COUNT(*) OVER (PARTITION BY list_name) AS total
                FROM joined
            )
            SELECT list_name, col1, col2, col3, rn, total
            FROM ranked
            WHERE rn <= :top_n
            ORDER BY list_name COLLATE NOCASE, rn
        """,
    },
    {
        "kind": "file",
        "sql": """
            WITH distinct_matches AS (
                SELECT DISTINCT
                       COALESCE(NULLIF(TRIM(m.reference_list_name), ''), :unnamed) AS list_name,
                       m.file_list_id AS artifact_id
                FROM file_list_matches m
                WHERE m.evidence_id = :eid
            ),
            joined AS (
                SELECT dm.list_name AS list_name,
                       f.id AS pk,
                       f.file_name AS col1,
                       f.file_path AS col2,
                       COALESCE(f.modified_ts, '') AS col3,
                       COALESCE(f.modified_ts, f.created_ts) AS ts,
                       f.file_path AS tie
                FROM distinct_matches dm
                JOIN file_list f ON f.id = dm.artifact_id
            ),
            ranked AS (
                SELECT list_name, col1, col2, col3,
                       ROW_NUMBER() OVER (
                           PARTITION BY list_name
                           -- pk ASC is the deterministic tiebreaker.
                           ORDER BY (ts IS NULL) ASC, ts DESC, tie ASC, pk ASC
                       ) AS rn,
                       COUNT(*) OVER (PARTITION BY list_name) AS total
                FROM joined
            )
            SELECT list_name, col1, col2, col3, rn, total
            FROM ranked
            WHERE rn <= :top_n
            ORDER BY list_name COLLATE NOCASE, rn
        """,
    },
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _get_distinct_artifact_types(
    conn: sqlite3.Connection,
    evidence_id: int,
    tag_id: int,
) -> List[str]:
    """Return artifact_type values stored against this tag, alphabetically."""
    rows = conn.execute(
        """
        SELECT DISTINCT artifact_type
        FROM tag_associations
        WHERE evidence_id = ? AND tag_id = ?
        ORDER BY artifact_type
        """,
        (evidence_id, tag_id),
    ).fetchall()
    return [r["artifact_type"] for r in rows]


def _fetch_artifact_rows(
    conn: sqlite3.Connection,
    evidence_id: int,
    tag_id: int,
    raw_artifact_types: Sequence[str],
    spec: Dict[str, Any],
    limit: int,
) -> Tuple[List[Sequence[Any]], int]:
    """
    Return ``(rows, total_count)`` for one canonical artifact type.

    Multiple raw ``artifact_type`` values that resolve to the same canonical
    type (legacy aliases such as "bookmark" / "bookmarks") are merged here
    so the export does not emit duplicate sections with split totals.

    Rows are ordered most recent first then alphabetically; ``total_count``
    is the distinct artifact count, not the raw association count.
    """
    table = spec["table"]
    if not _table_exists(conn, table):
        return [], 0

    select_exprs = ", ".join(spec["columns"])
    ts_expr = spec["ts_expr"]
    tie_expr = spec["tie_expr"]
    extra_where = spec.get("where")
    extra_where_sql = f" AND {extra_where}" if extra_where else ""

    type_placeholders = ", ".join("?" for _ in raw_artifact_types)
    type_params = list(raw_artifact_types)

    # Total distinct count of artifacts of this canonical type tagged with
    # this tag, across all aliased raw artifact_type values.
    total_row = conn.execute(
        f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT DISTINCT a.id
            FROM {table} a
            JOIN tag_associations ta
              ON ta.artifact_id = a.id
             AND ta.artifact_type IN ({type_placeholders})
            WHERE ta.tag_id = ?
              AND ta.evidence_id = ?
              AND a.evidence_id = ?
                            {extra_where_sql}
        )
        """,
        (*type_params, tag_id, evidence_id, evidence_id),
    ).fetchone()
    total = int(total_row["cnt"]) if total_row else 0
    if total == 0:
        return [], 0

    sql = f"""
        SELECT {select_exprs},
               {ts_expr} AS _ts,
               {tie_expr} AS _tie,
               a.id AS _pk
        FROM {table} a
        WHERE a.id IN (
            SELECT DISTINCT ta.artifact_id
            FROM tag_associations ta
            WHERE ta.tag_id = ?
              AND ta.artifact_type IN ({type_placeholders})
              AND ta.evidence_id = ?
        )
        AND a.evidence_id = ?
        {extra_where_sql}
        -- Final ASC on a.id is the deterministic tiebreaker so two
        -- runs against identical evidence always pick the same sample
        -- rows when ts/tie collide (e.g. timeline rows).
        ORDER BY (_ts IS NULL) ASC, _ts DESC, _tie ASC, _pk ASC
        LIMIT ?
    """
    cursor = conn.execute(
        sql, (tag_id, *type_params, evidence_id, evidence_id, limit)
    )

    column_count = len(spec["columns"])
    rows: List[Sequence[Any]] = []
    for raw in cursor.fetchall():
        cells: List[Any] = []
        for i in range(column_count):
            value = raw[i]
            cells.append("" if value is None else value)
        rows.append(cells)
    return rows, total


def get_tagged_artifact_export(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    top_n: int = 3,
) -> List[Dict[str, Any]]:
    """
    Return the per-tag, per-artifact-type export structure for one evidence.

    Returns:
        List of tag entries, alphabetically ordered by tag name::

            [
              {
                "tag_id": int,
                "tag_name": str,
                "sections": [
                  {
                    "artifact_type": str,             # canonical type
                    "raw_artifact_types": list[str],  # source DB values merged
                    "column_count": int,              # number of body columns
                    "rows": list[list[Any]],          # up to top_n entries
                    "total": int,                     # total distinct entries
                  },
                  ...
                ]
              },
              ...
            ]

    Presentation strings (section titles, column headers) are intentionally
    not returned here; consumers should attach them via
    :mod:`reports.tag_summary_presentation`.
    """
    tags = conn.execute(
        """
        SELECT DISTINCT t.id AS tag_id, t.name AS tag_name
        FROM tags t
        JOIN tag_associations ta ON ta.tag_id = t.id
        WHERE t.evidence_id = ? AND ta.evidence_id = ?
        ORDER BY t.name COLLATE NOCASE
        """,
        (evidence_id, evidence_id),
    ).fetchall()

    result: List[Dict[str, Any]] = []
    for tag in tags:
        tag_id = tag["tag_id"]

        # Group raw artifact_type values by canonical type so legacy
        # aliases (e.g. "bookmark" + "bookmarks") merge into one section.
        # Unknown types are kept under their canonical (== raw) key with
        # ``supported=False`` so the export surfaces the gap to the
        # investigator instead of silently dropping rows.
        canonical_to_raws: Dict[str, List[str]] = {}
        for raw_type in _get_distinct_artifact_types(conn, evidence_id, tag_id):
            canonical = _resolve_artifact_type(raw_type)
            canonical_to_raws.setdefault(canonical, []).append(raw_type)

        sections: List[Dict[str, Any]] = []
        for canonical in sorted(canonical_to_raws):
            raws = canonical_to_raws[canonical]
            spec = ARTIFACT_EXPORT_SPECS.get(canonical)
            if spec is None:
                # Unsupported artifact type — emit a placeholder section
                # with the raw association count so the investigator can
                # see that the export is incomplete for this tag.
                raw_total = _count_unsupported_associations(
                    conn, evidence_id, tag_id, raws
                )
                if raw_total == 0:
                    continue
                sections.append(
                    {
                        "artifact_type": canonical,
                        "raw_artifact_types": list(raws),
                        "column_count": 0,
                        "rows": [],
                        "total": raw_total,
                        "supported": False,
                    }
                )
                continue
            rows, total = _fetch_artifact_rows(
                conn, evidence_id, tag_id, raws, spec, top_n
            )
            if total == 0:
                continue
            sections.append(
                {
                    "artifact_type": canonical,
                    "raw_artifact_types": list(raws),
                    "column_count": len(spec["columns"]),
                    "rows": rows,
                    "total": total,
                    "supported": True,
                }
            )
        if sections:
            result.append(
                {
                    "tag_id": tag_id,
                    "tag_name": tag["tag_name"],
                    "sections": sections,
                }
            )
    return result


def _count_unsupported_associations(
    conn: sqlite3.Connection,
    evidence_id: int,
    tag_id: int,
    raw_artifact_types: Sequence[str],
) -> int:
    """Count tag_associations rows for artifact types we can't render."""
    if not raw_artifact_types:
        return 0
    placeholders = ", ".join("?" for _ in raw_artifact_types)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS cnt
        FROM tag_associations
        WHERE evidence_id = ?
          AND tag_id = ?
          AND artifact_type IN ({placeholders})
        """,
        (evidence_id, tag_id, *raw_artifact_types),
    ).fetchone()
    return int(row["cnt"]) if row else 0


def get_reference_list_match_export(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    top_n: int = 3,
) -> List[Dict[str, Any]]:
    """
    Return reference-list matches grouped by (list name, kind).

    Top-N selection and per-artifact deduplication happen in SQL so the
    helper never loads the full match set into Python memory.

    Returns:
        List of (list_name, kind) entries, alphabetically ordered::

            [
              {
                "list_name": str,           # never None — see _UNNAMED_LIST_NAME
                "kind": str,                # "url" | "image" | "file"
                "column_count": int,        # number of body columns
                "rows": list[list[Any]],    # up to top_n entries
                "total": int,               # distinct artifacts in this list
              },
              ...
            ]

        A separate entry is returned for each (list_name, kind) pair so
        that a single list name covering multiple match domains is shown
        in clearly distinguished sections.
    """
    output: List[Dict[str, Any]] = []

    for spec in REFERENCE_LIST_SPECS:
        kind = spec["kind"]
        try:
            cursor = conn.execute(
                spec["sql"],
                {
                    "eid": evidence_id,
                    "top_n": int(top_n),
                    "unnamed": _UNNAMED_LIST_NAME,
                },
            )
        except sqlite3.OperationalError:
            # Match table missing on very old / partial schemas — skip.
            continue

        # Group SQL results by list_name (already top-N filtered server-side).
        buckets: Dict[str, Dict[str, Any]] = {}
        for r in cursor.fetchall():
            list_name = r["list_name"] or _UNNAMED_LIST_NAME
            bucket = buckets.setdefault(
                list_name,
                {
                    "list_name": list_name,
                    "kind": kind,
                    "column_count": 3,
                    "rows": [],
                    "total": int(r["total"]),
                },
            )
            bucket["rows"].append(
                [
                    "" if r["col1"] is None else r["col1"],
                    "" if r["col2"] is None else r["col2"],
                    "" if r["col3"] is None else r["col3"],
                ]
            )

        for key in sorted(buckets.keys(), key=lambda k: k.lower()):
            output.append(buckets[key])

    return output
