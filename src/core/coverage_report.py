"""
Evidence file-list coverage report generator.

Generates a structured report showing what browser artifacts were:
- Present in file_list (source evidence)
- Extracted (copied to case workspace)
- Ingested (parsed into structured database tables)
- Unsupported (present but no parser exists)
- Missing joins (ingested artifacts that don't link back to file_list)

Usage:
    from core.coverage_report import generate_coverage_report

    report = generate_coverage_report(evidence_conn, evidence_id)
    for item in report["items"]:
        print(f"{item['status']:20s} {item['artifact_type']:20s} {item['path']}")
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from core.logging import get_logger

LOGGER = get_logger("core.coverage_report")

# Coverage status values
STATUS_EXTRACTED_INGESTED = "extracted_ingested"
STATUS_EXTRACTED_NOT_INGESTED = "extracted_not_ingested"
STATUS_PRESENT_NOT_EXTRACTED = "present_not_extracted"
STATUS_PRESENT_UNSUPPORTED = "present_unsupported"
STATUS_INGESTED_NO_FILE_LIST = "ingested_no_file_list_join"
STATUS_DUPLICATE_INGESTION = "duplicate_ingestion"


# Known browser artifact filenames and their artifact types
KNOWN_BROWSER_ARTIFACTS = {
    # Chrome/Chromium
    "History": "history",
    "Cookies": "cookies",
    "Login Data": "credentials",
    "Web Data": "autofill",
    "Bookmarks": "bookmarks",
    "Preferences": "preferences",
    "Local State": "local_state",
    "Favicons": "favicons",
    "Top Sites": "top_sites",
    "Shortcuts": "shortcuts",
    "Visited Links": "visited_links",
    "Network Action Predictor": "network_predictor",
    "Origin Bound Certs": "origin_certs",
    "History Provider Cache": "history_cache",
    "TransportSecurity": "transport_security",
    # Firefox
    "places.sqlite": "history",
    "cookies.sqlite": "cookies",
    "formhistory.sqlite": "autofill",
    "logins.json": "credentials",
    "key4.db": "credentials",
    "permissions.sqlite": "permissions",
    "content-prefs.sqlite": "content_prefs",
    "storage.sqlite": "storage",
    # Tor
    "torrc": "tor_config",
    "state": "tor_state",
}


def generate_coverage_report(
    evidence_conn: sqlite3.Connection,
    evidence_id: int,
) -> Dict[str, Any]:
    """
    Generate a file-list coverage report for one evidence.

    Args:
        evidence_conn: Evidence database connection
        evidence_id: Evidence ID

    Returns:
        Dict with:
        - items: List of coverage items
        - summary: Counts by status
        - warnings: List of coverage warning strings
    """
    evidence_conn.row_factory = sqlite3.Row
    items: List[Dict[str, Any]] = []
    warnings: List[str] = []

    # 1. Find known browser artifacts in file_list
    present_artifacts = _find_present_artifacts(evidence_conn, evidence_id)

    # 2. Get extracted files
    extracted_paths = _get_extracted_paths(evidence_conn, evidence_id)

    # 3. Get inventory with ingestion status
    inventory = _get_inventory(evidence_conn, evidence_id)

    # 4. Get unsupported artifact warnings
    unsupported = _get_unsupported_warnings(evidence_conn, evidence_id)
    unsupported_names = {w["item_name"].lower() for w in unsupported}

    # 5. Build coverage items from present artifacts
    for artifact in present_artifacts:
        path = artifact["file_path"]
        name = artifact["file_name"]
        name_lower = name.lower()

        # Check if extracted
        is_extracted = any(
            ep.lower().endswith("/" + name_lower) or ep.lower() == name_lower
            for ep in extracted_paths
        )

        # Check if in inventory with ingestion
        inv_match = None
        for inv in inventory:
            if name_lower in (inv.get("logical_path") or "").lower():
                inv_match = inv
                break

        # Determine status
        if name_lower in unsupported_names:
            status = STATUS_PRESENT_UNSUPPORTED
        elif inv_match and inv_match.get("ingestion_status") == "ok":
            status = STATUS_EXTRACTED_INGESTED
        elif is_extracted:
            status = STATUS_EXTRACTED_NOT_INGESTED
        else:
            status = STATUS_PRESENT_NOT_EXTRACTED

        items.append({
            "path": path,
            "file_name": name,
            "artifact_type": KNOWN_BROWSER_ARTIFACTS.get(name, "unknown"),
            "status": status,
            "partition_index": artifact.get("partition_index"),
            "size_bytes": artifact.get("size_bytes"),
            "ingestion_records": inv_match.get("records_parsed", 0) if inv_match else 0,
        })

    # 6. Check for ingested artifacts that don't join to file_list
    _check_orphan_ingestions(evidence_conn, evidence_id, items, warnings)

    # 7. Check for duplicate ingestions
    _check_duplicate_ingestions(evidence_conn, evidence_id, items, warnings)

    # Build summary
    summary = {}
    for item in items:
        status = item["status"]
        summary[status] = summary.get(status, 0) + 1

    return {
        "evidence_id": evidence_id,
        "total_items": len(items),
        "items": items,
        "summary": summary,
        "warnings": warnings,
    }


def _find_present_artifacts(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> List[Dict[str, Any]]:
    """Find known browser artifact files in file_list."""
    filenames = list(KNOWN_BROWSER_ARTIFACTS.keys())
    placeholders = ", ".join(["?" for _ in filenames])

    query = f"""
        SELECT file_path, file_name, partition_index, size_bytes
        FROM file_list
        WHERE evidence_id = ?
          AND file_name IN ({placeholders})
          AND COALESCE(deleted, 0) = 0
        ORDER BY file_path
    """
    params = [evidence_id] + filenames
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def _get_extracted_paths(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> List[str]:
    """Get all source_path values from extracted_files."""
    try:
        rows = conn.execute(
            "SELECT DISTINCT source_path FROM extracted_files WHERE evidence_id = ? AND source_path IS NOT NULL",
            (evidence_id,),
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def _get_inventory(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> List[Dict[str, Any]]:
    """Get browser_cache_inventory rows."""
    try:
        rows = conn.execute(
            "SELECT * FROM browser_cache_inventory WHERE evidence_id = ?",
            (evidence_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _get_unsupported_warnings(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> List[Dict[str, Any]]:
    """Get unsupported artifact warnings."""
    try:
        rows = conn.execute(
            """
            SELECT item_name, item_value, context_json
            FROM extraction_warnings
            WHERE evidence_id = ? AND warning_type = 'unsupported_artifact'
            """,
            (evidence_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _check_orphan_ingestions(
    conn: sqlite3.Connection,
    evidence_id: int,
    items: List[Dict[str, Any]],
    warnings: List[str],
) -> None:
    """Check for artifacts in structured tables that have no file_list entry."""
    # Check browser_extensions source_path join
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT be.source_path
            FROM browser_extensions be
            WHERE be.evidence_id = ?
              AND be.source_path IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM file_list fl
                  WHERE fl.evidence_id = be.evidence_id
                    AND fl.file_path = be.source_path
              )
            """,
            (evidence_id,),
        ).fetchall()
        for row in rows:
            items.append({
                "path": row[0],
                "file_name": "",
                "artifact_type": "extension",
                "status": STATUS_INGESTED_NO_FILE_LIST,
                "partition_index": None,
                "size_bytes": None,
                "ingestion_records": 0,
            })
            warnings.append(f"Extension source_path has no file_list join: {row[0]}")
    except sqlite3.OperationalError:
        pass  # Table may not exist
    except Exception as e:
        LOGGER.warning("Failed to check orphan ingestions: %s", e)


def _check_duplicate_ingestions(
    conn: sqlite3.Connection,
    evidence_id: int,
    items: List[Dict[str, Any]],
    warnings: List[str],
) -> None:
    """Check for duplicate extension records."""
    try:
        rows = conn.execute(
            """
            SELECT extension_id, version, source_path, COUNT(*) as cnt
            FROM browser_extensions
            WHERE evidence_id = ?
            GROUP BY extension_id, version, source_path
            HAVING COUNT(*) > 1
            """,
            (evidence_id,),
        ).fetchall()
        for row in rows:
            warnings.append(
                f"Duplicate extension: {row[0]} v{row[1]} from {row[2]} ({row[3]} rows)"
            )
            items.append({
                "path": row[2] or "",
                "file_name": f"{row[0]}@{row[1]}",
                "artifact_type": "extension",
                "status": STATUS_DUPLICATE_INGESTION,
                "partition_index": None,
                "size_bytes": None,
                "ingestion_records": row[3],
            })
    except sqlite3.OperationalError:
        pass  # Table may not exist
    except Exception as e:
        LOGGER.warning("Failed to check duplicate ingestions: %s", e)


def format_coverage_report(report: Dict[str, Any]) -> str:
    """
    Format a coverage report as a human-readable text table.

    Args:
        report: Output from generate_coverage_report()

    Returns:
        Formatted text report
    """
    lines = [
        f"Coverage Report for Evidence {report['evidence_id']}",
        f"{'=' * 70}",
        f"Total items: {report['total_items']}",
        "",
    ]

    # Summary
    lines.append("Summary:")
    for status, count in sorted(report["summary"].items()):
        lines.append(f"  {status}: {count}")
    lines.append("")

    # Items table
    lines.append(f"{'Status':<30s} {'Type':<20s} {'Path'}")
    lines.append(f"{'-' * 30} {'-' * 20} {'-' * 40}")
    for item in report["items"]:
        lines.append(
            f"{item['status']:<30s} {item['artifact_type']:<20s} {item['path']}"
        )

    # Warnings
    if report["warnings"]:
        lines.append("")
        lines.append("Warnings:")
        for w in report["warnings"]:
            lines.append(f"  - {w}")

    return "\n".join(lines)
