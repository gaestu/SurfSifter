"""
Presentation strings for the Tag Summary export workbook.

Kept separate from :mod:`core.database.helpers.tag_export` so the core
helper layer stays free of UI/report concerns.  Consumers (the
composer below or any other report-side code) attach these labels and
column headers to the structured data returned by the helpers before
handing them to :func:`reports.tag_summary_export.write_tag_summary_xlsx`.
"""
from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from core.database.helpers.tag_export import (
    get_reference_list_match_export,
    get_tagged_artifact_export,
)
from core.image_codecs import thumbnail_to_jpeg_bytes
from core.image_paths import evidence_workspace_root, resolve_case_image_path
from reports.tag_summary_export import TagSummaryExportData

__all__ = [
    "ARTIFACT_PRESENTATION",
    "REFERENCE_PRESENTATION",
    "compose_tag_summary",
]


# canonical artifact_type → (section label, column headers)
ARTIFACT_PRESENTATION: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "url": ("URLs", ("URL", "Visit timestamp", "Visit count", "Source")),
    "browser_history": (
        "Browser history",
        ("Title", "URL", "Visit time", "Browser/profile", "Visits"),
    ),
    "browser_search_term": (
        "Browser search terms",
        ("Term", "Timestamp", "Browser/profile"),
    ),
    "credential": ("Credentials", ("Username", "Site", "Browser/profile")),
    "stored_site": ("Stored sites", ("Site", "Browsers", "Keys")),
    "browser_download": (
        "Browser downloads",
        ("Filename", "Target path", "Timestamp", "Browser/profile"),
    ),
    "download": (
        "Downloaded files",
        ("Filename", "Destination path", "URL", "Completed", "Status"),
    ),
    "bookmark": ("Bookmarks", ("Title", "URL", "Browser/profile")),
    "cookie": ("Cookies", ("Host", "Name", "Browser/profile")),
    "autofill": ("Form data", ("Field name", "Value", "Browser/profile")),
    "image": ("Images", ("Filename", "Path", "Timestamp")),
    "file_list": ("Files", ("File name", "Path", "Modified")),
    "session_tab": ("Session tabs", ("Title", "URL", "Browser/profile")),
    "site_permission": (
        "Site permissions",
        ("Origin", "Permission", "Value", "Browser/profile"),
    ),
    "media_playback": ("Media playback", ("URL", "Origin", "Last played")),
    "local_storage": ("Local storage", ("Origin", "Key", "Browser/profile")),
    "session_storage": (
        "Session storage",
        ("Origin", "Key", "Browser/profile"),
    ),
    "jump_list_entry": (
        "Jump list entries",
        ("Title", "Target", "Browser"),
    ),
    "app_execution": (
        "Application execution",
        ("Application path", "Last run", "Run count", "Source"),
    ),
    "installed_software": (
        "Installed software",
        ("Software", "Publisher", "Version", "Install date"),
    ),
    "timeline": ("Timeline events", ("Kind", "Timestamp", "Reference")),
}


# reference-list match kind → column headers (label is "{list_name} ({kind})")
REFERENCE_PRESENTATION: Dict[str, Tuple[str, ...]] = {
    "url": ("URL", "Visit timestamp", "Source"),
    "image": ("Filename", "Path", "MD5"),
    "file": ("File name", "Path", "Modified"),
}


def _thumbnail_bytes_for_metadata(
    metadata: Dict[str, Any],
    *,
    case_folder: Optional[Path],
    evidence_id: int,
    evidence_label: str,
) -> Optional[bytes]:
    rel_path = str(metadata.get("rel_path") or "")
    if not case_folder:
        return None
    image_path = resolve_case_image_path(
        rel_path=rel_path,
        discovered_by=metadata.get("discovered_by"),
        case_folder=Path(case_folder),
        evidence_id=evidence_id,
        evidence_label=evidence_label,
        require_exists=True,
        allow_case_root_fallback=False,
    )
    if image_path is None:
        return None
    source_root = evidence_workspace_root(
        case_folder=Path(case_folder),
        evidence_id=evidence_id,
        evidence_label=evidence_label,
    )
    if source_root is None:
        return None
    return thumbnail_to_jpeg_bytes(image_path, containment_root=source_root)


def _attach_thumbnails(
    item: Dict[str, Any],
    *,
    case_folder: Optional[Path],
    evidence_id: int,
    evidence_label: str,
) -> Dict[str, Any]:
    metadata_rows = item.get("row_metadata") or []
    if not metadata_rows:
        return item
    thumbnails = [
        _thumbnail_bytes_for_metadata(
            metadata,
            case_folder=case_folder,
            evidence_id=evidence_id,
            evidence_label=evidence_label,
        )
        for metadata in metadata_rows
    ]
    return {**item, "thumbnail_bytes": thumbnails}


def _decorate_tags(
    raw_tags: List[Dict[str, Any]],
    *,
    case_folder: Optional[Path],
    evidence_id: int,
    evidence_label: str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for tag in raw_tags:
        sections: List[Dict[str, Any]] = []
        for s in tag["sections"]:
            if not s.get("supported", True):
                # Surface the gap to the investigator so the workbook
                # never silently omits tagged artifacts whose canonical
                # type has no XLSX presentation yet.
                raws = ", ".join(s.get("raw_artifact_types", []))
                label = (
                    f"Unsupported artifact type "
                    f"({s['artifact_type']})"
                )
                sections.append(
                    {
                        **s,
                        "label": label,
                        "headers": (),
                        "unsupported_note": (
                            f"This export does not yet render "
                            f"{s['total']} tagged artifact(s) of "
                            f"type {raws or s['artifact_type']!r}; "
                            "see the live Tag & Match Summary view."
                        ),
                    }
                )
                continue
            pres = ARTIFACT_PRESENTATION.get(s["artifact_type"])
            if pres is None:
                # Defensive — every supported canonical type is expected
                # to have a presentation entry (enforced by tests).
                continue
            label, headers = pres
            decorated = {**s, "label": label, "headers": headers}
            if s["artifact_type"] == "image":
                decorated = _attach_thumbnails(
                    decorated,
                    case_folder=case_folder,
                    evidence_id=evidence_id,
                    evidence_label=evidence_label,
                )
            sections.append(decorated)
        if sections:
            out.append({**tag, "sections": sections})
    return out


def _decorate_refs(
    raw_refs: List[Dict[str, Any]],
    *,
    case_folder: Optional[Path],
    evidence_id: int,
    evidence_label: str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for bucket in raw_refs:
        headers = REFERENCE_PRESENTATION.get(bucket["kind"])
        if headers is None:
            continue
        decorated = {**bucket, "headers": headers}
        if bucket["kind"] == "image":
            decorated = _attach_thumbnails(
                decorated,
                case_folder=case_folder,
                evidence_id=evidence_id,
                evidence_label=evidence_label,
            )
        out.append(decorated)
    return out


def compose_tag_summary(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    evidence_label: str,
    exported_at_iso: str,
    top_n: int,
    case_folder: Optional[Path] = None,
) -> TagSummaryExportData:
    """Pull data via core helpers and attach presentation strings."""
    raw_tags = get_tagged_artifact_export(conn, evidence_id, top_n=top_n)
    raw_refs = get_reference_list_match_export(conn, evidence_id, top_n=top_n)
    return TagSummaryExportData(
        evidence_label=evidence_label,
        exported_at_iso=exported_at_iso,
        top_n=top_n,
        tags=_decorate_tags(
            raw_tags,
            case_folder=case_folder,
            evidence_id=evidence_id,
            evidence_label=evidence_label,
        ),
        reference_list_matches=_decorate_refs(
            raw_refs,
            case_folder=case_folder,
            evidence_id=evidence_id,
            evidence_label=evidence_label,
        ),
    )
