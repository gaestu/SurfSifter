"""
Presentation strings for the Tag Summary export workbook.

Kept separate from :mod:`core.database.helpers.tag_export` so the core
helper layer stays free of UI/report concerns.  Consumers (the
composer below or any other report-side code) attach these labels and
column headers to the structured data returned by the helpers before
handing them to :func:`reports.tag_summary_export.write_tag_summary_xlsx`.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Tuple

from core.database.helpers.tag_export import (
    get_reference_list_match_export,
    get_tagged_artifact_export,
)
from reports.tag_summary_export import TagSummaryExportData

__all__ = [
    "ARTIFACT_PRESENTATION",
    "REFERENCE_PRESENTATION",
    "compose_tag_summary",
]


# canonical artifact_type → (section label, column headers)
ARTIFACT_PRESENTATION: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "url": ("URLs", ("URL", "Visit timestamp", "Visit count", "Source")),
    "browser_search_term": (
        "Browser search terms",
        ("Term", "Timestamp", "Browser/profile"),
    ),
    "credential": ("Credentials", ("Username", "Site", "Browser/profile")),
    "stored_site": ("Stored sites", ("Site", "Browsers", "Keys")),
    "browser_download": (
        "Downloads",
        ("Filename", "Target path", "Timestamp", "Browser/profile"),
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
    "timeline": ("Timeline events", ("Kind", "Timestamp", "Reference")),
}


# reference-list match kind → column headers (label is "{list_name} ({kind})")
REFERENCE_PRESENTATION: Dict[str, Tuple[str, ...]] = {
    "url": ("URL", "Visit timestamp", "Source"),
    "image": ("Filename", "Path", "MD5"),
    "file": ("File name", "Path", "Modified"),
}


def _decorate_tags(raw_tags: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
            sections.append({**s, "label": label, "headers": headers})
        if sections:
            out.append({**tag, "sections": sections})
    return out


def _decorate_refs(raw_refs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for bucket in raw_refs:
        headers = REFERENCE_PRESENTATION.get(bucket["kind"])
        if headers is None:
            continue
        out.append({**bucket, "headers": headers})
    return out


def compose_tag_summary(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    evidence_label: str,
    exported_at_iso: str,
    top_n: int,
) -> TagSummaryExportData:
    """Pull data via core helpers and attach presentation strings."""
    raw_tags = get_tagged_artifact_export(conn, evidence_id, top_n=top_n)
    raw_refs = get_reference_list_match_export(conn, evidence_id, top_n=top_n)
    return TagSummaryExportData(
        evidence_label=evidence_label,
        exported_at_iso=exported_at_iso,
        top_n=top_n,
        tags=_decorate_tags(raw_tags),
        reference_list_matches=_decorate_refs(raw_refs),
    )
