"""
Orchestration for the Tag Summary XLSX working document.

Lives in ``src/reports/`` (the self-contained reports module) rather
than in the app shim so the export pipeline — path validation,
data composition, workbook write, and forensic audit — does not leak
into the UI feature layer.

The function below intentionally takes only plain values (no Qt types)
so it can be exercised from tests and the architecture boundary
``features/* → reports/`` is preserved.
"""
from __future__ import annotations

import logging
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from core.database.helpers.process_log import insert_process_log
from core.safe_io import ensure_dir_no_follow, unlink_no_follow

from reports.tag_summary_export import write_tag_summary_xlsx
from reports.tag_summary_presentation import compose_tag_summary

__all__ = [
    "TagSummaryExportError",
    "TagSummaryExportResult",
    "export_tag_summary",
]


logger = logging.getLogger(__name__)


def _renderer_metadata() -> str:
    try:
        from PIL import __version__ as pillow_version
    except ImportError:
        pillow_version = None
    return json.dumps(
        {
            "workbook_writer": "reports.tag_summary_export",
            "thumbnail_renderer": "Pillow" if pillow_version else None,
            "pillow_version": pillow_version,
        },
        sort_keys=True,
    )


class TagSummaryExportError(Exception):
    """Raised when the tag-summary export pipeline fails.

    The ``stage`` attribute identifies *where* the failure happened so
    the UI layer can surface a tailored message without having to
    inspect exception types.
    """

    def __init__(self, message: str, *, stage: str):
        super().__init__(message)
        self.stage = stage


@dataclass(frozen=True)
class TagSummaryExportResult:
    """Successful-export outcome returned to callers."""

    output_path: Path
    record_count: int


def _resolve_within(child: Path, parent: Path) -> Optional[Path]:
    """Return ``child.resolve()`` if it lives under ``parent``, else None."""
    try:
        resolved = child.resolve()
        resolved.relative_to(parent)
    except (OSError, ValueError):
        return None
    return resolved


def export_tag_summary(
    *,
    conn: sqlite3.Connection,
    evidence_id: int,
    evidence_label: str,
    case_folder: Path,
    output_path: Path,
    exported_at_iso: str,
    finished_at_iso: Optional[str] = None,
    top_n: int = 10,
) -> TagSummaryExportResult:
    """
    Run the full tag-summary export pipeline.

    Steps (any failure raises :class:`TagSummaryExportError` with a
    ``stage`` discriminator):

    1. ``stage="path"``: refuse paths outside ``case_folder``.
    2. ``stage="compose"``: query the evidence DB via the helpers.
    3. ``stage="write"``: serialise the workbook to ``output_path``.
    4. ``stage="audit"``: append a ``process_log`` row.  If the audit
       insert fails *after* the workbook has been written, the
       workbook is removed so we never leave an exported artifact on
       disk without a provenance entry.

    The audit step intentionally runs *after* the file is on disk so
    its ``exit_code`` truthfully reflects the write outcome (a previous
    revision wrote the audit row before the workbook existed, which
    could record a successful export for a file that was never
    produced).
    """
    case_root = case_folder.resolve()
    safe_path = _resolve_within(output_path, case_root)
    if safe_path is None:
        raise TagSummaryExportError(
            "Refusing to write tag-summary export outside the case workspace.",
            stage="path",
        )
    if safe_path.exists():
        raise TagSummaryExportError(
            "Refusing to overwrite an existing tag-summary export.",
            stage="path",
        )

    try:
        ensure_dir_no_follow(safe_path.parent, containment_root=case_root)
    except Exception as exc:
        raise TagSummaryExportError(
            "Failed to create the case-workspace export directory.",
            stage="path",
        ) from exc

    try:
        data = compose_tag_summary(
            conn,
            evidence_id,
            evidence_label=evidence_label,
            exported_at_iso=exported_at_iso,
            top_n=top_n,
            case_folder=case_root,
        )
    except Exception as exc:
        logger.exception("Failed to gather tag-summary export data")
        raise TagSummaryExportError(
            "Failed to query the evidence database for the export.",
            stage="compose",
        ) from exc

    record_count = (
        sum(s["total"] for t in data.tags for s in t["sections"])
        + sum(b["total"] for b in data.reference_list_matches)
    )

    try:
        written = write_tag_summary_xlsx(data, safe_path, containment_root=case_root)
    except Exception as exc:
        logger.exception("Failed to write tag-summary XLSX")
        raise TagSummaryExportError(
            f"Failed to write XLSX to: {safe_path}",
            stage="write",
        ) from exc

    try:
        insert_process_log(
            conn,
            evidence_id,
            tool_name="reports.tag_summary_export",
            command_line="export_tag_summary",
            finished_at=finished_at_iso or exported_at_iso,
            exit_code=0,
            output_path=str(written),
            extractor_name="reports.tag_summary_export",
            record_count=record_count,
            metadata=_renderer_metadata(),
        )
    except Exception as exc:
        logger.exception(
            "Failed to record tag-summary export in process_log; "
            "rolling back workbook to preserve provenance integrity"
        )
        try:
            unlink_no_follow(written, containment_root=case_root, missing_ok=True)
        except OSError:
            logger.exception("Failed to remove un-audited workbook %s", written)
        raise TagSummaryExportError(
            "Failed to record the export in the forensic audit log. "
            "The workbook was removed so no exported artifact exists "
            "without a provenance entry.",
            stage="audit",
        ) from exc

    return TagSummaryExportResult(output_path=written, record_count=record_count)
