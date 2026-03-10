"""
Shared utility for opening an EvidenceFS from an evidence metadata dict.

Consolidates the mount logic that was previously duplicated in
ExtractionTab.mount_evidence_filesystem() and CaseWideExtractionTask._mount_evidence().
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from core.evidence_fs import EvidenceFS, MountedFS, PyEwfTskFS, find_ewf_segments

logger = logging.getLogger(__name__)


class EvidenceAccessError(Exception):
    """Raised when evidence cannot be opened."""


def open_evidence_fs(evidence: Dict[str, Any]) -> EvidenceFS:
    """Open an EvidenceFS for the given evidence metadata dict.

    Args:
        evidence: Dict from ``CaseDataAccess.get_evidence(evidence_id)``.
            Must contain at least ``source_path``.  ``partition_index``
            is optional (defaults to *-1* = auto-detect).

    Returns:
        A ready-to-use :class:`EvidenceFS` instance (either
        :class:`PyEwfTskFS` or :class:`MountedFS`).

    Raises:
        EvidenceAccessError: If the source cannot be opened for any reason
            (missing path, file not found, encrypted FS, unsupported format).
    """
    source_path_str = evidence.get("source_path", "")
    if not source_path_str:
        raise EvidenceAccessError("Evidence has no source_path — cannot open")

    source_path = Path(source_path_str)

    # Check existence (may fail on encrypted FS)
    try:
        exists = source_path.exists()
    except OSError as exc:
        if exc.errno == 126:
            raise EvidenceAccessError(
                f"Cannot access evidence — file is on encrypted filesystem "
                f"and key not loaded: {source_path}"
            ) from exc
        raise EvidenceAccessError(
            f"Cannot access evidence file: {exc}"
        ) from exc

    if not exists:
        raise EvidenceAccessError(f"Evidence source not found: {source_path}")

    # Directory → MountedFS
    if source_path.is_dir():
        try:
            return MountedFS(source_path)
        except Exception as exc:
            raise EvidenceAccessError(
                f"Failed to mount directory {source_path}: {exc}"
            ) from exc

    # E01 image → PyEwfTskFS
    if source_path.suffix.lower() in (".e01", ".e02", ".e03"):
        try:
            segments = find_ewf_segments(source_path)
            if not segments:
                raise EvidenceAccessError(
                    f"No E01 segments found for {source_path}"
                )

            partition_index = evidence.get("partition_index")
            if partition_index is None:
                partition_index = -1  # auto-detect

            fs = PyEwfTskFS(segments, partition_index=partition_index)
            logger.info(
                "Opened E01: %s (partition %s, fs_type=%s)",
                source_path.name,
                partition_index,
                fs.fs_type,
            )
            return fs
        except EvidenceAccessError:
            raise
        except Exception as exc:
            raise EvidenceAccessError(
                f"Failed to mount E01 {source_path.name}: {exc}"
            ) from exc

    raise EvidenceAccessError(f"Unsupported evidence format: {source_path.suffix}")


def evidence_has_source(evidence: Optional[Dict[str, Any]]) -> bool:
    """Check whether the evidence dict points to an accessible source.

    This is a *fast* check (stat only, no EWF open) suitable for deciding
    whether to show/enable export actions in the UI.

    Returns:
        ``True`` if ``source_path`` exists on disk (file or directory).
    """
    if evidence is None:
        return False

    source_path_str = evidence.get("source_path", "")
    if not source_path_str:
        return False

    try:
        return Path(source_path_str).exists()
    except OSError:
        return False
