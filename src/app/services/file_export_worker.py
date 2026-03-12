"""
Background worker for exporting files from an evidence image to disk.

Uses :class:`BaseTask` (QRunnable) so it runs on the shared
:class:`QThreadPool` with progress / cancellation support.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.services.evidence_access import EvidenceAccessError, open_evidence_fs
from app.services.workers import BaseTask
from core.evidence_fs import EvidenceFS

logger = logging.getLogger(__name__)


@dataclass
class FileExportRequest:
    """One file to export from evidence."""

    file_path: str  # path inside the evidence image
    partition_index: Optional[int] = None  # None → use evidence default


@dataclass
class FileExportResult:
    """Summary returned by :class:`FileExportTask`."""

    exported: int = 0
    failed: int = 0
    errors: List[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.exported + self.failed

    def summary_message(self) -> str:
        if self.failed == 0:
            return f"Successfully exported {self.exported:,} file(s)."
        lines = [
            f"Exported {self.exported:,} of {self.total:,} file(s). "
            f"{self.failed:,} failed:"
        ]
        # Show at most 20 errors
        for err in self.errors[:20]:
            lines.append(f"  • {err}")
        if len(self.errors) > 20:
            lines.append(f"  … and {len(self.errors) - 20} more")
        return "\n".join(lines)


# Characters not allowed in filenames on Windows / most Linux UIs
_UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def _safe_filename(name: str) -> str:
    """Sanitise a filename for the host filesystem."""
    return _UNSAFE_CHARS.sub("_", name).strip(". ") or "unnamed"


def _unique_dest(dest_dir: Path, name: str) -> Path:
    """Return *dest_dir/name*, appending ``_1``, ``_2`` … on collision."""
    candidate = dest_dir / name
    if not candidate.exists():
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix
    counter = 1
    while True:
        candidate = dest_dir / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


class FileExportTask(BaseTask):
    """Export selected files from evidence to a user-chosen directory.

    One :class:`EvidenceFS` instance is opened per unique ``partition_index``
    encountered in the request list (pyewf handles must not be shared across
    threads, and partition switches require a new handle).

    Emits ``signals.progress(percent, message)`` for each file.
    """

    def __init__(
        self,
        evidence: Dict[str, Any],
        files: List[FileExportRequest],
        dest_dir: Path,
    ) -> None:
        super().__init__()
        self._evidence = evidence
        self._files = files
        self._dest_dir = dest_dir

    # ------------------------------------------------------------------
    def run_task(self) -> FileExportResult:
        result = FileExportResult()
        total = len(self._files)
        if total == 0:
            return result

        # Group files by partition_index so we can re-use a single FS
        # handle for the common case (all files in the same partition).
        groups: Dict[Optional[int], List[FileExportRequest]] = {}
        for req in self._files:
            groups.setdefault(req.partition_index, []).append(req)

        processed = 0
        for partition_idx, requests in groups.items():
            self.raise_if_cancelled()

            # Build an evidence dict for this specific partition
            ev = dict(self._evidence)
            if partition_idx is not None:
                ev["partition_index"] = partition_idx

            try:
                fs = open_evidence_fs(ev)
            except EvidenceAccessError as exc:
                # All files in this partition group fail
                for req in requests:
                    result.failed += 1
                    result.errors.append(f"{req.file_path}: {exc}")
                    processed += 1
                    self.report_progress(
                        int(processed * 100 / total),
                        f"Failed: {req.file_path}",
                    )
                continue

            try:
                for req in requests:
                    self.raise_if_cancelled()
                    processed += 1
                    self.report_progress(
                        int(processed * 100 / total),
                        f"Exporting: {req.file_path}",
                    )
                    self._export_one(fs, req, result)
            finally:
                # Close the EvidenceFS handle (important for PyEwfTskFS)
                _close = getattr(fs, "close", None)
                if callable(_close):
                    try:
                        _close()
                    except Exception:
                        pass

        return result

    # ------------------------------------------------------------------
    def _export_one(
        self,
        fs: EvidenceFS,
        req: FileExportRequest,
        result: FileExportResult,
    ) -> None:
        """Stream a single file from evidence to *self._dest_dir*."""
        dest_path: Optional[Path] = None
        try:
            # Derive a safe output filename
            basename = Path(req.file_path).name
            safe_name = _safe_filename(basename) if basename else "unnamed"
            dest_path = _unique_dest(self._dest_dir, safe_name)

            with open(dest_path, "wb") as out:
                for chunk in fs.open_for_stream(req.file_path):
                    out.write(chunk)

            result.exported += 1
            logger.debug("Exported %s → %s", req.file_path, dest_path)

        except FileNotFoundError:
            result.failed += 1
            result.errors.append(f"{req.file_path}: file not found in evidence")
            self._cleanup_partial(dest_path)
        except Exception as exc:
            result.failed += 1
            result.errors.append(f"{req.file_path}: {exc}")
            logger.warning("Export failed for %s: %s", req.file_path, exc)
            self._cleanup_partial(dest_path)

    @staticmethod
    def _cleanup_partial(path: Optional[Path]) -> None:
        """Remove a partially-written output file, if it exists."""
        if path is None:
            return
        try:
            if path.exists():
                path.unlink()
                logger.debug("Removed partial file: %s", path)
        except OSError as exc:
            logger.warning("Could not remove partial file %s: %s", path, exc)
