"""
Firefox Cache Manifest Writer

Manages incremental JSONL part-files that are merged into final manifest.json.
Optimized for O(1) writes instead of O(n²) re-serialization.

Part-file system:
- Entries written as JSONL to part files (manifest.part-0001.jsonl, etc.)
- Each part file contains up to PART_FILE_MAX_ENTRIES entries
- On completion, all parts merged into single manifest.json
- Supports mid-extraction resume via _load_part_files()

Reference:
- Fixed O(n²) → O(n) performance issue
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Callable

from core.logging import get_logger

LOGGER = get_logger("extractors.cache_firefox.manifest")

# Manifest constants
PART_FILE_MAX_ENTRIES = 10000  # Entries per part file


class ManifestWriter:
    """
    Incremental manifest writer using JSONL part-files.

    Usage:
        writer = ManifestWriter(output_dir, run_id)
        writer.begin()

        for entry in entries:
            writer.append(entry)

        writer.finalize(summary)
    """

    def __init__(
        self,
        output_dir: Path,
        run_id: str,
        on_entry: Optional[Callable[[dict], None]] = None,
        extractor_version: str = "1.12.0",
    ):
        """
        Initialize manifest writer.

        Args:
            output_dir: Directory for manifest files
            run_id: Extraction run identifier
            on_entry: Optional callback for each entry added
            extractor_version: Extractor version for manifest metadata
        """
        self.output_dir = output_dir
        self.run_id = run_id
        self.on_entry = on_entry
        self.extractor_version = extractor_version

        # Timestamp when extraction started
        self.extraction_timestamp = datetime.now(timezone.utc).isoformat()

        # Part file state
        self._part_file_index: int = 1
        self._part_file_handle: Optional[Any] = None
        self._part_file_count: int = 0
        self._total_entries: int = 0

        # Track all part files for cleanup
        self._part_files: List[Path] = []

    def begin(self) -> None:
        """Begin manifest writing - initialize first part file."""
        self._init_part_file()

    def _get_part_file_path(self) -> Path:
        """Get path for current part file."""
        return self.output_dir / f"manifest.part-{self._part_file_index:04d}.jsonl"

    def _init_part_file(self) -> None:
        """Initialize a new part file for writing."""
        self._close_current_part_file()

        part_path = self._get_part_file_path()
        self._part_files.append(part_path)
        self._part_file_handle = open(part_path, 'w', encoding='utf-8')
        self._part_file_count = 0
        LOGGER.debug("Opened part file: %s", part_path)

    def _close_current_part_file(self) -> None:
        """Close current part file if open."""
        if self._part_file_handle is not None:
            self._part_file_handle.close()
            self._part_file_handle = None

    def append(self, entry: dict) -> None:
        """
        Append single entry to manifest.

        Args:
            entry: Entry dict to append
        """
        if self._part_file_handle is None:
            self._init_part_file()

        # Write entry as JSONL
        self._part_file_handle.write(json.dumps(entry, ensure_ascii=False) + '\n')
        self._part_file_count += 1
        self._total_entries += 1

        # Callback for UI updates
        if self.on_entry:
            self.on_entry(entry)

        # Rotate part file if needed
        if self._part_file_count >= PART_FILE_MAX_ENTRIES:
            self._close_current_part_file()
            self._part_file_index += 1
            self._init_part_file()

    def append_batch(self, entries: List[dict]) -> None:
        """
        Append multiple entries efficiently.

        Args:
            entries: List of entry dicts to append
        """
        for entry in entries:
            self.append(entry)

    def _load_part_files(self) -> Iterator[dict]:
        """
        Load all entries from existing part files.

        Yields:
            Entry dicts from all part files in order
        """
        # Find all existing part files (manifest.part-####.jsonl)
        part_files = sorted(self.output_dir.glob("manifest.part-*.jsonl"))

        for part_path in part_files:
            with open(part_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            yield json.loads(line)
                        except json.JSONDecodeError as e:
                            LOGGER.warning("Failed to parse JSONL line in %s: %s", part_path, e)

    def finalize(
        self,
        summary: Optional[Dict[str, Any]] = None,
        cleanup_parts: bool = True,
    ) -> Path:
        """
        Finalize manifest by merging all part files.

        Args:
            summary: Optional summary dict to include
            cleanup_parts: Whether to delete part files after merge

        Returns:
            Path to final manifest.json
        """
        self._close_current_part_file()

        # Build final manifest with proper metadata
        manifest = {
            "extractor": "cache_firefox",
            "extractor_version": self.extractor_version,
            "version": self.extractor_version,  # Legacy alias
            "run_id": self.run_id,
            "extraction_timestamp": self.extraction_timestamp,
            "timestamp": datetime.now(timezone.utc).isoformat(),  # Finalization time
        }

        # Add summary if provided
        if summary:
            manifest.update(summary)

        # Collect all entries from part files
        entries = list(self._load_part_files())
        manifest["files"] = entries
        manifest["entry_count"] = len(entries)

        # Write final manifest
        manifest_path = self.output_dir / "manifest.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

        LOGGER.info("Wrote manifest with %d entries: %s", len(entries), manifest_path)

        # Cleanup part files
        if cleanup_parts:
            for part_path in self._part_files:
                try:
                    if part_path.exists():
                        part_path.unlink()
                        LOGGER.debug("Removed part file: %s", part_path)
                except OSError as e:
                    LOGGER.warning("Failed to remove part file %s: %s", part_path, e)

        return manifest_path

    def write_partial(self, status: str = "in_progress") -> Path:
        """
        Write partial manifest for mid-extraction resume.

        Args:
            status: Status string (e.g., "in_progress", "cancelled")

        Returns:
            Path to partial manifest (uses manifest.partial.json to not block ingestion)
        """
        self._close_current_part_file()

        manifest = {
            "extractor": "cache_firefox",
            "extractor_version": self.extractor_version,
            "version": self.extractor_version,  # Legacy alias
            "run_id": self.run_id,
            "extraction_timestamp": self.extraction_timestamp,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "partial": True,
        }

        # Collect entries so far
        entries = list(self._load_part_files())
        manifest["files"] = entries
        manifest["entry_count"] = len(entries)

        # Write to manifest.partial.json (NOT manifest.json) to avoid blocking ingestion
        # of previous successful runs. can_run_ingestion looks for manifest.json with status=ok
        manifest_path = self.output_dir / "manifest.partial.json"
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

        LOGGER.info("Wrote partial manifest with %d entries: %s", len(entries), manifest_path)
        return manifest_path

    @property
    def total_entries(self) -> int:
        """Get total number of entries written."""
        return self._total_entries

    def cleanup(self) -> None:
        """Cleanup resources and remove part files."""
        self._close_current_part_file()
        for part_path in self._part_files:
            try:
                if part_path.exists():
                    part_path.unlink()
            except OSError:
                pass


def load_manifest(manifest_path: Path) -> Dict[str, Any]:
    """
    Load and validate a manifest file.

    Args:
        manifest_path: Path to manifest.json

    Returns:
        Manifest dict with files list
    """
    with open(manifest_path, 'r', encoding='utf-8') as f:
        manifest = json.load(f)

    # Ensure files list exists
    if "files" not in manifest:
        manifest["files"] = []

    return manifest


def create_extraction_summary(
    extraction_mode: str,
    total_discovered: int,
    total_extracted: int,
    total_errors: int,
    duration_seconds: float,
    **kwargs,
) -> Dict[str, Any]:
    """
    Create summary dict for manifest.

    Args:
        extraction_mode: Mode used (icat, concurrent, sequential)
        total_discovered: Total files discovered
        total_extracted: Successfully extracted count
        total_errors: Error count
        duration_seconds: Extraction duration
        **kwargs: Additional summary fields

    Returns:
        Summary dict
    """
    summary = {
        "extraction_mode": extraction_mode,
        "total_discovered": total_discovered,
        "total_extracted": total_extracted,
        "total_errors": total_errors,
        "duration_seconds": round(duration_seconds, 2),
    }
    summary.update(kwargs)
    return summary
