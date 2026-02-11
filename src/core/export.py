"""
Case export system for packaging cases into ZIP archives.

Enables investigators to create portable case packages for archival, sharing,
or migration. Supports selective inclusion of large artifacts with SHA256
checksums for integrity verification.

Module design per  requirements Q2 (selective export), Q3 (no encryption).
"""
from __future__ import annotations

import getpass
import hashlib
import json
import os
import sqlite3
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, Optional

from .database import DatabaseManager
from .logging import get_logger

LOGGER = get_logger("core.export")

# Export format version (for future compatibility tracking)
EXPORT_FORMAT_VERSION = "1.0"

# Chunk size for streaming large files (100 MB)
CHUNK_SIZE = 100 * 1024 * 1024


def _find_case_database(case_folder: Path) -> Optional[Path]:
    """
    Find the case database file in the case folder.

    Delegates to the shared find_case_database() for primary + legacy pattern lookup.

    Args:
        case_folder: Path to case workspace

    Returns:
        Path to case database file, or None if not found
    """
    from core.database import find_case_database
    result = find_case_database(case_folder)
    if result is None:
        LOGGER.warning("No case database found in %s", case_folder)
    return result


@dataclass
class ExportOptions:
    """
    User-configurable export options.

    Attributes:
        include_source_evidence: Include E01/raw evidence files (can be 100+ GB)
        include_cached_artifacts: Include carved/, cache/, thumbnails/ directories
        include_logs: Include case audit and evidence log files
        include_reports: Include reports/ directory

    Note:
        The following are ALWAYS included (not configurable):
        - Case database and companion -wal/-shm/-journal files
        - Evidence databases (and companion files)
        - Multi-segment E01 files (all segments)
        - Process logs (embedded in evidence databases)
        - Manifests (embedded in evidence databases)
    """
    include_source_evidence: bool = False
    include_cached_artifacts: bool = False
    include_logs: bool = False
    include_reports: bool = True  # Default to True


@dataclass
class FileEntry:
    """
    Single file entry in export manifest.

    Attributes:
        Rel paths are relative to case root (e.g., "CASE-001_surfsifter.sqlite", "evidences/evidence_1.sqlite")
        size_bytes: File size in bytes
        sha256: SHA256 checksum (hex string)
        category: File category ("database", "evidence", "artifact", "log")
    """
    rel_path: str
    size_bytes: int
    sha256: str
    category: str


@dataclass
class ExportManifest:
    """
    Manifest embedded in export package.

    Contains metadata and file checksums for integrity verification.
    Serialized as JSON and included in ZIP root as "export_manifest.json".

    Attributes:
        export_version: Export format version (e.g., "1.0")
        case_id: Case identifier
        case_title: Case title
        investigator: Investigator name (may be None)
        exported_at_utc: ISO 8601 timestamp of export
        exported_by: System username or "unknown"
        schema_version: Database schema version (from migrations)
        evidence_count: Number of evidences in case
        file_list: List of all files with checksums
        total_size_bytes: Total size of all files
    """
    export_version: str
    case_id: str
    case_title: str
    investigator: Optional[str]
    exported_at_utc: str
    exported_by: str
    schema_version: int
    evidence_count: int
    file_list: List[FileEntry]
    total_size_bytes: int

    def to_dict(self) -> dict:
        """Convert manifest to dictionary for JSON serialization."""
        return {
            "export_version": self.export_version,
            "case_id": self.case_id,
            "case_title": self.case_title,
            "investigator": self.investigator,
            "exported_at_utc": self.exported_at_utc,
            "exported_by": self.exported_by,
            "schema_version": self.schema_version,
            "evidence_count": self.evidence_count,
            "file_list": [
                {
                    "rel_path": entry.rel_path,
                    "size_bytes": entry.size_bytes,
                    "sha256": entry.sha256,
                    "category": entry.category,
                }
                for entry in self.file_list
            ],
            "total_size_bytes": self.total_size_bytes,
        }


@dataclass
class ExportResult:
    """
    Result of export operation.

    Attributes:
        success: True if export completed successfully
        export_path: Path to created ZIP file (None if failed)
        exported_files: Number of files exported
        total_size_bytes: Total size of exported data
        duration_seconds: Time taken to complete export
        error_message: Error message if failed (None if succeeded)
    """
    success: bool
    export_path: Optional[Path] = None
    exported_files: int = 0
    total_size_bytes: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None


def estimate_export_size(
    case_folder: Path,
    options: ExportOptions,
    *,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> int:
    """
    Calculate total size of files to be exported.

    Walks file tree and sums sizes for selected categories. Used to show
    size estimate in ExportDialog before creating package.

    Args:
        case_folder: Path to case workspace
        options: Export options (which categories to include)
        progress_callback: Optional callback(current_file_index, total_files)

    Returns:
        Total size in bytes

    Raises:
        FileNotFoundError: If case_folder doesn't exist
        PermissionError: If files not readable

    Example:
        >>> options = ExportOptions(include_source_evidence=True)
        >>> size = estimate_export_size(Path("/cases/CASE-001"), options)
        >>> print(f"Export size: {size / (1024**3):.2f} GB")
        Export size: 2.45 GB
    """
    if not case_folder.exists():
        LOGGER.error("Case folder not found: %s", case_folder)
        raise FileNotFoundError(f"Case folder not found: {case_folder}")

    LOGGER.info("Estimating export size for %s", case_folder)

    total_size = 0
    files_to_scan: List[Path] = []

    # Always include: case database (any name) and evidences/**/*.sqlite
    case_db = _find_case_database(case_folder)
    if case_db:
        files_to_scan.append(case_db)
    else:
        LOGGER.warning("No case database found in %s", case_folder)

    evidences_dir = case_folder / "evidences"
    if evidences_dir.exists():
        # Evidence DBs are in subdirectories: evidences/<slug>/evidence_<slug>.sqlite
        files_to_scan.extend(evidences_dir.rglob("*.sqlite"))

    # Optional: Source evidence files
    if options.include_source_evidence:
        # Query case database for evidence source paths
        try:
            # Use the detected case database path
            if case_db:
                db_mgr = DatabaseManager(case_folder, case_db_path=case_db)
                conn = db_mgr.get_case_conn()
            else:
                raise FileNotFoundError("No case database found")
            cursor = conn.execute("SELECT source_path FROM evidences")
            for row in cursor:
                source_path = Path(row[0])
                if source_path.exists():
                    files_to_scan.append(source_path)
            conn.close()
        except Exception as exc:
            LOGGER.warning("Failed to query evidence source paths: %s", exc)

    # Optional: Cached artifacts (all extractor outputs and thumbnails)
    if options.include_cached_artifacts:
        # Case-level artifact directories
        for artifact_dir in ["carved", "cache", "thumbnails", ".thumbs"]:
            artifact_path = case_folder / artifact_dir
            if artifact_path.exists():
                files_to_scan.extend(artifact_path.rglob("*"))

        # Evidence-level: Include ALL files in evidence subdirectories
        # (except .sqlite and companion files which are already handled)
        evidences_dir = case_folder / "evidences"
        if evidences_dir.exists():
            for evidence_subdir in evidences_dir.iterdir():
                if evidence_subdir.is_dir():
                    for f in evidence_subdir.rglob("*"):
                        if f.is_file() and f.suffix != ".sqlite" and not _is_sqlite_companion_file(f):
                            files_to_scan.append(f)

    # Optional: Reports
    if options.include_reports:
        reports_dir = case_folder / "reports"
        if reports_dir.exists():
            files_to_scan.extend(reports_dir.rglob("*"))

    # Optional: Log files
    if options.include_logs:
        # Case audit log
        case_audit_log = case_folder / "case_audit.log"
        if case_audit_log.exists():
            files_to_scan.append(case_audit_log)
        # Rotated case logs
        for backup in case_folder.glob("case_audit.log.*"):
            files_to_scan.append(backup)

        # Evidence logs
        logs_dir = case_folder / "logs"
        if logs_dir.exists():
            files_to_scan.extend(logs_dir.glob("evidence_*.log*"))

    # Calculate total size
    total_files = len(files_to_scan)
    for idx, file_path in enumerate(files_to_scan):
        if file_path.is_file():
            try:
                total_size += file_path.stat().st_size
            except OSError as exc:
                LOGGER.warning("Failed to stat file %s: %s", file_path, exc)

        if progress_callback and idx % 10 == 0:
            progress_callback(idx, total_files)

    if progress_callback:
        progress_callback(total_files, total_files)

    LOGGER.info("Estimated export size: %d bytes (%d files)", total_size, total_files)
    return total_size


def _calculate_file_sha256(file_path: Path, chunk_size: int = CHUNK_SIZE) -> str:
    """
    Calculate SHA256 checksum of a file.

    Streams file in chunks to handle large files without memory issues.

    Args:
        file_path: Path to file
        chunk_size: Size of chunks to read (default: 100 MB)

    Returns:
        SHA256 checksum as hex string
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


# SQLite companion file suffixes (WAL mode, journal mode)
_SQLITE_COMPANION_SUFFIXES = ("-wal", "-shm", "-journal")


def _is_sqlite_companion_file(path: Path) -> bool:
    """Check if a path is a SQLite companion file (WAL, SHM, or journal).

    These files are handled separately by _add_sqlite_with_companions and
    should not be collected again during artifact scanning.

    Args:
        path: Path to check

    Returns:
        True if this is a SQLite companion file
    """
    name = path.name
    return any(name.endswith(suffix) for suffix in _SQLITE_COMPANION_SUFFIXES)


def _add_sqlite_with_companions(
    files_to_export: List[tuple],
    db_path: Path,
    arcname: str,
    category: str
) -> None:
    """Add SQLite database and its WAL/journal companions to export list.

    Args:
        files_to_export: List to append (source_path, arcname, category) tuples
        db_path: Path to the .sqlite file
        arcname: Archive name for the main database
        category: Category string ("database", "artifact", etc.)
    """
    # Add main database
    files_to_export.append((db_path, arcname, category))

    # Add companion files with matching arcnames
    # WAL/journal files must be alongside the DB in archive for SQLite to use them
    for suffix in ["-wal", "-shm", "-journal"]:
        companion = db_path.parent / (db_path.name + suffix)
        if companion.exists():
            companion_arcname = arcname + suffix
            files_to_export.append((companion, companion_arcname, category))


def _collect_multi_segment_evidence(
    source_path: Path,
    label: str,
) -> List[tuple]:
    """Collect all segments of multi-segment evidence files (E01, etc.).

    Args:
        source_path: Path to the first segment (e.g., image.E01)
        label: Evidence label for arcname prefix

    Returns:
        List of (source_path, arcname, category) tuples
    """
    files_to_export = []
    source_lower = source_path.suffix.lower()

    # Check if this is a multi-segment format
    if source_lower in ('.e01', '.ex01', '.s01'):
        base_name = source_path.stem
        parent = source_path.parent

        # Find all segments with case-insensitive matching (Linux compatibility)
        all_segments = []
        for f in parent.iterdir():
            if f.is_file() and f.stem.lower() == base_name.lower():
                suffix_lower = f.suffix.lower()
                # Match .e01-.e99, .eaa-.ezz patterns (EWF segments)
                if len(suffix_lower) == 4 and suffix_lower.startswith('.e'):
                    # Check if it's a valid segment suffix (e01-e99 or eaa-ezz)
                    suffix_chars = suffix_lower[2:]
                    if suffix_chars.isdigit() or suffix_chars.isalpha():
                        all_segments.append(f)
                # Also match .s01 (split raw) and .ex01 (EnCase v7+)
                elif suffix_lower.startswith('.s') or suffix_lower.startswith('.ex'):
                    all_segments.append(f)

        # Sort segments for deterministic ordering
        all_segments.sort(key=lambda p: p.suffix.lower())

        for segment in all_segments:
            arcname = f"evidence_sources/{label}_{segment.name}"
            files_to_export.append((segment, arcname, "evidence"))
    else:
        # Single file evidence (DD, raw, etc.)
        arcname = f"evidence_sources/{label}_{source_path.name}"
        files_to_export.append((source_path, arcname, "evidence"))

    return files_to_export


def _get_schema_version(case_folder: Path) -> int:
    """
    Get current schema version from case database.

    Args:
        case_folder: Path to case workspace

    Returns:
        Schema version (e.g., 9 for migration 0009)
    """
    try:
        db_path = _find_case_database(case_folder)
        if not db_path:
            LOGGER.warning("No case database found for schema version check")
            return 0

        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT MAX(version) FROM schema_version")
        row = cursor.fetchone()
        conn.close()
        return row[0] if row and row[0] else 0
    except Exception as exc:
        LOGGER.warning("Failed to get schema version: %s", exc)
        return 0


def generate_export_manifest(
    case_folder: Path,
    file_list: List[FileEntry],
    options: ExportOptions,
) -> ExportManifest:
    """
    Generate export manifest with metadata and file checksums.

    Queries case database for case metadata and constructs manifest.

    Args:
        case_folder: Path to case workspace
        file_list: List of files included in export (with checksums)
        options: Export options used

    Returns:
        ExportManifest ready for JSON serialization

    Raises:
        FileNotFoundError: If case database doesn't exist
        sqlite3.DatabaseError: If database query fails
    """
    db_path = _find_case_database(case_folder)
    if not db_path:
        raise FileNotFoundError(f"No case database found in {case_folder}")

    try:
        # Pass the detected database path to DatabaseManager
        db_mgr = DatabaseManager(case_folder, case_db_path=db_path)
        conn = db_mgr.get_case_conn()
        conn.row_factory = sqlite3.Row

        # Query case metadata - with fallback to folder name if no record exists
        cursor = conn.execute("SELECT case_id, title, investigator FROM cases LIMIT 1")
        row = cursor.fetchone()

        if row:
            case_id = row["case_id"]
            case_title = row["title"] or "Untitled Case"
            investigator = row["investigator"] or "Unknown"
        else:
            # Fallback: use folder name if no case record exists
            LOGGER.warning("No case record found, using folder name as case_id")
            case_id = case_folder.name
            case_title = case_folder.name
            investigator = "Unknown"

        # Count evidences
        cursor = conn.execute("SELECT COUNT(*) as count FROM evidences")
        evidence_count = cursor.fetchone()["count"]

        conn.close()

        # Get schema version
        schema_version = _get_schema_version(case_folder)

        # Calculate total size
        total_size = sum(entry.size_bytes for entry in file_list)

        # Get system username
        try:
            exported_by = getpass.getuser()
        except Exception:
            exported_by = "unknown"

        manifest = ExportManifest(
            export_version=EXPORT_FORMAT_VERSION,
            case_id=case_id,
            case_title=case_title,
            investigator=investigator,
            exported_at_utc=datetime.now(tz=timezone.utc).isoformat(),
            exported_by=exported_by,
            schema_version=schema_version,
            evidence_count=evidence_count,
            file_list=file_list,
            total_size_bytes=total_size,
        )

        LOGGER.info(
            "Generated manifest: case_id=%s, %d files, %d bytes",
            case_id, len(file_list), total_size
        )

        return manifest

    except sqlite3.DatabaseError as exc:
        LOGGER.exception("Database error generating manifest")
        raise


def create_export_package(
    case_folder: Path,
    dest_path: Path,
    options: ExportOptions,
    *,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> ExportResult:
    """
    Create ZIP export package with manifest and checksums.

    Creates a ZIP file containing selected case artifacts with SHA256
    checksums for integrity verification. Generates export_manifest.json
    and includes it in the package root.

    Args:
        case_folder: Path to case workspace
        dest_path: Destination path for ZIP file (e.g., /exports/CASE-001.zip)
        options: Export options controlling what to include
        progress_callback: Optional callback(current_bytes, total_bytes, current_file)

    Returns:
        ExportResult with success status and metadata

    Raises:
        FileNotFoundError: If case_folder doesn't exist
        PermissionError: If destination not writable
        OSError: If ZIP creation fails

    Example:
        >>> result = create_export_package(
        ...     Path("/cases/CASE-001"),
        ...     Path("/exports/CASE-001.zip"),
        ...     options
        ... )
        >>> if result.success:
        ...     print(f"Exported {result.exported_files} files to {result.export_path}")

    ZIP Structure:
        CASE-2025-001.zip/
        ├── export_manifest.json         # Manifest with checksums
        ├── CASE-2025-001_surfsifter.sqlite # Case database
        ├── CASE-2025-001_surfsifter.sqlite-wal  # SQLite WAL (if exists)
        ├── evidences/
        │   ├── evidence_1_evid-e01.sqlite
        │   ├── evidence_2_test-img.sqlite
        │   └── ...
        ├── evidence_sources/            # If include_source_evidence
        │   ├── EVID-E01.E01
        │   ├── EVID-E01.E02             # All segments included
        │   └── ...
        ├── reports/                     # If include_reports (default)
        │   └── *.pdf
        ├── carved/                      # If include_cached_artifacts
        ├── cache/
        └── thumbnails/
    """
    start_time = time.time()

    if not case_folder.exists():
        error_msg = f"Case folder not found: {case_folder}"
        LOGGER.error(error_msg)
        return ExportResult(success=False, error_message=error_msg)

    # Ensure destination directory exists
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Starting export: %s -> %s", case_folder, dest_path)

    try:
        # Collect files to export
        files_to_export: List[tuple[Path, str, str]] = []  # (source_path, arcname, category)

        # Always include: case database with companion files
        case_db = _find_case_database(case_folder)
        if case_db:
            arcname = case_db.name
            _add_sqlite_with_companions(files_to_export, case_db, arcname, "database")

        # Always include: evidences/**/*.sqlite with companions
        evidences_dir = case_folder / "evidences"
        if evidences_dir.exists():
            for evidence_db in evidences_dir.rglob("*.sqlite"):
                # Skip companion files (handled by _add_sqlite_with_companions)
                if any(evidence_db.name.endswith(s) for s in ["-wal", "-shm", "-journal"]):
                    continue
                rel_path = evidence_db.relative_to(case_folder)
                arcname = str(rel_path)
                _add_sqlite_with_companions(files_to_export, evidence_db, arcname, "database")

        # Optional: Source evidence files (with multi-segment support)
        if options.include_source_evidence:
            try:
                if case_db:
                    db_mgr = DatabaseManager(case_folder, case_db_path=case_db)
                    conn = db_mgr.get_case_conn()
                else:
                    raise FileNotFoundError("No case database found")
                cursor = conn.execute("SELECT id, label, source_path FROM evidences")
                for row in cursor:
                    source_path = Path(row[2])
                    if source_path.exists() and source_path.is_file():
                        evidence_id = row[0]
                        label = row[1] or f"evidence_{evidence_id}"
                        # Use helper for multi-segment support
                        segment_files = _collect_multi_segment_evidence(source_path, label)
                        files_to_export.extend(segment_files)
                conn.close()
            except Exception as exc:
                LOGGER.warning("Failed to include source evidence files: %s", exc)

        # Optional: Cached artifacts (all extractor outputs and thumbnails)
        if options.include_cached_artifacts:
            # Case-level artifact directories (carved, cache, thumbnails, .thumbs)
            for artifact_dir_name in ["carved", "cache", "thumbnails", ".thumbs"]:
                artifact_dir = case_folder / artifact_dir_name
                if artifact_dir.exists():
                    for artifact_file in artifact_dir.rglob("*"):
                        if artifact_file.is_file():
                            rel_path = artifact_file.relative_to(case_folder)
                            arcname = str(rel_path)
                            files_to_export.append((artifact_file, arcname, "artifact"))

            # Evidence-level: Include ALL files in evidence subdirectories
            # (except .sqlite and companion files which are already handled)
            if evidences_dir.exists():
                for evidence_subdir in evidences_dir.iterdir():
                    if evidence_subdir.is_dir():
                        for f in evidence_subdir.rglob("*"):
                            if f.is_file() and f.suffix != ".sqlite" and not _is_sqlite_companion_file(f):
                                rel_path = f.relative_to(case_folder)
                                arcname = str(rel_path)
                                files_to_export.append((f, arcname, "artifact"))

        # Optional: Reports directory
        if options.include_reports:
            reports_dir = case_folder / "reports"
            if reports_dir.exists():
                for report_file in reports_dir.rglob("*"):
                    if report_file.is_file():
                        rel_path = report_file.relative_to(case_folder)
                        arcname = str(rel_path)
                        files_to_export.append((report_file, arcname, "report"))

        # Optional: Log files
        if options.include_logs:
            # Case audit log
            case_audit_log = case_folder / "case_audit.log"
            if case_audit_log.exists():
                files_to_export.append((case_audit_log, "case_audit.log", "log"))
            # Rotated case logs
            for backup in case_folder.glob("case_audit.log.*"):
                files_to_export.append((backup, backup.name, "log"))

            # Evidence logs directory
            logs_dir = case_folder / "logs"
            if logs_dir.exists():
                for log_file in logs_dir.glob("evidence_*.log*"):
                    arcname = f"logs/{log_file.name}"
                    files_to_export.append((log_file, arcname, "log"))

        # Calculate total size for progress tracking
        total_bytes = sum(f[0].stat().st_size for f in files_to_export if f[0].is_file())
        current_bytes = 0

        # Create ZIP file
        file_entries: List[FileEntry] = []

        with zipfile.ZipFile(dest_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for source_path, arcname, category in files_to_export:
                if not source_path.is_file():
                    continue

                # Calculate checksum while reading file
                LOGGER.debug("Adding to ZIP: %s -> %s", source_path, arcname)
                sha256 = _calculate_file_sha256(source_path)
                size_bytes = source_path.stat().st_size

                # Add to ZIP
                zipf.write(source_path, arcname)

                # Record in manifest
                file_entries.append(FileEntry(
                    rel_path=arcname,
                    size_bytes=size_bytes,
                    sha256=sha256,
                    category=category,
                ))

                # Update progress
                current_bytes += size_bytes
                if progress_callback:
                    progress_callback(current_bytes, total_bytes, arcname)

            # Generate and add manifest
            manifest = generate_export_manifest(case_folder, file_entries, options)
            manifest_json = json.dumps(manifest.to_dict(), indent=2, sort_keys=True)
            zipf.writestr("export_manifest.json", manifest_json)

        duration = time.time() - start_time

        LOGGER.info(
            "Export complete: %d files, %d bytes, %.2f seconds",
            len(file_entries), total_bytes, duration
        )

        return ExportResult(
            success=True,
            export_path=dest_path,
            exported_files=len(file_entries),
            total_size_bytes=total_bytes,
            duration_seconds=duration,
        )

    except PermissionError as exc:
        error_msg = f"Permission denied writing to {dest_path}: {exc}"
        LOGGER.error(error_msg)
        return ExportResult(success=False, error_message=error_msg, duration_seconds=time.time() - start_time)

    except OSError as exc:
        error_msg = f"OS error during export: {exc}"
        LOGGER.exception(error_msg)
        # Clean up partial ZIP file
        if dest_path.exists():
            try:
                dest_path.unlink()
                LOGGER.info("Cleaned up partial export file: %s", dest_path)
            except Exception:
                pass
        return ExportResult(success=False, error_message=error_msg, duration_seconds=time.time() - start_time)

    except Exception as exc:
        error_msg = f"Unexpected error during export: {exc}"
        LOGGER.exception(error_msg)
        # Clean up partial ZIP file
        if dest_path.exists():
            try:
                dest_path.unlink()
            except Exception:
                pass
        return ExportResult(success=False, error_message=error_msg, duration_seconds=time.time() - start_time)


def create_export_package_cancellable(
    case_folder: Path,
    dest_path: Path,
    options: ExportOptions,
    *,
    cancel_check: Callable[[], bool],
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> ExportResult:
    """Create ZIP export package with cancellation support.

    Same as create_export_package but checks cancel_check before each file.
    Cleans up partial ZIP on cancellation.

    Args:
        case_folder: Path to case workspace
        dest_path: Destination path for ZIP file
        options: Export options controlling what to include
        cancel_check: Callable that returns True if cancellation requested
        progress_callback: Optional callback(current_bytes, total_bytes, current_file)

    Returns:
        ExportResult with success=False and error_message="Cancelled by user" if cancelled
    """
    start_time = time.time()

    if not case_folder.exists():
        error_msg = f"Case folder not found: {case_folder}"
        LOGGER.error(error_msg)
        return ExportResult(success=False, error_message=error_msg)

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Starting cancellable export: %s -> %s", case_folder, dest_path)

    try:
        # Collect files (same logic as create_export_package)
        files_to_export: List[tuple[Path, str, str]] = []

        case_db = _find_case_database(case_folder)
        if case_db:
            arcname = case_db.name
            _add_sqlite_with_companions(files_to_export, case_db, arcname, "database")

        evidences_dir = case_folder / "evidences"
        if evidences_dir.exists():
            for evidence_db in evidences_dir.rglob("*.sqlite"):
                if any(evidence_db.name.endswith(s) for s in ["-wal", "-shm", "-journal"]):
                    continue
                rel_path = evidence_db.relative_to(case_folder)
                arcname = str(rel_path)
                _add_sqlite_with_companions(files_to_export, evidence_db, arcname, "database")

        if options.include_source_evidence:
            try:
                if case_db:
                    db_mgr = DatabaseManager(case_folder, case_db_path=case_db)
                    conn = db_mgr.get_case_conn()
                    cursor = conn.execute("SELECT id, label, source_path FROM evidences")
                    for row in cursor:
                        source_path = Path(row[2])
                        if source_path.exists() and source_path.is_file():
                            label = row[1] or f"evidence_{row[0]}"
                            segment_files = _collect_multi_segment_evidence(source_path, label)
                            files_to_export.extend(segment_files)
                    conn.close()
            except Exception as exc:
                LOGGER.warning("Failed to include source evidence files: %s", exc)

        if options.include_cached_artifacts:
            # Case-level artifact directories (carved, cache, thumbnails, .thumbs)
            for artifact_dir_name in ["carved", "cache", "thumbnails", ".thumbs"]:
                artifact_dir = case_folder / artifact_dir_name
                if artifact_dir.exists():
                    for artifact_file in artifact_dir.rglob("*"):
                        if artifact_file.is_file():
                            rel_path = artifact_file.relative_to(case_folder)
                            files_to_export.append((artifact_file, str(rel_path), "artifact"))
            # Evidence-level: Include ALL files in evidence subdirectories
            # (except .sqlite and companion files which are already handled)
            if evidences_dir.exists():
                for evidence_subdir in evidences_dir.iterdir():
                    if evidence_subdir.is_dir():
                        for f in evidence_subdir.rglob("*"):
                            if f.is_file() and f.suffix != ".sqlite" and not _is_sqlite_companion_file(f):
                                rel_path = f.relative_to(case_folder)
                                files_to_export.append((f, str(rel_path), "artifact"))

        if options.include_reports:
            reports_dir = case_folder / "reports"
            if reports_dir.exists():
                for report_file in reports_dir.rglob("*"):
                    if report_file.is_file():
                        rel_path = report_file.relative_to(case_folder)
                        files_to_export.append((report_file, str(rel_path), "report"))

        if options.include_logs:
            case_audit_log = case_folder / "case_audit.log"
            if case_audit_log.exists():
                files_to_export.append((case_audit_log, "case_audit.log", "log"))
            for backup in case_folder.glob("case_audit.log.*"):
                files_to_export.append((backup, backup.name, "log"))
            logs_dir = case_folder / "logs"
            if logs_dir.exists():
                for log_file in logs_dir.glob("evidence_*.log*"):
                    files_to_export.append((log_file, f"logs/{log_file.name}", "log"))

        total_bytes = sum(f[0].stat().st_size for f in files_to_export if f[0].is_file())
        current_bytes = 0
        file_entries: List[FileEntry] = []

        with zipfile.ZipFile(dest_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for source_path, arcname, category in files_to_export:
                # Check for cancellation before each file
                if cancel_check():
                    LOGGER.info("Export cancelled by user")
                    zipf.close()
                    # Clean up partial ZIP
                    if dest_path.exists():
                        try:
                            dest_path.unlink()
                            LOGGER.info("Cleaned up partial export: %s", dest_path)
                        except Exception:
                            pass
                    return ExportResult(
                        success=False,
                        error_message="Cancelled by user",
                        duration_seconds=time.time() - start_time
                    )

                if not source_path.is_file():
                    continue

                LOGGER.debug("Adding to ZIP: %s -> %s", source_path, arcname)
                sha256 = _calculate_file_sha256(source_path)
                size_bytes = source_path.stat().st_size
                zipf.write(source_path, arcname)

                file_entries.append(FileEntry(
                    rel_path=arcname,
                    size_bytes=size_bytes,
                    sha256=sha256,
                    category=category,
                ))

                current_bytes += size_bytes
                if progress_callback:
                    progress_callback(current_bytes, total_bytes, arcname)

            manifest = generate_export_manifest(case_folder, file_entries, options)
            manifest_json = json.dumps(manifest.to_dict(), indent=2, sort_keys=True)
            zipf.writestr("export_manifest.json", manifest_json)

        duration = time.time() - start_time
        LOGGER.info("Export complete: %d files, %d bytes, %.2f seconds", len(file_entries), total_bytes, duration)

        return ExportResult(
            success=True,
            export_path=dest_path,
            exported_files=len(file_entries),
            total_size_bytes=total_bytes,
            duration_seconds=duration,
        )

    except Exception as exc:
        error_msg = f"Export failed: {exc}"
        LOGGER.exception(error_msg)
        if dest_path.exists():
            try:
                dest_path.unlink()
            except Exception:
                pass
        return ExportResult(success=False, error_message=error_msg, duration_seconds=time.time() - start_time)
