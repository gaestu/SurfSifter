"""
Case import system for forensic workstation.

Provides functionality to import case packages created by the export system,
with 5-step validation, collision detection, and three collision strategies.

Validation Steps:
    1. ZIP integrity check (can be opened without errors)
    2. Manifest presence (export_manifest.json exists)
    3. File presence (all files listed in manifest exist in ZIP)
    4. Checksum verification (SHA256 matches manifest)
    5. Schema compatibility (exported schema version supported)

Collision Strategies:
    - CANCEL: Abort import if case already exists
    - RENAME: Import with "-imported" suffix (e.g., CASE-001-imported)
    - OVERWRITE: Replace existing case (requires case_id confirmation)

Example:
    >>> from core.import_case import import_case, ImportOptions, CollisionStrategy
    >>> options = ImportOptions(collision_strategy=CollisionStrategy.RENAME)
    >>> result = import_case(
    ...     zip_path=Path("/exports/CASE-001.zip"),
    ...     dest_cases_dir=Path("/cases"),
    ...     options=options
    ... )
    >>> if result.success:
    ...     print(f"Imported {result.imported_files} files to {result.imported_case_id}")
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, List, Optional

from core.database import DatabaseManager
from core.export import EXPORT_FORMAT_VERSION, FileEntry, _calculate_file_sha256

LOGGER = logging.getLogger(__name__)

# Schema versions we can import
# Version 1: Consolidated schema
# Version 2: File list partition support
# Version 3: Extractor statistics
SUPPORTED_SCHEMA_VERSIONS = [1, 2, 3]
MIN_SCHEMA_VERSION = 1
MAX_SCHEMA_VERSION = 3


class CollisionStrategy(Enum):
    """How to handle case ID collisions during import."""

    CANCEL = "cancel"       # Abort import if case exists
    RENAME = "rename"       # Import with -imported suffix
    OVERWRITE = "overwrite" # Replace existing case (requires confirmation)


@dataclass
class ValidationResult:
    """Result of export package validation."""

    valid: bool
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    manifest: Optional[dict] = None

    # Detailed validation flags
    zip_valid: bool = False
    manifest_present: bool = False
    files_present: bool = False
    checksums_valid: bool = False
    schema_compatible: bool = False


@dataclass
class ImportOptions:
    """Options controlling import behavior."""

    collision_strategy: CollisionStrategy = CollisionStrategy.CANCEL
    case_id_confirmation: Optional[str] = None  # Required for OVERWRITE strategy


@dataclass
class ImportResult:
    """Result of case import operation.

    Attributes:
        success: True if import completed successfully
        imported_case_id: The final case ID (may differ from original if renamed)
        imported_path: Full path to the imported case folder
        imported_files: Number of files extracted
        total_size_bytes: Total size of extracted data
        duration_seconds: Time taken to complete import
        error_message: Error message if failed (None if succeeded)
    """

    success: bool
    imported_case_id: Optional[str] = None
    imported_path: Optional[Path] = None
    imported_files: int = 0
    total_size_bytes: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None


def validate_export_package(
    zip_path: Path,
    *,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> ValidationResult:
    """
    Validate export package with 5-step verification.

    Performs comprehensive validation without extracting files:
    1. ZIP integrity (can be opened)
    2. Manifest presence (export_manifest.json exists)
    3. File presence (all manifest files exist in ZIP)
    4. Checksum verification (SHA256 matches manifest)
    5. Schema compatibility (schema version supported)

    Args:
        zip_path: Path to ZIP export package
        progress_callback: Optional callback(current_step, total_steps, step_name)

    Returns:
        ValidationResult with detailed validation flags

    Example:
        >>> result = validate_export_package(Path("/exports/CASE-001.zip"))
        >>> if result.valid:
        ...     print(f"Valid package: {result.manifest['case_id']}")
        >>> else:
        ...     print(f"Invalid: {result.error_message}")
    """
    LOGGER.info("Validating export package: %s", zip_path)

    result = ValidationResult(valid=False)
    total_steps = 5

    # Step 1: ZIP integrity
    if progress_callback:
        progress_callback(1, total_steps, "Checking ZIP integrity")

    try:
        zipf = zipfile.ZipFile(zip_path, "r")
        # Test ZIP integrity
        if zipf.testzip() is not None:
            result.error_message = "ZIP file is corrupted"
            LOGGER.error("ZIP integrity check failed: %s", zip_path)
            return result
        result.zip_valid = True
    except zipfile.BadZipFile as exc:
        result.error_message = f"Invalid ZIP file: {exc}"
        LOGGER.error("Bad ZIP file: %s", exc)
        return result
    except Exception as exc:
        result.error_message = f"Failed to open ZIP: {exc}"
        LOGGER.error("Failed to open ZIP: %s", exc)
        return result

    # Step 2: Manifest presence
    if progress_callback:
        progress_callback(2, total_steps, "Checking manifest")

    try:
        manifest_data = zipf.read("export_manifest.json")
        manifest = json.loads(manifest_data.decode("utf-8"))
        result.manifest = manifest
        result.manifest_present = True
    except KeyError:
        result.error_message = "export_manifest.json not found in ZIP"
        LOGGER.error("Manifest not found in ZIP: %s", zip_path)
        zipf.close()
        return result
    except json.JSONDecodeError as exc:
        result.error_message = f"Invalid manifest JSON: {exc}"
        LOGGER.error("Invalid manifest JSON: %s", exc)
        zipf.close()
        return result

    # Step 3: File presence
    if progress_callback:
        progress_callback(3, total_steps, "Checking file presence")

    try:
        zip_members = set(zipf.namelist())
        file_list = manifest.get("file_list", [])

        missing_files = []
        for file_entry in file_list:
            rel_path = file_entry["rel_path"]
            if rel_path not in zip_members:
                missing_files.append(rel_path)

        if missing_files:
            result.error_message = f"Missing {len(missing_files)} file(s): {missing_files[:3]}"
            LOGGER.error("Missing files in ZIP: %s", missing_files[:5])
            zipf.close()
            return result

        result.files_present = True
    except Exception as exc:
        result.error_message = f"Failed to check file presence: {exc}"
        LOGGER.error("File presence check failed: %s", exc)
        zipf.close()
        return result

    # Step 4: Checksum verification (sample 10% of files for performance)
    if progress_callback:
        progress_callback(4, total_steps, "Verifying checksums")

    try:
        file_list = manifest.get("file_list", [])

        # Sample files for checksum verification (every 10th file, minimum 3)
        if len(file_list) <= 10:
            files_to_check = file_list
        else:
            step = max(1, len(file_list) // 10)
            files_to_check = file_list[::step][:10]

        checksum_failures = []
        for file_entry in files_to_check:
            rel_path = file_entry["rel_path"]
            expected_sha256 = file_entry["sha256"]

            # Read file data from ZIP
            file_data = zipf.read(rel_path)

            # Calculate SHA256 of extracted data
            import hashlib
            actual_sha256 = hashlib.sha256(file_data).hexdigest()

            if actual_sha256 != expected_sha256:
                checksum_failures.append(rel_path)
                LOGGER.warning(
                    "Checksum mismatch for %s: expected %s, got %s",
                    rel_path, expected_sha256, actual_sha256
                )

        if checksum_failures:
            result.error_message = f"Checksum mismatch for {len(checksum_failures)} file(s)"
            LOGGER.error("Checksum verification failed for: %s", checksum_failures)
            zipf.close()
            return result

        result.checksums_valid = True

        # Add warning if we only sampled files
        if len(files_to_check) < len(file_list):
            result.warnings.append(
                f"Checksums verified for {len(files_to_check)}/{len(file_list)} files (sampling)"
            )
    except Exception as exc:
        result.error_message = f"Failed to verify checksums: {exc}"
        LOGGER.error("Checksum verification failed: %s", exc)
        zipf.close()
        return result

    # Step 5: Schema compatibility
    if progress_callback:
        progress_callback(5, total_steps, "Checking schema compatibility")

    try:
        export_version = manifest.get("export_version")
        if export_version != EXPORT_FORMAT_VERSION:
            result.warnings.append(
                f"Export format version mismatch: {export_version} (expected {EXPORT_FORMAT_VERSION})"
            )

        schema_version = manifest.get("schema_version")
        if schema_version is None:
            result.warnings.append("No schema_version in manifest (legacy export)")
            # Allow but warn - legacy export
        elif schema_version > MAX_SCHEMA_VERSION:
            # HARD FAIL: Future export on older tool
            result.error_message = (
                f"Schema version {schema_version} is newer than supported "
                f"(max: {MAX_SCHEMA_VERSION}). Please update the tool."
            )
            LOGGER.error("Schema version too new: %d > %d", schema_version, MAX_SCHEMA_VERSION)
            zipf.close()
            return result
        elif schema_version < MIN_SCHEMA_VERSION:
            # WARN: Old export may lack fields but try anyway
            result.warnings.append(
                f"Schema version {schema_version} is older than minimum supported "
                f"({MIN_SCHEMA_VERSION}). Some features may not work correctly."
            )
            LOGGER.warning("Old schema version: %d < %d", schema_version, MIN_SCHEMA_VERSION)

        result.schema_compatible = True
    except Exception as exc:
        result.error_message = f"Failed to check schema compatibility: {exc}"
        LOGGER.error("Schema compatibility check failed: %s", exc)
        zipf.close()
        return result

    zipf.close()

    # All validation steps passed
    result.valid = True
    LOGGER.info(
        "Validation successful: %s (case_id=%s, %d files, schema v%d)",
        zip_path, manifest.get("case_id"), len(file_list), schema_version
    )

    return result


def detect_case_collision(case_id: str, cases_dir: Path) -> bool:
    """
    Check if a case with the given ID already exists.

    Uses find_case_database() to look for case database files.

    Args:
        case_id: Case identifier to check
        cases_dir: Root directory containing all cases

    Returns:
        True if case exists, False otherwise

    Example:
        >>> if detect_case_collision("CASE-001", Path("/cases")):
        ...     print("Case already exists")
    """
    from core.database import find_case_database
    case_folder = cases_dir / case_id

    # Check if case folder exists and contains a case database
    exists = case_folder.exists() and find_case_database(case_folder) is not None

    if exists:
        LOGGER.info("Case collision detected: %s exists", case_id)
    else:
        LOGGER.debug("No collision: %s does not exist", case_id)

    return exists


def _is_safe_path(base_dir: Path, target_path: Path) -> bool:
    """Check if target_path is safely within base_dir (zip slip prevention).

    Args:
        base_dir: The base directory (import destination)
        target_path: The resolved target path for extraction

    Returns:
        True if target is within base_dir, False otherwise
    """
    try:
        # Resolve both paths to absolute, following symlinks
        base_resolved = base_dir.resolve()
        target_resolved = target_path.resolve()
        # Check if target is relative to base
        target_resolved.relative_to(base_resolved)
        return True
    except ValueError:
        return False


def _find_case_database_in_folder(folder: Path) -> Optional[Path]:
    """Find the case database in a folder.

    Delegates to find_case_database() for the primary + legacy pattern lookup.
    Falls back to any non-evidence .sqlite file in the folder root.

    Args:
        folder: Path to search for case database

    Returns:
        Path to case database, or None if not found
    """
    from core.database import find_case_database
    result = find_case_database(folder)
    if result:
        return result

    # Fallback: any .sqlite in root that's NOT an evidence DB
    for db_file in folder.glob("*.sqlite"):
        # Skip if it's in evidences/ subdirectory or has evidence in name
        if db_file.parent == folder and "evidence" not in db_file.stem.lower():
            return db_file

    return None


class _ImportCancelled(Exception):
    """Internal exception for import cancellation flow control.

    Used to break out of extraction loop and ensure cleanup via try/except.
    Not exposed to callers; converted to ImportResult with error_message.
    """
    pass


def import_case(
    zip_path: Path,
    dest_cases_dir: Path,
    options: ImportOptions,
    *,
    cancel_check: Optional[Callable[[], bool]] = None,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> ImportResult:
    """
    Import case from export package with collision handling.

    Extracts case from ZIP to destination directory, applying collision
    strategy if case ID already exists. Updates case metadata to record
    import provenance.

    Security:
        - Validates all paths against zip slip attacks
        - Extracts to temp directory first, then moves atomically
        - Cleans up partial extraction on failure or cancellation

    Args:
        zip_path: Path to ZIP export package
        dest_cases_dir: Root directory for imported cases (created if doesn't exist)
        options: Import options (collision strategy, confirmation)
        cancel_check: Optional callable returning True if cancellation requested
        progress_callback: Optional callback(current_bytes, total_bytes, current_file)

    Returns:
        ImportResult with success status and metadata

    Raises:
        ValueError: If OVERWRITE strategy used without case_id_confirmation
        FileExistsError: If CANCEL strategy and case exists

    Example:
        >>> options = ImportOptions(
        ...     collision_strategy=CollisionStrategy.RENAME
        ... )
        >>> result = import_case(
        ...     Path("/exports/CASE-001.zip"),
        ...     Path("/cases"),
        ...     options
        ... )
        >>> print(f"Imported to: {result.imported_case_id}")
    """
    start_time = time.time()
    temp_extract_dir: Optional[Path] = None
    zipf: Optional[zipfile.ZipFile] = None

    LOGGER.info("Starting import: %s -> %s", zip_path, dest_cases_dir)

    if not zip_path.exists():
        error_msg = f"Export package not found: {zip_path}"
        LOGGER.error(error_msg)
        return ImportResult(success=False, error_message=error_msg)

    try:
        # Ensure destination directory exists
        if not dest_cases_dir.exists():
            LOGGER.info("Creating destination directory: %s", dest_cases_dir)
            dest_cases_dir.mkdir(parents=True, exist_ok=True)

        # Verify writability
        if not os.access(dest_cases_dir, os.W_OK):
            error_msg = f"Destination directory not writable: {dest_cases_dir}"
            LOGGER.error(error_msg)
            return ImportResult(success=False, error_message=error_msg)

        # Open ZIP and read manifest
        zipf = zipfile.ZipFile(zip_path, "r")
        manifest_data = zipf.read("export_manifest.json")
        manifest = json.loads(manifest_data.decode("utf-8"))

        original_case_id = manifest["case_id"]
        file_list = manifest.get("file_list", [])

        # Handle collision
        collision = detect_case_collision(original_case_id, dest_cases_dir)
        final_case_id = original_case_id

        if collision:
            if options.collision_strategy == CollisionStrategy.CANCEL:
                error_msg = f"Case {original_case_id} already exists (CANCEL strategy)"
                LOGGER.error(error_msg)
                zipf.close()
                raise FileExistsError(error_msg)

            elif options.collision_strategy == CollisionStrategy.RENAME:
                # Find unique suffix: try -imported, then -imported-2, etc.
                suffix_num = 1
                while True:
                    if suffix_num == 1:
                        final_case_id = f"{original_case_id}-imported"
                    else:
                        final_case_id = f"{original_case_id}-imported-{suffix_num}"

                    if not detect_case_collision(final_case_id, dest_cases_dir):
                        break
                    suffix_num += 1

                LOGGER.info(
                    "Collision resolved via rename: %s -> %s",
                    original_case_id, final_case_id
                )

            elif options.collision_strategy == CollisionStrategy.OVERWRITE:
                # Require case_id confirmation
                if options.case_id_confirmation != original_case_id:
                    error_msg = (
                        f"OVERWRITE requires case_id_confirmation='{original_case_id}' "
                        f"(got '{options.case_id_confirmation}')"
                    )
                    LOGGER.error(error_msg)
                    zipf.close()
                    raise ValueError(error_msg)

                LOGGER.warning("Overwrite confirmed for: %s", original_case_id)

        # Determine final destination
        dest_case_folder = dest_cases_dir / final_case_id

        # Check if destination already exists (outside collision strategy)
        if dest_case_folder.exists() and options.collision_strategy != CollisionStrategy.OVERWRITE:
            # This shouldn't happen if collision detection worked, but be safe
            error_msg = f"Destination folder already exists: {dest_case_folder}"
            LOGGER.error(error_msg)
            zipf.close()
            return ImportResult(success=False, error_message=error_msg)

        # Create temp directory for safe extraction
        temp_extract_dir = Path(tempfile.mkdtemp(prefix="import_", dir=dest_cases_dir))
        temp_case_folder = temp_extract_dir / final_case_id
        temp_case_folder.mkdir(parents=True)

        LOGGER.info("Extracting to temp: %s", temp_case_folder)

        total_bytes = sum(entry["size_bytes"] for entry in file_list)
        current_bytes = 0

        # Extraction loop with try/finally for cleanup
        try:
            for file_entry in file_list:
                # Check for cancellation
                if cancel_check and cancel_check():
                    LOGGER.info("Import cancelled by user during extraction")
                    raise _ImportCancelled()

                rel_path = file_entry["rel_path"]
                size_bytes = file_entry["size_bytes"]

                # SECURITY: Validate path before extraction (zip slip prevention)
                dest_file = temp_case_folder / rel_path
                if not _is_safe_path(temp_case_folder, dest_file):
                    error_msg = f"Unsafe path in archive (possible zip slip attack): {rel_path}"
                    LOGGER.error(error_msg)
                    raise ValueError(error_msg)

                # Extract file
                dest_file.parent.mkdir(parents=True, exist_ok=True)

                with zipf.open(rel_path) as source, dest_file.open("wb") as dest:
                    shutil.copyfileobj(source, dest)
                current_bytes += size_bytes

                if progress_callback:
                    progress_callback(current_bytes, total_bytes, rel_path)

            zipf.close()
            zipf = None

            # Final cancellation check before commit
            if cancel_check and cancel_check():
                LOGGER.info("Import cancelled by user before commit")
                raise _ImportCancelled()

        except _ImportCancelled:
            # Clean up temp directory on cancellation
            if temp_extract_dir and temp_extract_dir.exists():
                LOGGER.info("Cleaning up temp dir after cancellation: %s", temp_extract_dir)
                shutil.rmtree(temp_extract_dir)
                temp_extract_dir = None
            return ImportResult(
                success=False,
                error_message="Cancelled by user",
                duration_seconds=time.time() - start_time
            )

        # Atomic move from temp to final destination
        if dest_case_folder.exists():
            # OVERWRITE strategy: remove existing first
            LOGGER.warning("Removing existing case for overwrite: %s", dest_case_folder)
            shutil.rmtree(dest_case_folder)

        # Move extracted case to final location
        shutil.move(str(temp_case_folder), str(dest_case_folder))
        temp_extract_dir.rmdir()  # Clean up empty temp parent
        temp_extract_dir = None  # Mark as cleaned up

        LOGGER.info("Moved to final destination: %s", dest_case_folder)

        # Update case metadata to record import provenance
        try:
            # Find the case database using proper pattern
            case_db = _find_case_database_in_folder(dest_case_folder)

            if not case_db:
                LOGGER.warning("No case database found in %s for metadata update", dest_case_folder)
            else:
                conn = sqlite3.connect(case_db)

                # Add import metadata columns if they don't exist (forward compatibility)
                try:
                    conn.execute("ALTER TABLE cases ADD COLUMN imported_from TEXT")
                except sqlite3.OperationalError:
                    pass  # Column already exists

                try:
                    conn.execute("ALTER TABLE cases ADD COLUMN import_timestamp TEXT")
                except sqlite3.OperationalError:
                    pass  # Column already exists

                try:
                    conn.execute("ALTER TABLE cases ADD COLUMN import_source_path TEXT")
                except sqlite3.OperationalError:
                    pass  # Column already exists

                # Update case_id if renamed
                if final_case_id != original_case_id:
                    conn.execute(
                        "UPDATE cases SET case_id = ? WHERE case_id = ?",
                        (final_case_id, original_case_id)
                    )

                # Record import metadata
                conn.execute(
                    """
                    UPDATE cases SET
                        imported_from = ?,
                        import_timestamp = ?,
                        import_source_path = ?
                    WHERE case_id = ?
                    """,
                    (
                        original_case_id,
                        manifest["exported_at_utc"],
                        str(zip_path),
                        final_case_id
                    )
                )

                conn.commit()
                conn.close()

                LOGGER.info("Updated case metadata with import provenance")
        except Exception as exc:
            LOGGER.warning("Failed to update case metadata: %s", exc)
            # Non-fatal, import still succeeded

        duration = time.time() - start_time

        result = ImportResult(
            success=True,
            imported_case_id=final_case_id,
            imported_path=dest_case_folder,
            imported_files=len(file_list),
            total_size_bytes=total_bytes,
            duration_seconds=duration,
        )

        LOGGER.info(
            "Import successful: %s (%d files, %.2fs)",
            final_case_id, len(file_list), duration
        )

        return result

    except FileExistsError:
        # Re-raise collision errors
        raise

    except ValueError:
        # Re-raise validation errors
        raise

    except Exception as exc:
        # CLEANUP: Close ZIP file if still open
        if zipf:
            try:
                zipf.close()
            except Exception:
                pass

        # CLEANUP: Remove partial extraction on any failure
        if temp_extract_dir and temp_extract_dir.exists():
            LOGGER.warning("Cleaning up partial extraction: %s", temp_extract_dir)
            try:
                shutil.rmtree(temp_extract_dir)
            except Exception as cleanup_exc:
                LOGGER.error("Failed to clean up temp dir: %s", cleanup_exc)

        error_msg = f"Import failed: {exc}"
        LOGGER.exception("Import failed")

        return ImportResult(
            success=False,
            error_message=error_msg,
            duration_seconds=time.time() - start_time,
        )
