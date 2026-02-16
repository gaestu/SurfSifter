"""
SleuthKit-based file list generator for EWF images.

Uses `fls` command to rapidly enumerate all files in an EWF image,
populating the file_list table for fast SQL-based discovery.

This enables:
- Instant SQL queries by extension (vs hours of pytsk3 walking)
- Pre-populated file list usable by ALL extractors
- Multi-partition support with partition context preservation

Requirements:
- SleuthKit binaries available (bundled or in PATH)
- EWF (E01/Ex01) image format only

See Also:
    - docs/user/INVESTIGATOR_GUIDE.md for installation instructions
    - planning/wip/filesystem_images_discovery_performance.md for design
"""
from __future__ import annotations

import logging
import re
import sqlite3
import subprocess
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, Optional

from .bodyfile_parser import BodyfileEntry, BodyfileParser
from .sleuthkit_utils import get_sleuthkit_bin

__all__ = ["SleuthKitFileListGenerator", "GenerationResult"]

logger = logging.getLogger(__name__)


# Indexes to drop before bulk insert and recreate after
# NOTE: idx_file_list_unique_path is NOT included because it's required for
# INSERT OR IGNORE to work correctly. Without the unique constraint,
# INSERT OR IGNORE won't detect duplicates and will insert them anyway.
FILE_LIST_INDEXES = [
    "idx_file_list_extension",
    "idx_file_list_evidence_extension",
    "idx_file_list_partition",
    "idx_file_list_name",
    "idx_file_list_path",
    # "idx_file_list_unique_path",  # Keep this! Required for INSERT OR IGNORE
]


@dataclass
class GenerationResult:
    """
    Result of file list generation.

    Attributes:
        success: True if generation completed successfully
        total_files: Total files added to file_list table
        partitions_processed: Number of partitions enumerated
        duration_seconds: Time taken for generation
        error_message: Error description if success is False
        partition_stats: Per-partition file counts
        cancelled: True if generation was cancelled by user
        fls_errors: List of fls non-zero exit codes encountered
    """
    success: bool
    total_files: int
    partitions_processed: int
    duration_seconds: float
    error_message: Optional[str] = None
    partition_stats: dict = field(default_factory=dict)
    cancelled: bool = False
    fls_errors: list = field(default_factory=list)


class SleuthKitFileListGenerator:
    """
    Generate file list from EWF image using SleuthKit fls.

    This class uses the `fls` command-line tool to rapidly enumerate
    all files in an E01 image, storing results in the file_list table
    with partition context for accurate extraction.

    Features:
    - Automatic partition enumeration via list_ewf_partitions()
    - Block size detection for 4K sector support
    - Deferred index creation for faster bulk inserts
    - Progress callbacks for UI integration
    - Robust bodyfile parsing with edge case handling

    Example:
        >>> generator = SleuthKitFileListGenerator(
        ...     evidence_conn, evidence_id, [Path("image.E01")]
        ... )
        >>> if generator.fls_available:
        ...     result = generator.generate()
        ...     print(f"Generated {result.total_files} files")
    """

    BATCH_SIZE = 5000  # Rows per batch insert (larger than CSV importer for speed)

    def __init__(
        self,
        evidence_conn: sqlite3.Connection,
        evidence_id: int,
        ewf_paths: List[Path],
    ):
        """
        Initialize generator.

        Args:
            evidence_conn: SQLite connection to evidence database
            evidence_id: Evidence ID for file_list records
            ewf_paths: List of EWF segment paths (E01, E02, etc.)
        """
        self.evidence_conn = evidence_conn
        self.evidence_id = evidence_id
        self.ewf_paths = ewf_paths
        self._fls_path = get_sleuthkit_bin("fls")
        self._mmls_path = get_sleuthkit_bin("mmls")

    @property
    def fls_available(self) -> bool:
        """Check if fls command is available (bundled or in PATH)."""
        return self._fls_path is not None

    def _get_partitions_via_mmls(self) -> List[dict]:
        """
        Get partition info using SleuthKit mmls command.

        This avoids opening the EWF file via pyewf which can cause
        threading issues when the file is already open in another thread.

        Returns:
            List of partition dicts with keys:
                index, offset, length, description, block_size, filesystem_readable
        """
        if not self._mmls_path:
            logger.warning("mmls not available, falling back to direct fls (single partition)")
            return [{
                'index': 0,
                'offset': 0,
                'block_size': 512,
                'description': 'Direct filesystem',
                'filesystem_readable': True,
            }]

        # Run mmls to get partition info
        cmd = [self._mmls_path, str(self.ewf_paths[0])]
        logger.info("Running: %s", " ".join(cmd))

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except subprocess.TimeoutExpired:
            logger.warning("mmls timed out, falling back to direct fls")
            return [{
                'index': 0,
                'offset': 0,
                'block_size': 512,
                'description': 'Direct filesystem (mmls timeout)',
                'filesystem_readable': True,
            }]
        except Exception as e:
            logger.warning("mmls failed: %s, falling back to direct fls", e)
            return [{
                'index': 0,
                'offset': 0,
                'block_size': 512,
                'description': f'Direct filesystem (mmls error: {e})',
                'filesystem_readable': True,
            }]

        if result.returncode != 0:
            # No partition table - direct filesystem
            logger.info("No partition table found (mmls returned %d)", result.returncode)
            return [{
                'index': 0,
                'offset': 0,
                'block_size': 512,
                'description': 'Direct filesystem',
                'filesystem_readable': True,
            }]

        # Parse mmls output
        # Example output:
        # DOS Partition Table
        # Offset Sector: 0
        # Units are in 512-byte sectors
        #
        #       Slot      Start        End          Length       Description
        # 000:  Meta      0000000000   0000000000   0000000001   Primary Table (#0)
        # 001:  -------   0000000000   0000002047   0000002048   Unallocated
        # 002:  000:000   0000002048   0001026047   0001024000   NTFS / exFAT (0x07)
        # 003:  000:001   0001026048   ...

        partitions = []
        block_size = 512  # Default sector size
        partition_index = 1

        for line in result.stdout.splitlines():
            line = line.strip()

            # Extract block size from "Units are in X-byte sectors"
            if 'Units are in' in line and 'sectors' in line:
                try:
                    # Extract number before "-byte"
                    match = re.search(r'(\d+)-byte', line)
                    if match:
                        block_size = int(match.group(1))
                        logger.debug("Detected block size: %d", block_size)
                except ValueError:
                    pass
                continue

            # Skip non-partition lines
            if not line or line.startswith('#') or ':' not in line:
                continue

            # Parse partition line
            # mmls format: Slot Type Start End Length [Description]
            # GPT partitions often have NO description (only 5 fields)
            # DOS partitions usually have description like "NTFS / exFAT (0x07)"
            parts = line.split()
            if len(parts) < 5:
                # Need at minimum: slot, type, start, end, length
                continue

            slot = parts[0].rstrip(':')

            # Skip meta entries and unallocated
            if 'Meta' in line or 'Unallocated' in line or '-------' in parts[1]:
                continue

            try:
                start_sector = int(parts[2])
                end_sector = int(parts[3])
                length_sectors = int(parts[4])
                # Description is optional - GPT partitions often have none
                description = ' '.join(parts[5:]) if len(parts) > 5 else ''
                desc_upper = description.upper()

                # Calculate byte offset
                offset = start_sector * block_size
                length = length_sectors * block_size

                # All non-Meta/non-Unallocated partitions are considered
                # potentially readable. Let fls attempt enumeration and handle
                # errors gracefully. The old keyword whitelist was too restrictive
                # and missed Apple partitions (e.g. _DS_DEV_DISK_X_), recovery
                # volumes (Untitled), and other valid filesystems.
                fs_readable = True

                partitions.append({
                    'index': partition_index,
                    'offset': offset,
                    'length': length,
                    'block_size': block_size,
                    'description': description,
                    'filesystem_readable': fs_readable,
                })
                partition_index += 1

            except (ValueError, IndexError) as e:
                logger.debug("Skipping unparseable mmls line: %s (%s)", line, e)
                continue

        if not partitions:
            logger.info("No parseable partitions from mmls, trying direct filesystem")
            return [{
                'index': 0,
                'offset': 0,
                'block_size': block_size,
                'description': 'Direct filesystem',
                'filesystem_readable': True,
            }]

        logger.info("Found %d partition(s) via mmls", len(partitions))
        return partitions

    def generate(
        self,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> GenerationResult:
        """
        Generate file list for all partitions in the EWF image.

        This method:
        1. Enumerates all readable partitions (via mmls)
        2. Drops indexes for faster bulk inserts
        3. Runs fls on each partition, streaming output
        4. Parses bodyfile output and inserts into file_list
        5. Recreates indexes after completion

        Args:
            progress_callback: Optional callback(files_processed, partition_index, message)
                              Called periodically during generation.
                              Raise InterruptedError to cancel generation.

        Returns:
            GenerationResult with statistics and success status.
            On cancellation, returns result with cancelled=True and partial data info.

        Raises:
            InterruptedError: Re-raised when cancelled, after cleanup.
        """
        if not self.fls_available:
            return GenerationResult(
                success=False,
                total_files=0,
                partitions_processed=0,
                duration_seconds=0,
                error_message="fls command not found. Install sleuthkit or bundle binaries: "
                              "Linux: apt install sleuthkit, "
                              "macOS: brew install sleuthkit, "
                              "Windows: download from sleuthkit.org",
            )

        start_time = datetime.now(timezone.utc)
        import_timestamp = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Track state for partial progress reporting
        total_files = 0
        partition_stats = {}
        partitions_completed = 0
        fls_errors: List[dict] = []
        cancelled = False
        error_message: Optional[str] = None

        try:
            # Get partition info via mmls (avoids pyewf threading issues)
            logger.info("Getting partition info via mmls...")
            partitions = self._get_partitions_via_mmls()
            logger.info("Got %d partitions from mmls", len(partitions))
            readable_partitions = [p for p in partitions if p['filesystem_readable']]

            if not readable_partitions:
                return GenerationResult(
                    success=False,
                    total_files=0,
                    partitions_processed=0,
                    duration_seconds=0,
                    error_message="No readable partitions found in EWF image",
                )

            logger.info(
                "Found %d readable partition(s) in EWF image",
                len(readable_partitions)
            )

            # Ensure unique index exists BEFORE dropping other indexes.
            # This is critical for INSERT OR IGNORE to work correctly.
            self._ensure_unique_index()

            # Drop non-unique indexes for faster bulk insert
            self._drop_indexes()

            for part in readable_partitions:
                partition_index = part['index']

                if progress_callback:
                    progress_callback(
                        total_files,
                        partition_index,
                        f"Processing partition {partition_index}..."
                    )

                files_in_partition, pass_errors = self._process_partition(
                    part,
                    import_timestamp,
                    progress_callback,
                )

                total_files += files_in_partition
                partition_stats[partition_index] = files_in_partition
                fls_errors.extend(pass_errors)
                partitions_completed += 1

                logger.info(
                    "Partition %d: %d files",
                    partition_index,
                    files_in_partition
                )

            # Check for fls errors - treat as failure if any critical errors
            if fls_errors:
                # Log all errors but continue - data may still be useful
                for err in fls_errors:
                    logger.warning(
                        "fls error on partition %d (%s): exit code %d - %s",
                        err['partition'], err['pass_type'],
                        err['exit_code'], err['stderr']
                    )

            logger.info(
                "File list generation complete: %d files from %d partition(s) in %.2fs",
                total_files,
                partitions_completed,
                (datetime.now(timezone.utc) - start_time).total_seconds(),
            )

        except InterruptedError:
            # Cancellation requested - clean up and re-raise
            cancelled = True
            error_message = "Generation cancelled by user"
            logger.info("File list generation cancelled after %d files from %d partition(s)",
                       total_files, partitions_completed)

        except Exception as e:
            logger.exception("File list generation failed")
            error_message = str(e)

        finally:
            # Always recreate indexes
            try:
                self._create_indexes()
            except Exception as idx_err:
                logger.warning("Failed to recreate indexes: %s", idx_err)

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()

        # Build result with partial progress info
        result = GenerationResult(
            success=(not cancelled and error_message is None),
            total_files=total_files,
            partitions_processed=partitions_completed,
            duration_seconds=duration,
            error_message=error_message,
            partition_stats=partition_stats,
            cancelled=cancelled,
            fls_errors=fls_errors,
        )

        # Re-raise InterruptedError so caller can handle cancellation
        if cancelled:
            raise InterruptedError(error_message)

        return result

    def _process_partition(
        self,
        partition: dict,
        import_timestamp: str,
        progress_callback: Optional[Callable],
    ) -> tuple[int, List[dict]]:
        """
        Process single partition with fls.

        Runs fls twice to enumerate both allocated and deleted files:
        1. First pass: allocated files (no -d flag)
        2. Second pass: deleted files only (-d flag)

        Args:
            partition: Partition info dict from list_ewf_partitions()
            import_timestamp: ISO timestamp for this import
            progress_callback: Optional progress callback

        Returns:
            Tuple of (files_processed, list of error dicts)

        Raises:
            InterruptedError: If cancelled via progress_callback
        """
        partition_index = partition['index']
        block_size = partition.get('block_size', 512)
        offset = partition['offset']

        # Calculate sector offset
        sector_offset = offset // block_size

        # Determine if NTFS metadata filtering should be applied
        # Check description for NTFS or common Windows filesystem identifiers
        description = partition.get('description', '').upper()
        is_ntfs = 'NTFS' in description or 'EXFAT' in description or '0X07' in description

        # Default to True for safety when we can't determine filesystem type
        # NTFS filtering doesn't harm non-NTFS filesystems (just won't match anything)
        # This covers:
        # - Empty description
        # - "Direct filesystem" fallback (raw NTFS without partition table)
        # - Unknown partition types
        if not is_ntfs:
            # Non-Windows filesystems we can confidently skip filtering for
            # Includes Apple-specific labels (e.g. _DS_DEV_DISK_X_, Apple_HFS)
            non_windows_fs = [
                'EXT', 'HFS', 'APFS', 'LINUX', 'SWAP', 'BSD', 'UFS',
                'APPLE', '_DS_DEV_', 'CORESTORAGE',
            ]
            is_non_windows = any(fs in description for fs in non_windows_fs)
            if not is_non_windows:
                logger.debug(
                    "Enabling NTFS metadata filtering for partition %d (description: %r)",
                    partition_index, partition.get('description', '')
                )
                is_ntfs = True

        total_files = 0
        all_errors: List[dict] = []

        # Pass 1: Enumerate allocated files
        allocated_count, alloc_errors = self._run_fls_pass(
            partition_index=partition_index,
            sector_offset=sector_offset,
            block_size=block_size,
            is_ntfs=is_ntfs,
            deleted_only=False,
            import_timestamp=import_timestamp,
            progress_callback=progress_callback,
            current_total=total_files,
        )
        total_files += allocated_count
        all_errors.extend(alloc_errors)

        # Pass 2: Enumerate deleted files
        deleted_count, del_errors = self._run_fls_pass(
            partition_index=partition_index,
            sector_offset=sector_offset,
            block_size=block_size,
            is_ntfs=is_ntfs,
            deleted_only=True,
            import_timestamp=import_timestamp,
            progress_callback=progress_callback,
            current_total=total_files,
        )
        total_files += deleted_count
        all_errors.extend(del_errors)

        logger.info(
            "Partition %d: %d allocated + %d deleted = %d total files",
            partition_index,
            allocated_count,
            deleted_count,
            total_files,
        )

        return total_files, all_errors

    def _run_fls_pass(
        self,
        partition_index: int,
        sector_offset: int,
        block_size: int,
        is_ntfs: bool,
        deleted_only: bool,
        import_timestamp: str,
        progress_callback: Optional[Callable],
        current_total: int,
    ) -> tuple[int, List[dict]]:
        """
        Run a single fls pass (allocated or deleted files).

        Args:
            partition_index: Partition index for tagging entries
            sector_offset: Sector offset for -o flag
            block_size: Block size for -b flag
            is_ntfs: Whether to apply NTFS metadata filtering
            deleted_only: If True, use -d flag for deleted files only
            import_timestamp: ISO timestamp for records
            progress_callback: Optional progress callback
            current_total: Current total files for progress reporting

        Returns:
            Tuple of (files_processed, list of error dicts)

        Raises:
            InterruptedError: If cancelled via progress_callback
        """
        # Build fls command
        cmd = [
            self._fls_path,
            "-r",  # Recursive
            "-p",  # Full path
            "-m", "/",  # Bodyfile format with / as mount point
        ]

        # Add -d flag for deleted files only pass
        if deleted_only:
            cmd.append("-d")

        # Add block size if not default 512
        if block_size != 512:
            cmd.extend(["-b", str(block_size)])

        # Add offset if not 0
        if sector_offset > 0:
            cmd.extend(["-o", str(sector_offset)])

        # Add image path (first segment)
        cmd.append(str(self.ewf_paths[0]))

        pass_type = "deleted" if deleted_only else "allocated"
        logger.info("Running fls (%s): %s", pass_type, " ".join(cmd))

        # Run fls with stderr going to /dev/null to avoid deadlock
        # We check exit code for errors instead
        process: Optional[subprocess.Popen] = None
        stderr_lines: List[str] = []
        errors: List[dict] = []

        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # Line buffered
            )
        except FileNotFoundError:
            raise RuntimeError(f"fls command not found at {self._fls_path}")
        except PermissionError:
            raise RuntimeError(f"Permission denied running fls at {self._fls_path}")

        # Start a thread to drain stderr to avoid deadlock
        # fls can write warnings/errors to stderr while outputting to stdout
        def drain_stderr():
            try:
                for line in process.stderr:
                    stderr_lines.append(line.rstrip())
            except Exception:
                pass

        stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        stderr_thread.start()

        # Track how many new rows were actually inserted into SQLite.
        # IMPORTANT: The schema enforces a unique path per evidence+partition:
        #   idx_file_list_unique_path (evidence_id, COALESCE(partition_index,-1), file_path)
        # and we use INSERT OR IGNORE, so many parsed entries (especially deleted)
        # will be ignored as duplicates. Returning inserted counts keeps UI/manifest
        # consistent with what's actually stored in the DB.
        before_count = 0
        try:
            before_count = self.evidence_conn.execute(
                "SELECT COUNT(*) FROM file_list WHERE evidence_id = ? AND partition_index = ?",
                (self.evidence_id, partition_index),
            ).fetchone()[0]
        except Exception:
            # Best-effort only; generation should not fail due to a COUNT() issue.
            before_count = 0

        parser = BodyfileParser(
            partition_index=partition_index,
            skip_ntfs_metadata=is_ntfs,
        )
        batch: List[BodyfileEntry] = []
        parsed_in_pass = 0

        try:
            # Stream and parse fls output
            for entry in parser.parse_lines(iter(process.stdout.readline, '')):
                batch.append(entry)

                if len(batch) >= self.BATCH_SIZE:
                    self._insert_batch(batch, import_timestamp)
                    parsed_in_pass += len(batch)

                    # Report progress - may raise InterruptedError
                    if progress_callback:
                        progress_callback(
                            current_total + parsed_in_pass,
                            partition_index,
                            f"Partition {partition_index} ({pass_type}): {current_total + parsed_in_pass:,} records parsed..."
                        )

                    batch = []

            # Insert remaining batch
            if batch:
                self._insert_batch(batch, import_timestamp)
                parsed_in_pass += len(batch)

        except InterruptedError:
            # Cancellation requested - terminate fls and re-raise
            logger.info("Cancellation requested, terminating fls process")
            if process:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            raise

        finally:
            # Ensure process is cleaned up
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

        # Wait for stderr thread to finish
        stderr_thread.join(timeout=2)

        after_count = before_count
        try:
            after_count = self.evidence_conn.execute(
                "SELECT COUNT(*) FROM file_list WHERE evidence_id = ? AND partition_index = ?",
                (self.evidence_id, partition_index),
            ).fetchone()[0]
        except Exception:
            after_count = before_count

        inserted_in_pass = max(0, after_count - before_count)
        ignored_as_duplicates = max(0, parsed_in_pass - inserted_in_pass)

        # Check exit code
        if process.returncode != 0:
            stderr_text = '\n'.join(stderr_lines) if stderr_lines else "(no stderr)"
            logger.warning(
                "fls (%s) exited with code %d: %s",
                pass_type,
                process.returncode,
                stderr_text[:500]  # Truncate long stderr
            )
            errors.append({
                'partition': partition_index,
                'pass_type': pass_type,
                'exit_code': process.returncode,
                'stderr': stderr_text[:500],
            })

        logger.info(
            "Parser stats for partition %d (%s): %s",
            partition_index, pass_type, parser.stats
        )

        if ignored_as_duplicates:
            logger.info(
                "Partition %d (%s): %d parsed, %d inserted, %d ignored (duplicate file_path)",
                partition_index,
                pass_type,
                parsed_in_pass,
                inserted_in_pass,
                ignored_as_duplicates,
            )

        return inserted_in_pass, errors

    def _insert_batch(self, entries: List[BodyfileEntry], import_timestamp: str) -> None:
        """
        Insert batch of entries using executemany.

        Uses INSERT OR IGNORE which means:
        - First occurrence of a (evidence_id, partition_index, file_path) wins
        - Since allocated files are processed before deleted files, allocated wins
        - A deleted file with same path as an allocated file is silently skipped

        This is acceptable because:
        - The allocated version has more reliable metadata
        - The deleted flag can be inferred from inode analysis if needed
        - For forensic completeness, both versions would need schema changes

        Args:
            entries: List of BodyfileEntry objects
            import_timestamp: ISO timestamp for this import
        """
        if not entries:
            return

        rows = [
            (
                self.evidence_id,
                e.file_path,
                e.file_name,
                e.extension,
                e.size_bytes,
                e.created_ts,
                e.modified_ts,
                e.accessed_ts,
                e.md5_hash,
                None,  # sha1_hash - not computed by fls
                None,  # sha256_hash - not computed by fls
                None,  # file_type
                1 if e.deleted else 0,
                None,  # metadata
                "fls",  # import_source
                import_timestamp,
                e.partition_index,
                e.inode,
            )
            for e in entries
        ]

        try:
            self.evidence_conn.executemany(
                """
                INSERT OR IGNORE INTO file_list (
                    evidence_id, file_path, file_name, extension,
                    size_bytes, created_ts, modified_ts, accessed_ts,
                    md5_hash, sha1_hash, sha256_hash, file_type,
                    deleted, metadata, import_source, import_timestamp,
                    partition_index, inode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self.evidence_conn.commit()
        except sqlite3.Error as e:
            logger.error("Batch insert failed: %s", e)
            raise

    def _drop_indexes(self) -> None:
        """Drop indexes for faster bulk insert (except unique constraint)."""
        for index_name in FILE_LIST_INDEXES:
            try:
                self.evidence_conn.execute(f"DROP INDEX IF EXISTS {index_name}")
            except sqlite3.Error:
                pass  # Index may not exist
        self.evidence_conn.commit()
        logger.debug("Dropped file_list indexes for bulk insert (kept unique constraint)")

    def _ensure_unique_index(self) -> None:
        """Ensure the unique index exists before bulk insert.

        This is critical for INSERT OR IGNORE to work correctly.
        Without this index, duplicates will be inserted instead of ignored.
        """
        try:
            self.evidence_conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_file_list_unique_path
                ON file_list(evidence_id, COALESCE(partition_index, -1), file_path)
            """)
            self.evidence_conn.commit()
        except sqlite3.Error as e:
            logger.warning("Could not ensure unique index (may need deduplication): %s", e)

    def _create_indexes(self) -> None:
        """Recreate indexes after bulk insert."""
        index_definitions = [
            "CREATE INDEX IF NOT EXISTS idx_file_list_extension ON file_list(extension)",
            "CREATE INDEX IF NOT EXISTS idx_file_list_evidence_extension ON file_list(evidence_id, extension)",
            "CREATE INDEX IF NOT EXISTS idx_file_list_partition ON file_list(evidence_id, partition_index)",
            "CREATE INDEX IF NOT EXISTS idx_file_list_name ON file_list(file_name)",
            "CREATE INDEX IF NOT EXISTS idx_file_list_path ON file_list(file_path)",
            # Use COALESCE to match schema definition - handles NULL partition_index correctly
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_file_list_unique_path ON file_list(evidence_id, COALESCE(partition_index, -1), file_path)",
        ]
        for sql in index_definitions:
            try:
                self.evidence_conn.execute(sql)
            except sqlite3.Error as e:
                logger.warning("Failed to create index: %s - %s", sql, e)
        self.evidence_conn.commit()
        logger.debug("Recreated file_list indexes")

    def clear_existing(self) -> int:
        """
        Clear existing file_list entries for this evidence.

        Returns:
            Number of rows deleted
        """
        cursor = self.evidence_conn.execute(
            "DELETE FROM file_list WHERE evidence_id = ?",
            (self.evidence_id,)
        )
        self.evidence_conn.commit()
        deleted = cursor.rowcount
        logger.info("Cleared %d existing file_list entries for evidence %d",
                   deleted, self.evidence_id)
        return deleted

    def get_file_count(self) -> int:
        """
        Get current file count for this evidence.

        Returns:
            Number of files in file_list for this evidence
        """
        cursor = self.evidence_conn.execute(
            "SELECT COUNT(*) FROM file_list WHERE evidence_id = ?",
            (self.evidence_id,)
        )
        return cursor.fetchone()[0]
