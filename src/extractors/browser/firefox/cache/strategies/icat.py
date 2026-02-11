"""
Firefox Cache Extraction - icat Strategy

Uses SleuthKit's icat command for fast inode-based extraction from E01 images.
This is the fastest strategy when inodes are available in the file_list.
"""

from __future__ import annotations

import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Event
from typing import List, Optional, Tuple, Any

from core.logging import get_logger
from extractors.system.file_list.sleuthkit_utils import get_sleuthkit_bin
from .base import (
    ExtractionStrategy,
    ExtractionContext,
    ExtractionResult,
    IcatResult,
    DiscoveredFile,
    compute_file_hashes,
    extract_profile_from_path,
    CHUNK_SIZE,
)

LOGGER = get_logger("extractors.cache_firefox.strategies.icat")


class IcatExtractionStrategy(ExtractionStrategy):
    """
    Extraction strategy using SleuthKit's icat command.

    Fastest approach for E01 images with inode information.
    Runs icat commands in parallel with configurable worker count.
    """

    def __init__(self, max_workers: int = 4):
        """
        Initialize icat strategy.

        Args:
            max_workers: Maximum parallel icat processes
        """
        self._max_workers = max_workers
        self._icat_path: Optional[str] = None

    @property
    def name(self) -> str:
        return "icat"

    def can_run(self, context: ExtractionContext) -> bool:
        """
        Check if icat is available and evidence has inode data.

        Requires:
        - icat binary available in PATH
        - Evidence has EWF source path (E01/Ex01 image)
        """
        # Check for icat binary
        self._icat_path = get_sleuthkit_bin("icat")
        if not self._icat_path:
            LOGGER.debug("icat not available (bundled or PATH)")
            return False

        # Check if evidence_fs has ewf_paths or source_path (is EWF image)
        evidence_fs = context.evidence_fs
        ewf_paths = getattr(evidence_fs, 'ewf_paths', None)
        source_path = getattr(evidence_fs, 'source_path', None)
        if not ewf_paths and not source_path:
            LOGGER.debug("Evidence does not have ewf_paths/source_path - not an EWF image")
            return False

        return True

    def run(
        self,
        files: List[DiscoveredFile],
        context: ExtractionContext,
    ) -> Tuple[int, int]:
        """
        Extract files using parallel icat commands.

        Args:
            files: List of discovered files (must have inode set)
            context: Extraction context

        Returns:
            Tuple of (extracted_count, error_count)
        """
        if not files:
            return 0, 0

        # Filter to files with inodes
        files_with_inodes = [f for f in files if f.inode is not None]
        if not files_with_inodes:
            LOGGER.warning("No files with inode information for icat extraction")
            return 0, len(files)

        evidence_fs = context.evidence_fs
        # Get E01 path (prefer ewf_paths[0], fallback to source_path)
        ewf_paths = getattr(evidence_fs, 'ewf_paths', None)
        e01_path = ewf_paths[0] if ewf_paths else getattr(evidence_fs, 'source_path', None)

        extracted = 0
        errors = 0
        total = len(files_with_inodes)

        # Get partition info for offset calculation
        partition_info = self._get_partition_info(evidence_fs)

        # Track used output filenames to avoid collisions
        used_filenames: set = set()

        # Process in parallel
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {}

            for i, file in enumerate(files_with_inodes):
                # Check cancellation (unified method)
                if context.is_cancelled():
                    LOGGER.info("Extraction cancelled")
                    break

                # Calculate offset for partition
                offset = self._calculate_offset(file.partition_index, partition_info)

                # Generate unique output path with collision handling
                base_filename = Path(file.path).name
                output_filename = base_filename
                counter = 0
                while output_filename in used_filenames:
                    counter += 1
                    output_filename = f"{base_filename}_{counter}"
                used_filenames.add(output_filename)
                output_path = context.output_dir / output_filename

                # Submit icat task
                future = executor.submit(
                    self._run_icat_single,
                    e01_path=str(e01_path),
                    inode=file.inode,
                    output_path=output_path,
                    offset=offset,
                )
                futures[future] = file

            # Collect results
            for future in as_completed(futures):
                file = futures[future]

                # Check cancellation (unified method)
                if context.is_cancelled():
                    break

                try:
                    result = future.result()

                    if result.success and result.output_path and result.output_path.exists():
                        # Compute hashes if enabled
                        md5, sha256 = None, None
                        if context.compute_hash:
                            md5, sha256 = compute_file_hashes(result.output_path)

                        # Create extraction result with correct field names
                        entry = ExtractionResult(
                            success=True,
                            source_path=file.path,
                            extracted_path=result.output_path.name,  # Just filename
                            size_bytes=result.file_size,
                            md5=md5,
                            sha256=sha256,
                            partition_index=file.partition_index,
                            inode=file.inode,
                            logical_path=file.path,
                            profile=extract_profile_from_path(file.path),
                            artifact_type=file.artifact_type,
                        )

                        # Write to manifest
                        context.manifest_writer.append(entry.to_dict())
                        extracted += 1
                    else:
                        entry = ExtractionResult(
                            success=False,
                            source_path=file.path,
                            error=result.error or "icat failed",
                            partition_index=file.partition_index,
                            inode=file.inode,
                        )
                        context.manifest_writer.append(entry.to_dict())
                        errors += 1

                except Exception as e:
                    LOGGER.error("Error processing icat result for %s: %s", file.path, e)
                    entry = ExtractionResult(
                        success=False,
                        source_path=file.path,
                        error=str(e),
                        partition_index=file.partition_index,
                        inode=file.inode,
                    )
                    context.manifest_writer.append(entry.to_dict())
                    errors += 1

                # Progress callback
                if context.progress_callback:
                    context.progress_callback(
                        extracted + errors,
                        total,
                        f"Extracted {extracted}, errors {errors}"
                    )

        return extracted, errors

    def _run_icat_single(
        self,
        e01_path: str,
        inode: int,
        output_path: Path,
        offset: int = 0,
    ) -> IcatResult:
        """
        Run single icat command to extract file by inode.

        Args:
            e01_path: Path to E01 image
            inode: File inode number
            output_path: Destination path
            offset: Partition offset in sectors

        Returns:
            IcatResult with success status
        """
        try:
            # Build icat command
            cmd = [self._icat_path]

            # Add offset if non-zero
            if offset > 0:
                cmd.extend(["-o", str(offset)])

            cmd.extend([e01_path, str(inode)])

            # Run icat and capture output to file
            with open(output_path, 'wb') as f:
                result = subprocess.run(
                    cmd,
                    stdout=f,
                    stderr=subprocess.PIPE,
                    timeout=60,  # 1 minute timeout per file
                )

            if result.returncode != 0:
                stderr = result.stderr.decode('utf-8', errors='replace')
                LOGGER.debug("icat failed for inode %d: %s", inode, stderr)
                # Remove partial file
                if output_path.exists():
                    output_path.unlink()
                return IcatResult(success=False, error=stderr[:200])

            file_size = output_path.stat().st_size
            return IcatResult(
                success=True,
                output_path=output_path,
                file_size=file_size,
            )

        except subprocess.TimeoutExpired:
            LOGGER.warning("icat timeout for inode %d", inode)
            if output_path.exists():
                output_path.unlink()
            return IcatResult(success=False, error="timeout")
        except Exception as e:
            LOGGER.error("icat error for inode %d: %s", inode, e)
            if output_path.exists():
                output_path.unlink()
            return IcatResult(success=False, error=str(e))

    def _get_partition_info(self, evidence_fs: Any) -> dict:
        """
        Get partition information from evidence filesystem.

        Uses list_ewf_partitions to get accurate byte offsets for each partition.
        Falls back to _partition_index if available.

        Args:
            evidence_fs: EvidenceFS instance (PyEwfTskFS)

        Returns:
            Dict mapping partition_index to sector offset (offset in sectors, not bytes)
        """
        info = {}

        # Try to get partition offsets via list_ewf_partitions (most reliable)
        ewf_paths = getattr(evidence_fs, 'ewf_paths', None)
        if ewf_paths:
            try:
                from core.evidence_fs import list_ewf_partitions
                partitions = list_ewf_partitions(ewf_paths)
                # list_ewf_partitions returns byte offsets; icat needs sector offsets
                for p in partitions:
                    idx = p.get("index", 0)
                    byte_offset = p.get("offset", 0)
                    # Convert byte offset to sector offset (512-byte sectors)
                    sector_offset = byte_offset // 512
                    info[idx] = sector_offset
                if info:
                    LOGGER.debug("Got partition offsets from list_ewf_partitions: %s", info)
                    return info
            except Exception as e:
                LOGGER.debug("list_ewf_partitions failed: %s", e)

        # Fallback: PyEwfTskFS single partition case
        # When opening via PyEwfTskFS with partition_index, offset is handled internally
        # so we return 0 to indicate no additional offset needed
        partition_index = getattr(evidence_fs, '_partition_index', None)
        if partition_index is not None:
            info[partition_index] = 0  # Offset already applied by PyEwfTskFS
            LOGGER.debug("Using single partition fallback: index=%d, offset=0", partition_index)

        return info

    def _calculate_offset(
        self,
        partition_index: Optional[int],
        partition_info: dict,
    ) -> int:
        """
        Calculate sector offset for partition.

        Args:
            partition_index: Partition index (0-based)
            partition_info: Partition info dict

        Returns:
            Sector offset
        """
        if partition_index is None:
            return 0

        return partition_info.get(partition_index, 0)


def icat_available() -> bool:
    """Check if icat command is available."""
    return get_sleuthkit_bin("icat") is not None
