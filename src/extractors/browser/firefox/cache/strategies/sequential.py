"""
Firefox Cache Extraction - Sequential Strategy

Simple sequential extraction using single EvidenceFS handle.
Fallback strategy for MountedFS or when parallel extraction is disabled.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, List, Tuple

from core.logging import get_logger
from .base import (
    ExtractionStrategy,
    ExtractionContext,
    ExtractionResult,
    DiscoveredFile,
    stream_copy_with_hash,
    stream_copy_with_hash_from_iterator,
    extract_profile_from_path,
    CHUNK_SIZE,
)

LOGGER = get_logger("extractors.cache_firefox.strategies.sequential")


class SequentialExtractionStrategy(ExtractionStrategy):
    """
    Sequential extraction using single EvidenceFS handle.

    Simple, reliable strategy that processes files one at a time.
    Used for:
    - Mounted filesystems (no need for parallel I/O)
    - Small batches where parallel overhead isn't worth it
    - When parallel extraction is explicitly disabled
    """

    @property
    def name(self) -> str:
        return "sequential"

    def can_run(self, context: ExtractionContext) -> bool:
        """Sequential extraction always works."""
        return True

    def run(
        self,
        files: List[DiscoveredFile],
        context: ExtractionContext,
    ) -> Tuple[int, int]:
        """
        Extract files sequentially.

        Args:
            files: List of discovered files
            context: Extraction context

        Returns:
            Tuple of (extracted_count, error_count)
        """
        if not files:
            return 0, 0

        extracted = 0
        errors = 0
        total = len(files)

        evidence_fs = context.evidence_fs

        # Track extracted hashes for dedup
        extracted_hashes: set = set()

        for i, file in enumerate(files):
            # Check cancellation (uses unified is_cancelled() method)
            if context.is_cancelled():
                LOGGER.info("Extraction cancelled at %d/%d", i, total)
                break

            result = self._extract_single(
                file=file,
                evidence_fs=evidence_fs,
                context=context,
                extracted_hashes=extracted_hashes,
            )

            # Write to manifest
            context.manifest_writer.append(result.to_dict())

            if result.success:
                extracted += 1
            else:
                errors += 1

            # Progress callback (every 50 files or so)
            if context.progress_callback and (i % 50 == 0 or i == total - 1):
                context.progress_callback(
                    i + 1,
                    total,
                    f"Extracted {extracted}, errors {errors}"
                )

            # Log callback
            if context.log_callback and i % 100 == 0:
                context.log_callback(f"Processing {i}/{total} files...")

        return extracted, errors

    def _extract_single(
        self,
        file: DiscoveredFile,
        evidence_fs: Any,
        context: ExtractionContext,
        extracted_hashes: set,
    ) -> ExtractionResult:
        """
        Extract a single file.

        Args:
            file: File to extract
            evidence_fs: EvidenceFS instance
            context: Extraction context
            extracted_hashes: Set of already extracted SHA256 hashes

        Returns:
            ExtractionResult
        """
        try:
            # Generate output path
            output_filename = Path(file.path).name
            output_path = context.output_dir / output_filename

            # Handle duplicates
            counter = 0
            while output_path.exists():
                counter += 1
                output_path = context.output_dir / f"{output_filename}_{counter}"

            # Read file from evidence
            try:
                # Check if it's a mounted filesystem
                if hasattr(evidence_fs, 'is_mounted') and evidence_fs.is_mounted:
                    # Direct file read for mounted FS
                    full_path = Path(evidence_fs.mount_point) / file.path.lstrip('/')
                    if not full_path.exists():
                        return ExtractionResult(
                            success=False,
                            source_path=file.path,
                            error="File not found",
                            partition_index=file.partition_index,
                        )

                    # Copy with hashing (honor compute_hash setting)
                    with open(full_path, 'rb') as src:
                        file_size, md5, sha256 = stream_copy_with_hash(
                            src, output_path,
                            compute_hash=context.compute_hash,
                        )

                # Streaming read via iterator (partition_index handled internally by EvidenceFS)
                elif hasattr(evidence_fs, 'open_for_stream'):
                    # open_for_stream returns an iterator, not a context manager
                    chunks = evidence_fs.open_for_stream(file.path)
                    file_size, md5, sha256 = stream_copy_with_hash_from_iterator(
                        chunks, output_path,
                        compute_hash=context.compute_hash,
                    )

                # Chunk iterator (partition_index handled internally by EvidenceFS)
                elif hasattr(evidence_fs, 'iter_file_chunks'):
                    chunks = evidence_fs.iter_file_chunks(
                        file.path,
                        chunk_size=CHUNK_SIZE,
                    )
                    file_size, md5, sha256 = stream_copy_with_hash_from_iterator(
                        chunks, output_path,
                        compute_hash=context.compute_hash,
                    )

                # Full read fallback (partition_index handled internally by EvidenceFS)
                elif hasattr(evidence_fs, 'read_file'):
                    data = evidence_fs.read_file(file.path)
                    if data is None:
                        return ExtractionResult(
                            success=False,
                            source_path=file.path,
                            error="File not found or empty",
                            partition_index=file.partition_index,
                        )

                    if context.compute_hash:
                        md5 = hashlib.md5(data).hexdigest()
                        sha256 = hashlib.sha256(data).hexdigest()
                    else:
                        md5 = None
                        sha256 = None
                    file_size = len(data)

                    output_path.write_bytes(data)

                else:
                    return ExtractionResult(
                        success=False,
                        source_path=file.path,
                        error="EvidenceFS does not support file reading",
                        partition_index=file.partition_index,
                    )

            except FileNotFoundError:
                return ExtractionResult(
                    success=False,
                    source_path=file.path,
                    error="File not found",
                    partition_index=file.partition_index,
                )
            except Exception as e:
                # Clean up partial file
                if output_path.exists():
                    try:
                        output_path.unlink()
                    except OSError:
                        pass
                return ExtractionResult(
                    success=False,
                    source_path=file.path,
                    error=f"Read error: {e}",
                    partition_index=file.partition_index,
                )

            # Check for duplicate content (only when hashing is enabled)
            if sha256 is not None and sha256 in extracted_hashes:
                # Remove duplicate
                output_path.unlink()
                return ExtractionResult(
                    success=False,
                    source_path=file.path,
                    error="Duplicate content (SHA256)",
                    partition_index=file.partition_index,
                )
            if sha256 is not None:
                extracted_hashes.add(sha256)

            return ExtractionResult(
                success=True,
                source_path=file.path,
                extracted_path=output_path.name,  # Just filename, ingestion joins with run_dir
                size_bytes=file_size,
                md5=md5,
                sha256=sha256,
                partition_index=file.partition_index,
                inode=file.inode,
                logical_path=file.path,
                profile=extract_profile_from_path(file.path),
                artifact_type=file.artifact_type,
            )

        except Exception as e:
            LOGGER.error("Error extracting %s: %s", file.path, e)
            return ExtractionResult(
                success=False,
                source_path=file.path,
                error=str(e),
                partition_index=file.partition_index,
            )
