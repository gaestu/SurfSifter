from __future__ import annotations

import fnmatch
import io
import os
import re
import stat as stat_module
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Dict, Iterator, List, Optional

from .logging import get_logger

LOGGER = get_logger("core.evidence_fs")


@dataclass
class EvidenceFileStat:
    """
    File metadata from evidence filesystem.

    Provides forensically-accurate timestamp and inode information.
    All timestamps are Unix epochs (float) for precision.

    Timestamp semantics:
        - mtime: Modification time (content changed)
        - atime: Access time (file read)
        - ctime: Metadata change time (Unix: inode change, Windows: often same as crtime)
        - crtime: Creation time (NTFS $SI Create, ext4 crtime if available)
    """
    size_bytes: int
    mtime_epoch: Optional[float]   # Modification time (Unix epoch)
    atime_epoch: Optional[float]   # Access time
    ctime_epoch: Optional[float]   # Metadata change time (NOT creation on Unix!)
    crtime_epoch: Optional[float]  # Creation time (NTFS $SI, ext4 crtime)
    inode: Optional[int]           # Inode/MFT entry number
    is_file: bool                  # True if regular file
    is_dir: bool = False           # True if directory


def find_ewf_segments(first_segment: Path) -> List[Path]:
    """
    Given the first segment of an EWF image (e.g., image.E01 or image.e01),
    discover all related segments in the same directory.

    Supports:
    - Uppercase: .E01, .E02, ..., .E99
    - Lowercase: .e01, .e02, ..., .e99
    - Mixed extensions: .e0x pattern

    Returns a sorted list of all found segments.
    """
    if not first_segment.exists():
        raise FileNotFoundError(f"E01 segment not found: {first_segment}")

    # Determine naming pattern from first segment
    stem = first_segment.stem
    suffix = first_segment.suffix.lower()
    parent = first_segment.parent

    # Match .e01, .e02, etc. or .e0x pattern
    if not re.match(r'\.e\d{2}', suffix) and suffix != '.e01':
        # Try uppercase
        suffix = first_segment.suffix.upper()
        if not re.match(r'\.E\d{2}', suffix) and suffix != '.E01':
            LOGGER.warning("Unexpected EWF extension: %s", first_segment.suffix)
            return [first_segment]

    segments = [first_segment]
    is_uppercase = first_segment.suffix[1].isupper()

    # Look for additional segments (.E02, .E03, etc.)
    for i in range(2, 100):
        if is_uppercase:
            next_path = parent / f"{stem}.E{i:02d}"
        else:
            next_path = parent / f"{stem}.e{i:02d}"

        if next_path.exists():
            segments.append(next_path)
        else:
            # No more segments found
            break

    LOGGER.info("Discovered %d EWF segment(s) for %s", len(segments), first_segment.name)
    return segments


def list_ewf_partitions(ewf_paths: List[Path]) -> List[Dict[str, Any]]:
    """
    List all partitions in an E01 image with metadata.

    Returns a list of partition info dictionaries:
    [
        {
            'index': 1,
            'addr': 0,  # pytsk3 partition address
            'offset': 1048576,  # bytes
            'length': 107374182400,  # bytes
            'block_size': 512,  # sector/block size in bytes
            'description': 'NTFS',
            'filesystem_readable': True,
            'root_file_count': 42
        },
        ...
    ]

    Note: block_size is detected from volume info (typically 512 or 4096).
    For direct filesystems (no partition table), block_size comes from FS_Info.
    """
    try:
        import pyewf  # type: ignore
        import pytsk3  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(
            "list_ewf_partitions requires pyewf and pytsk3 to be installed."
        ) from exc

    # Open EWF image
    handle = pyewf.handle()
    handle.open([str(path) for path in ewf_paths])

    try:
        img_info = _PyEwfImgInfo(handle, pytsk3)

        # Try to read partition table
        try:
            volume = pytsk3.Volume_Info(img_info)
        except OSError:
            # No partition table - direct filesystem
            LOGGER.debug("No partition table found - direct filesystem")

            # Try to get block_size from filesystem info
            fs_block_size = 512  # Default fallback
            try:
                fs = pytsk3.FS_Info(img_info, offset=0)
                fs_block_size = fs.info.block_size
            except Exception:
                pass

            handle.close()
            return [{
                'index': 0,
                'addr': 0,
                'offset': 0,
                'length': handle.get_media_size() if hasattr(handle, 'get_media_size') else 0,
                'block_size': fs_block_size,
                'description': 'Direct Filesystem',
                'filesystem_readable': True,
                'root_file_count': None
            }]

        # Extract partition info
        partitions = []
        partition_index = 1
        volume_block_size = volume.info.block_size

        for part in volume:
            # Skip metadata partitions and unallocated space
            if part.flags != pytsk3.TSK_VS_PART_FLAG_ALLOC:
                continue

            offset = part.start * volume_block_size
            length = part.len * volume_block_size
            desc = part.desc.decode('utf-8', 'ignore')

            # Try to detect filesystem
            fs_readable = False
            root_count = None
            try:
                fs = pytsk3.FS_Info(img_info, offset=offset)
                fs_readable = True
                # Try to count root directory entries
                try:
                    root_dir = fs.open_dir("/")
                    root_count = len([
                        e for e in root_dir
                        if e.info.name.name.decode('utf-8', 'ignore') not in {'.', '..'}
                    ])
                except Exception:
                    pass
            except Exception as e:
                LOGGER.debug("Partition %d filesystem not readable: %s", part.addr, e)

            partitions.append({
                'index': partition_index,
                'addr': part.addr,
                'offset': offset,
                'length': length,
                'block_size': volume_block_size,
                'description': desc,
                'filesystem_readable': fs_readable,
                'root_file_count': root_count
            })
            partition_index += 1

        handle.close()
        LOGGER.info("Found %d partition(s) in E01 image", len(partitions))
        return partitions

    except Exception:
        handle.close()
        raise


def open_ewf_partition(
    ewf_paths: List[Path],
    partition_index: int = -1,
) -> "PyEwfTskFS":
    """
    Open a specific partition from an EWF image.

    This is a convenience wrapper around PyEwfTskFS that makes it easy
    to open specific partitions by index.

    Args:
        ewf_paths: List of EWF segment paths (E01, E02, etc.)
        partition_index: Partition to open (from list_ewf_partitions):
            -1 = auto-select (largest NTFS/FAT with Windows folder)
             0 = direct filesystem (no partition table)
            1+ = specific partition index (from list_ewf_partitions)

    Returns:
        PyEwfTskFS instance for the requested partition

    Raises:
        ValueError: If partition_index is invalid
        RuntimeError: If partition cannot be opened

    Example:
        >>> partitions = list_ewf_partitions(ewf_paths)
        >>> for part in partitions:
        ...     if part['filesystem_readable']:
        ...         fs = open_ewf_partition(ewf_paths, partition_index=part['index'])
        ...         # Use fs.open_for_read(), fs.iter_paths(), etc.

    Note:
        The partition_index values from list_ewf_partitions() map directly
        to PyEwfTskFS partition_index parameter:
        - 0 = direct filesystem
        - 1+ = specific partition
    """
    if partition_index == -1:
        # Use existing auto-select behavior
        return PyEwfTskFS(ewf_paths, partition_index=-1)

    if partition_index == 0:
        # Direct filesystem (no partition table)
        return PyEwfTskFS(ewf_paths, partition_index=0)

    # For partition_index >= 1, validate it exists
    partitions = list_ewf_partitions(ewf_paths)

    # Find partition by index
    for part in partitions:
        if part['index'] == partition_index:
            if not part['filesystem_readable']:
                raise RuntimeError(
                    f"Partition {partition_index} is not readable: {part['description']}"
                )
            # PyEwfTskFS uses 1-based index into its internal partition list
            # which matches list_ewf_partitions() index (both start at 1)
            return PyEwfTskFS(ewf_paths, partition_index=partition_index)

    raise ValueError(
        f"Partition index {partition_index} not found. "
        f"Available: {[p['index'] for p in partitions]}"
    )


class EvidenceFS(ABC):
    """Abstract read-only view over an evidence filesystem."""

    @abstractmethod
    def iter_paths(self, glob_pattern: str) -> Iterator[str]:
        """Yield normalized paths that match the provided glob pattern."""

    @abstractmethod
    def open_for_read(self, path: str) -> BinaryIO:
        """Return a binary file-like object for the specified path."""

    @abstractmethod
    def list_users(self) -> List[str]:
        """Return a list of discovered user profile identifiers."""

    @abstractmethod
    def stat(self, path: str) -> EvidenceFileStat:
        """
        Get file metadata without reading content.

        Args:
            path: Path to file in evidence

        Returns:
            EvidenceFileStat with size, timestamps, and inode

        Raises:
            FileNotFoundError: If path does not exist
        """

    @abstractmethod
    def iter_all_files(self) -> Iterator[str]:
        """
        Yield all file paths in filesystem (full recursive walk).

        Unlike iter_paths(glob), this walks the entire filesystem.
        Used by filesystem_images extractor to find all images.
        Includes cycle detection for NTFS junctions.

        Yields:
            Normalized file paths (directories excluded)
        """

    def iter_all_files_with_stat(self) -> Iterator[tuple[str, EvidenceFileStat]]:
        """
        Yield file paths with stat info (default: iter_all_files + stat).

        Subclasses can override to avoid extra file opens during discovery.
        """
        for path in self.iter_all_files():
            yield path, self.stat(path)

    @abstractmethod
    def open_for_stream(self, path: str, chunk_size: int = 65536) -> Iterator[bytes]:
        """
        Yield file content in chunks without full buffering.

        CRITICAL: Unlike open_for_read() which loads entire file into memory,
        this yields chunks for memory-efficient hash computation.

        Args:
            path: File path in evidence
            chunk_size: Bytes per chunk (default 64KB)

        Yields:
            Chunks of file content

        Raises:
            FileNotFoundError: If path does not exist or is not a file
        """

    @property
    def has_fast_stat_walk(self) -> bool:
        """Return True if iter_all_files_with_stat is optimized in this implementation."""
        return False

    def read_file(self, path: str) -> bytes:
        """
        Read entire file content as bytes.

        Convenience wrapper around open_for_read().

        Args:
            path: Path to file in evidence

        Returns:
            Complete file content as bytes
        """
        with self.open_for_read(path) as f:
            return f.read()

    def walk_directory(self, dir_path: str) -> Iterator[str]:
        """
        Walk a specific directory and yield all file paths within it.

        This is more efficient than iter_paths(dir/**/*) when you already
        know the directory exists and just want its contents.

        Default implementation uses iter_paths() with recursive glob.
        Subclasses can override for better performance.

        Args:
            dir_path: Path to directory to walk

        Yields:
            Normalized file paths (files only, not directories)
        """
        # Default fallback - subclasses should override for efficiency
        normalized = dir_path.strip("/")
        pattern = f"{normalized}/**/*"
        for path in self.iter_paths(pattern):
            yield path


class PyEwfTskFS(EvidenceFS):
    """Evidence filesystem backed by pyewf + pytsk3."""

    def __init__(self, ewf_paths: List[Path], partition_index: int = -1) -> None:
        """
        Initialize PyEwfTskFS to read an E01 image.

        Args:
            ewf_paths: List of E01 segment paths (e.g., [image.E01, image.E02, ...])
            partition_index: Which partition to mount:
                - -1 (default): Auto-detect (use largest NTFS/FAT/ext partition)
                - 0: Try direct filesystem (no partition table)
                - 1+: Specific partition number
        """
        if not ewf_paths:
            raise ValueError("At least one EWF segment must be provided.")
        self.ewf_paths = ewf_paths
        try:
            import pyewf  # type: ignore
            import pytsk3  # type: ignore
        except ModuleNotFoundError as exc:  # pragma: no cover - dependency guard
            raise RuntimeError(
                "PyEwfTskFS requires pyewf and pytsk3 to be installed."
            ) from exc

        self._pyewf = pyewf
        self._pytsk3 = pytsk3
        self._handle = pyewf.handle()
        self._handle.open([str(path) for path in ewf_paths])
        self._img_info = _PyEwfImgInfo(self._handle, pytsk3)

        # Store requested partition index; will be updated by _open_filesystem
        self._partition_index = partition_index

        # Try to open filesystem - handle both direct FS and partitioned disk
        self._fs = self._open_filesystem(partition_index)
        LOGGER.debug("Initialized PyEwfTskFS with %s segments (partition: %s).",
                   len(ewf_paths), self._partition_index)

    def _open_filesystem(self, partition_index: int):
        """
        Open the filesystem, handling both direct filesystems and partitioned disks.

        Returns the pytsk3.FS_Info object for the selected partition.
        """
        # Strategy:
        # 1. If partition_index >= 0, try direct filesystem first
        # 2. If that fails (or partition_index == -1), try partition detection
        # 3. If both fail, raise error

        direct_fs_error = None

        # Try direct filesystem access (no partition table)
        if partition_index >= 0:
            try:
                fs = self._pytsk3.FS_Info(self._img_info)
                # Direct filesystem - use index 0
                self._partition_index = 0
                return fs
            except OSError as exc:
                LOGGER.debug("Direct filesystem access failed: %s", exc)
                direct_fs_error = exc
                if partition_index == 0:
                    # User explicitly requested direct access - don't try partitions
                    raise RuntimeError(
                        f"Unable to open E01 image as direct filesystem: {exc}\n"
                        "The image may contain a partition table. Try auto-detection (partition_index=-1)."
                    ) from exc

        # Try to open as partitioned disk
        try:
            volume = self._pytsk3.Volume_Info(self._img_info)
        except OSError as volume_exc:
            LOGGER.debug("Partition detection failed: %s", volume_exc)
            # Neither direct FS nor partitioned disk worked
            if direct_fs_error:
                # We tried both - neither worked
                raise RuntimeError(
                    f"Unable to open E01 image:\n"
                    f"  Direct filesystem: {direct_fs_error}\n"
                    f"  Partition detection: {volume_exc}\n"
                    "The image may be corrupted or in an unsupported format."
                ) from volume_exc
            else:
                # Only tried partition detection - try direct FS now
                try:
                    fs = self._pytsk3.FS_Info(self._img_info)
                    self._partition_index = 0
                    return fs
                except OSError as fs_exc:
                    raise RuntimeError(
                        f"Unable to open E01 image: {fs_exc}\n"
                        "The image is neither a direct filesystem nor a partitioned disk. "
                        "It may be corrupted or in an unsupported format."
                    ) from fs_exc

        # Find partitions
        partitions = []
        for part in volume:
            # Skip metadata partitions and unallocated space
            if part.flags == self._pytsk3.TSK_VS_PART_FLAG_ALLOC:
                partitions.append(part)
                LOGGER.debug(
                    "Found partition %d: offset=%d, length=%d, desc=%s",
                    part.addr, part.start * volume.info.block_size,
                    part.len * volume.info.block_size, part.desc.decode('utf-8', 'ignore')
                )

        if not partitions:
            raise RuntimeError(
                "No valid partitions found in the E01 image. "
                "The image may be corrupted or unformatted."
            )

        # Select partition
        if partition_index == -1:
            # Auto-select: prefer largest NTFS/FAT/ext partition
            selected = self._auto_select_partition(partitions, volume)
        elif 0 < partition_index <= len(partitions):
            selected = partitions[partition_index - 1]
        else:
            raise ValueError(
                f"Partition index {partition_index} out of range. "
                f"Found {len(partitions)} partition(s)."
            )

        # Store the selected partition index for workers to reuse
        # This is the 1-based index into the partitions list
        self._partition_index = partitions.index(selected) + 1

        # Open filesystem at partition offset
        offset = selected.start * volume.info.block_size
        LOGGER.debug(
            "Opening partition %d at offset %d (desc: %s)",
            selected.addr, offset, selected.desc.decode('utf-8', 'ignore')
        )

        try:
            return self._pytsk3.FS_Info(self._img_info, offset=offset)
        except OSError as exc:
            raise RuntimeError(
                f"Unable to open filesystem on partition {selected.addr} "
                f"at offset {offset}: {exc}\n"
                "The partition may be encrypted, corrupted, or in an unsupported format."
            ) from exc

    @property
    def partition_index(self) -> int:
        """
        Return the partition index that was used to open this filesystem.

        This value can be passed to new PyEwfTskFS instances (e.g., worker threads)
        to avoid re-running partition auto-detection.

        Returns:
            0 for direct filesystem, 1+ for partitioned disks, -1 if unknown
        """
        return getattr(self, "_partition_index", -1)

    @property
    def has_fast_stat_walk(self) -> bool:
        """PyEwfTskFS can yield stats during walk without extra file opens."""
        return True

    @property
    def source_path(self) -> Path:
        """
        Return first E01 segment path for manifest/logging.

        Used by forensic logging to record source evidence file in manifests.
        Week 0 infrastructure requirement for.
        """
        return self.ewf_paths[0]

    @property
    def fs_type(self) -> str:
        """
        Return filesystem type (ntfs, ext4, apfs, fat32).

        Maps pytsk3 filesystem type constants to friendly names.
        Week 0 infrastructure requirement for.

        Returns:
            Filesystem type string or "unknown" if not detectable
        """
        if self._fs is None:
            return "unknown"

        try:
            fs_type_id = self._fs.info.ftype
            # Map common pytsk3 FS_TYPE constants to friendly names
            # See: https://github.com/py4n6/pytsk/blob/master/pytsk3/tsk3.h
            type_map = {
                0x01: "ntfs",       # TSK_FS_TYPE_NTFS
                0x02: "fat12",      # TSK_FS_TYPE_FAT12
                0x03: "fat16",      # TSK_FS_TYPE_FAT16
                0x04: "fat32",      # TSK_FS_TYPE_FAT32
                0x08: "ext",        # TSK_FS_TYPE_EXT (covers ext2/3/4)
                0x10: "hfs",        # TSK_FS_TYPE_HFS
                0x20: "iso9660",    # TSK_FS_TYPE_ISO9660
                0x40: "yaffs2",     # TSK_FS_TYPE_YAFFS2
            }
            fs_name = type_map.get(fs_type_id, f"unknown_0x{fs_type_id:02x}")
            LOGGER.debug("Detected filesystem type: %s (pytsk3 type_id=0x%02x)", fs_name, fs_type_id)
            return fs_name
        except Exception as e:
            LOGGER.warning("Failed to detect filesystem type: %s", e)
            return "unknown"

    def _auto_select_partition(self, partitions, volume):
        """
        Auto-select the most likely Windows system partition.

        Strategy:
        1. Probe each partition to determine actual filesystem type
        2. Among NTFS partitions, prefer ones with a Windows folder (OS partition)
        3. If no Windows folder found, prefer largest NTFS
        4. Then largest FAT
        5. Fallback to largest partition

        When partition descriptions don't indicate filesystem type (e.g., "Basic data partition"),
        probes the actual filesystem to determine type.
        """
        ntfs_parts = []
        fat_parts = []
        other_parts = []

        for part in partitions:
            desc = part.desc.decode('utf-8', 'ignore').lower()
            size = part.len * volume.info.block_size
            offset = part.start * volume.info.block_size

            # First check description for filesystem type hints
            if 'ntfs' in desc or '0x07' in desc:  # NTFS or HPFS/exFAT
                ntfs_parts.append((size, part, offset))
                LOGGER.debug("Partition %d: NTFS (from description)", part.addr)
            elif 'fat' in desc or '0x0b' in desc or '0x0c' in desc:
                fat_parts.append((size, part, offset))
                LOGGER.debug("Partition %d: FAT (from description)", part.addr)
            elif 'basic data' in desc or 'primary' in desc:
                # Generic description - probe actual filesystem type
                try:
                    fs = self._pytsk3.FS_Info(self._img_info, offset=offset)
                    fs_type = fs.info.ftype
                    # TSK_FS_TYPE_NTFS = 0x01, TSK_FS_TYPE_FAT variants = 0x02-0x04
                    if fs_type == 0x01:  # NTFS
                        ntfs_parts.append((size, part, offset))
                        LOGGER.debug("Partition %d: NTFS (probed at offset %d)", part.addr, offset)
                    elif fs_type in (0x02, 0x03, 0x04):  # FAT12/16/32
                        fat_parts.append((size, part, offset))
                        LOGGER.debug("Partition %d: FAT (probed at offset %d)", part.addr, offset)
                    else:
                        other_parts.append((size, part, offset))
                        LOGGER.debug("Partition %d: Other fs_type=0x%02x (probed)", part.addr, fs_type)
                except Exception as e:
                    # Can't probe - add to other
                    other_parts.append((size, part, offset))
                    LOGGER.debug("Partition %d: Failed to probe filesystem: %s", part.addr, e)
            else:
                other_parts.append((size, part, offset))

        # Among NTFS partitions, prefer ones with Windows folder (OS partition)
        if ntfs_parts:
            LOGGER.info("Checking %d NTFS partitions for Windows folder...", len(ntfs_parts))
            windows_parts = []
            for size, part, offset in ntfs_parts:
                try:
                    LOGGER.debug("Checking partition %d at offset %d for Windows folder", part.addr, offset)
                    fs = self._pytsk3.FS_Info(self._img_info, offset=offset)
                    root_dir = fs.open_dir("/")
                    has_windows = False
                    has_users = False
                    entry_count = 0
                    for entry in root_dir:
                        entry_count += 1
                        # Safety limit - don't iterate too many entries
                        if entry_count > 200:
                            LOGGER.debug("Partition %d: Stopping after 200 entries", part.addr)
                            break
                        name = entry.info.name.name
                        if isinstance(name, bytes):
                            name = name.decode('utf-8', 'ignore')
                        name_lower = name.lower()
                        if name_lower == 'windows':
                            has_windows = True
                        elif name_lower == 'users':
                            has_users = True
                        if has_windows and has_users:
                            break

                    if has_windows:
                        windows_parts.append((size, part, offset, has_users))
                        LOGGER.info("Partition %d: Has Windows folder (Users=%s)", part.addr, has_users)
                    else:
                        LOGGER.debug("Partition %d: No Windows folder found (%d entries checked)", part.addr, entry_count)
                except Exception as e:
                    LOGGER.warning("Partition %d: Failed to check for Windows folder: %s", part.addr, e)

            # Prefer partition with both Windows and Users folders
            windows_with_users = [(s, p, o) for s, p, o, u in windows_parts if u]
            if windows_with_users:
                # Pick largest Windows+Users partition
                windows_with_users.sort(reverse=True, key=lambda x: x[0])
                selected = windows_with_users[0][1]
                LOGGER.info("Auto-selected Windows OS partition with Users (addr=%d, size=%.2f GB)",
                           selected.addr, windows_with_users[0][0] / 1024 / 1024 / 1024)
                return selected

            # Otherwise pick any partition with Windows folder
            if windows_parts:
                windows_parts_sorted = sorted(windows_parts, reverse=True, key=lambda x: x[0])
                selected = windows_parts_sorted[0][1]
                LOGGER.info("Auto-selected Windows OS partition (addr=%d, size=%.2f GB)",
                           selected.addr, windows_parts_sorted[0][0] / 1024 / 1024 / 1024)
                return selected

            # No Windows folder found - pick largest NTFS
            ntfs_parts.sort(reverse=True, key=lambda x: x[0])
            selected = ntfs_parts[0][1]
            LOGGER.info("Auto-selected largest NTFS partition (addr=%d, size=%.2f GB)",
                       selected.addr, ntfs_parts[0][0] / 1024 / 1024 / 1024)
            return selected

        # Then largest FAT
        if fat_parts:
            fat_parts.sort(reverse=True, key=lambda x: x[0])
            selected = fat_parts[0][1]
            LOGGER.info("Auto-selected largest FAT partition (addr=%d, size=%.2f GB)",
                       selected.addr, fat_parts[0][0] / 1024 / 1024 / 1024)
            return selected

        # Fallback: largest partition
        all_parts = [(p.len * volume.info.block_size, p) for p in partitions]
        all_parts.sort(reverse=True, key=lambda x: x[0])
        selected = all_parts[0][1]
        LOGGER.info("Auto-selected largest partition (unknown type, addr=%d, size=%.2f GB)",
                   selected.addr, all_parts[0][0] / 1024 / 1024 / 1024)
        return selected

    def iter_paths(self, glob_pattern: str) -> Iterator[str]:
        """
        Yield paths matching glob pattern.

        OPTIMIZATION (Bug Fix #14): For patterns with wildcards in middle segments
        (like "Users/*/AppData/Local/Chrome/*/History"), use targeted traversal
        instead of walking entire filesystem. This reduces search time from
        45+ minutes to seconds on large images (1M+ files).
        """
        LOGGER.debug("PyEwfTskFS.iter_paths(%s)", glob_pattern)
        match_count = 0
        walk_count = 0

        # OPTIMIZATION: Try targeted traversal for patterns like "Users/*/AppData/..."
        # This avoids walking all 1M+ files on large Windows 11 images
        if self._can_use_targeted_search(glob_pattern):
            LOGGER.debug("Using targeted search for pattern: %s", glob_pattern)
            for path in self._targeted_glob(glob_pattern):
                match_count += 1
                walk_count += 1  # Count for consistency
                yield path
            LOGGER.info("PyEwfTskFS.iter_paths(%s) targeted search: matched %d paths",
                       glob_pattern, match_count)
            return

        # FALLBACK: Full filesystem walk (slow but comprehensive)
        LOGGER.debug("Using full walk for pattern: %s", glob_pattern)
        from pathlib import PurePosixPath

        for path in self._walk("/"):
            walk_count += 1
            normalized = path.lstrip("/")

            # Use pathlib's match for ** recursive patterns, fnmatch for simple patterns
            try:
                if '**' in glob_pattern:
                    # pathlib.Path.match() supports ** for recursive matching
                    # Use case-insensitive matching for Windows compatibility
                    if PurePosixPath(normalized.lower()).match(glob_pattern.lower()):
                        match_count += 1
                        yield normalized
                else:
                    # Simple pattern - use fnmatch
                    # Use case-insensitive matching
                    if fnmatch.fnmatch(normalized.lower(), glob_pattern.lower()):
                        match_count += 1
                        yield normalized
            except Exception as e:
                LOGGER.debug("Error matching path %s against pattern %s: %s", normalized, glob_pattern, e)
                continue

        LOGGER.debug("PyEwfTskFS.iter_paths(%s) walked %d paths, matched %d",
                    glob_pattern, walk_count, match_count)

    def _can_use_targeted_search(self, pattern: str) -> bool:
        """
        Check if pattern can use targeted search optimization.

        Patterns like:
        - "Users/*/AppData/Local/Chrome/*/History" → YES (wildcard in Users)
        - "%USERPROFILE%/AppData/..." → YES (will be expanded first)
        - "**/*.txt" → NO (needs full walk)
        - "*.log" → NO (root level search)
        """
        # Must have wildcards but NOT ** (recursive)
        if '**' in pattern:
            return False

        # Must have at least one directory separator
        if '/' not in pattern:
            return False

        # If we have wildcards (*, ?, [), we can use targeted search
        # If we have no wildcards (exact path), we can ALSO use targeted search (it's just a direct lookup)
        return True

    def _targeted_glob(self, pattern: str) -> Iterator[str]:
        """
        Targeted glob implementation for patterns with wildcards in middle segments.

        Example: "Users/*/AppData/Local/Chrome/*/History"
        1. Split into segments: ["Users", "*", "AppData", "Local", "Chrome", "*", "History"]
        2. Expand first wildcard: ["Users/john", "Users/admin", ...]
        3. For each expansion, navigate directly and expand next wildcard
        4. Yield final matches

        This avoids walking 1M+ files by navigating directly to target directories.
        """
        parts = pattern.split('/')
        LOGGER.info("Starting targeted glob for pattern: %s (segments: %d)", pattern, len(parts))

        # Start with root
        current_paths = [""]

        for i, part in enumerate(parts):
            if not part:  # Skip empty segments
                continue

            is_last_part = (i == len(parts) - 1)
            next_paths = []

            for base_path in current_paths:
                # Check for any wildcard characters
                is_wildcard = any(c in part for c in ['*', '?', '['])

                if is_wildcard:
                    # Expand wildcard at current level
                    try:
                        dir_path = "/" + base_path if base_path else "/"
                        LOGGER.debug("Expanding wildcard '%s' in directory: %s", part, dir_path)
                        directory = self._fs.open_dir(path=dir_path)

                        for entry in directory:
                            name = getattr(entry.info.name, "name", b"").decode("utf-8", "ignore")
                            if name in {".", ".."} or not name:
                                continue

                            # Check if name matches pattern (simple * wildcard)
                            # Use case-insensitive matching
                            if fnmatch.fnmatch(name.lower(), part.lower()):
                                new_path = f"{base_path}/{name}" if base_path else name

                                # If this is the last part, yield both files and directories
                                if is_last_part:
                                    next_paths.append(new_path)
                                else:
                                    # Not last part - only keep directories for further traversal
                                    if entry.info.meta and entry.info.meta.type == self._pytsk3.TSK_FS_META_TYPE_DIR:
                                        next_paths.append(new_path)
                    except (IOError, OSError) as e:
                        LOGGER.debug("Cannot open directory %s: %s", base_path, e)
                        continue
                else:
                    # Fixed segment - try exact match first (fast), then case-insensitive scan (slow)
                    exact_path = f"{base_path}/{part}" if base_path else part
                    test_path = "/" + exact_path
                    found = False

                    # 1. Try exact match
                    try:
                        if not is_last_part:
                            # Must be directory
                            self._fs.open_dir(path=test_path)
                            next_paths.append(exact_path)
                            found = True
                        else:
                            # Can be file or directory
                            try:
                                self._fs.open(path=test_path)
                                next_paths.append(exact_path)
                                found = True
                            except (IOError, OSError):
                                self._fs.open_dir(path=test_path)
                                next_paths.append(exact_path)
                                found = True
                    except (IOError, OSError):
                        pass

                    if found:
                        continue

                    # 2. Fallback to directory listing (case-insensitive)
                    try:
                        dir_path = "/" + base_path if base_path else "/"
                        directory = self._fs.open_dir(path=dir_path)

                        for entry in directory:
                            name = getattr(entry.info.name, "name", b"").decode("utf-8", "ignore")
                            if name.lower() == part.lower():
                                new_path = f"{base_path}/{name}" if base_path else name
                                if is_last_part:
                                    next_paths.append(new_path)
                                else:
                                    if entry.info.meta and entry.info.meta.type == self._pytsk3.TSK_FS_META_TYPE_DIR:
                                        next_paths.append(new_path)
                                break # Found it, stop scanning this dir
                    except (IOError, OSError):
                        continue

            current_paths = next_paths

            if not current_paths:
                # No matches at this level, stop
                break

        # Yield final paths
        LOGGER.info("Targeted glob complete: found %d matches for pattern %s", len(current_paths), pattern)
        for path in current_paths:
            yield path

    def open_for_read(self, path: str) -> BinaryIO:
        normalized = self._normalize(path)
        file_object = self._fs.open(path=normalized)
        meta = getattr(file_object, "info", None)
        size = getattr(meta.meta, "size", None) if meta and meta.meta else None
        if size is None:
            raise FileNotFoundError(f"Unable to determine size for {path}")
        data = file_object.read_random(0, size)
        return io.BytesIO(data)

    def list_users(self) -> List[str]:
        users: List[str] = []
        try:
            directory = self._fs.open_dir(path="/Users")
        except IOError:
            return users
        for entry in directory:
            name = getattr(entry.info.name, "name", b"").decode("utf-8", "ignore")
            if name in {".", "..", "Public", "Default", "Default User"}:
                continue
            if entry.info.meta and entry.info.meta.type == self._pytsk3.TSK_FS_META_TYPE_DIR:
                users.append(name)
        return sorted(users)

    def stat(self, path: str) -> EvidenceFileStat:
        """
        Get file metadata from pytsk3.

        Maps TSK metadata fields to EvidenceFileStat:
            - size_bytes: meta.size
            - mtime_epoch: meta.mtime (modification time)
            - atime_epoch: meta.atime (access time)
            - ctime_epoch: meta.ctime (metadata change time, NOT creation on Unix)
            - crtime_epoch: meta.crtime (creation time - NTFS $SI, ext4 crtime)
            - inode: meta.addr (MFT entry number / inode)
        """
        normalized = self._normalize(path)
        try:
            file_obj = self._fs.open(path=normalized)
            meta = file_obj.info.meta
            if meta is None:
                raise FileNotFoundError(f"No metadata for {path}")

            return EvidenceFileStat(
                size_bytes=meta.size if meta.size else 0,
                mtime_epoch=float(meta.mtime) if meta.mtime else None,
                atime_epoch=float(meta.atime) if meta.atime else None,
                ctime_epoch=float(meta.ctime) if meta.ctime else None,
                crtime_epoch=float(meta.crtime) if meta.crtime else None,
                inode=meta.addr if meta.addr else None,
                is_file=(meta.type == self._pytsk3.TSK_FS_META_TYPE_REG),
                is_dir=(meta.type == self._pytsk3.TSK_FS_META_TYPE_DIR),
            )
        except IOError as e:
            raise FileNotFoundError(f"Cannot stat {path}: {e}") from e

    def iter_all_files(self) -> Iterator[str]:
        """
        Yield all file paths in filesystem (full recursive walk).

        Filters to files only (excludes directories).
        Uses _walk() internally which includes cycle detection.

        Note: Progress is logged every 10,000 paths to help diagnose slow walks.
        """
        yielded_count = 0
        for path, meta in self._walk_entries("/"):
            if meta and meta.type == self._pytsk3.TSK_FS_META_TYPE_REG:
                yielded_count += 1
                if yielded_count % 10000 == 0:
                    LOGGER.debug("iter_all_files progress: %d files found", yielded_count)
                yield path.lstrip("/")

    def iter_all_files_with_stat(self) -> Iterator[tuple[str, EvidenceFileStat]]:
        """
        Yield all file paths with stat info using directory metadata.

        Avoids per-file open calls by reusing metadata from directory entries.
        """
        yielded_count = 0
        for path, meta in self._walk_entries("/"):
            if not meta or meta.type != self._pytsk3.TSK_FS_META_TYPE_REG:
                continue

            yielded_count += 1
            if yielded_count % 10000 == 0:
                LOGGER.debug("iter_all_files_with_stat progress: %d files found", yielded_count)

            yield path.lstrip("/"), EvidenceFileStat(
                size_bytes=meta.size if meta.size else 0,
                mtime_epoch=float(meta.mtime) if meta.mtime else None,
                atime_epoch=float(meta.atime) if meta.atime else None,
                ctime_epoch=float(meta.ctime) if meta.ctime else None,
                crtime_epoch=float(meta.crtime) if meta.crtime else None,
                inode=meta.addr if meta.addr else None,
                is_file=True,
                is_dir=False,
            )

    def open_for_stream(self, path: str, chunk_size: int = 65536) -> Iterator[bytes]:
        """
        Yield file content in chunks (memory-efficient).

        Unlike open_for_read() which loads entire file into memory,
        this reads and yields in chunks for streaming hash computation.
        """
        normalized = self._normalize(path)
        try:
            file_obj = self._fs.open(path=normalized)
        except IOError as e:
            raise FileNotFoundError(f"Cannot open {path}: {e}") from e

        meta = file_obj.info.meta
        if meta is None or meta.size is None:
            raise FileNotFoundError(f"Unable to determine size for {path}")

        size = meta.size
        offset = 0
        while offset < size:
            read_size = min(chunk_size, size - offset)
            chunk = file_obj.read_random(offset, read_size)
            if not chunk:
                break
            yield chunk
            offset += len(chunk)

    def _walk(self, path: str) -> Iterator[str]:
        """
        Walk the filesystem starting from path, yielding all file paths.

        Includes cycle detection via inode tracking to prevent infinite loops
        caused by NTFS junctions and symlinks (e.g., Application Data -> AppData).
        Also tracks visited paths as fallback when inode unavailable.
        """
        visited_inodes: set[int] = set()
        visited_paths: set[str] = set()  # Fallback for missing inodes
        queue = [path]

        while queue:
            current = queue.pop()

            # Path-based cycle detection fallback
            normalized_current = current.lower()
            if normalized_current in visited_paths:
                LOGGER.debug("Path loop detected: %s already visited", current)
                continue
            visited_paths.add(normalized_current)

            try:
                directory = self._fs.open_dir(path=current)
            except IOError:
                continue
            for entry in directory:
                name = getattr(entry.info.name, "name", b"").decode("utf-8", "ignore")
                if name in {".", ".."}:
                    continue
                full_path = f"{current.rstrip('/')}/{name}" if current != "/" else f"/{name}"

                # Check if this is a directory and if we should descend
                if entry.info.meta and entry.info.meta.type == self._pytsk3.TSK_FS_META_TYPE_DIR:
                    # Get inode number for cycle detection
                    inode = getattr(entry.info.meta, "addr", None)
                    if inode is not None:
                        if inode in visited_inodes:
                            # Junction loop detected - yield path but don't descend
                            LOGGER.debug(
                                "Junction loop detected: %s (inode %d already visited), skipping descent",
                                full_path, inode
                            )
                            yield full_path
                            continue
                        visited_inodes.add(inode)
                    # Note: if inode is None, path-based detection in next iteration handles it
                    queue.append(full_path)
                yield full_path

    def _walk_entries(self, path: str) -> Iterator[tuple[str, Optional[Any]]]:
        """
        Walk the filesystem yielding (path, meta) for each entry.

        Uses the same cycle detection as _walk(), but exposes entry metadata
        to avoid extra file opens when collecting stats.
        """
        visited_inodes: set[int] = set()
        visited_paths: set[str] = set()
        queue = [path]

        while queue:
            current = queue.pop()
            normalized_current = current.lower()
            if normalized_current in visited_paths:
                LOGGER.debug("Path loop detected: %s already visited", current)
                continue
            visited_paths.add(normalized_current)

            try:
                directory = self._fs.open_dir(path=current)
            except IOError:
                continue

            for entry in directory:
                name = getattr(entry.info.name, "name", b"").decode("utf-8", "ignore")
                if name in {".", ".."}:
                    continue
                full_path = f"{current.rstrip('/')}/{name}" if current != "/" else f"/{name}"
                meta = entry.info.meta

                if meta and meta.type == self._pytsk3.TSK_FS_META_TYPE_DIR:
                    inode = getattr(meta, "addr", None)
                    if inode is not None:
                        if inode in visited_inodes:
                            LOGGER.debug(
                                "Junction loop detected: %s (inode %d already visited), skipping descent",
                                full_path, inode
                            )
                            yield full_path, meta
                            continue
                        visited_inodes.add(inode)
                    queue.append(full_path)

                yield full_path, meta

    def walk_directory(self, dir_path: str) -> Iterator[str]:
        """
        Walk a specific directory efficiently using direct TSK directory operations.

        Much faster than iter_paths(dir/**/*) because it doesn't involve
        pattern matching - just direct directory traversal.

        Args:
            dir_path: Path to directory to walk

        Yields:
            Normalized file paths (files only, not directories)
        """
        normalized = self._normalize(dir_path.strip("/"))
        LOGGER.debug("walk_directory: Starting walk of %s", normalized)

        # Verify directory exists first
        try:
            self._fs.open_dir(path=normalized)
        except IOError:
            LOGGER.debug("walk_directory: Directory not found: %s", dir_path)
            return

        # Use _walk_entries starting from the specific directory
        file_count = 0
        for path, meta in self._walk_entries(normalized):
            if meta and meta.type == self._pytsk3.TSK_FS_META_TYPE_REG:
                file_count += 1
                if file_count % 100 == 0:
                    LOGGER.debug("walk_directory: Found %d files so far in %s", file_count, dir_path)
                yield path.lstrip("/")

        LOGGER.debug("walk_directory: Completed walk of %s, found %d files", dir_path, file_count)

    @staticmethod
    def _normalize(path: str) -> str:
        if not path.startswith("/"):
            return f"/{path}"
        return path

    def close(self) -> None:
        """
        Close the EWF handle and release resources.

        Safe to call multiple times - subsequent calls are no-ops.
        """
        if hasattr(self, "_handle") and self._handle is not None:
            try:
                self._handle.close()
            except Exception:
                pass
            self._handle = None

    def __del__(self) -> None:  # pragma: no cover - defensive cleanup
        self.close()


class MountedFS(EvidenceFS):
    """Evidence filesystem wrapper for a locally mounted read-only path."""

    def __init__(self, mount_point: Path) -> None:
        if not mount_point.exists():
            raise FileNotFoundError(f"Mount point {mount_point} does not exist.")
        self.mount_point = mount_point
        LOGGER.info("MountedFS bound to %s", mount_point)

    def iter_paths(self, glob_pattern: str) -> Iterator[str]:
        LOGGER.debug("MountedFS iterating for pattern %s", glob_pattern)
        for path in self.mount_point.rglob("*"):
            rel = path.relative_to(self.mount_point).as_posix()
            if fnmatch.fnmatch(rel, glob_pattern):
                yield rel

    def open_for_read(self, path: str) -> BinaryIO:
        resolved = self._resolve_under_mount(path)
        if not resolved.is_file():
            raise FileNotFoundError(f"Path {path} not found under mount {self.mount_point}.")
        LOGGER.debug("Opening %s for read (MountedFS)", resolved)
        return resolved.open("rb")

    def list_users(self) -> List[str]:
        users_dir = self.mount_point / "Users"
        if not users_dir.exists():
            return []
        profiles = [
            child.name
            for child in users_dir.iterdir()
            if child.is_dir() and child.name not in {"Public", "Default", "Default User"}
        ]
        LOGGER.debug("Discovered user profiles: %s", profiles)
        return sorted(profiles)

    @property
    def source_path(self) -> Path:
        """
        Return mounted root path for forensic logging.

        Week 0 infrastructure requirement for.
        """
        return self.mount_point

    @property
    def fs_type(self) -> str:
        """
        Return filesystem type (best-effort detection).

        For mounted directories, attempts to detect from mount info or filesystem.
        Week 0 infrastructure requirement for.

        Returns:
            Filesystem type string or "mounted_dir" if not detectable
        """
        # For mounted directories, try to detect from mount info
        # This is best-effort and may return "mounted_dir"
        try:
            # Try to read filesystem type from /proc/mounts (Linux)
            import platform
            if platform.system() == "Linux":
                with open("/proc/mounts", "r") as f:
                    for line in f:
                        parts = line.split()
                        if len(parts) >= 3 and Path(parts[1]).resolve() == self.mount_point.resolve():
                            fs_type = parts[2]
                            LOGGER.debug("Detected mounted filesystem type: %s", fs_type)
                            return fs_type
        except Exception as e:
            LOGGER.debug("Failed to detect mounted filesystem type: %s", e)

        return "mounted_dir"

    def stat(self, path: str) -> EvidenceFileStat:
        """
        Get file metadata from mounted filesystem.

        Uses os.stat() to retrieve metadata. Note that crtime (birth time)
        is only available on some platforms (e.g., macOS, Windows via NTFS).
        """
        resolved = self._resolve_under_mount(path)
        if not resolved.exists():
            raise FileNotFoundError(f"Path {path} not found under mount {self.mount_point}.")
        st = os.stat(resolved)

        # crtime (birth time) only available on some platforms
        crtime: Optional[float] = None
        if hasattr(st, 'st_birthtime'):
            crtime = st.st_birthtime

        return EvidenceFileStat(
            size_bytes=st.st_size,
            mtime_epoch=st.st_mtime,
            atime_epoch=st.st_atime,
            ctime_epoch=st.st_ctime,
            crtime_epoch=crtime,
            inode=st.st_ino,
            is_file=stat_module.S_ISREG(st.st_mode),
            is_dir=stat_module.S_ISDIR(st.st_mode),
        )

    def _resolve_under_mount(self, path: str) -> Path:
        """
        Resolve a user-provided path and enforce mount root confinement.

        This prevents path traversal such as '../..' from escaping the mounted
        evidence root.
        """
        base = self.mount_point.resolve()
        resolved = (self.mount_point / path).resolve()
        try:
            resolved.relative_to(base)
        except ValueError as exc:
            raise ValueError(
                f"Path traversal attempt: {path!r} resolves outside mount {self.mount_point}"
            ) from exc
        return resolved

    def iter_all_files(self) -> Iterator[str]:
        """
        Walk mounted filesystem and yield all file paths (files only).

        Normalizes path separators to forward slashes.
        """
        for root, dirs, files in os.walk(self.mount_point):
            for name in files:
                full_path = os.path.join(root, name)
                rel_path = os.path.relpath(full_path, self.mount_point)
                # Normalize separators to forward slashes
                yield rel_path.replace(os.sep, "/")

    def walk_directory(self, dir_path: str) -> Iterator[str]:
        """
        Walk a specific directory and yield all file paths within it.

        Uses os.walk() starting from the specified directory.

        Args:
            dir_path: Path to directory to walk (relative to mount point)

        Yields:
            Normalized file paths (files only, not directories)
        """
        resolved = (self.mount_point / dir_path).resolve()
        if not resolved.exists() or not resolved.is_dir():
            LOGGER.debug("walk_directory: Directory not found: %s", dir_path)
            return

        for root, dirs, files in os.walk(resolved):
            for name in files:
                full_path = os.path.join(root, name)
                rel_path = os.path.relpath(full_path, self.mount_point)
                yield rel_path.replace(os.sep, "/")

    def open_for_stream(self, path: str, chunk_size: int = 65536) -> Iterator[bytes]:
        """
        Yield file content in chunks (memory-efficient).

        Opens file and yields in chunks for streaming hash computation.
        """
        resolved = (self.mount_point / path).resolve()
        if not resolved.is_file():
            raise FileNotFoundError(f"Path {path} not found under mount {self.mount_point}.")
        with open(resolved, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    @property
    def root(self) -> Path:
        return self.mount_point


class _PyEwfImgInfo:
    def __new__(cls, ewf_handle, pytsk3_module):  # type: ignore[override]
        class ImgInfo(pytsk3_module.Img_Info):  # type: ignore
            def __init__(self, handle):
                self._ewf_handle = handle
                super().__init__(url="", type=pytsk3_module.TSK_IMG_TYPE_EXTERNAL)

            def close(self):  # pragma: no cover - cleanup
                self._ewf_handle.close()

            def read(self, offset: int, size: int) -> bytes:
                self._ewf_handle.seek(offset)
                return self._ewf_handle.read(size)

            def get_size(self) -> int:
                return self._ewf_handle.get_media_size()

        return ImgInfo(ewf_handle)
