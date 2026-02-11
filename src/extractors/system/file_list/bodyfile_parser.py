"""
Bodyfile parser for SleuthKit fls output.

Handles the mactime/bodyfile format:
MD5|path|inode|mode|UID|GID|size|atime|mtime|ctime|crtime

Edge cases handled:
- Pipe characters in paths (via rsplit)
- Deleted file marker (*) prefix
- Deleted file suffix (deleted), (deleted-realloc)
- Empty/zero MD5 placeholder
- Unicode paths
- Malformed lines (graceful skip)
- NTFS metadata attributes ($FILE_NAME, $DATA, etc.) filtered out
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Iterator, Optional

__all__ = ["BodyfileParser", "BodyfileEntry"]

logger = logging.getLogger(__name__)


@dataclass
class BodyfileEntry:
    """
    Parsed bodyfile entry.

    Attributes:
        file_path: Full path to file (may include mount point prefix)
        file_name: Base filename extracted from path
        extension: File extension (lowercase with leading dot, e.g., '.txt') or None
        size_bytes: File size in bytes
        created_ts: Creation timestamp (ISO 8601 UTC) or None
        modified_ts: Modification timestamp (ISO 8601 UTC) or None
        accessed_ts: Access timestamp (ISO 8601 UTC) or None
        md5_hash: MD5 hash if computed by fls, else None
        inode: Inode/MFT entry string (e.g., '12345-128-1')
        deleted: True if file was marked as deleted (*prefix)
        partition_index: Partition index this entry belongs to (set by parser)
    """
    file_path: str
    file_name: str
    extension: Optional[str]
    size_bytes: int
    created_ts: Optional[str]  # ISO 8601
    modified_ts: Optional[str]  # ISO 8601
    accessed_ts: Optional[str]  # ISO 8601
    md5_hash: Optional[str]
    inode: str
    deleted: bool
    partition_index: int  # Set by caller


# Regex to match NTFS metadata attribute suffixes that should be filtered
# These are internal NTFS attributes, not actual user files:
# - ($FILE_NAME) - filename attribute (duplicate entry for 8.3 names)
# - ($DATA) - default data stream marker (redundant)
# - ($STANDARD_INFORMATION) - standard info attribute
# - ($I30) - directory index attribute
# - :$DATA - alternate data stream marker
# - :Zone.Identifier:$DATA - common ADS from downloads
# NOTE: We do NOT filter standalone files like $MFT, $Bitmap etc. as they
# have forensic value and are real system files, not attribute suffixes.
NTFS_METADATA_PATTERN = re.compile(
    r'\s*\(\$[A-Z0-9_]+\)\s*(?:\(deleted(?:-realloc)?\))?$|:\$[A-Z0-9_]+$',
    re.IGNORECASE
)

# Regex to match deleted file markers in fls output
# fls can output deleted status in two ways:
# 1. * prefix on the path (standard bodyfile format)
# 2. (deleted) or (deleted-realloc) suffix (fls-specific extension)
DELETED_SUFFIX_PATTERN = re.compile(
    r'\s*\(deleted(?:-realloc)?\)$',
    re.IGNORECASE
)


class BodyfileParser:
    """
    Parser for SleuthKit bodyfile format.

    The bodyfile format (used by fls -m) has 11 pipe-delimited fields:
    MD5|path|inode|mode|UID|GID|size|atime|mtime|ctime|crtime

    This parser handles edge cases:
    - Paths containing pipe characters (uses rsplit from right)
    - Deleted file markers (*) stripped from paths
    - Zero/empty MD5 values normalized to None
    - Invalid timestamps normalized to None
    - Malformed lines logged and skipped
    - NTFS metadata attributes ($FILE_NAME, $DATA, etc.) filtered out
    - Directory entries skipped by default (mode starts with 'd')

    Example:
        >>> parser = BodyfileParser(partition_index=1)
        >>> for entry in parser.parse_lines(lines):
        ...     print(entry.file_path, entry.extension)
    """

    def __init__(
        self,
        partition_index: int = 0,
        skip_ntfs_metadata: bool = True,
        skip_directories: bool = True,
    ):
        """
        Initialize parser.

        Args:
            partition_index: Partition index to tag all entries with
            skip_ntfs_metadata: If True, skip NTFS metadata entries like ($FILE_NAME)
            skip_directories: If True, skip directory entries (mode starts with 'd')
        """
        self.partition_index = partition_index
        self.skip_ntfs_metadata = skip_ntfs_metadata
        self.skip_directories = skip_directories
        self._line_count = 0
        self._error_count = 0
        self._parsed_count = 0
        self._skipped_metadata = 0
        self._skipped_directories = 0

    def parse_lines(self, lines: Iterator[str]) -> Iterator[BodyfileEntry]:
        """
        Parse bodyfile lines, yielding entries.

        Args:
            lines: Iterator of bodyfile lines (from file or subprocess)

        Yields:
            BodyfileEntry for each valid line
        """
        for line in lines:
            self._line_count += 1
            line = line.strip()
            if not line:
                continue

            entry = self._parse_line(line)
            if entry:
                self._parsed_count += 1
                yield entry

    def _parse_line(self, line: str) -> Optional[BodyfileEntry]:
        """
        Parse single bodyfile line with robust edge case handling.

        Bodyfile format: MD5|path|inode|mode|UID|GID|size|atime|mtime|ctime|crtime
        Path can contain | characters, so we parse carefully.

        Args:
            line: Single bodyfile line

        Returns:
            BodyfileEntry if valid, None if malformed or NTFS metadata
        """
        # Split off MD5 (first field)
        first_pipe = line.find("|")
        if first_pipe == -1:
            self._log_error("No pipe delimiter found")
            return None

        md5 = line[:first_pipe]
        rest = line[first_pipe + 1:]

        # Split remaining 9 fields from the RIGHT (path may contain |)
        right_parts = rest.rsplit("|", 9)
        if len(right_parts) < 10:
            self._log_error(f"Insufficient fields: {len(right_parts) + 1} (expected 11)")
            return None

        # right_parts[0] = path (may contain |)
        # right_parts[1:] = inode, mode, uid, gid, size, atime, mtime, ctime, crtime
        raw_path = right_parts[0]

        # Filter out NTFS metadata entries before further processing
        # These entries like "path ($FILE_NAME)" are not real files
        if self.skip_ntfs_metadata and NTFS_METADATA_PATTERN.search(raw_path):
            self._skipped_metadata += 1
            return None

        inode = right_parts[1]
        mode_str = right_parts[2]
        # uid, gid = right_parts[3:5]  # Not currently used
        size_str = right_parts[5]
        atime_str = right_parts[6]
        mtime_str = right_parts[7]
        # ctime = right_parts[8]  # Metadata change time (not creation)
        crtime_str = right_parts[9]

        if self.skip_directories and mode_str and mode_str.lower().startswith("d"):
            self._skipped_directories += 1
            return None

        # Handle deleted file markers:
        # 1. * prefix (standard bodyfile format)
        # 2. (deleted) or (deleted-realloc) suffix (fls extension)
        deleted = raw_path.startswith("*")
        file_path = raw_path[1:] if deleted else raw_path

        # Check for (deleted) or (deleted-realloc) suffix and strip it
        deleted_suffix_match = DELETED_SUFFIX_PATTERN.search(file_path)
        if deleted_suffix_match:
            deleted = True
            file_path = file_path[:deleted_suffix_match.start()]

        # Extract filename and extension
        file_name, extension = self._extract_name_and_extension(file_path)

        # Parse size
        try:
            size_bytes = int(size_str) if size_str else 0
        except ValueError:
            size_bytes = 0

        return BodyfileEntry(
            file_path=file_path,
            file_name=file_name,
            extension=extension,
            size_bytes=size_bytes,
            created_ts=self._epoch_to_iso(crtime_str),
            modified_ts=self._epoch_to_iso(mtime_str),
            accessed_ts=self._epoch_to_iso(atime_str),
            md5_hash=md5 if md5 and md5 != "0" else None,
            inode=inode,
            deleted=deleted,
            partition_index=self.partition_index,
        )

    def _extract_name_and_extension(self, file_path: str) -> tuple[str, Optional[str]]:
        """
        Extract filename and extension from path.

        Args:
            file_path: Full file path

        Returns:
            Tuple of (filename, extension or None)
        """
        try:
            path_obj = PurePosixPath(file_path)
            file_name = path_obj.name
            # Handle extension - suffix returns empty string for no extension
            extension = path_obj.suffix.lower() if path_obj.suffix else None
        except Exception:
            # Fallback for weird paths
            file_name = file_path.rsplit("/", 1)[-1] if "/" in file_path else file_path
            extension = None
            if "." in file_name:
                # Get last extension only (handles file.backup.jpg -> .jpg)
                ext = file_name.rsplit(".", 1)[-1].lower()
                extension = f".{ext}"

        return file_name, extension

    def _epoch_to_iso(self, epoch_str: str) -> Optional[str]:
        """
        Convert Unix epoch string to ISO 8601 UTC.

        Args:
            epoch_str: Unix epoch as string (seconds since 1970-01-01)

        Returns:
            ISO 8601 formatted string or None if invalid
        """
        if not epoch_str or epoch_str == "0":
            return None
        try:
            epoch = int(epoch_str)
            if epoch <= 0:
                return None
            # Handle overflow for very large timestamps
            if epoch > 32503680000:  # Year 3000
                return None
            dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, OSError, OverflowError):
            return None

    def _log_error(self, message: str) -> None:
        """Log parsing error with line context (throttled after 10 errors)."""
        self._error_count += 1
        if self._error_count <= 10:
            logger.warning(f"Bodyfile line {self._line_count}: {message}")
        elif self._error_count == 11:
            logger.warning("Suppressing further bodyfile parsing warnings...")

    @property
    def stats(self) -> dict:
        """
        Return parsing statistics.

        Returns:
            Dictionary with lines_processed, parsed_count, errors, skipped_metadata,
            and skipped_directories
        """
        return {
            "lines_processed": self._line_count,
            "parsed_count": self._parsed_count,
            "errors": self._error_count,
            "skipped_metadata": self._skipped_metadata,
            "skipped_directories": self._skipped_directories,
        }

    def reset_stats(self) -> None:
        """Reset parsing statistics (for reuse of parser instance)."""
        self._line_count = 0
        self._parsed_count = 0
        self._error_count = 0
        self._skipped_metadata = 0
        self._skipped_directories = 0
