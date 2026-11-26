"""
File list-based multi-partition discovery for extractors.

This module provides utilities for discovering artifacts across ALL partitions
in an E01 image by querying the pre-populated file_list table instead of
walking the filesystem (which only scans the auto-selected main partition).

Features:
- SQL-based discovery (fast, seconds vs minutes)
- Multi-partition support with partition_index preservation
- Fallback to filesystem walk when file_list is empty
- Context manager for safe partition handle management

Usage:
    from extractors._shared.file_list_discovery import (
        discover_from_file_list,
        open_partition_for_extraction,
    )

    # Discover History files across all partitions
    result = discover_from_file_list(
        evidence_conn, evidence_id,
        filename_patterns=["History"],
        path_patterns=["%Chrome%", "%Google%Chrome%"],
    )

    # Extract from each partition
    for partition_idx, matches in result.matches_by_partition.items():
        with open_partition_for_extraction(ewf_paths, partition_idx) as fs:
            for match in matches:
                data = fs.open_for_read(match.file_path).read()

See Also:
    - planning/wip/multi_partition_discovery.md for design
    - src/extractors/media/filesystem_images/extractor.py for reference implementation
"""
from __future__ import annotations

import fnmatch
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set, Tuple, TYPE_CHECKING

from core.logging import get_logger

if TYPE_CHECKING:
    from core.evidence_fs import PyEwfTskFS

__all__ = [
    "FileListMatch",
    "FileListDiscoveryResult",
    "FileListQueryError",
    "discover_from_file_list",
    "open_partition_for_extraction",
    "glob_to_sql_like",
    "get_ewf_paths_from_evidence_fs",
]

# Under the application logger, so that a failed query reaches processing.log
LOGGER = get_logger("extractors._shared.file_list_discovery")

# Largest number of patterns (file name and path patterns together) in one
# query. SQLite fails with "Expression tree is too large" at a depth of 1000,
# which a list of about 990 OR-ed patterns reaches, and builds before 3.32
# accept at most 999 bound values per query. 850 stays below both and leaves
# room for the extension and partition filters. Selections of up to 850
# patterns stay one query.
MAX_PATTERNS_PER_QUERY = 850


class FileListQueryError(RuntimeError):
    """
    A file_list query failed, so it is not known which files exist.

    Raised by extractors for a discovery result whose ``error`` is set. It must
    end the run with an error; it must not be handled like "no match".
    """


@dataclass
class FileListMatch:
    """
    Single file match from file_list query.

    Attributes:
        file_path: Full path within the filesystem (e.g., "Users/John/AppData/...")
        file_name: Filename only (e.g., "History")
        partition_index: Partition index (1-based for partitioned disks, 0 for direct FS)
        inode: Inode number for direct extraction via icat (optional)
        size_bytes: File size in bytes (optional)
        extension: File extension (e.g., ".sqlite")
    """
    file_path: str
    file_name: str
    partition_index: int
    inode: Optional[int] = None
    size_bytes: Optional[int] = None
    extension: Optional[str] = None
    deleted: bool = False

    def to_dict(self) -> Dict:
        """Convert to dictionary for extractor compatibility."""
        d = {
            "file_path": self.file_path,
            "file_name": self.file_name,
            "partition_index": self.partition_index,
            "inode": self.inode,
            "size_bytes": self.size_bytes,
            "extension": self.extension,
            # Common aliases used by extractors
            "logical_path": self.file_path,
            "path": self.file_path,
        }
        if self.deleted:
            d["deleted"] = True
        return d


@dataclass
class FileListDiscoveryResult:
    """
    Results from file_list discovery, grouped by partition.

    Attributes:
        matches_by_partition: Dict mapping partition_index -> list of FileListMatch
        total_matches: Total number of matches across all partitions
        partitions_with_matches: List of partition indices that have matches
        query_info: Optional info about the query performed (for logging)
        error: Set when the file_list query failed. The result is then empty,
            but "no matches" must not be concluded from it.
        notes: Parts of the search that were left out, for the caller to put
            into its run log and manifest. The matches are still valid.
    """
    matches_by_partition: Dict[int, List[FileListMatch]] = field(default_factory=dict)
    total_matches: int = 0
    partitions_with_matches: List[int] = field(default_factory=list)
    query_info: Optional[str] = None
    error: Optional[str] = None
    notes: List[str] = field(default_factory=list)

    @property
    def is_multi_partition(self) -> bool:
        """Return True if matches span multiple partitions."""
        return len(self.partitions_with_matches) > 1

    @property
    def is_empty(self) -> bool:
        """Return True if no matches found."""
        return self.total_matches == 0

    def get_all_matches(self) -> List[FileListMatch]:
        """Return flat list of all matches across partitions."""
        all_matches = []
        for matches in self.matches_by_partition.values():
            all_matches.extend(matches)
        return all_matches

    def get_partition_summary(self) -> str:
        """Return human-readable summary of partition distribution."""
        if self.is_empty:
            return "No matches found"

        parts = []
        for part_idx in sorted(self.partitions_with_matches):
            count = len(self.matches_by_partition.get(part_idx, []))
            parts.append(f"partition {part_idx}: {count}")

        return f"{self.total_matches} matches ({', '.join(parts)})"


def glob_to_sql_like(glob_pattern: str) -> str:
    """
    Convert a glob pattern to SQL LIKE pattern.

    Handles common glob syntax:
    - * -> %
    - ? -> _
    - ** -> %
    - Escapes SQL special characters in literal parts

    Args:
        glob_pattern: Glob pattern (e.g., "Users/*/AppData/Local/Google/Chrome/*")

    Returns:
        SQL LIKE pattern (e.g., "Users/%/AppData/Local/Google/Chrome/%")

    Examples:
        >>> glob_to_sql_like("Users/*/AppData")
        'Users/%/AppData'
        >>> glob_to_sql_like("**/*.sqlite")
        '%/%.sqlite'
        >>> glob_to_sql_like("file_name.db")
        'file_name.db'
    """
    # Handle ** first (recursive match)
    pattern = glob_pattern.replace("**", "*")

    # Convert glob wildcards to SQL wildcards
    result = []
    i = 0
    while i < len(pattern):
        char = pattern[i]
        if char == '*':
            result.append('%')
        elif char == '?':
            result.append('_')
        elif char in ('%', '_'):
            # Escape SQL special characters in literal text
            result.append(f'\\{char}')
        else:
            result.append(char)
        i += 1

    return ''.join(result)


def _build_filename_clause(
    filename_patterns: List[str],
    params: List,
) -> str:
    """
    Build SQL clause for filename matching.

    Supports both exact matches and wildcard patterns.
    """
    if not filename_patterns:
        return ""

    clauses = []
    for pattern in filename_patterns:
        if '*' in pattern or '?' in pattern:
            # Wildcard pattern - use LIKE
            like_pattern = glob_to_sql_like(pattern)
            clauses.append("file_name LIKE ? ESCAPE '\\'")
            params.append(like_pattern)
        else:
            # Exact match (case-insensitive)
            clauses.append("LOWER(file_name) = LOWER(?)")
            params.append(pattern)

    return f"AND ({' OR '.join(clauses)})"


def _build_path_clause(
    path_patterns: List[str],
    params: List,
) -> str:
    """
    Build SQL clause for path matching.

    Path patterns are SQL LIKE patterns (use % for wildcards).
    """
    if not path_patterns:
        return ""

    clauses = []
    for pattern in path_patterns:
        # Path patterns are expected to be SQL LIKE format
        # Support both % (SQL style) and * (glob style)
        sql_pattern = pattern
        if '*' in pattern and '%' not in pattern:
            sql_pattern = glob_to_sql_like(pattern)

        # Ensure patterns without a leading slash or wildcard still match
        # file_list paths that may have a leading / prefix.
        # E.g. pattern "Users/*/AppData/..." becomes "%Users/%/AppData/..."
        # to match "/Users/foo/AppData/..." in file_list.
        # Trade-off: theoretically could match "/OtherPrefix/Users/..." but
        # patterns from _patterns.py are specific enough that false positives
        # are negligible, and without this the extraction is completely broken.
        if sql_pattern and not sql_pattern.startswith('%') and not sql_pattern.startswith('/'):
            sql_pattern = '%' + sql_pattern

        clauses.append("file_path LIKE ? ESCAPE '\\'")
        params.append(sql_pattern)

    return f"AND ({' OR '.join(clauses)})"


def _build_extension_clause(
    extensions: List[str],
    params: List,
) -> str:
    """Build SQL clause for extension filtering."""
    if not extensions:
        return ""

    # Normalize extensions (ensure they start with .)
    normalized = []
    for ext in extensions:
        if not ext.startswith('.'):
            ext = '.' + ext
        normalized.append(ext.lower())

    placeholders = ', '.join(['?' for _ in normalized])
    params.extend(normalized)

    return f"AND LOWER(extension) IN ({placeholders})"


def discover_from_file_list(
    evidence_conn: sqlite3.Connection,
    evidence_id: int,
    filename_patterns: Optional[List[str]] = None,
    path_patterns: Optional[List[str]] = None,
    extension_filter: Optional[List[str]] = None,
    exclude_deleted: bool = True,
    partition_filter: Optional[Set[int]] = None,
) -> FileListDiscoveryResult:
    """
    Query file_list for matching artifacts across ALL partitions.

    This function provides a fast, SQL-based alternative to filesystem walking
    that discovers files across all partitions in an E01 image.

    Args:
        evidence_conn: Evidence database connection
        evidence_id: Evidence ID for file_list records
        filename_patterns: Exact filenames or glob patterns to match
            Examples: ["History", "Cookies", "places.sqlite", "*.db"]
        path_patterns: SQL LIKE or glob patterns for path filtering
            Examples: ["%Chrome%", "%/Firefox/%", "Users/*/AppData/*"]
        extension_filter: Optional list of extensions to include
            Examples: [".sqlite", ".db", ".json"]
        exclude_deleted: If True (default), exclude deleted files
        partition_filter: If provided, only return matches from these partitions

    Returns:
        FileListDiscoveryResult with matches grouped by partition_index.
        If a query fails, the result is empty and its ``error`` is set: an
        empty result alone does not mean "no match".

    Any number of patterns is accepted. More than MAX_PATTERNS_PER_QUERY file
    name and path patterns together are spread over several queries; a row is
    returned once, and the result is then ordered by rowid.

    Examples:
        # Find all Chrome History files
        result = discover_from_file_list(
            conn, evidence_id,
            filename_patterns=["History"],
            path_patterns=["%Chrome%User Data%"],
        )

        # Find all SQLite databases in browser directories
        result = discover_from_file_list(
            conn, evidence_id,
            path_patterns=["%Chrome%", "%Firefox%", "%Edge%"],
            extension_filter=[".sqlite", ".db"],
        )
    """
    if filename_patterns is None and path_patterns is None and extension_filter is None:
        LOGGER.warning("discover_from_file_list called with no patterns - returning empty result")
        return FileListDiscoveryResult(query_info="No patterns specified")

    # SQLite limits the depth of an expression (1000 by default), and a list
    # of OR-ed patterns is one level per pattern. Many patterns are therefore
    # spread over several queries. With one chunk each, this is one query.
    filename_chunks, path_chunks = _chunk_pattern_lists(filename_patterns, path_patterns)
    query_count = len(filename_chunks) * len(path_chunks)

    query_info = f"filename={filename_patterns}, path={path_patterns}, ext={extension_filter}"

    # A row matches when it matches a file name pattern AND a path pattern, so
    # the union over all pairs of chunks is the result of the single query.
    rows_by_rowid: Dict[int, FileListMatch] = {}

    try:
        for filename_chunk in filename_chunks:
            for path_chunk in path_chunks:
                query, params = _build_discovery_query(
                    evidence_id,
                    filename_chunk,
                    path_chunk,
                    extension_filter,
                    exclude_deleted,
                    partition_filter,
                )

                LOGGER.debug("file_list discovery query: %s", query)
                LOGGER.debug("file_list discovery params: %s", params)

                for row in evidence_conn.execute(query, params):
                    rowid = row[0]
                    # A row can match patterns of more than one chunk
                    if rowid not in rows_by_rowid:
                        rows_by_rowid[rowid] = _row_to_match(row)

    except sqlite3.Error as e:
        LOGGER.error("file_list query failed: %s", e)
        return FileListDiscoveryResult(
            query_info=f"Query failed: {e}",
            error=str(e),
        )

    if query_count > 1:
        # Several queries: order by rowid, so that the result does not depend
        # on how the patterns were spread over the queries.
        ordered = [rows_by_rowid[rowid] for rowid in sorted(rows_by_rowid)]
    else:
        # One query: keep the order SQLite returned
        ordered = list(rows_by_rowid.values())

    matches_by_partition: Dict[int, List[FileListMatch]] = {}
    for match in ordered:
        matches_by_partition.setdefault(match.partition_index, []).append(match)

    # Build result
    partitions_with_matches = sorted(matches_by_partition.keys())

    result = FileListDiscoveryResult(
        matches_by_partition=matches_by_partition,
        total_matches=len(ordered),
        partitions_with_matches=partitions_with_matches,
        query_info=query_info,
    )

    LOGGER.info(
        "file_list discovery: %d matches across %d partition(s) (%d quer%s)",
        len(ordered),
        len(partitions_with_matches),
        query_count,
        "y" if query_count == 1 else "ies",
    )

    return result


def _chunk_pattern_lists(
    filename_patterns: Optional[List[str]],
    path_patterns: Optional[List[str]],
) -> Tuple[List[Optional[List[str]]], List[Optional[List[str]]]]:
    """
    Split the patterns so that no query has more than MAX_PATTERNS_PER_QUERY.

    Every file name chunk is combined with every path chunk, so a file name
    chunk and a path chunk together stay within the limit. With both kinds,
    file name patterns get at most half of it. No patterns of a kind give one
    part without a clause (``None``).
    """
    if filename_patterns and path_patterns:
        filename_size = min(len(filename_patterns), max(1, MAX_PATTERNS_PER_QUERY // 2))
    else:
        filename_size = max(1, MAX_PATTERNS_PER_QUERY)
    path_size = max(1, MAX_PATTERNS_PER_QUERY - (filename_size if filename_patterns else 0))
    return _chunk_patterns(filename_patterns, filename_size), _chunk_patterns(path_patterns, path_size)


def _chunk_patterns(patterns: Optional[List[str]], size: int) -> List[Optional[List[str]]]:
    """Split a pattern list into parts of at most ``size``; no patterns give ``[None]``."""
    if not patterns:
        return [None]
    return [patterns[start:start + size] for start in range(0, len(patterns), size)]


def _build_discovery_query(
    evidence_id: int,
    filename_patterns: Optional[List[str]],
    path_patterns: Optional[List[str]],
    extension_filter: Optional[List[str]],
    exclude_deleted: bool,
    partition_filter: Optional[Set[int]],
) -> Tuple[str, List]:
    """Build one file_list query and its parameters."""
    params: List = [evidence_id]

    query_parts = [
        "SELECT rowid, file_path, file_name, partition_index, inode, size_bytes, extension, deleted",
        "FROM file_list",
        "WHERE evidence_id = ?",
    ]

    # Add filename clause
    if filename_patterns:
        query_parts.append(_build_filename_clause(filename_patterns, params))

    # Add path clause
    if path_patterns:
        query_parts.append(_build_path_clause(path_patterns, params))

    # Add extension clause
    if extension_filter:
        query_parts.append(_build_extension_clause(extension_filter, params))

    # Exclude deleted files
    if exclude_deleted:
        query_parts.append("AND COALESCE(deleted, 0) = 0")

    # Partition filter
    if partition_filter:
        placeholders = ', '.join(['?' for _ in partition_filter])
        query_parts.append(f"AND partition_index IN ({placeholders})")
        params.extend(sorted(partition_filter))

    return '\n'.join(query_parts), params


def _row_to_match(row) -> FileListMatch:
    """Convert a row of the discovery query (after its rowid) to a FileListMatch."""
    file_path = row[1] or ""
    file_name = row[2] or ""
    partition_index = row[3]
    inode = row[4]
    size_bytes = row[5]
    extension = row[6]
    deleted_flag = bool(row[7]) if row[7] else False

    # Handle None partition_index (legacy data)
    if partition_index is None:
        partition_index = 0
    else:
        try:
            partition_index = int(partition_index)
        except (TypeError, ValueError):
            partition_index = 0

    # Parse inode - may be integer or NTFS MFT format like "3869-128-4"
    parsed_inode = None
    if inode is not None:
        try:
            parsed_inode = int(inode)
        except (TypeError, ValueError):
            # NTFS MFT format: "MFT_RECORD-ATTRIBUTE_TYPE-ATTRIBUTE_ID"
            # Extract MFT record number (first component)
            if isinstance(inode, str) and '-' in inode:
                try:
                    parsed_inode = int(inode.split('-')[0])
                except (ValueError, IndexError):
                    parsed_inode = None

    # Parse size_bytes
    parsed_size = None
    if size_bytes is not None:
        try:
            parsed_size = int(size_bytes)
        except (TypeError, ValueError):
            pass

    return FileListMatch(
        file_path=file_path,
        file_name=file_name,
        partition_index=partition_index,
        inode=parsed_inode,
        size_bytes=parsed_size,
        extension=extension,
        deleted=deleted_flag,
    )


def get_ewf_paths_from_evidence_fs(evidence_fs) -> Optional[List[Path]]:
    """
    Extract EWF paths from an evidence filesystem object.

    Args:
        evidence_fs: Evidence filesystem (PyEwfTskFS or similar)

    Returns:
        List of EWF segment paths, or None if not an EWF-backed filesystem
    """
    # Try ewf_paths attribute (PyEwfTskFS)
    ewf_paths = getattr(evidence_fs, 'ewf_paths', None)
    if ewf_paths:
        return list(ewf_paths)

    # Try source_path attribute
    source_path = getattr(evidence_fs, 'source_path', None)
    if source_path:
        source = Path(source_path)
        if source.suffix.lower() in ('.e01', '.ex01'):
            return [source]

    return None


@contextmanager
def open_partition_for_extraction(
    evidence_fs_or_paths,
    partition_index: Optional[int],
) -> Iterator:
    """
    Context manager to open a specific partition for extraction.

    Handles proper resource cleanup and logging. Use this when you need
    to extract files from a partition different from the main evidence_fs.

    Args:
        evidence_fs_or_paths: Either an existing evidence_fs object to use directly
                              (when partition_index is None), or a list of EWF paths
                              to open a specific partition.
        partition_index: Partition index to open (1-based for partitioned disks).
                         If None, uses the provided evidence_fs directly.

    Yields:
        Filesystem instance for the specified partition, or the original evidence_fs

    Raises:
        RuntimeError: If partition cannot be opened

    Example:
        # For multi-partition support:
        ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
        for partition_idx in result.partitions_with_matches:
            with open_partition_for_extraction(ewf_paths, partition_idx) as fs:
                for match in result.matches_by_partition[partition_idx]:
                    data = fs.open_for_read(match.file_path).read()

        # For single partition fallback (partition_index=None):
        with open_partition_for_extraction(evidence_fs, None) as fs:
            # fs is the original evidence_fs
    """
    # If partition_index is None, use the provided evidence_fs directly
    if partition_index is None:
        yield evidence_fs_or_paths
        return

    # Otherwise, open the specified partition from EWF paths
    from core.evidence_fs import open_ewf_partition

    ewf_paths = evidence_fs_or_paths
    LOGGER.debug("Opening partition %d from %s", partition_index, ewf_paths[0] if ewf_paths else "unknown")

    fs = None
    try:
        fs = open_ewf_partition(ewf_paths, partition_index=partition_index)
        yield fs
    finally:
        if fs is not None:
            try:
                # PyEwfTskFS may have a close method
                close_method = getattr(fs, 'close', None)
                if close_method and callable(close_method):
                    close_method()
            except Exception as e:
                LOGGER.debug("Error closing partition %d handle: %s", partition_index, e)


def check_file_list_available(
    evidence_conn: sqlite3.Connection,
    evidence_id: int,
) -> tuple[bool, int]:
    """
    Check if file_list table is populated for this evidence.

    Args:
        evidence_conn: Evidence database connection
        evidence_id: Evidence ID

    Returns:
        Tuple of (is_available, file_count)
    """
    try:
        cursor = evidence_conn.execute(
            "SELECT COUNT(*) FROM file_list WHERE evidence_id = ?",
            (evidence_id,),
        )
        count = cursor.fetchone()[0]
        return count > 0, count
    except sqlite3.Error:
        return False, 0


def get_partition_stats(
    evidence_conn: sqlite3.Connection,
    evidence_id: int,
) -> Dict[int, int]:
    """
    Get file count statistics per partition.

    Args:
        evidence_conn: Evidence database connection
        evidence_id: Evidence ID

    Returns:
        Dict mapping partition_index -> file_count
    """
    stats = {}
    try:
        cursor = evidence_conn.execute(
            """
            SELECT partition_index, COUNT(*) as file_count
            FROM file_list
            WHERE evidence_id = ?
            GROUP BY partition_index
            ORDER BY partition_index
            """,
            (evidence_id,),
        )
        for row in cursor:
            partition_idx = row[0] if row[0] is not None else 0
            stats[partition_idx] = row[1]
    except sqlite3.Error as e:
        LOGGER.error("Failed to get partition stats: %s", e)

    return stats
