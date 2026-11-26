"""Helpers for tests of file list discovery and of extraction from several partitions.

Directories stand for the partitions of an EWF image: ``PartitionFS`` is a
``MountedFS`` that reports a partition index and EWF paths, and
``patch_open_ewf_partition`` replaces ``core.evidence_fs.open_ewf_partition``
so that it returns the directory of the requested partition.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Optional
from unittest.mock import Mock

from core.evidence_fs import MountedFS

EWF_PATHS = [Path("/evidence/image.E01")]


class PartitionFS(MountedFS):
    """A directory that stands for one partition of an EWF image.

    Like ``PyEwfTskFS`` it accepts the absolute paths that ``fls`` writes to
    the file list. A subclass with ``strip_leading_slash = False`` behaves like
    a plain ``MountedFS``, which rejects them.
    """

    strip_leading_slash = True

    def __init__(self, root: Path, partition_index: int, ewf_paths=None) -> None:
        super().__init__(root)
        self._partition_index = partition_index
        self.ewf_paths = ewf_paths
        self.scanned_patterns: list[str] = []
        self.read_paths: list[str] = []
        self.closed = False

    @property
    def partition_index(self) -> int:
        return self._partition_index

    @property
    def fs_type(self) -> str:
        return f"fs-of-partition-{self._partition_index}"

    def iter_paths(self, glob_pattern: str):
        self.scanned_patterns.append(glob_pattern)
        return super().iter_paths(glob_pattern)

    def open_for_read(self, path: str):
        self.read_paths.append(path)
        return super().open_for_read(path.lstrip("/") if self.strip_leading_slash else path)

    def close(self) -> None:
        self.closed = True


def make_partition_roots(tmp_path: Path, count: int = 2) -> Dict[int, Path]:
    """Create the directories of partitions 1..count."""
    roots = {}
    for index in range(1, count + 1):
        root = tmp_path / f"partition{index}"
        root.mkdir()
        roots[index] = root
    return roots


def patch_open_ewf_partition(monkeypatch, roots: Dict[int, Path], fs_class=PartitionFS) -> Dict[int, PartitionFS]:
    """Replace open_ewf_partition; the returned dict maps partition index -> opened filesystem."""
    opened: Dict[int, PartitionFS] = {}

    def fake_open_ewf_partition(ewf_paths, partition_index=-1):
        fs = fs_class(roots[partition_index], partition_index, ewf_paths)
        opened[partition_index] = fs
        return fs

    monkeypatch.setattr("core.evidence_fs.open_ewf_partition", fake_open_ewf_partition)
    return opened


def write_file(root: Path, rel_path: str, content: bytes) -> None:
    """Create a file below a partition directory."""
    target = root / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)


def add_to_file_list(
    conn,
    evidence_id: int,
    path: str,
    partition_index: int,
    inode: Optional[str] = None,
) -> None:
    """Add one row to the file list of a real evidence database."""
    file_name = path.rsplit("/", 1)[-1]
    extension = "." + file_name.rsplit(".", 1)[-1] if "." in file_name else None
    conn.execute(
        """
        INSERT INTO file_list
        (evidence_id, file_path, file_name, extension, import_timestamp, partition_index, inode)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (evidence_id, path, file_name, extension, "2026-01-01T00:00:00+00:00", partition_index, inode),
    )
    conn.commit()


def mock_callbacks() -> Mock:
    """Extractor callbacks that never cancel."""
    callbacks = Mock()
    callbacks.is_cancelled = Mock(return_value=False)
    return callbacks


class QueryRecorder:
    """Connection stand-in that records the queries and can make the n-th one fail."""

    def __init__(self, conn, fail_on_query: Optional[int] = None, error: str = "database is locked"):
        self._conn = conn
        self._fail_on_query = fail_on_query
        self._error = error
        self.queries: list[str] = []

    def execute(self, sql, params=()):
        self.queries.append(sql)
        if self._fail_on_query == len(self.queries):
            raise sqlite3.OperationalError(self._error)
        return self._conn.execute(sql, params)
