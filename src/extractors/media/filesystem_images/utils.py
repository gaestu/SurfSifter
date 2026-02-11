"""
Filesystem Images Extractor Utilities

Shared utility functions for filesystem image extraction.
Decoupled from main extractor to avoid GUI dependencies in worker code.
"""

from __future__ import annotations

import hashlib
from typing import Optional


def compute_flat_rel_path(
    fs_path: str,
    filename: Optional[str],
    inode: Optional[int],
    prefix: Optional[str] = None,
) -> str:
    """
    Compute relative path for flat extraction mode.

    Format: [prefix/]inode-hash_filename or [prefix/]hash_filename
    - inode: forensic identifier (unique within partition)
    - hash: 8-char SHA256 of full path (collision avoidance)
    - filename: original name for easy identification

    This is shared by parallel and sequential extraction to ensure
    deterministic output regardless of extraction mode.

    Args:
        fs_path: Full filesystem path of the source file
        filename: Original filename (or None to extract from fs_path)
        inode: Inode number (or None if not available)
        prefix: Optional path prefix (e.g., "partition_1")

    Returns:
        Relative path for the extracted file
    """
    path_hash = hashlib.sha256(
        fs_path.encode("utf-8", "replace")
    ).hexdigest()[:8]
    name = filename or fs_path.rsplit("/", 1)[-1]
    prefix_str = f"{prefix}/" if prefix else ""

    if inode is not None:
        return f"{prefix_str}{inode}-{path_hash}_{name}"
    else:
        return f"{prefix_str}{path_hash}_{name}"
