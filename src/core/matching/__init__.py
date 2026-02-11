"""
Matching module - Reference list management and pattern matching.

This module provides:
- ReferenceListManager: Manage reference lists (hashlists, filelists, urllists)
- ReferenceListMatcher: Match file_list entries against reference lists
- URLMatcher: Match URLs against URL reference lists
- Hash database functions for SQLite-based hash lookup

Usage:
    from core.matching import ReferenceListManager, ReferenceListMatcher, URLMatcher
    from core.matching import import_hash_list, lookup_hash, rebuild_hash_db
"""
from __future__ import annotations

# Manager for reference list files
from .manager import (
    ReferenceListManager,
    ConflictPolicy,
    ImportResult,
    install_predefined_lists,
    MAX_HASHLIST_SIZE,
)

# Matchers for database records
from .file_matcher import ReferenceListMatcher
from .url_matcher import URLMatcher

# Hash database functions
from .hash_db import (
    init_hash_db,
    import_hash_list,
    rebuild_hash_db,
    lookup_hash,
    list_hash_lists,
    compute_file_hash,
    parse_hash_line,
    HASH_DB_SCHEMA,
    MD5_PATTERN,
    SHA256_PATTERN,
)

__all__ = [
    # Manager
    "ReferenceListManager",
    "ConflictPolicy",
    "ImportResult",
    "install_predefined_lists",
    "MAX_HASHLIST_SIZE",
    # Matchers
    "ReferenceListMatcher",
    "URLMatcher",
    # Hash DB
    "init_hash_db",
    "import_hash_list",
    "rebuild_hash_db",
    "lookup_hash",
    "list_hash_lists",
    "compute_file_hash",
    "parse_hash_line",
    "HASH_DB_SCHEMA",
    "MD5_PATTERN",
    "SHA256_PATTERN",
]
