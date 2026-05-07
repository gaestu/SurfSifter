"""
Matching module - Reference list management and pattern matching.

This module provides:
- ReferenceListManager: Manage reference lists (hashlists, filelists, urllists)
- ReferenceListMatcher: Match file_list entries against reference lists
- URLMatcher: Match URLs against URL reference lists

Usage:
    from core.matching import ReferenceListManager, ReferenceListMatcher, URLMatcher
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
]
