"""
Chromium Bookmarks JSON schema definitions.

This module defines the known JSON keys, bookmark types, and root folders
used across Chromium browsers for bookmark storage. Any keys or values not
in these sets will be flagged by the schema warning system for investigator
review.

Chromium Bookmarks Format:
- JSON file at: {profile}/Bookmarks
- Backup at: {profile}/Bookmarks.bak
- Structure: { "checksum": "...", "roots": { "bookmark_bar": {...}, ... }, "version": 1 }

Schema Evolution:
- The bookmarks JSON format has been stable since early Chrome versions
- Edge/Brave/Opera use identical format
- Main changes are additional metadata fields (meta_info, sync data)

Initial schema definitions for warning support
"""

from __future__ import annotations

from typing import Dict, Set

# =============================================================================
# Root-Level JSON Keys
# =============================================================================
# Keys expected at the top level of the Bookmarks JSON file.

KNOWN_ROOT_KEYS: Set[str] = {
    "checksum",      # MD5 hash of roots content
    "roots",         # Contains bookmark folders (bookmark_bar, other, synced)
    "version",       # Schema version (typically 1)
    "sync_metadata", # Sync-related metadata (Chromium 80+)
}

# =============================================================================
# Root Folder Names
# =============================================================================
# Standard root folders inside "roots" object.

KNOWN_ROOT_FOLDERS: Dict[str, str] = {
    "bookmark_bar": "Bookmarks Bar",    # Main toolbar bookmarks
    "other": "Other Bookmarks",          # Other bookmarks (not on bar)
    "synced": "Mobile Bookmarks",        # Synced from mobile devices
    "account": "Account Bookmarks",      # Account-level bookmarks (Chromium 120+)
}

# For discovery - just the key names
KNOWN_ROOT_FOLDER_KEYS: Set[str] = set(KNOWN_ROOT_FOLDERS.keys())

# =============================================================================
# Bookmark Node Keys
# =============================================================================
# Keys expected in individual bookmark/folder nodes.

KNOWN_BOOKMARK_NODE_KEYS: Set[str] = {
    # Core identification
    "id",            # Unique ID within file (string number)
    "guid",          # GUID for sync
    "name",          # Display name
    "type",          # "url" or "folder"

    # URL bookmarks only
    "url",           # The bookmarked URL

    # Timestamps (WebKit format as string)
    "date_added",    # When bookmark was created
    "date_modified", # When folder contents changed (folders only)
    "date_last_used", # Last time bookmark was opened (Chromium 118+)

    # Folder structure
    "children",      # Array of child nodes (folders only)

    # Metadata
    "meta_info",     # Additional metadata dict (visit count, last visited, etc.)
    "show_icon",     # Whether to show favicon
    "source",        # Where bookmark came from (import, user, etc.)

    # Sync-related
    "sync_transaction_version", # Sync version tracking
    "unsynced_meta_info",       # Pending sync metadata
}

# =============================================================================
# Bookmark Types
# =============================================================================
# Valid values for the "type" field in bookmark nodes.

KNOWN_BOOKMARK_TYPES: Set[str] = {
    "url",      # A URL bookmark
    "folder",   # A bookmark folder containing children
}

# =============================================================================
# Meta Info Keys
# =============================================================================
# Keys that may appear in the "meta_info" object within bookmark nodes.
# We report these as "info" level since they're metadata we're aware of
# but choose not to extract in detail.

KNOWN_META_INFO_KEYS: Set[str] = {
    "last_visited_desktop",  # Last visit timestamp (WebKit format)
    "power_bookmark_meta",   # Power bookmark data (Chromium 100+)
    "shopping_specifics",    # Shopping-related data
    "partner_bookmark_id",   # Partner/affiliate tracking
}

# =============================================================================
# Discovery Patterns
# =============================================================================
# Patterns for filtering unknown keys during discovery.
# Keys matching these patterns are likely bookmark-related and worth flagging.

BOOKMARK_KEY_PATTERNS: list[str] = [
    "bookmark",
    "sync",
    "meta",
    "date",
    "time",
    "url",
    "folder",
    "icon",
]
