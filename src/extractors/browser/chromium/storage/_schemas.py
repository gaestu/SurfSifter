"""
Known schemas and patterns for Chromium Browser Storage extraction warnings.

This module defines known LevelDB key prefixes, record types, and patterns
for detecting unknown schemas during extraction. Used with ExtractionWarningCollector
to report new/unknown data formats that may contain forensic value.

Initial implementation with schema warnings
"""
from __future__ import annotations

from typing import Dict, Set

# =============================================================================
# Local Storage - Known Key Prefixes
# =============================================================================

# Local Storage keys start with '_' followed by origin
# Format: _<origin>\x00<key>
KNOWN_LOCAL_STORAGE_PREFIXES: Set[str] = {
    "_",  # Standard local storage prefix
    "META:",  # LevelDB metadata
    "VERSION",  # Database version marker
}

# Known Local Storage internal keys (not user data)
KNOWN_LOCAL_STORAGE_INTERNAL_KEYS: Set[str] = {
    "META:global-metadata",
    "META:next-map-id",
    "VERSION",
}

# =============================================================================
# Session Storage - Known Key Prefixes
# =============================================================================

# Session Storage uses different format than Local Storage
# Key format varies by Chrome version
KNOWN_SESSION_STORAGE_PREFIXES: Set[str] = {
    "namespace-",  # Namespace prefix (older format)
    "map-",  # Map prefix
    "next-map-id",  # Internal counter
    "session-",  # Session prefix (newer format)
    "META:",  # LevelDB metadata
    "VERSION",  # Database version
}

# =============================================================================
# IndexedDB - Known Object Store Patterns
# =============================================================================

# Common IndexedDB object store names across web applications
# Not exhaustive - used to identify potentially interesting stores
KNOWN_INDEXEDDB_OBJECT_STORES: Set[str] = {
    # Browser internals
    "database",
    "meta",
    "indexes",
    "object_data",
    # Common web app patterns
    "messages",
    "conversations",
    "contacts",
    "files",
    "media",
    "cache",
    "offline",
    "sync",
    "keyval",
    "keyvaluepairs",
    "logs",
    "events",
    "notifications",
    "settings",
    "preferences",
}

# IndexedDB origin patterns to flag as potentially interesting
INTERESTING_INDEXEDDB_ORIGINS: Set[str] = {
    "whatsapp",
    "telegram",
    "signal",
    "messenger",
    "discord",
    "slack",
    "teams",
    "gmail",
    "outlook",
    "protonmail",
    "drive",
    "dropbox",
    "onedrive",
    "photos",
}

# =============================================================================
# LevelDB Record Types and State Values
# =============================================================================

# Known LevelDB record states (from ccl_chromium_reader)
KNOWN_LEVELDB_STATES: Dict[int, str] = {
    0: "live",
    1: "deleted",
}

# Value type classifications
KNOWN_VALUE_TYPES: Set[str] = {
    "string",
    "json",
    "number",
    "boolean",
    "empty",
    "null",
    "blob",
    "array",
    "object",
    "unknown",
}

# =============================================================================
# Storage Type Identifiers
# =============================================================================

STORAGE_TYPE_LOCAL = "local_storage"
STORAGE_TYPE_SESSION = "session_storage"
STORAGE_TYPE_INDEXEDDB = "indexeddb"

ALL_STORAGE_TYPES: Set[str] = {
    STORAGE_TYPE_LOCAL,
    STORAGE_TYPE_SESSION,
    STORAGE_TYPE_INDEXEDDB,
}

# =============================================================================
# Artifact Type for Warnings
# =============================================================================

ARTIFACT_TYPE_LOCAL_STORAGE = "local_storage"
ARTIFACT_TYPE_SESSION_STORAGE = "session_storage"
ARTIFACT_TYPE_INDEXEDDB = "indexeddb"
ARTIFACT_TYPE_INDEXEDDB_BLOB = "indexeddb_blob"


# =============================================================================
# Helper Functions
# =============================================================================

def is_known_local_storage_prefix(key: bytes) -> bool:
    """Check if a LevelDB key has a known Local Storage prefix."""
    if not key:
        return False

    try:
        key_str = key.decode('utf-8', errors='replace')
        return any(key_str.startswith(prefix) for prefix in KNOWN_LOCAL_STORAGE_PREFIXES)
    except Exception:
        return False


def is_internal_local_storage_key(key: str) -> bool:
    """Check if a key is an internal/metadata key (not user data)."""
    return key in KNOWN_LOCAL_STORAGE_INTERNAL_KEYS


def is_known_session_storage_prefix(key: bytes) -> bool:
    """Check if a LevelDB key has a known Session Storage prefix."""
    if not key:
        return False

    try:
        key_str = key.decode('utf-8', errors='replace')
        return any(key_str.startswith(prefix) for prefix in KNOWN_SESSION_STORAGE_PREFIXES)
    except Exception:
        return False


def is_interesting_indexeddb_origin(origin: str) -> bool:
    """Check if an IndexedDB origin is potentially interesting for forensics."""
    if not origin:
        return False

    origin_lower = origin.lower()
    return any(pattern in origin_lower for pattern in INTERESTING_INDEXEDDB_ORIGINS)


def extract_unknown_prefix(key: bytes, known_prefixes: Set[str]) -> str | None:
    """
    Extract the prefix from a key that doesn't match known prefixes.

    Returns the first 20 chars of the key as the "unknown prefix" for reporting.
    """
    if not key:
        return None

    try:
        key_str = key.decode('utf-8', errors='replace')

        # Check if it matches any known prefix
        for prefix in known_prefixes:
            if key_str.startswith(prefix):
                return None

        # Return first 20 chars as unknown prefix
        return key_str[:20] if len(key_str) > 20 else key_str
    except Exception:
        # Return hex for binary keys
        return key[:20].hex()
