"""
Firefox Session schema definitions for extraction warning support.

This module defines known JSON keys and structures for Firefox session files
to enable discovery of unknown/new fields added in newer Firefox versions.

Firefox Session File Evolution:
- Firefox < 56: Uncompressed JSON (.js files)
- Firefox 56+: Mozilla LZ4 compressed JSON (.jsonlz4, .baklz4)
- Session data structure has evolved with new features (tab groups, containers, etc.)

To add new known keys:
1. Add to the appropriate set below
2. Document the Firefox version where it was introduced if known

References:
- Firefox source: browser/components/sessionstore/
- Session format: browser/components/sessionstore/SessionStore.jsm
"""

from __future__ import annotations

from typing import Dict, Set


# =============================================================================
# Top-Level Session Keys
# =============================================================================
# Keys at the root level of sessionstore.jsonlz4

KNOWN_SESSION_KEYS: Set[str] = {
    # Core session structure
    "version",              # Session format version (array: [schema, build])
    "windows",              # Array of active window objects
    "selectedWindow",       # Index of currently focused window (1-based)

    # Closed data (forensically valuable!)
    "_closedWindows",       # Array of closed window objects with their tabs

    # Session metadata
    "session",              # Session metadata object
    "global",               # Global session state
    "cookies",              # Session cookies (when privacy.clearOnShutdown.cookies=false)

    # Firefox Sync
    "scratchpad",           # Developer tools scratchpad state

    # Tab groups (Firefox 89+, removed in Firefox 45, re-added differently)
    "tabGroups",            # Tab group definitions (legacy)

    # Browser state
    "browserState",         # Overall browser state flags
    "lastSessionState",     # State from previous session
}


# =============================================================================
# Window-Level Keys
# =============================================================================
# Keys within each window object in the "windows" array

KNOWN_WINDOW_KEYS: Set[str] = {
    # Tab management
    "tabs",                 # Array of tab objects
    "_closedTabs",          # Recently closed tabs in this window
    "selected",             # Index of selected tab (1-based)

    # Window geometry
    "width",                # Window width in pixels
    "height",               # Window height in pixels
    "screenX",              # Window X position
    "screenY",              # Window Y position
    "sizemode",             # "normal", "maximized", "minimized", "fullscreen"

    # Window state
    "hidden",               # Whether window is hidden
    "title",                # Window title
    "busy",                 # Whether window is loading

    # Sidebars and panels
    "sidebar",              # Sidebar state

    # Container tabs (Firefox 50+)
    "userContextId",        # Default container for new tabs

    # Workspaces/spaces (newer Firefox)
    "workspaces",           # Workspace definitions

    # Tab groups (Firefox 89+ Proton redesign)
    "groups",               # Tab group definitions

    # Private browsing indicator
    "isPrivate",            # Whether this is a private window

    # Extension state
    "extData",              # Extension-stored window data

    # Z-order
    "zIndex",               # Window stacking order

    # Closed timestamp (for _closedWindows)
    "closedAt",             # When window was closed (ms since epoch)
    "closedId",             # Unique ID for closed window
}


# =============================================================================
# Tab-Level Keys
# =============================================================================
# Keys within each tab object

KNOWN_TAB_KEYS: Set[str] = {
    # Navigation state
    "entries",              # Array of history entries (back/forward list)
    "index",                # Current entry index (1-based)

    # Tab metadata
    "pinned",               # Whether tab is pinned
    "hidden",               # Whether tab is hidden
    "muted",                # Whether tab audio is muted
    "mutedReason",          # Why tab is muted

    # Timestamps (forensically critical!)
    "lastAccessed",         # Last access time (ms since epoch)
    "createdAt",            # When tab was created (ms since epoch, newer Firefox)

    # Tab relationships
    "parent",               # ID of parent tab (for tree-style tabs)
    "openerTabId",          # ID of tab that opened this one

    # User agent override
    "userTypedValue",       # Text user typed in URL bar before navigation
    "userTypedClear",       # Counter for clearing typed value

    # Container tabs (Firefox 50+)
    "userContextId",        # Container ID (0 = default, 1+ = container)

    # Tab groups (various implementations)
    "groupId",              # Tab group ID
    "group",                # Group name/identifier
    "extData",              # Extension-stored tab data (may contain group info)

    # Scroll/viewport state
    "scroll",               # Scroll position
    "zoom",                 # Zoom level

    # Image/media
    "image",                # Tab icon URL
    "iconLoadingPrincipal", # Security principal for icon loading

    # Form data (forensically valuable!)
    "formdata",             # Form field values entered by user

    # Search engine keywords
    "keyword",              # Search keyword used

    # Session store internals
    "attributes",           # DOM attributes
    "storage",              # Session storage data
    "disallow",             # What's disallowed (e.g., "images")
    "searchMode",           # Search mode state

    # Reader mode
    "readerMode",           # Reader mode state

    # Media state
    "mediaBlocked",         # Whether media autoplay is blocked

    # Closed tab info (for _closedTabs)
    "closedAt",             # When tab was closed (ms since epoch)
    "closedId",             # Unique ID for closed tab
    "state",                # Full tab state (within _closedTabs entries)
    "pos",                  # Original position in tab bar
    "title",                # Tab title at close time

    # Request blocking
    "requestBlockingData",  # Request blocking state

    # Successor tab
    "successorTabId",       # Tab to activate when this one closes
}


# =============================================================================
# History Entry Keys
# =============================================================================
# Keys within each entry in a tab's "entries" array

KNOWN_ENTRY_KEYS: Set[str] = {
    # Core navigation
    "url",                  # Page URL
    "title",                # Page title
    "subframe",             # Whether this is a subframe entry

    # Timestamps (may not always be present)
    "lastAccessed",         # When this entry was last viewed
    "lastModified",         # When entry was last modified

    # Security
    "triggeringPrincipal_base64",  # Security principal that triggered navigation
    "csp",                  # Content Security Policy
    "principalToInherit_base64",   # Principal to inherit
    "resultPrincipalURI",   # Result principal URI

    # Children (frames/iframes)
    "children",             # Array of child frame entries

    # Form data (forensically valuable!)
    "formdata",             # Form field values for this page
    "ID",                   # Form element IDs
    "xpath",                # XPath to form elements
    "innerHTML",            # Form content

    # Scroll position
    "scroll",               # Scroll position for this entry
    "scrollX",              # Horizontal scroll
    "scrollY",              # Vertical scroll

    # Document state
    "docIdentifier",        # Document identifier
    "docshellID",           # Docshell ID
    "structuredCloneState", # Structured clone state
    "structuredCloneVersion",  # Structured clone version

    # Cache control
    "cacheKey",             # Cache key
    "loadReplace",          # Whether to replace in history
    "persist",              # Whether to persist

    # Post data (forensically valuable!)
    "postdata_b64",         # POST data in base64

    # Referrer
    "referrer",             # Referrer URL
    "referrerInfo",         # Detailed referrer info

    # Original URI
    "originalURI",          # Original URI before redirects

    # Content type
    "contentType",          # MIME type

    # SRCDoc
    "srcdocData",           # srcdoc content for iframes
    "baseURI",              # Base URI

    # State object (history.pushState)
    "stateData",            # State data from history API

    # Partition key
    "partitionKey",         # Storage partition key (Firefox 91+)

    # Has user interaction
    "hasUserInteraction",   # Whether user interacted with page

    # Sharing state
    "sharingState",         # Sharing state
}


# =============================================================================
# Form Data Keys
# =============================================================================
# Keys within formdata objects

KNOWN_FORMDATA_KEYS: Set[str] = {
    "id",                   # Form element IDs -> values
    "xpath",                # XPath expressions -> values
    "#ifname",              # Frame name for nested forms
    "innerHTML",            # Rich text editor content
    "url",                  # Form submission URL
}


# =============================================================================
# Session Metadata Keys
# =============================================================================
# Keys within the "session" object

KNOWN_SESSION_METADATA_KEYS: Set[str] = {
    "lastUpdate",           # Last update timestamp
    "startTime",            # Session start time
    "recentCrashes",        # Recent crash count
    "state",                # Session state (e.g., "running", "stopped")
}


# =============================================================================
# Filter Patterns for Unknown Key Discovery
# =============================================================================
# Patterns to filter which unknown keys are worth reporting
# (Avoid reporting known Firefox-internal or extension-generated keys)

IGNORED_KEY_PATTERNS: Set[str] = {
    # Firefox internals that change frequently
    "__",                   # Double underscore internal keys
    "moz_",                 # Mozilla internal prefix
    "_moz",                 # Mozilla internal suffix
}


# =============================================================================
# Session File Types
# =============================================================================
# Classification of session file types and their forensic significance

SESSION_FILE_TYPES: Dict[str, Dict[str, str]] = {
    "sessionstore_jsonlz4": {
        "description": "Current active session",
        "forensic_value": "high",
        "notes": "Written on graceful shutdown, may be stale if crash",
    },
    "recovery_jsonlz4": {
        "description": "Auto-saved recovery point",
        "forensic_value": "high",
        "notes": "Written every ~15 seconds, most recent state",
    },
    "recovery_baklz4": {
        "description": "Backup of recovery before last write",
        "forensic_value": "medium",
        "notes": "Previous recovery state, useful for comparison",
    },
    "previous_jsonlz4": {
        "description": "Session from previous browser run",
        "forensic_value": "high",
        "notes": "Full session from before last restart",
    },
    "upgrade_jsonlz4": {
        "description": "Pre-upgrade session snapshot",
        "forensic_value": "critical",
        "notes": "Preserved before Firefox updates, historically valuable",
    },
    "sessionstore_js": {
        "description": "Legacy uncompressed session (Firefox < 56)",
        "forensic_value": "high",
        "notes": "Plain JSON, older Firefox versions",
    },
    "recovery_js": {
        "description": "Legacy recovery point",
        "forensic_value": "high",
        "notes": "Plain JSON recovery file",
    },
    "previous_js": {
        "description": "Legacy previous session",
        "forensic_value": "high",
        "notes": "Plain JSON previous session",
    },
}
