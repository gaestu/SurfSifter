"""
Core Enumerations

Centralized enum definitions for consistent typing across the codebase.
Using StrEnum (Python 3.11+) for string-based enums that serialize naturally.
"""

from enum import StrEnum


class Browser(StrEnum):
    """Supported browser identifiers matching BROWSER_PATTERNS keys."""

    CHROME = "chrome"
    EDGE = "edge"
    FIREFOX = "firefox"
    SAFARI = "safari"
    OPERA = "opera"
    BRAVE = "brave"

    @classmethod
    def chromium_browsers(cls) -> tuple["Browser", ...]:
        """Return browsers using Chromium engine (shared DB schemas)."""
        return (cls.CHROME, cls.EDGE, cls.OPERA, cls.BRAVE)

    @classmethod
    def all_browsers(cls) -> tuple["Browser", ...]:
        """Return all supported browsers."""
        return tuple(cls)


class BrowserEngine(StrEnum):
    """Browser rendering engine types."""

    CHROMIUM = "chromium"
    GECKO = "gecko"      # Firefox
    WEBKIT = "webkit"    # Safari


class ExtractionStatus(StrEnum):
    """Status values for extraction operations."""

    OK = "ok"
    PARTIAL = "partial"  # Some records processed, some errors
    ERROR = "error"
    SKIPPED = "skipped"


class ProcessingStatus(StrEnum):
    """Status values for UI processing steps."""

    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    ERROR = "error"
    SKIPPED = "skipped"


class IngestionMode(StrEnum):
    """Modes for batch ingestion operations."""

    OVERWRITE = "overwrite"  # Clear existing, insert new
    APPEND = "append"        # Keep existing, add new
    SKIP = "skip"            # Skip if data exists


class ArtifactType(StrEnum):
    """Supported artifact types for database operations."""

    # URLs and browsing
    URL = "url"
    BROWSER_HISTORY = "browser_history"

    # Images
    IMAGE = "image"

    # Cookies and storage
    COOKIE = "cookie"
    LOCAL_STORAGE = "local_storage"
    SESSION_STORAGE = "session_storage"
    INDEXEDDB = "indexeddb"

    # Bookmarks and downloads
    BOOKMARK = "bookmark"
    BROWSER_DOWNLOAD = "browser_download"
    DOWNLOAD = "download"  # Investigator downloads

    # Sessions and tabs
    SESSION_WINDOW = "session_window"
    SESSION_TAB = "session_tab"
    CLOSED_TAB = "closed_tab"

    # Forms and credentials
    AUTOFILL = "autofill"
    CREDENTIAL = "credential"
    CREDIT_CARD = "credit_card"

    # Permissions and settings
    PERMISSION = "permission"

    # Media
    MEDIA_PLAYBACK = "media_playback"
    MEDIA_SESSION = "media_session"

    # Security
    HSTS_ENTRY = "hsts_entry"

    # Extensions
    EXTENSION = "extension"

    # System
    JUMP_LIST_ENTRY = "jump_list_entry"
    OS_INDICATOR = "os_indicator"

    # Timeline
    TIMELINE_EVENT = "timeline_event"

    # File list
    FILE_LIST = "file_list"

    # Favicons
    FAVICON = "favicon"
    TOP_SITE = "top_site"


class PermissionRiskLevel(StrEnum):
    """Risk classification for browser extension permissions."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"


class DownloadState(StrEnum):
    """Browser download states."""

    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"
    INTERRUPTED_NETWORK = "interrupted_network"
    UNKNOWN = "unknown"


class DangerType(StrEnum):
    """Browser download danger classifications."""

    NOT_DANGEROUS = "not_dangerous"
    DANGEROUS_FILE = "dangerous_file"
    DANGEROUS_URL = "dangerous_url"
    UNCOMMON_CONTENT = "uncommon_content"
    DANGEROUS_HOST = "dangerous_host"
    POTENTIALLY_UNWANTED = "potentially_unwanted"
    UNKNOWN = "unknown"


class CookieSameSite(StrEnum):
    """SameSite cookie attribute values."""

    NO_RESTRICTION = "no_restriction"
    LAX = "lax"
    STRICT = "strict"
    UNSPECIFIED = "unspecified"


class HashAlgorithm(StrEnum):
    """Supported hash algorithms."""

    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    PHASH = "phash"  # Perceptual hash for images


# =============================================================================
# Image Source Constants
# =============================================================================

# Browser artifact image sources (cache extractors, IndexedDB, Safari)
# Used for UI badges/filters and export grouping
BROWSER_IMAGE_SOURCES: frozenset[str] = frozenset({
    "cache_simple",
    "cache_blockfile",
    "cache_firefox",
    "browser_storage_indexeddb",
    "safari",
})
