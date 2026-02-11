"""
Chromium Media History schema definitions for schema warning support.

This module defines known tables, columns, and enum values for the Chromium
Media History database. Used by the extractor to detect unknown schemas that
may contain forensically valuable data we're not capturing.

Schema Evolution:
- Chrome 80+: Initial Media History database with playback/playbackSession
- Chrome 90+: Added playbackSession table
- Chrome 100+: Added mediaImage table for album art
- Future: Likely to add more metadata tables

References:
- Chromium source: chrome/browser/media/history/media_history_store.h
- Chromium source: chrome/browser/media/history/media_history_store.cc

Initial implementation
"""

from __future__ import annotations

from typing import Set, List


# =============================================================================
# Known Tables
# =============================================================================
# Tables we expect in a Chromium Media History database.
# Used to detect if new media-related tables have been added.

KNOWN_MEDIA_HISTORY_TABLES: Set[str] = {
    "playback",
    "playbackSession",
    "origin",
    "mediaImage",
    "sessionImage",  # Session-specific images (Chromium 110+)
    "meta",  # Schema version tracking
}

# Patterns to filter relevant unknown tables (media-related)
MEDIA_HISTORY_TABLE_PATTERNS: List[str] = [
    "playback",
    "session",
    "media",
    "origin",
    "image",
]


# =============================================================================
# Known Columns - playback table
# =============================================================================
# Columns we currently parse from the playback table.
# Unknown columns will be reported as warnings.

KNOWN_PLAYBACK_COLUMNS: Set[str] = {
    "id",
    "origin_id",
    "url",
    "watch_time_s",
    "watchtime",  # Older schema variant
    "has_video",
    "has_audio",
    "last_updated_time_s",
    "last_updated_time",  # Older schema variant
}


# =============================================================================
# Known Columns - playbackSession table
# =============================================================================

KNOWN_PLAYBACK_SESSION_COLUMNS: Set[str] = {
    "id",
    "origin_id",
    "url",
    "duration_ms",
    "duration",  # Older schema variant
    "position_ms",
    "position",  # Older schema variant
    "title",
    "artist",
    "album",
    "source_title",
    "last_updated_time_s",
    "last_updated_time",  # Older schema variant
}


# =============================================================================
# Known Columns - origin table
# =============================================================================

KNOWN_ORIGIN_COLUMNS: Set[str] = {
    "id",
    "origin",
    "last_updated_time_s",
    "last_updated_time",  # Older schema variant
    "aggregate_watchtime_audio_video_s",  # Aggregates (computed, may not be present)
    # Media Engagement fields (, Chromium 90+)
    "has_media_engagement",
    "media_engagement_visits",
    "media_engagement_playbacks",
    "media_engagement_last_playback_time",
    "media_engagement_has_high_score",
}


# =============================================================================
# Known Columns - mediaImage table
# =============================================================================

KNOWN_MEDIA_IMAGE_COLUMNS: Set[str] = {
    "id",
    "playback_origin_id",
    "url",
    "src_url",
    "mime_type",
    "data",  # Image blob
    "image_type",
}


# =============================================================================
# Known Columns - sessionImage table
# =============================================================================
# Session-specific images (Chromium 110+), similar to mediaImage

KNOWN_SESSION_IMAGE_COLUMNS: Set[str] = {
    "id",
    "session_id",  # References playbackSession
    "url",
    "src_url",
    "mime_type",
    "data",  # Image blob
    "image_type",
}


# =============================================================================
# Mapping: Table name -> Known columns set
# =============================================================================

KNOWN_COLUMNS_BY_TABLE = {
    "playback": KNOWN_PLAYBACK_COLUMNS,
    "playbackSession": KNOWN_PLAYBACK_SESSION_COLUMNS,
    "origin": KNOWN_ORIGIN_COLUMNS,
    "mediaImage": KNOWN_MEDIA_IMAGE_COLUMNS,
    "sessionImage": KNOWN_SESSION_IMAGE_COLUMNS,
}
