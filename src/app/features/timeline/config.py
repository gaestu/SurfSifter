"""
Timeline configuration module.

Refactored from src/core/timelines.py
Configuration is now hardcoded (no external YAML dependency).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

from core.logging import get_logger

LOGGER = get_logger("app.features.timeline.config")


@dataclass(slots=True)
class TimelineConfig:
    """Timeline source and fusion configuration.

    Attributes:
        sources: Per-source mappings (timestamp_field → event kind, note template)
        confidence_weights: Weight multipliers for high/medium/low confidence
        cluster_window_seconds: Temporal clustering window for event fusion
        min_confidence: Minimum confidence threshold for timeline inclusion
    """
    sources: Dict[str, Dict[str, Any]]
    confidence_weights: Dict[str, float]
    cluster_window_seconds: int
    min_confidence: float


# =============================================================================
# Default Timeline Configuration
# =============================================================================
# This is the single source of truth for timeline mappings.
# Covers 13 artifact sources with 19 event kinds.
# =============================================================================

DEFAULT_TIMELINE_CONFIG = TimelineConfig(
    sources={
        "browser_history": {
            "confidence": "high",
            "mappings": [{
                "timestamp_field": "ts_utc",
                "kind": "browser_visit",
                "note_template": "{browser} visit: {title}"
            }]
        },
        "urls": {
            "confidence": "medium",
            "mappings": [{
                "timestamp_field": "first_seen_utc",
                "kind": "url_discovered",
                "note_template": "URL: {url} (via {discovered_by})"
            }]
        },
        "images": {
            "confidence": "medium",
            "mappings": [{
                "timestamp_field": "ts_utc",
                "kind": "image_extracted",
                "note_template": "Image: {filename}"
            }]
        },
        "os_indicators": {
            "confidence": "low",
            "mappings": [{
                "timestamp_field": "detected_at_utc",
                "kind": "os_artifact",
                "note_template": "OS: {type} - {name}"
            }]
        },
        "cookies": {
            "confidence": "medium",
            "mappings": [
                {
                    "timestamp_field": "creation_utc",
                    "kind": "cookie_created",
                    "note_template": "Cookie created: {domain} ({name})"
                },
                {
                    "timestamp_field": "last_access_utc",
                    "kind": "cookie_accessed",
                    "note_template": "Cookie accessed: {domain} ({name})"
                }
            ]
        },
        "bookmarks": {
            "confidence": "high",
            "mappings": [{
                "timestamp_field": "date_added_utc",
                "kind": "bookmark_added",
                "note_template": "Bookmark: {title}"
            }]
        },
        "browser_downloads": {
            "confidence": "high",
            "mappings": [
                {
                    "timestamp_field": "start_time_utc",
                    "kind": "download_started",
                    "note_template": "Download started: {filename}"
                },
                {
                    "timestamp_field": "end_time_utc",
                    "kind": "download_completed",
                    "note_template": "Download completed: {filename}"
                }
            ]
        },
        "session_tabs": {
            "confidence": "medium",
            "mappings": [{
                "timestamp_field": "last_accessed_utc",
                "kind": "tab_accessed",
                "note_template": "Tab: {title}"
            }]
        },
        "autofill": {
            "confidence": "medium",
            "mappings": [
                {
                    "timestamp_field": "date_created_utc",
                    "kind": "autofill_created",
                    "note_template": "Autofill saved: {name}"
                },
                {
                    "timestamp_field": "date_last_used_utc",
                    "kind": "autofill_used",
                    "note_template": "Autofill used: {name}"
                }
            ]
        },
        "credentials": {
            "confidence": "high",
            "mappings": [
                {
                    "timestamp_field": "date_created_utc",
                    "kind": "credential_saved",
                    "note_template": "Credential saved: {origin_url}"
                },
                {
                    "timestamp_field": "date_last_used_utc",
                    "kind": "credential_used",
                    "note_template": "Credential used: {origin_url}"
                }
            ]
        },
        "media_playback": {
            "confidence": "medium",
            "mappings": [{
                "timestamp_field": "last_played_utc",
                "kind": "media_played",
                "note_template": "Media: {url} ({watch_time_seconds}s)"
            }]
        },
        "hsts_entries": {
            "confidence": "low",
            "mappings": [
                {
                    "timestamp_field": "sts_observed",
                    "kind": "hsts_observed",
                    "note_template": "HSTS: {host}"
                },
                {
                    "timestamp_field": "expiry",
                    "kind": "hsts_expiry",
                    "note_template": "HSTS expiry: {host}"
                }
            ]
        },
        "jump_list_entries": {
            "confidence": "medium",
            "mappings": [
                {
                    "timestamp_field": "lnk_access_time",
                    "kind": "jumplist_accessed",
                    "note_template": "Jump list: {url}"
                },
                {
                    "timestamp_field": "lnk_creation_time",
                    "kind": "jumplist_created",
                    "note_template": "Jump list created: {url}"
                }
            ]
        }
    },
    confidence_weights={"high": 1.0, "medium": 0.7, "low": 0.4},
    cluster_window_seconds=300,
    min_confidence=0.3
)


def get_timeline_config() -> TimelineConfig:
    """
    Get the timeline configuration.

    Returns the hardcoded DEFAULT_TIMELINE_CONFIG.
    This function exists for API consistency and future extensibility.

    Returns:
        TimelineConfig with all source mappings and fusion parameters.
    """
    return DEFAULT_TIMELINE_CONFIG


def load_timeline_config(rules_dir: Path = None) -> TimelineConfig:
    """
    Load timeline configuration.

    This is a backward-compatible function that ignores the rules_dir parameter
    and returns the hardcoded default configuration.

    Args:
        rules_dir: Ignored (kept for backward compatibility)

    Returns:
        TimelineConfig with all source mappings and fusion parameters.
    """
    # Configuration is now hardcoded, rules_dir is ignored
    if rules_dir is not None:
        LOGGER.debug("load_timeline_config: rules_dir parameter ignored (config is hardcoded)")
    return DEFAULT_TIMELINE_CONFIG
