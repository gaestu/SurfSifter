"""
Known Extensions Reference Loader

Loads and matches extensions against the known_extensions.yml reference list.
Supports both exact ID matching and pattern-based name matching.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Any, List, Optional

import yaml

from core.logging import get_logger

LOGGER = get_logger("extractors.browser_extensions.known")


# Default path to known extensions file
# 4 levels up: known_extensions.py -> _shared -> extractors -> src -> root
DEFAULT_KNOWN_EXTENSIONS_PATH = Path(__file__).parent.parent.parent.parent / "reference_lists" / "known_extensions.yml"


def load_known_extensions(path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Load known extensions reference list from YAML file.

    Args:
        path: Path to YAML file (uses default if None)

    Returns:
        Dict with 'extensions' list and 'patterns' list
    """
    if path is None:
        path = DEFAULT_KNOWN_EXTENSIONS_PATH

    if not path.exists():
        LOGGER.warning("Known extensions file not found: %s", path)
        return {"extensions": [], "patterns": [], "version": "0.0.0"}

    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)

        return {
            "version": data.get("version", "0.0.0"),
            "extensions": data.get("extensions", []),
            "patterns": data.get("patterns", []),
        }

    except Exception as e:
        LOGGER.error("Failed to load known extensions: %s", e)
        return {"extensions": [], "patterns": [], "version": "0.0.0"}


def match_known_extension(
    extension_id: str,
    extension_name: str,
    known_data: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Match an extension against the known extensions list.

    First tries exact ID match, then falls back to name pattern matching.

    Args:
        extension_id: Extension ID to match
        extension_name: Extension name to match
        known_data: Loaded known extensions data

    Returns:
        Match dict with category, risk_indicator, notes or None if no match
    """
    # Try exact ID match first
    for ext in known_data.get("extensions", []):
        if ext.get("id") == extension_id:
            return {
                "category": ext.get("category"),
                "risk_indicator": ext.get("risk_indicator"),
                "notes": ext.get("notes"),
                "match_type": "exact_id",
                "matched_entry": ext.get("name"),
            }

    # Try name pattern matching
    for pattern in known_data.get("patterns", []):
        name_pattern = pattern.get("name_pattern")
        if name_pattern:
            try:
                if re.search(name_pattern, extension_name, re.IGNORECASE):
                    return {
                        "category": pattern.get("category"),
                        "risk_indicator": pattern.get("risk_indicator"),
                        "notes": pattern.get("notes"),
                        "match_type": "name_pattern",
                        "matched_pattern": name_pattern,
                    }
            except re.error:
                LOGGER.warning("Invalid regex pattern: %s", name_pattern)

    return None


def get_category_description(category: str) -> str:
    """
    Get human-readable description for an extension category.

    Args:
        category: Category string

    Returns:
        Description string
    """
    descriptions = {
        "vpn": "VPN or Proxy Service",
        "wallet": "Cryptocurrency Wallet",
        "adblock": "Ad Blocker",
        "gambling": "Gambling Related",
        "privacy": "Privacy Enhancement",
        "security": "Security/Password Manager",
        "developer": "Developer Tools",
        "automation": "Browser Automation",
        "downloader": "Download Manager",
        "other": "Other/Uncategorized",
    }
    return descriptions.get(category, category.title() if category else "Unknown")


def get_category_emoji(category: str) -> str:
    """
    Get emoji indicator for category (for UI).

    Args:
        category: Category string

    Returns:
        Emoji string
    """
    emojis = {
        "vpn": "🔒",
        "wallet": "💰",
        "adblock": "🛡️",
        "gambling": "🎰",
        "privacy": "👁️",
        "security": "🔑",
        "developer": "🔧",
        "automation": "🤖",
        "downloader": "📥",
        "other": "📦",
    }
    return emojis.get(category, "📦")
