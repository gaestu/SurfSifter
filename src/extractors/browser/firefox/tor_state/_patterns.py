"""
Tor Browser file path patterns.

Defines the Tor Data directory paths where Tor-specific artifacts are stored.
These paths are separate from the Firefox profile paths (TorBrowser/Data/Browser)
and contain the Tor daemon configuration and state.

Artifacts include:
- torrc, torrc-defaults: Tor configuration
- state: Tor state with timestamps and guards
- cached-*: Relay/circuit cache files
- control_auth_cookie: Controller authentication
- geoip, geoip6: IP geolocation databases
- pt_state/: Pluggable transport state
- keys/: Cryptographic keys

Initial implementation (extracted from extractor.py)
"""
from __future__ import annotations

from typing import List

__all__ = [
    "TOR_DATA_ROOTS",
    "TOR_ARTIFACT_PATTERNS",
    "TOR_CONFIG_FILES",
    "TOR_STATE_FILES",
    "TOR_CACHE_FILES",
    "get_all_tor_patterns",
]


# Tor Data directory roots (NOT the Browser profile - that's in firefox/_patterns.py)
# These contain the Tor daemon files, not Firefox browser data
TOR_DATA_ROOTS: List[str] = [
    # =========================================================================
    # Windows (portable bundle)
    # =========================================================================
    "Users/*/Desktop/Tor Browser/Browser/TorBrowser/Data/Tor",
    "Users/*/Downloads/Tor Browser/Browser/TorBrowser/Data/Tor",
    "Users/*/AppData/Local/Tor Browser/Browser/TorBrowser/Data/Tor",
    # Windows portable (generic locations)
    "*/Tor Browser/Browser/TorBrowser/Data/Tor",
    "*/TorBrowser/Browser/TorBrowser/Data/Tor",

    # =========================================================================
    # macOS
    # =========================================================================
    "Applications/Tor Browser.app/Contents/Resources/TorBrowser/Data/Tor",
    "Users/*/Applications/Tor Browser.app/Contents/Resources/TorBrowser/Data/Tor",

    # =========================================================================
    # Linux
    # =========================================================================
    "home/*/tor-browser*/Browser/TorBrowser/Data/Tor",
    "home/*/.tor-browser/Browser/TorBrowser/Data/Tor",
    # Tails OS
    "home/amnesia/Persistent/Tor Browser/Browser/TorBrowser/Data/Tor",
]


# Artifact patterns relative to TOR_DATA_ROOTS
# These are the files we want to extract
TOR_ARTIFACT_PATTERNS: List[str] = [
    # Configuration files
    "torrc",
    "torrc-defaults",
    "torrc-defaults-impl",

    # State file
    "state",

    # Authentication
    "control_auth_cookie",

    # Cache files (relay/circuit info)
    "cached-*",

    # GeoIP databases
    "geoip",
    "geoip6",

    # Pluggable transport state
    "pt_state/*",

    # Cryptographic keys
    "keys/*",
]


# File classification groups for parsing
TOR_CONFIG_FILES = {"torrc", "torrc-defaults", "torrc-defaults-impl"}
TOR_STATE_FILES = {"state"}
TOR_CACHE_FILES = {
    "cached-certs",
    "cached-microdesc",
    "cached-microdescs",
    "cached-microdescs.new",
    "cached-consensus",
}


def get_all_tor_patterns() -> List[str]:
    """
    Generate all full patterns by combining roots with artifact patterns.

    Returns:
        List of full glob patterns for discovery
    """
    patterns = []
    for root in TOR_DATA_ROOTS:
        for artifact in TOR_ARTIFACT_PATTERNS:
            patterns.append(f"{root}/{artifact}")
    return patterns


def classify_tor_file(path: str) -> str:
    """
    Classify a Tor file based on its path/name.

    Args:
        path: File path

    Returns:
        File type classification
    """
    from pathlib import Path
    name = Path(path).name.lower()

    if name.startswith("torrc"):
        return "torrc"
    if name == "state" or name.startswith("state."):
        return "state"
    if name.startswith("cached-"):
        return "cached"
    if name == "control_auth_cookie":
        return "control_auth_cookie"
    if name in ("geoip", "geoip6"):
        return name
    if "/pt_state/" in path.replace("\\", "/"):
        return "pt_state"
    if "/keys/" in path.replace("\\", "/"):
        return "keys"
    return "other"
