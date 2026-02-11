"""
Firefox Transport Security schema definitions.

This module defines known formats, entry types, and state values for
Firefox SiteSecurityServiceState.txt parsing.

Firefox Format:
    <host>:<type>\t<score>\t<last_access>\t<expiry_ms>,<state>,<include_subdomains>

- host: cleartext domain name (HIGH forensic value - unlike Chromium hashing)
- type: entry type (usually 'HSTS')
- score: internal scoring value (not forensically significant)
- last_access: days since Unix epoch (PRTime / 86400000000)
- expiry_ms: milliseconds since Unix epoch
- state: 0=SecurityPropertySet (active), 1=SecurityPropertyKnockout (expired/removed)
- include_subdomains: 0 or 1

Initial schema file extraction from extractor.py
"""

from __future__ import annotations

from typing import Dict, Set

# =============================================================================
# Entry Type Constants
# =============================================================================

# Known entry types in SiteSecurityServiceState.txt
# Format: host:TYPE where TYPE is one of these
KNOWN_ENTRY_TYPES: Set[str] = {
    "HSTS",  # HTTP Strict Transport Security (most common)
    # Future Firefox versions might add:
    # "PKP",  # Public Key Pinning (deprecated in browsers)
    # "CT",   # Certificate Transparency
}

# Human-readable names for entry types
ENTRY_TYPE_NAMES: Dict[str, str] = {
    "HSTS": "HTTP Strict Transport Security",
}


# =============================================================================
# State Value Constants
# =============================================================================

# Known state values in the data field
# Source: https://searchfox.org/mozilla-central/source/security/manager/ssl/nsISiteSecurityService.idl
KNOWN_STATE_VALUES: Dict[int, str] = {
    0: "SecurityPropertySet",      # HSTS is active for this domain
    1: "SecurityPropertyKnockout",  # HSTS was explicitly removed/expired
}

# Map state to mode string for hsts_entries table
STATE_TO_MODE: Dict[int, str] = {
    0: "force-https",  # Active HSTS
    1: "knockout",     # Removed/expired
}


# =============================================================================
# File Format Constants
# =============================================================================

# Firefox stores last_access as days since Unix epoch
# PRTime is microseconds since epoch, divided by 86400000000 = days
PRTIME_DAYS_TO_SECONDS = 86400  # seconds per day

# Minimum number of tab-separated fields in a valid line
MIN_LINE_FIELDS = 4

# Expected number of comma-separated values in data field
EXPECTED_DATA_PARTS = 3


# =============================================================================
# Table Patterns for Warning Discovery
# =============================================================================

# Patterns to identify transport security related files
# Used when discovering unknown file formats
FILE_PATTERNS = [
    "SiteSecurityServiceState",
    "transport_security",
    "hsts",
]


# =============================================================================
# Warning Category Constants
# =============================================================================

# Category for extraction warnings
WARNING_CATEGORY = "text"  # Text file format

# Artifact type for warnings
WARNING_ARTIFACT_TYPE = "transport_security"
