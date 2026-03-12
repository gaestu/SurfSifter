from __future__ import annotations


class DPAPIError(Exception):
    """Base exception for all DPAPI operations."""


class BootKeyError(DPAPIError):
    """Failed to extract boot key from SYSTEM hive."""


class SAMError(DPAPIError):
    """Failed to extract NTLM hashes from SAM hive."""


class MasterKeyError(DPAPIError):
    """Failed to decrypt a DPAPI master key."""


class ChromiumKeyError(DPAPIError):
    """Failed to extract or decrypt Chromium encryption key."""


class IntegrityError(DPAPIError):
    """HMAC or GCM tag verification failed."""


class LSAError(DPAPIError):
    """Failed to extract LSA secrets."""
