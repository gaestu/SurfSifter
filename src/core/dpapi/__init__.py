"""DPAPI offline decryption — pure crypto module.

Provides offline DPAPI master key decryption and Chromium v10 blob decryption.
No Qt, extractor, or database dependencies.
"""

from __future__ import annotations

from ._bootkey import extract_boot_key
from ._chromium import decrypt_v10_blob, extract_chromium_key, unwrap_chromium_key
from ._errors import (
    BootKeyError,
    ChromiumKeyError,
    DPAPIError,
    IntegrityError,
    LSAError,
    MasterKeyError,
    SAMError,
)
from ._lsa import extract_dpapi_system_keys
from ._masterkey import (
    decrypt_master_key_with_key,
    decrypt_master_key_with_ntlm,
    decrypt_master_key_with_password,
)
from ._sam import extract_ntlm_hashes
from ._structures import (
    parse_dpapi_blob,
    parse_master_key_file,
    parse_preferred_file,
    uuid_bytes_to_string,
)
from ._types import (
    ChromiumKeyResult,
    DPAPIBlob,
    DPAPIMasterKeyFile,
    DPAPISystemKeys,
    DecryptResult,
    MasterKeyResult,
    NTLMHashResult,
    PreferredFile,
)

__all__ = [
    # Errors
    "DPAPIError",
    "BootKeyError",
    "SAMError",
    "LSAError",
    "MasterKeyError",
    "ChromiumKeyError",
    "IntegrityError",
    # Types
    "NTLMHashResult",
    "MasterKeyResult",
    "ChromiumKeyResult",
    "DecryptResult",
    "DPAPIMasterKeyFile",
    "DPAPIBlob",
    "DPAPISystemKeys",
    "PreferredFile",
    # Boot key
    "extract_boot_key",
    # SAM
    "extract_ntlm_hashes",
    # LSA
    "extract_dpapi_system_keys",
    # Master key
    "decrypt_master_key_with_password",
    "decrypt_master_key_with_ntlm",
    "decrypt_master_key_with_key",
    # Structures
    "parse_master_key_file",
    "parse_dpapi_blob",
    "parse_preferred_file",
    "uuid_bytes_to_string",
    # Chromium
    "extract_chromium_key",
    "unwrap_chromium_key",
    "decrypt_v10_blob",
]
