from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class NTLMHashResult:
    rid: int
    username: str
    sid: str
    ntlm_hash: Optional[bytes]  # 16 bytes
    lm_hash: Optional[bytes]


@dataclass
class MasterKeyResult:
    guid: str
    decrypted_key: bytes  # 64 bytes typically
    method: str  # "password" | "ntlm_hash" | "ntlm_hash_legacy"
    verified: bool


@dataclass
class ChromiumKeyResult:
    aes_key: bytes  # 32-byte AES-256 key
    master_key_guid: str
    local_state_path: str


@dataclass
class DecryptResult:
    plaintext: bytes
    version: str  # "v10" etc
    verified: bool


@dataclass
class DPAPIMasterKeyFile:
    version: int
    guid: str
    flags: int
    master_key_len: int
    backup_key_len: int
    credential_history_len: int
    domain_key_len: int
    # Master key sub-fields:
    salt: bytes
    rounds: int
    hash_algo: str  # "sha1" | "sha512"
    cipher_algo: str  # "3des" | "aes256" | "aes128"
    encrypted_key: bytes
    hmac_key_data: bytes  # HMAC key material from master key struct


@dataclass
class DPAPIBlob:
    version: int
    provider_guid: str
    master_key_version: int
    master_key_guid: str
    flags: int
    description: str
    cipher_algo_id: int
    key_length: int
    salt: bytes
    hmac_key_blob: bytes
    hash_algo_id: int
    hash_algo_len: int
    hmac_blob: bytes
    encrypted_data: bytes
    sign_data: bytes  # data used for HMAC validation
    sign: bytes = b""


@dataclass
class DPAPISystemKeys:
    machine_key: bytes  # 20 bytes
    user_key: bytes     # 20 bytes


@dataclass
class PreferredFile:
    guid: str
    timestamp: Optional[str]  # ISO format
