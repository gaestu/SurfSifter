from __future__ import annotations

import struct
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from core.logging import get_logger

from ._errors import DPAPIError
from ._types import DPAPIBlob, DPAPIMasterKeyFile, PreferredFile

logger = get_logger(__name__)

# --- Hash algorithm IDs ---
_HASH_ALGOS = {
    0x8003: "md5",
    0x8004: "sha1",
    0x8009: "sha256",
    0x800E: "sha512",
}

# --- Cipher algorithm IDs ---
_CIPHER_ALGOS = {
    0x6603: "3des",
    0x6610: "aes256",   # CALG_AES_256 (key=32 bytes)
    0x660E: "aes128",   # CALG_AES_128 (key=16 bytes)
}

# FILETIME epoch: 1601-01-01 00:00:00 UTC
_FILETIME_EPOCH = datetime(1601, 1, 1, tzinfo=timezone.utc)


def uuid_bytes_to_string(b: bytes) -> str:
    """Convert 16-byte little-endian GUID bytes to standard string format."""
    return str(uuid.UUID(bytes_le=b))


def _filetime_to_iso(ft: int) -> Optional[str]:
    """Convert a Windows FILETIME (100-ns intervals since 1601) to ISO string."""
    if ft == 0:
        return None
    try:
        delta = ft / 10_000_000  # convert 100-ns to seconds
        dt = _FILETIME_EPOCH + timedelta(seconds=delta)
        return dt.isoformat(timespec="seconds")
    except (OverflowError, OSError, ValueError):
        return None


def parse_master_key_file(data: bytes) -> DPAPIMasterKeyFile:
    """Parse a DPAPI master key file from raw bytes.

    File layout (impacket/dpapick3-compatible):
      Offset 0:   DWORD  dwVersion
      Offset 4:   DWORD  dwReserved1
      Offset 8:   DWORD  dwReserved2
      Offset 12:  WCHAR  szGuid[36]  (72 bytes, UTF-16LE)
      Offset 84:  DWORD  dwUnused
      Offset 88:  DWORD  dwFlags
      Offset 92:  DWORD  dwPolicy
      Offset 96:  QWORD  qwMasterKeyLen
      Offset 104: QWORD  qwBackupKeyLen
      Offset 112: QWORD  qwCredHistLen
      Offset 120: QWORD  qwDomainKeyLen
      Total header: 128 bytes

    Master key blob (at offset 128, master_key_len bytes):
      Offset 0:  DWORD  version
      Offset 4:  BYTE   salt[16]
      Offset 20: DWORD  pbkdf2_iterations
      Offset 24: DWORD  hash_algo_id
      Offset 28: DWORD  cipher_algo_id
      Offset 32: BYTE   encrypted_key[master_key_len - 32]
    """
    _HEADER_SIZE = 128

    if len(data) < _HEADER_SIZE:
        raise DPAPIError(
            f"Master key file too short: {len(data)} bytes (need >= {_HEADER_SIZE})"
        )

    version, reserved1, reserved2 = struct.unpack_from("<III", data, 0)
    guid_raw = data[12:84]
    try:
        guid = guid_raw.decode("utf-16-le").rstrip("\x00")
    except UnicodeDecodeError:
        raise DPAPIError("Failed to decode GUID from master key file header")

    flags = struct.unpack_from("<I", data, 88)[0]
    mk_len, bk_len, ch_len, dk_len = struct.unpack_from("<QQQQ", data, 96)

    # Parse master key sub-structure
    mk_offset = _HEADER_SIZE
    mk_end = mk_offset + mk_len
    if mk_len < 32 or len(data) < mk_end:
        raise DPAPIError(
            f"Master key blob truncated: need {mk_end} bytes, have {len(data)}"
        )

    mk_blob = data[mk_offset:mk_end]
    mk_version = struct.unpack_from("<I", mk_blob, 0)[0]
    salt = mk_blob[4:20]
    rounds = struct.unpack_from("<I", mk_blob, 20)[0]
    hash_algo_id = struct.unpack_from("<I", mk_blob, 24)[0]
    cipher_algo_id = struct.unpack_from("<I", mk_blob, 28)[0]
    encrypted_key = mk_blob[32:]

    hash_algo = _HASH_ALGOS.get(hash_algo_id)
    if hash_algo is None:
        raise DPAPIError(f"Unknown hash algorithm ID: 0x{hash_algo_id:04X}")

    cipher_algo = _CIPHER_ALGOS.get(cipher_algo_id)
    if cipher_algo is None:
        raise DPAPIError(f"Unknown cipher algorithm ID: 0x{cipher_algo_id:04X}")

    # HMAC key data: the entire master key sub-blob is used for HMAC derivation
    hmac_key_data = mk_blob

    return DPAPIMasterKeyFile(
        version=version,
        guid=guid,
        flags=flags,
        master_key_len=mk_len,
        backup_key_len=bk_len,
        credential_history_len=ch_len,
        domain_key_len=dk_len,
        salt=salt,
        rounds=rounds,
        hash_algo=hash_algo,
        cipher_algo=cipher_algo,
        encrypted_key=encrypted_key,
        hmac_key_data=hmac_key_data,
    )


def parse_dpapi_blob(data: bytes) -> DPAPIBlob:
    """Parse a DPAPI blob from raw bytes.

    Layout:
      version(4), provider_guid(16), mk_version(4), mk_guid(16), flags(4),
      desc_len(4), description(desc_len bytes UTF-16LE),
      cipher_algo_id(4), key_len(4),
      salt_len(4), salt(salt_len),
      hmac_key_len(4), hmac_key(hmac_key_len),
      hash_algo_id(4), hash_algo_len(4),
      hmac_len(4), hmac(hmac_len),
      data_len(4), data(data_len),
      sign_len(4), sign(sign_len)

    sign_data = everything from version through salt (for HMAC validation).
    """
    if len(data) < 48:
        raise DPAPIError(f"DPAPI blob too short: {len(data)} bytes")

    # DPAPI blob binary layout (all little-endian):
    #   version(4)  provider_guid(16)  mk_version(4)
    #   mk_guid(16)  flags(4)  desc_len(4)  desc(desc_len)  ...
    offset = 0

    version = struct.unpack_from("<I", data, offset)[0]
    offset += 4  # 4

    provider_guid = uuid_bytes_to_string(data[offset : offset + 16])
    offset += 16  # 20

    mk_version = struct.unpack_from("<I", data, offset)[0]
    offset += 4  # 24

    mk_guid = uuid_bytes_to_string(data[offset : offset + 16])
    offset += 16  # 40

    flags = struct.unpack_from("<I", data, offset)[0]
    offset += 4  # 44

    desc_len = struct.unpack_from("<I", data, offset)[0]
    offset += 4  # 48

    if len(data) < offset + desc_len:
        raise DPAPIError("DPAPI blob truncated at description")
    desc_bytes = data[offset : offset + desc_len]
    offset += desc_len
    try:
        description = desc_bytes.decode("utf-16-le").rstrip("\x00")
    except UnicodeDecodeError:
        description = ""

    if len(data) < offset + 8:
        raise DPAPIError("DPAPI blob truncated at cipher info")
    cipher_algo_id = struct.unpack_from("<I", data, offset)[0]
    offset += 4
    key_length = struct.unpack_from("<I", data, offset)[0]
    offset += 4

    # Salt
    if len(data) < offset + 4:
        raise DPAPIError("DPAPI blob truncated at salt length")
    salt_len = struct.unpack_from("<I", data, offset)[0]
    offset += 4
    if len(data) < offset + salt_len:
        raise DPAPIError("DPAPI blob truncated at salt data")
    salt = data[offset : offset + salt_len]
    offset += salt_len

    # sign_data = everything from start through salt
    sign_data = data[:offset]

    # HMAC key
    if len(data) < offset + 4:
        raise DPAPIError("DPAPI blob truncated at HMAC key length")
    hmac_key_len = struct.unpack_from("<I", data, offset)[0]
    offset += 4
    if len(data) < offset + hmac_key_len:
        raise DPAPIError("DPAPI blob truncated at HMAC key data")
    hmac_key_blob = data[offset : offset + hmac_key_len]
    offset += hmac_key_len

    # Hash algorithm
    if len(data) < offset + 4:
        raise DPAPIError("DPAPI blob truncated at hash algorithm ID")
    hash_algo_id = struct.unpack_from("<I", data, offset)[0]
    offset += 4
    if len(data) < offset + 4:
        raise DPAPIError("DPAPI blob truncated at hash algorithm length")
    hash_algo_len = struct.unpack_from("<I", data, offset)[0]
    offset += 4

    # HMAC
    if len(data) < offset + 4:
        raise DPAPIError("DPAPI blob truncated at HMAC length")
    hmac_len = struct.unpack_from("<I", data, offset)[0]
    offset += 4
    if len(data) < offset + hmac_len:
        raise DPAPIError("DPAPI blob truncated at HMAC data")
    hmac_blob = data[offset : offset + hmac_len]
    offset += hmac_len

    # Encrypted data
    if len(data) < offset + 4:
        raise DPAPIError("DPAPI blob truncated at data length")
    data_len = struct.unpack_from("<I", data, offset)[0]
    offset += 4
    if len(data) < offset + data_len:
        raise DPAPIError("DPAPI blob truncated at encrypted data")
    encrypted_data = data[offset : offset + data_len]
    offset += data_len

    # Signature (optional trailing data)
    sign = b""
    if len(data) >= offset + 4:
        sign_len = struct.unpack_from("<I", data, offset)[0]
        offset += 4
        if len(data) >= offset + sign_len:
            sign = data[offset : offset + sign_len]

    return DPAPIBlob(
        version=version,
        provider_guid=provider_guid,
        master_key_version=mk_version,
        master_key_guid=mk_guid,
        flags=flags,
        description=description,
        cipher_algo_id=cipher_algo_id,
        key_length=key_length,
        salt=salt,
        hmac_key_blob=hmac_key_blob,
        hash_algo_id=hash_algo_id,
        hash_algo_len=hash_algo_len,
        hmac_blob=hmac_blob,
        encrypted_data=encrypted_data,
        sign_data=sign_data,
        sign=sign,
    )


def parse_preferred_file(data: bytes) -> PreferredFile:
    """Parse a DPAPI Preferred file (24 bytes: GUID(16) + FILETIME(8))."""
    if len(data) < 24:
        raise DPAPIError(f"Preferred file too short: {len(data)} bytes (need 24)")

    guid = uuid_bytes_to_string(data[:16])
    filetime = struct.unpack_from("<Q", data, 16)[0]
    timestamp = _filetime_to_iso(filetime)

    return PreferredFile(guid=guid, timestamp=timestamp)
