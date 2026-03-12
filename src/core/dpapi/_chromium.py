from __future__ import annotations

import base64
import hashlib
import hmac as hmac_lib
import json
from typing import Dict

from core.logging import get_logger

from ._errors import ChromiumKeyError, IntegrityError
from ._structures import parse_dpapi_blob
from ._types import ChromiumKeyResult, DPAPIBlob, DecryptResult

logger = get_logger(__name__)

_DPAPI_PREFIX = b"DPAPI"
_V10_PREFIX = b"v10"


def extract_chromium_key(local_state_bytes: bytes) -> bytes:
    """Parse Chromium Local State JSON and return the raw DPAPI blob bytes.

    The encrypted key is stored in ``os_crypt.encrypted_key`` as base64.
    After base64 decoding, the first 5 bytes (``DPAPI``) are stripped.

    Args:
        local_state_bytes: Raw bytes of the Local State JSON file.

    Returns:
        DPAPI blob bytes (without the DPAPI prefix).

    Raises:
        ChromiumKeyError: If parsing fails or the key format is unexpected.
    """
    try:
        state = json.loads(local_state_bytes)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ChromiumKeyError(f"Failed to parse Local State JSON: {exc}") from exc

    try:
        encrypted_key_b64 = state["os_crypt"]["encrypted_key"]
    except (KeyError, TypeError) as exc:
        raise ChromiumKeyError(
            "Local State missing os_crypt.encrypted_key"
        ) from exc

    try:
        encrypted_key = base64.b64decode(encrypted_key_b64)
    except Exception as exc:
        raise ChromiumKeyError(f"Failed to base64 decode key: {exc}") from exc

    if not encrypted_key.startswith(_DPAPI_PREFIX):
        raise ChromiumKeyError(
            f"Encrypted key does not start with DPAPI prefix "
            f"(got {encrypted_key[:5]!r})"
        )

    return encrypted_key[len(_DPAPI_PREFIX) :]


def unwrap_chromium_key(
    dpapi_blob_bytes: bytes,
    master_keys: Dict[str, bytes],
    local_state_path: str = "",
) -> ChromiumKeyResult:
    """DPAPI-decrypt a Chromium encrypted key to obtain the 32-byte AES key.

    Args:
        dpapi_blob_bytes: Raw DPAPI blob bytes (after DPAPI prefix removal).
        master_keys: Mapping of master key GUID → decrypted 64-byte master key.
        local_state_path: Path to the Local State file (for provenance tracking).

    Returns:
        ChromiumKeyResult with the 32-byte AES-256 key.

    Raises:
        ChromiumKeyError: If the blob cannot be parsed or the needed master key
            is not available.
    """
    try:
        blob: DPAPIBlob = parse_dpapi_blob(dpapi_blob_bytes)
    except Exception as exc:
        raise ChromiumKeyError(f"Failed to parse DPAPI blob: {exc}") from exc

    mk_guid = blob.master_key_guid
    master_key = master_keys.get(mk_guid)
    if master_key is None:
        raise ChromiumKeyError(
            f"Master key {mk_guid} not found in provided key set "
            f"(have: {list(master_keys.keys())})"
        )

    # DPAPI blob decryption:
    # 1. Derive session key from master key and HMAC key blob
    # 2. Decrypt the encrypted data
    try:
        aes_key = _dpapi_decrypt_blob(blob, master_key)
    except IntegrityError:
        raise
    except Exception as exc:
        raise ChromiumKeyError(
            f"DPAPI blob decryption failed: {exc}"
        ) from exc

    if len(aes_key) != 32:
        raise ChromiumKeyError(
            f"Decrypted key is {len(aes_key)} bytes, expected 32"
        )

    return ChromiumKeyResult(
        aes_key=aes_key,
        master_key_guid=mk_guid,
        local_state_path=local_state_path,
    )


def _dpapi_decrypt_blob(blob: DPAPIBlob, master_key: bytes) -> bytes:
    """Decrypt a DPAPI blob using the given master key.

    Implements the DPAPI blob decryption algorithm (matching impacket):
    1. keyHash = SHA1(master_key)
    2. sessionKey = HMAC(keyHash, salt, hashModule)
    3. derivedKey = deriveKey(sessionKey) — extend if needed via ipad/opad
    4. Decrypt with ALL-ZERO IV
    5. Strip PKCS7 padding
    """
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    # Hash/cipher algorithm parameter tables
    HASH_BLOCK_SIZES = {0x8003: 64, 0x8004: 64, 0x8009: 64, 0x800C: 64, 0x800E: 128}
    CIPHER_KEY_SIZES = {0x6603: 24, 0x660E: 16, 0x6610: 32}
    CIPHER_IV_SIZES = {0x6603: 8, 0x660E: 16, 0x6610: 16}
    HASH_NAMES = {0x8003: "md5", 0x8004: "sha1", 0x8009: "sha256", 0x800C: "sha1", 0x800E: "sha512"}

    cipher_algo_id = blob.cipher_algo_id
    hash_algo_id = blob.hash_algo_id

    hash_name = HASH_NAMES.get(hash_algo_id)
    if hash_name is None:
        raise ChromiumKeyError(f"Unsupported blob hash algorithm: 0x{hash_algo_id:04X}")

    key_size = CIPHER_KEY_SIZES.get(cipher_algo_id)
    if key_size is None:
        raise ChromiumKeyError(f"Unsupported blob cipher: 0x{cipher_algo_id:04X}")

    iv_size = CIPHER_IV_SIZES[cipher_algo_id]
    block_size = HASH_BLOCK_SIZES[hash_algo_id]

    # Step 1: keyHash = SHA1(master_key)
    key_hash = hashlib.sha1(master_key).digest()

    # Step 2: sessionKey = HMAC(keyHash, hmac_key_blob + salt, hashModule)
    session_key = hmac_lib.new(
        key_hash, blob.hmac_key_blob + blob.salt, hash_name
    ).digest()

    # Step 3: Derive cipher key from session key
    derived_key = _blob_derive_key(session_key, hash_name, block_size, key_size)

    # Step 4: Decrypt with all-zero IV
    cipher_key = derived_key[:key_size]
    iv = b"\x00" * iv_size

    if cipher_algo_id == 0x6603:  # 3DES
        cipher = Cipher(algorithms.TripleDES(cipher_key), modes.CBC(iv))
    else:
        cipher = Cipher(algorithms.AES(cipher_key), modes.CBC(iv))

    decryptor = cipher.decryptor()
    plaintext = decryptor.update(blob.encrypted_data) + decryptor.finalize()

    # Step 5: Strip PKCS7 padding
    if plaintext:
        pad_byte = plaintext[-1]
        pad_size = iv_size if cipher_algo_id == 0x6603 else 16
        if 0 < pad_byte <= pad_size and all(
            b == pad_byte for b in plaintext[-pad_byte:]
        ):
            plaintext = plaintext[:-pad_byte]

    # Signature verification (best-effort, don't fail on mismatch)
    try:
        # Method: HMAC(keyHash, hmac_blob + sign_data)
        hmac_calc = hmac_lib.new(
            key_hash, blob.hmac_blob + blob.sign_data, hash_name
        ).digest()
        if blob.sign and hmac_calc[: len(blob.sign)] != blob.sign:
            logger.warning("DPAPI blob signature verification failed (non-fatal)")
    except Exception:
        pass

    return plaintext


def _blob_derive_key(
    session_key: bytes, hash_name: str, block_size: int, key_size: int
) -> bytes:
    """Derive the blob cipher key from the session key.

    If session_key is longer than block_size, hash it.
    If shorter than key_size, extend using HMAC ipad/opad derivation.
    """
    if len(session_key) > block_size:
        derived_key = hashlib.new(hash_name, session_key).digest()
    else:
        derived_key = session_key

    if len(derived_key) < key_size:
        # Extend key using ipad/opad
        pad = derived_key + b"\x00" * block_size
        ipad = bytes(b ^ 0x36 for b in pad[:block_size])
        opad = bytes(b ^ 0x5C for b in pad[:block_size])
        derived_key = (
            hashlib.new(hash_name, ipad).digest()
            + hashlib.new(hash_name, opad).digest()
        )

    return derived_key


def decrypt_v10_blob(encrypted_blob: bytes, aes_key: bytes) -> DecryptResult:
    """Decrypt a Chromium v10 encrypted blob using AES-256-GCM.

    Args:
        encrypted_blob: Full encrypted blob starting with ``v10``.
        aes_key: 32-byte AES-256 key from ``unwrap_chromium_key``.

    Returns:
        DecryptResult with the decrypted plaintext.

    Raises:
        ChromiumKeyError: If the blob format is invalid.
        IntegrityError: If GCM tag verification fails.
    """
    if len(aes_key) != 32:
        raise ChromiumKeyError(
            f"AES key must be 32 bytes, got {len(aes_key)}"
        )

    if not encrypted_blob.startswith(_V10_PREFIX):
        raise ChromiumKeyError(
            f"Blob does not start with v10 prefix (got {encrypted_blob[:3]!r})"
        )

    # v10 layout: "v10"(3) + nonce(12) + ciphertext+tag(rest)
    if len(encrypted_blob) < 3 + 12 + 16:
        raise ChromiumKeyError(
            f"v10 blob too short: {len(encrypted_blob)} bytes "
            f"(minimum {3 + 12 + 16})"
        )

    nonce = encrypted_blob[3:15]
    ciphertext_with_tag = encrypted_blob[15:]

    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        aesgcm = AESGCM(aes_key)
        plaintext = aesgcm.decrypt(nonce, ciphertext_with_tag, None)
    except ImportError as exc:
        raise ChromiumKeyError(
            "cryptography library required for AES-GCM"
        ) from exc
    except Exception as exc:
        # AESGCM raises InvalidTag on authentication failure
        if "InvalidTag" in type(exc).__name__ or "tag" in str(exc).lower():
            raise IntegrityError(
                "AES-GCM tag verification failed — wrong key or corrupted data"
            ) from exc
        raise ChromiumKeyError(
            f"AES-GCM decryption failed: {exc}"
        ) from exc

    return DecryptResult(
        plaintext=plaintext,
        version="v10",
        verified=True,
    )
