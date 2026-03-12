from __future__ import annotations

import hashlib
import hmac as hmac_lib
import struct

from core.logging import get_logger

from ._errors import MasterKeyError
from ._types import DPAPIMasterKeyFile, MasterKeyResult

logger = get_logger(__name__)

# Algorithm parameters from impacket's ALGORITHMS_DATA.
# Cipher: (key_size, iv_size)
_CIPHER_PARAMS = {
    "3des": (24, 8),
    "aes128": (16, 16),
    "aes256": (32, 16),
}

# Hash algo numeric ID → (hash_name, hmac_compare_len, block_size)
_HASH_ALGO_PARAMS = {
    0x8003: ("md5", 16, 64),
    0x8004: ("sha1", 20, 64),
    0x8009: ("sha256", 32, 64),
    0x800C: ("sha1", 20, 64),  # CALG_HMAC → treated as SHA1
    0x800E: ("sha512", 16, 128),
}

# Hash algo name → numeric ID (for master key file parsing)
_HASH_NAME_TO_ID = {
    "md5": 0x8003,
    "sha1": 0x8004,
    "sha256": 0x8009,
    "sha512": 0x800E,
}


def _hmac(key: bytes, data: bytes, hash_name: str) -> bytes:
    """Compute HMAC with the named hash algorithm."""
    return hmac_lib.new(key, data, hash_name).digest()


def _ms_pbkdf2(passphrase: bytes, salt: bytes, keylen: int, count: int, hash_name: str) -> bytes:
    """Microsoft's modified PBKDF2 — feeds XOR accumulation back, not previous U.

    This differs from RFC 2898: at each round the XOR accumulation so far is
    fed back into HMAC (not the previous U block).
    """
    def prf(key: bytes, data: bytes) -> bytes:
        return hmac_lib.new(key, data, hash_name).digest()

    key_material = b""
    i = 1
    while len(key_material) < keylen:
        U = salt + struct.pack("!I", i)
        i += 1
        derived = bytearray(prf(passphrase, U))
        for _ in range(count - 1):
            actual = bytearray(prf(passphrase, derived))
            derived = bytearray(
                (int.from_bytes(derived, "little") ^ int.from_bytes(actual, "little"))
                .to_bytes(len(actual), "little")
            )
        key_material += derived
    return key_material[:keylen]


def _derive_user_keys_from_password(sid: str, password: str) -> list[bytes]:
    """Derive DPAPI user keys from a plaintext password and SID.

    Returns [HMAC-SHA1(SHA1(pwd), SID_bytes), HMAC-SHA1(NTLM(pwd), SID_bytes)].
    """
    password_bytes = password.encode("utf-16-le")
    sid_bytes = (sid + "\0").encode("utf-16-le")

    # key1: HMAC-SHA1(SHA1(password_UTF16LE), SID_UTF16LE)
    sha1_pw = hashlib.sha1(password_bytes).digest()
    key1 = _hmac(sha1_pw, sid_bytes, "sha1")

    # key2: HMAC-SHA1(MD4(password_UTF16LE), SID_UTF16LE)  — MD4 = NTLM hash
    try:
        ntlm_hash = hashlib.new("md4", password_bytes).digest()
    except ValueError:
        # MD4 may not be available on some OpenSSL builds
        ntlm_hash = None

    keys = [key1]
    if ntlm_hash is not None:
        key2 = _hmac(ntlm_hash, sid_bytes, "sha1")
        keys.append(key2)

    return keys


def _derive_user_keys_from_ntlm(sid: str, ntlm_hash: bytes) -> list[bytes]:
    """Derive DPAPI user keys from an NTLM hash and SID.

    Returns [HMAC-SHA1(ntlm_hash, SID_bytes)].
    """
    sid_bytes = (sid + "\0").encode("utf-16-le")
    key1 = _hmac(ntlm_hash, sid_bytes, "sha1")
    return [key1]


def _try_decrypt_master_key(
    mk_file: DPAPIMasterKeyFile, user_key: bytes
) -> bytes | None:
    """Attempt to decrypt the master key encrypted data with a single user key.

    Returns the 64-byte decrypted master key on success, or None.
    """
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    # Look up hash algorithm parameters from the parsed hash_algo name
    hash_algo_id = _HASH_NAME_TO_ID.get(mk_file.hash_algo)
    if hash_algo_id is None:
        raise MasterKeyError(f"Unsupported hash algorithm: {mk_file.hash_algo}")

    hash_name, hmac_compare_len, _block_size = _HASH_ALGO_PARAMS[hash_algo_id]

    # Cipher parameters
    cipher_params = _CIPHER_PARAMS.get(mk_file.cipher_algo)
    if cipher_params is None:
        raise MasterKeyError(f"Unsupported cipher: {mk_file.cipher_algo}")
    key_size, iv_size = cipher_params

    # Derive key material via Microsoft modified PBKDF2
    keylen = key_size + iv_size
    derived_blob = _ms_pbkdf2(user_key, mk_file.salt, keylen, mk_file.rounds, hash_name)

    crypt_key = derived_blob[:key_size]
    iv = derived_blob[key_size:][:iv_size]

    # Decrypt
    if mk_file.cipher_algo == "3des":
        cipher = Cipher(algorithms.TripleDES(crypt_key), modes.CBC(iv))
    else:
        cipher = Cipher(algorithms.AES(crypt_key), modes.CBC(iv))

    decryptor = cipher.decryptor()
    cleartext = decryptor.update(mk_file.encrypted_key) + decryptor.finalize()

    # Verify HMAC
    # cleartext layout: hmacSalt(16) + hmac(hmac_compare_len) + ... + decryptedKey(64)
    if len(cleartext) < 16 + hmac_compare_len + 64:
        return None

    hmac_salt = cleartext[:16]
    hmac_stored = cleartext[16:][:hmac_compare_len]
    decrypted_key = cleartext[-64:]

    # HMAC verification: HMAC(HMAC(user_key, hmac_salt), decrypted_key)
    hmac_key = _hmac(user_key, hmac_salt, hash_name)
    hmac_calc = _hmac(hmac_key, decrypted_key, hash_name)[:hmac_compare_len]

    if hmac_calc == hmac_stored:
        return decrypted_key
    return None


def decrypt_master_key_with_password(
    mk_file: DPAPIMasterKeyFile, password: str, sid: str
) -> MasterKeyResult:
    """Decrypt a DPAPI master key using a user password.

    Derives HMAC-SHA1(SHA1(password), SID) and HMAC-SHA1(NTLM(password), SID)
    keys and tries each.

    Raises:
        MasterKeyError: If decryption fails with all derived keys.
    """
    user_keys = _derive_user_keys_from_password(sid, password)

    for i, user_key in enumerate(user_keys):
        decrypted = _try_decrypt_master_key(mk_file, user_key)
        if decrypted is not None:
            method = "password" if i == 0 else "password_ntlm"
            return MasterKeyResult(
                guid=mk_file.guid,
                decrypted_key=decrypted,
                method=method,
                verified=True,
            )

    raise MasterKeyError(
        f"Password-based decryption failed for master key {mk_file.guid}"
    )


def decrypt_master_key_with_ntlm(
    mk_file: DPAPIMasterKeyFile, ntlm_hash: bytes, sid: str
) -> MasterKeyResult:
    """Decrypt a DPAPI master key using an NTLM hash.

    Derives HMAC-SHA1(ntlm_hash, SID) and tries decryption.

    Raises:
        MasterKeyError: If decryption fails.
    """
    if len(ntlm_hash) != 16:
        raise MasterKeyError(
            f"Invalid NTLM hash length: {len(ntlm_hash)} (expected 16)"
        )

    user_keys = _derive_user_keys_from_ntlm(sid, ntlm_hash)

    for user_key in user_keys:
        decrypted = _try_decrypt_master_key(mk_file, user_key)
        if decrypted is not None:
            return MasterKeyResult(
                guid=mk_file.guid,
                decrypted_key=decrypted,
                method="ntlm_hash",
                verified=True,
            )

    raise MasterKeyError(
        f"NTLM-based decryption failed for master key {mk_file.guid}"
    )


def decrypt_master_key_with_key(
    mk_file: DPAPIMasterKeyFile, key: bytes
) -> MasterKeyResult:
    """Decrypt a DPAPI master key using a raw key (e.g., DPAPI_SYSTEM key).

    For DPAPI_SYSTEM keys, the key is used directly — no SID-based derivation.

    Raises:
        MasterKeyError: If decryption fails.
    """
    decrypted = _try_decrypt_master_key(mk_file, key)
    if decrypted is not None:
        return MasterKeyResult(
            guid=mk_file.guid,
            decrypted_key=decrypted,
            method="dpapi_system",
            verified=True,
        )

    raise MasterKeyError(
        f"Direct key decryption failed for master key {mk_file.guid}"
    )
