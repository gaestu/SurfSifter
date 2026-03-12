"""LSA secrets extraction — extract DPAPI_SYSTEM keys from SECURITY hive."""

from __future__ import annotations

import struct
import tempfile
from pathlib import Path

from core.logging import get_logger

from ._errors import LSAError
from ._registry_helpers import get_key_robust
from ._types import DPAPISystemKeys

logger = get_logger(__name__)


def _derive_lsa_aes_key(base_key: bytes, salt: bytes) -> bytes:
    """Derive AES-256 key for LSA decryption: SHA256(base_key + salt*1000)."""
    import hashlib

    h = hashlib.sha256()
    h.update(base_key)
    for _ in range(1000):
        h.update(salt)
    return h.digest()


def _decrypt_aes_per_block(key: bytes, value: bytes) -> bytes:
    """Decrypt using per-16-byte-block AES-CBC with zero IV (impacket pattern).

    Each 16-byte block is decrypted independently with a fresh AES-CBC cipher
    using an all-zero IV. This is how Windows LSA decrypts secrets.
    """
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    plaintext = b""
    zero_iv = b"\x00" * 16
    for i in range(0, len(value), 16):
        block = value[i:i + 16]
        if len(block) < 16:
            break
        cipher = Cipher(algorithms.AES(key), modes.CBC(zero_iv))
        decryptor = cipher.decryptor()
        plaintext += decryptor.update(block) + decryptor.finalize()
    return plaintext


def _decrypt_lsa_secret(base_key: bytes, encrypted_data: bytes) -> bytes:
    """Decrypt an LSA secret using the Vista+ pattern.

    Encrypted data layout (after 28-byte outer header):
      salt (32 bytes) + ciphertext (rest)

    Decryption: AES key = SHA256(base_key + salt*1000), per-block AES-CBC.
    """
    if len(encrypted_data) < 60:  # 28 header + 32 salt minimum
        raise LSAError(
            f"LSA encrypted data too short: {len(encrypted_data)} bytes"
        )

    enc_body = encrypted_data[28:]
    salt = enc_body[:32]
    ciphertext = enc_body[32:]

    aes_key = _derive_lsa_aes_key(base_key, salt)
    return _decrypt_aes_per_block(aes_key, ciphertext)


def _extract_lsa_key(security_hive_bytes: bytes, boot_key: bytes) -> bytes:
    """Extract the LSA decryption key from the SECURITY hive.

    Reads Policy\\PolEKList (Vista+) and decrypts with boot-key-derived AES key.
    The decrypted data contains: length(4) + ... + secret at offset 16.
    Within the secret, the 32-byte LSA key is at offset 52.

    Raises:
        LSAError: If the key cannot be extracted.
    """
    try:
        from regipy.registry import RegistryHive
    except ImportError as exc:
        raise LSAError("regipy is required for LSA extraction") from exc

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".hive", delete=False) as tmp:
            tmp.write(security_hive_bytes)
            tmp_path = Path(tmp.name)

        hive = RegistryHive(str(tmp_path))

        # Try Vista+ path first: Policy\PolEKList
        try:
            pol_key = get_key_robust(hive, "Policy\\PolEKList")
            encrypted_data = pol_key.get_value("(default)")
            if encrypted_data is None:
                # Try unnamed value
                for val in pol_key.iter_values():
                    if val.name in ("(default)", "", "(Default)"):
                        encrypted_data = val.value
                        break
            if encrypted_data is None:
                raise LSAError("PolEKList default value not found")
        except KeyError:
            raise LSAError(
                "Policy\\PolEKList not found — pre-Vista systems not supported"
            )

        if isinstance(encrypted_data, str):
            encrypted_data = bytes.fromhex(encrypted_data)

        if len(encrypted_data) < 76:
            raise LSAError(
                f"PolEKList data too short: {len(encrypted_data)} bytes"
            )

        # Decrypt PolEKList: salt(32) + ciphertext after 28-byte header
        decrypted = _decrypt_lsa_secret(boot_key, encrypted_data)

        # Decrypted structure: secret_len(4) + padding(12) + secret(secret_len)
        # Within the secret, LSA key is at offset 52, 32 bytes
        secret_len = struct.unpack_from("<I", decrypted, 0)[0]
        secret = decrypted[16:16 + secret_len]

        if len(secret) < 84:
            raise LSAError(
                f"Decrypted PolEKList secret too short: {len(secret)} bytes"
            )

        lsa_key = secret[52:84]
        return lsa_key

    except LSAError:
        raise
    except Exception as exc:
        raise LSAError(f"Failed to extract LSA key: {exc}") from exc
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass


def extract_dpapi_system_keys(
    security_hive_bytes: bytes, boot_key: bytes
) -> DPAPISystemKeys:
    """Extract DPAPI_SYSTEM machine and user keys from the SECURITY hive.

    Algorithm:
    1. Extract LSA key from Policy\\PolEKList using boot key
    2. Read Policy\\Secrets\\DPAPI_SYSTEM\\CurrVal
    3. Decrypt with LSA key (per-block AES)
    4. Parse: skip first 16 bytes of plaintext, then:
       version(4) at offset 0, machine_key(20) at offset 4, user_key(20) at offset 24

    Args:
        security_hive_bytes: Raw bytes of the SECURITY registry hive.
        boot_key: 16-byte boot key from the SYSTEM hive.

    Returns:
        DPAPISystemKeys with machine_key and user_key (20 bytes each).

    Raises:
        LSAError: If extraction fails.
    """
    try:
        from regipy.registry import RegistryHive
    except ImportError as exc:
        raise LSAError("regipy is required for LSA extraction") from exc

    if len(boot_key) != 16:
        raise LSAError(f"Invalid boot key length: {len(boot_key)} (expected 16)")

    lsa_key = _extract_lsa_key(security_hive_bytes, boot_key)

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".hive", delete=False) as tmp:
            tmp.write(security_hive_bytes)
            tmp_path = Path(tmp.name)

        hive = RegistryHive(str(tmp_path))

        # Read DPAPI_SYSTEM secret
        try:
            secret_key = get_key_robust(
                hive, "Policy\\Secrets\\DPAPI_SYSTEM\\CurrVal"
            )
            encrypted_secret = secret_key.get_value("(default)")
            if encrypted_secret is None:
                for val in secret_key.iter_values():
                    if val.name in ("(default)", "", "(Default)"):
                        encrypted_secret = val.value
                        break
            if encrypted_secret is None:
                raise LSAError("DPAPI_SYSTEM CurrVal default value not found")
        except KeyError as exc:
            raise LSAError(
                f"Policy\\Secrets\\DPAPI_SYSTEM\\CurrVal not found: {exc}"
            ) from exc

        if isinstance(encrypted_secret, str):
            encrypted_secret = bytes.fromhex(encrypted_secret)

        if len(encrypted_secret) < 60:
            raise LSAError(
                f"DPAPI_SYSTEM encrypted data too short: {len(encrypted_secret)} bytes"
            )

        # Decrypt secret using LSA key (same salt+derive pattern)
        decrypted = _decrypt_lsa_secret(lsa_key, encrypted_secret)

        # Decrypted structure: length(4) + padding(12) + secret(length)
        # Secret structure: version(4) + machine_key(20) + user_key(20) = 44 bytes
        # Note: the original debug script labels offset 4 as "user_key" and offset 24
        # as "machine_key", but pypykatz labels them the opposite way:
        #   offset 4 = machine_key, offset 24 = user_key
        # We follow pypykatz's convention (matching Microsoft documentation).
        secret_len = struct.unpack_from("<I", decrypted, 0)[0]
        dpapi_secret = decrypted[16:16 + secret_len]

        if len(dpapi_secret) < 44:
            raise LSAError(
                f"Decrypted DPAPI_SYSTEM secret too short: {len(dpapi_secret)} bytes"
            )

        # version = struct.unpack_from("<I", dpapi_secret, 0)[0]
        machine_key = dpapi_secret[4:24]
        user_key = dpapi_secret[24:44]

        logger.debug(
            "Extracted DPAPI_SYSTEM keys: machine=%s... user=%s...",
            machine_key[:4].hex(),
            user_key[:4].hex(),
        )

        return DPAPISystemKeys(machine_key=machine_key, user_key=user_key)

    except LSAError:
        raise
    except Exception as exc:
        raise LSAError(f"Failed to extract DPAPI_SYSTEM keys: {exc}") from exc
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
