from __future__ import annotations

import struct
import tempfile
from pathlib import Path
from typing import Dict

from core.logging import get_logger

from ._errors import SAMError
from ._registry_helpers import get_key_robust
from ._types import NTLMHashResult

logger = get_logger(__name__)

# Well-known constants for SAM decryption
_QWERTY = b"!@#$%^&*()qwertyUIOPAzxcvbnmQQQQQQQQQQQQ)(*@&%\0"
_DIGITS = b"0123456789012345678901234567890123456789\0"


def _parse_sid_binary(data: bytes) -> str | None:
    """Parse a binary SID into its string form (e.g., S-1-5-21-...).

    Binary SID layout:
      revision(1), sub_authority_count(1), authority(6 big-endian),
      sub_authorities(4*count little-endian)
    """
    if len(data) < 8:
        return None
    revision = data[0]
    sub_count = data[1]
    if len(data) < 8 + 4 * sub_count:
        return None
    # Authority: 6 bytes big-endian (typically 5 for NT Authority)
    authority = int.from_bytes(data[2:8], "big")
    parts = [f"S-{revision}-{authority}"]
    for i in range(sub_count):
        offset = 8 + 4 * i
        sub = struct.unpack_from("<I", data, offset)[0]
        parts.append(str(sub))
    return "-".join(parts)




def _rid_to_des_keys(rid: int) -> tuple[bytes, bytes]:
    """Convert a RID to two 8-byte DES keys for NTLM hash decryption."""
    s = rid.to_bytes(4, "little")
    # Spread 7 bytes into 8 bytes with parity for each DES key
    key1_raw = bytes([
        s[0] >> 1,
        ((s[0] & 0x01) << 6) | (s[1] >> 2),
        ((s[1] & 0x03) << 5) | (s[2] >> 3),
        ((s[2] & 0x07) << 4) | (s[3] >> 4),
        ((s[3] & 0x0F) << 3) | (s[0] >> 5),  # wraps around
        ((s[0] & 0x1F) << 2) | (s[1] >> 6),
        ((s[1] & 0x3F) << 1) | (s[2] >> 7),
        s[2] & 0x7F,
    ])

    key2_raw = bytes([
        s[3] >> 1,
        ((s[3] & 0x01) << 6) | (s[0] >> 2),
        ((s[0] & 0x03) << 5) | (s[1] >> 3),
        ((s[1] & 0x07) << 4) | (s[2] >> 4),
        ((s[2] & 0x0F) << 3) | (s[3] >> 5),
        ((s[3] & 0x1F) << 2) | (s[0] >> 6),
        ((s[0] & 0x3F) << 1) | (s[1] >> 7),
        s[1] & 0x7F,
    ])

    return _add_parity(key1_raw), _add_parity(key2_raw)


def _add_parity(key_56: bytes) -> bytes:
    """Add odd parity bits to a 7-byte key to produce an 8-byte DES key."""
    result = bytearray(8)
    for i in range(8):
        b = key_56[i]
        # Shift left by 1 and set parity bit
        b = (b << 1) & 0xFE
        # Count bits and add odd parity
        parity = bin(b).count("1")
        if parity % 2 == 0:
            b |= 1
        result[i] = b
    return bytes(result)


def _decrypt_aes_sam_key(f_value: bytes, boot_key: bytes) -> bytes:
    """Decrypt SAM encryption key from F value using AES (post-Win10 1607)."""
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    # F value layout for AES revision:
    # Offset 0x58: revision (4 bytes)
    # Offset 0x70: salt (16 bytes)
    # Offset 0x80: encrypted key (16 bytes)  -- AES-CBC encrypted with boot_key
    if len(f_value) < 0xA0:
        raise SAMError(f"F value too short for AES SAM key: {len(f_value)} bytes")

    salt = f_value[0x70:0x80]
    encrypted = f_value[0x80:0xA0]

    cipher = Cipher(algorithms.AES(boot_key), modes.CBC(salt))
    decryptor = cipher.decryptor()
    decrypted = decryptor.update(encrypted) + decryptor.finalize()
    return decrypted[:16]


def _decrypt_rc4_sam_key(f_value: bytes, boot_key: bytes) -> bytes:
    """Decrypt SAM encryption key from F value using RC4 (pre-Win10 1607)."""
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms

    if len(f_value) < 0xA0:
        raise SAMError(f"F value too short for RC4 SAM key: {len(f_value)} bytes")

    rc4_key_material = f_value[0x70:0x80]

    md5 = hashes.Hash(hashes.MD5())
    md5.update(boot_key)
    md5.update(rc4_key_material)
    md5.update(_QWERTY)
    md5.update(rc4_key_material)
    md5.update(_DIGITS)
    rc4_key = md5.finalize()

    cipher = Cipher(algorithms.ARC4(rc4_key), mode=None)
    decryptor = cipher.decryptor()
    decrypted = decryptor.update(f_value[0x80:0xA0]) + decryptor.finalize()
    return decrypted[:16]


def _decrypt_ntlm_hash_aes(
    encrypted: bytes, sam_key: bytes, rid: int
) -> bytes:
    """Decrypt NTLM hash using AES (post-Win10 1607)."""
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    if len(encrypted) < 24:
        raise SAMError(f"Encrypted NTLM data too short: {len(encrypted)} bytes")

    # AES-encrypted hash layout: revision(2) + data_len(2) + checks(4) + salt(16) + encrypted(remaining)
    # Actually the V-value per-user hash block for AES:
    # After locating the hash block: revision(2), length(2), check(4), salt(16), data(16)
    salt = encrypted[8:24]
    cipher_data = encrypted[24:]

    if len(cipher_data) < 16:
        # No encrypted hash data → account has blank/empty password
        # Return the well-known NTLM hash of the empty string
        return bytes.fromhex("31d6cfe0d16ae931b73c59d7e0c089c0")

    cipher = Cipher(algorithms.AES(sam_key), modes.CBC(salt))
    decryptor = cipher.decryptor()
    decrypted = decryptor.update(cipher_data) + decryptor.finalize()

    # DES-decrypt the final 16-byte hash with RID-derived keys
    return _des_decrypt_hash(decrypted[:16], rid)


def _decrypt_ntlm_hash_rc4(
    encrypted: bytes, sam_key: bytes, rid: int
) -> bytes:
    """Decrypt NTLM hash using RC4 (pre-Win10 1607)."""
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms

    if len(encrypted) < 20:
        raise SAMError(f"Encrypted NTLM data too short: {len(encrypted)} bytes")

    # Skip first 4 bytes (revision/check), next 16 is RC4-encrypted
    hash_data = encrypted[4:20]

    md5 = hashes.Hash(hashes.MD5())
    md5.update(sam_key)
    md5.update(rid.to_bytes(4, "little"))
    md5.update(b"NTPASSWORD\0")
    rc4_key = md5.finalize()

    cipher = Cipher(algorithms.ARC4(rc4_key), mode=None)
    decryptor = cipher.decryptor()
    decrypted = decryptor.update(hash_data) + decryptor.finalize()

    return _des_decrypt_hash(decrypted, rid)


def _des_decrypt_hash(encrypted_hash: bytes, rid: int) -> bytes:
    """Final DES decryption step using RID-derived keys."""
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    if len(encrypted_hash) < 16:
        return b""

    key1, key2 = _rid_to_des_keys(rid)

    # TripleDES with identical subkeys (K1=K2=K3=K) is cryptographically
    # equivalent to single DES — used here because cryptography has no DES class.
    cipher1 = Cipher(algorithms.TripleDES(key1 + key1 + key1[:8]), modes.ECB())
    dec1 = cipher1.decryptor()
    part1 = dec1.update(encrypted_hash[:8]) + dec1.finalize()

    cipher2 = Cipher(algorithms.TripleDES(key2 + key2 + key2[:8]), modes.ECB())
    dec2 = cipher2.decryptor()
    part2 = dec2.update(encrypted_hash[8:16]) + dec2.finalize()

    return part1 + part2


def _parse_v_value(v_data: bytes) -> tuple[str, int, int, int, int]:
    """Parse SAM V value to extract username offset/length and NTLM hash offset/length.

    Returns:
        (username, ntlm_offset, ntlm_length, lm_offset, lm_length)
    """
    if len(v_data) < 0xCC + 4:
        raise SAMError(f"V value too short: {len(v_data)} bytes")

    # V value structure contains offsets relative to 0xCC
    # Username: offset at 0x0C (4 bytes), length at 0x10 (4 bytes)
    # LM hash:  offset at 0x9C (4 bytes), length at 0xA0 (4 bytes)
    # NT hash:  offset at 0xA8 (4 bytes), length at 0xAC (4 bytes)
    name_offset = struct.unpack_from("<I", v_data, 0x0C)[0] + 0xCC
    name_length = struct.unpack_from("<I", v_data, 0x10)[0]

    lm_offset = struct.unpack_from("<I", v_data, 0x9C)[0] + 0xCC
    lm_length = struct.unpack_from("<I", v_data, 0xA0)[0]

    nt_offset = struct.unpack_from("<I", v_data, 0xA8)[0] + 0xCC
    nt_length = struct.unpack_from("<I", v_data, 0xAC)[0]

    if name_offset + name_length > len(v_data):
        raise SAMError("Username extends beyond V value")

    try:
        username = v_data[name_offset : name_offset + name_length].decode(
            "utf-16-le"
        )
    except UnicodeDecodeError:
        username = "<unknown>"

    return username, nt_offset, nt_length, lm_offset, lm_length


def extract_ntlm_hashes(
    sam_hive_bytes: bytes, boot_key: bytes
) -> Dict[str, NTLMHashResult]:
    """Extract NTLM hashes from a SAM registry hive.

    Args:
        sam_hive_bytes: Raw bytes of the SAM registry hive.
        boot_key: 16-byte boot key from the SYSTEM hive.

    Returns:
        Dictionary mapping SID strings to NTLMHashResult.

    Raises:
        SAMError: If the hive cannot be parsed.
    """
    try:
        from regipy.registry import RegistryHive
    except ImportError as exc:
        raise SAMError("regipy is required for SAM hash extraction") from exc

    if len(boot_key) != 16:
        raise SAMError(f"Invalid boot key length: {len(boot_key)} (expected 16)")

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".hive", delete=False) as tmp:
            tmp.write(sam_hive_bytes)
            tmp_path = Path(tmp.name)

        hive = RegistryHive(str(tmp_path))

        # Get domain SID from SAM\Domains\Account\V
        try:
            account_key = get_key_robust(hive, "SAM\\Domains\\Account")
        except (Exception, KeyError) as exc:
            raise SAMError(f"Cannot open SAM\\Domains\\Account: {exc}") from exc

        # Get F value for SAM encryption key
        # Use get_value() instead of iter_values() — regipy 5.x iter_values()
        # truncates REG_BINARY data at 128 bytes, but get_value() returns full bytes.
        f_value = account_key.get_value("F")
        if f_value is None:
            raise SAMError("SAM\\Domains\\Account\\F value not found")
        if isinstance(f_value, str):
            f_value = bytes.fromhex(f_value)

        # Determine encryption revision and decrypt SAM key
        if len(f_value) < 0x70:
            raise SAMError(f"F value too short: {len(f_value)} bytes")

        revision = struct.unpack_from("<I", f_value, 0x68)[0]
        if revision >= 2:
            sam_key = _decrypt_aes_sam_key(f_value, boot_key)
        else:
            sam_key = _decrypt_rc4_sam_key(f_value, boot_key)

        # Get domain SID from V value
        v_data = account_key.get_value("V")
        if isinstance(v_data, str):
            v_data = bytes.fromhex(v_data)

        domain_sid = None
        if v_data and len(v_data) >= 24:
            # The Account domain V value has a variable-length header of
            # descriptor entries followed by data.  Rather than assuming
            # fixed descriptor offsets (which differ between domain-level
            # and user-level V values), scan for the S-1-5-21 SID pattern
            # (revision=1, sub_count=4, authority=5, first_sub=21).
            _SID_MARKER = b"\x01\x04\x00\x00\x00\x00\x00\x05\x15\x00\x00\x00"
            pos = v_data.find(_SID_MARKER)
            if pos >= 0 and pos + 24 <= len(v_data):
                sid_bytes = v_data[pos:pos + 24]
                candidate = _parse_sid_binary(sid_bytes)
                if candidate and candidate.startswith("S-1-5-21-") and candidate.count("-") == 6:
                    domain_sid = candidate

        # Iterate user keys
        results: Dict[str, NTLMHashResult] = {}
        try:
            users_key = get_key_robust(hive, "SAM\\Domains\\Account\\Users")
        except (Exception, KeyError) as exc:
            raise SAMError(
                f"Cannot open SAM\\Domains\\Account\\Users: {exc}"
            ) from exc

        for user_key in users_key.iter_subkeys():
            name = user_key.name
            if name == "Names":
                continue

            try:
                rid = int(name, 16)
            except ValueError:
                continue

            v_value = user_key.get_value("V")
            if v_value is None:
                continue
            if isinstance(v_value, str):
                v_value = bytes.fromhex(v_value)

            try:
                username, nt_offset, nt_length, lm_offset, lm_length = (
                    _parse_v_value(v_value)
                )
            except SAMError:
                logger.debug("Failed to parse V value for RID %d", rid)
                continue

            ntlm_hash = None
            lm_hash = None

            # Decrypt NTLM hash
            if nt_length > 4:
                nt_data = v_value[nt_offset : nt_offset + nt_length]
                try:
                    if revision >= 2:
                        ntlm_hash = _decrypt_ntlm_hash_aes(nt_data, sam_key, rid)
                    else:
                        ntlm_hash = _decrypt_ntlm_hash_rc4(nt_data, sam_key, rid)
                    if len(ntlm_hash) != 16:
                        ntlm_hash = None
                except Exception:
                    logger.debug(
                        "Failed to decrypt NTLM hash for RID %d", rid
                    )

            # Decrypt LM hash (if present)
            if lm_length > 4:
                lm_data = v_value[lm_offset : lm_offset + lm_length]
                try:
                    if revision >= 2:
                        lm_hash = _decrypt_ntlm_hash_aes(lm_data, sam_key, rid)
                    else:
                        lm_hash = _decrypt_ntlm_hash_rc4(lm_data, sam_key, rid)
                    if len(lm_hash) != 16:
                        lm_hash = None
                except Exception:
                    logger.debug("Failed to decrypt LM hash for RID %d", rid)

            # Construct SID — use placeholder domain SID if not found
            sid = f"S-1-5-21-unknown-{rid}"
            if domain_sid:
                sid = f"{domain_sid}-{rid}"

            results[sid] = NTLMHashResult(
                rid=rid,
                username=username,
                sid=sid,
                ntlm_hash=ntlm_hash,
                lm_hash=lm_hash,
            )

        logger.debug("Extracted %d user hashes from SAM", len(results))
        return results

    except SAMError:
        raise
    except Exception as exc:
        raise SAMError(f"Failed to extract NTLM hashes: {exc}") from exc
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
