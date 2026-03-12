from __future__ import annotations

import tempfile
from pathlib import Path

from core.logging import get_logger

from ._errors import BootKeyError
from ._registry_helpers import get_class_name_safe, get_key_robust

logger = get_logger(__name__)

# Permutation table for boot key derivation
_BOOT_KEY_PERMUTATION = [8, 5, 4, 2, 11, 9, 13, 3, 0, 6, 1, 12, 14, 10, 15, 7]

# LSA subkey names whose class names form the scrambled boot key
_LSA_SUBKEYS = ("JD", "Skew1", "GBG", "Data")


def extract_boot_key(system_hive_bytes: bytes) -> bytes:
    """Extract the 16-byte boot key (SysKey) from a SYSTEM registry hive.

    Args:
        system_hive_bytes: Raw bytes of the SYSTEM registry hive.

    Returns:
        16-byte boot key after permutation.

    Raises:
        BootKeyError: If the hive cannot be parsed or required keys are missing.
    """
    try:
        from regipy.registry import RegistryHive
    except ImportError as exc:
        raise BootKeyError("regipy is required for boot key extraction") from exc

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            suffix=".hive", delete=False
        ) as tmp:
            tmp.write(system_hive_bytes)
            tmp_path = Path(tmp.name)

        hive = RegistryHive(str(tmp_path))

        # Determine active ControlSet from Select\Current
        # Try Select\Current first, then fall back to trying all control sets
        cs_candidates: list[str] = []
        try:
            select_key = hive.get_key("Select")
            for val in select_key.iter_values():
                if val.name == "Current":
                    cs_candidates.append(f"ControlSet{int(val.value):03d}")
                    break
        except Exception:
            logger.debug("Could not read Select\\Current, will try common ControlSet names")

        # Add fallbacks for ControlSet001 and ControlSet002
        for cs in ("ControlSet001", "ControlSet002"):
            if cs not in cs_candidates:
                cs_candidates.append(cs)

        lsa_key = None
        lsa_path = ""
        for cs_name in cs_candidates:
            lsa_path = f"{cs_name}\\Control\\Lsa"
            try:
                lsa_key = get_key_robust(hive, lsa_path)
                break
            except (Exception, KeyError):
                logger.debug("Could not open %s, trying next", lsa_path)
                continue

        if lsa_key is None:
            tried = ", ".join(cs_candidates)
            raise BootKeyError(f"Could not open Control\\Lsa in any ControlSet ({tried})")

        # Build subkey lookup (case-insensitive)
        subkeys_by_name: dict[str, object] = {}
        for sk in lsa_key.iter_subkeys():
            subkeys_by_name[sk.name.upper()] = sk

        # Concatenate class names from JD, Skew1, GBG, Data
        scrambled_hex = ""
        for name in _LSA_SUBKEYS:
            sk = subkeys_by_name.get(name.upper())
            if sk is None:
                raise BootKeyError(f"Missing LSA subkey: {name}")
            class_name = get_class_name_safe(sk)
            if class_name is None:
                raise BootKeyError(f"LSA subkey {name} has no class name")
            scrambled_hex += class_name

        try:
            scrambled_bytes = bytes.fromhex(scrambled_hex)
        except ValueError as exc:
            raise BootKeyError(
                f"Invalid hex in LSA class names: {scrambled_hex!r}"
            ) from exc

        if len(scrambled_bytes) < 16:
            raise BootKeyError(
                f"Scrambled key too short: {len(scrambled_bytes)} bytes"
            )

        # Apply permutation
        boot_key = bytes(scrambled_bytes[i] for i in _BOOT_KEY_PERMUTATION)
        logger.debug("Boot key extracted successfully (%d bytes)", len(boot_key))
        return boot_key

    except BootKeyError:
        raise
    except Exception as exc:
        raise BootKeyError(f"Failed to extract boot key: {exc}") from exc
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
