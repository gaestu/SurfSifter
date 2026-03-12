"""Regipy helper utilities for robust registry key navigation.

Some hives cause ``RegistryHive.get_key()`` to fail on multi-level paths
even when the subkeys exist (hash-table/encoding issues in certain hives).
This module provides a fallback that walks the key tree manually via
``iter_subkeys()``.
"""

from __future__ import annotations

from core.logging import get_logger

logger = get_logger(__name__)


def get_key_robust(hive, path: str):
    """Navigate to a registry key by *path*, with manual-walk fallback.

    Args:
        hive: A ``regipy.registry.RegistryHive`` instance (already opened).
        path: Backslash-separated key path, e.g. ``"ControlSet001\\Control\\Lsa"``.

    Returns:
        The target ``NKRecord`` key object.

    Raises:
        KeyError: If no key is found at the given path.
    """
    # 1. Fast path — try regipy's built-in lookup first
    try:
        return hive.get_key(path)
    except Exception:
        logger.debug("get_key('%s') failed, falling back to manual walk", path)

    # 2. Slow path — walk each segment via iter_subkeys()
    parts = [p for p in path.replace("/", "\\").split("\\") if p]
    current = hive.root
    for depth, part in enumerate(parts):
        found = None
        for sk in current.iter_subkeys():
            if sk.name.lower() == part.lower():
                found = sk
                break
        if found is None:
            traversed = "\\".join(parts[: depth + 1])
            raise KeyError(f"Subkey not found: {traversed}")
        current = found
    return current


def get_class_name_safe(key) -> str | None:
    """Return the class name string of a registry key, or *None*.

    Handles both the ``class_name`` property (older regipy) and the
    ``get_class_name()`` method (regipy ≥ 5.x).
    """
    # Prefer the property (fast, no I/O)
    cn = getattr(key, "class_name", None)
    if cn is not None:
        return cn or None

    # Fall back to the method (regipy 5.x NKRecord)
    getter = getattr(key, "get_class_name", None)
    if getter is not None:
        cn = getter()
        return cn or None

    return None
