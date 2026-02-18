"""
NSKeyedArchiver deserialization helpers for Safari extractors.

This module provides a focused stdlib-only decoder for binary plist blobs that
use NSKeyedArchiver object graphs.
"""

from __future__ import annotations

import plistlib
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin

from core.logging import get_logger

LOGGER = get_logger("extractors.browser.safari.nska")


def resolve_uid(value: Any, objects: List[Any]) -> Any:
    """Resolve plistlib.UID against the $objects list when possible."""
    if isinstance(value, plistlib.UID):
        try:
            index = int(value.data)
        except (TypeError, ValueError):
            return value
        if 0 <= index < len(objects):
            return objects[index]
    return value


def decode_ns_dict(node: Dict[Any, Any], objects: List[Any]) -> Dict[str, Any]:
    """Decode NS.keys/NS.objects dictionary representation."""
    raw_keys = node.get("NS.keys")
    raw_values = node.get("NS.objects")
    if not isinstance(raw_keys, list) or not isinstance(raw_values, list):
        return {}

    decoded: Dict[str, Any] = {}
    for key_obj, value_obj in zip(raw_keys, raw_values):
        key_resolved = _decode_node(key_obj, objects, set())
        if not isinstance(key_resolved, str) or not key_resolved.strip():
            continue
        decoded[key_resolved.strip()] = resolve_uid(value_obj, objects)
    return decoded


def deserialize_nska(blob: bytes) -> Optional[Dict[str, Any]]:
    """
    Deserialize an NSKeyedArchiver binary plist into plain Python objects.

    Returns:
        A decoded mapping on success, or None on parse/decode failure.
    """
    if not blob:
        return None

    try:
        archive = plistlib.loads(blob)
    except Exception:
        return None

    try:
        if isinstance(archive, dict):
            objects_raw = archive.get("$objects", [])
            objects = objects_raw if isinstance(objects_raw, list) else []

            if objects:
                top = archive.get("$top")
                if isinstance(top, dict) and top:
                    root_value = top.get("root")
                    if root_value is None:
                        root_value = next(iter(top.values()))
                    decoded = _decode_node(root_value, objects, set())
                else:
                    decoded = _decode_node(archive, objects, set())
            else:
                decoded = _decode_node(archive, [], set())
        else:
            decoded = _decode_node(archive, [], set())

        if isinstance(decoded, dict):
            return decoded
        return {"root": decoded}
    except Exception as exc:
        LOGGER.debug("Failed to decode NSKeyedArchiver blob: %s", exc)
        return None


def _decode_node(value: Any, objects: List[Any], seen: Set[int]) -> Any:
    """Recursively decode archive objects."""
    value = resolve_uid(value, objects)

    if isinstance(value, (str, int, float, bool, bytes, type(None))):
        return value

    if isinstance(value, list):
        marker = id(value)
        if marker in seen:
            return []
        seen.add(marker)
        return [_decode_node(item, objects, seen) for item in value]

    if isinstance(value, tuple):
        marker = id(value)
        if marker in seen:
            return ()
        seen.add(marker)
        return tuple(_decode_node(item, objects, seen) for item in value)

    if isinstance(value, dict):
        marker = id(value)
        if marker in seen:
            return {}
        seen.add(marker)

        ns_dict = decode_ns_dict(value, objects)
        if ns_dict:
            decoded = {k: _decode_node(v, objects, seen) for k, v in ns_dict.items()}
            _augment_url_fields(decoded)
            return decoded

        decoded_obj: Dict[str, Any] = {}
        for key, item in value.items():
            key_resolved = _decode_node(key, objects, seen)
            if isinstance(key_resolved, bytes):
                key_str = key_resolved.decode("utf-8", errors="ignore")
            else:
                key_str = str(key_resolved)
            decoded_obj[key_str] = _decode_node(item, objects, seen)

        _augment_url_fields(decoded_obj)
        return decoded_obj

    return value


def _augment_url_fields(mapping: Dict[str, Any]) -> None:
    """Add convenience URL field when NS.base/NS.relative appears."""
    relative = mapping.get("NS.relative")
    base = mapping.get("NS.base")
    string_value = mapping.get("NS.string")

    if isinstance(relative, str) and relative:
        if "://" in relative or relative.startswith(("about:", "data:", "file:", "blob:")):
            mapping.setdefault("url", relative)
        elif isinstance(base, str) and base:
            mapping.setdefault("url", urljoin(base, relative))

    if isinstance(string_value, str) and string_value:
        mapping.setdefault("url", string_value)
