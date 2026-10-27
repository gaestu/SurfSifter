"""
Small SQL building utilities shared by helpers and the UI data layer.
"""
from __future__ import annotations

# Escape character for LIKE patterns built with ``escape_like``; use it as
# ``... LIKE ? ESCAPE '\'``.
LIKE_ESCAPE_CHAR = "\\"


def escape_like(value: str) -> str:
    """Escape ``value`` so SQL LIKE matches it literally (with ``ESCAPE '\\'``).

    ``%``, ``_`` and the escape character itself are escaped; add any
    wildcards around the result yourself (e.g. ``f"%{escape_like(s)}%"``).
    """
    return (
        value.replace(LIKE_ESCAPE_CHAR, LIKE_ESCAPE_CHAR * 2)
        .replace("%", LIKE_ESCAPE_CHAR + "%")
        .replace("_", LIKE_ESCAPE_CHAR + "_")
    )
