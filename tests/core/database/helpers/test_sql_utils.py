"""Shared LIKE escaping helper (issue #67)."""
import sqlite3

import pytest

from core.database.helpers import LIKE_ESCAPE_CHAR, escape_like


@pytest.mark.parametrize(
    "value, expected",
    [
        ("plain", "plain"),
        ("50%", "50\\%"),
        ("a_b", "a\\_b"),
        ("a\\b", "a\\\\b"),
        ("\\%_", "\\\\\\%\\_"),
        ("", ""),
    ],
)
def test_escape_like_escapes_metacharacters(value, expected):
    assert escape_like(value) == expected


@pytest.mark.parametrize("needle", ["%", "_", "\\", "a%b", "a_b", "a\\b", "\\%"])
def test_escaped_pattern_matches_literally(needle):
    conn = sqlite3.connect(":memory:")
    haystack = ["a%b", "axb", "a_b", "a\\b", "x\\%y", "plain"]
    rows = [
        value
        for value in haystack
        if conn.execute(
            f"SELECT ? LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'", (value, f"%{escape_like(needle)}%")
        ).fetchone()[0]
    ]
    assert rows == [value for value in haystack if needle.lower() in value.lower()]
    conn.close()
