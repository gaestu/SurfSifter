"""Guard against accidental reintroduction of src.* imports in tests."""

from __future__ import annotations

from pathlib import Path
import re


def test_no_src_namespace_imports_in_tests() -> None:
    """Ensure tests only use top-level packages (app/core/extractors/reports)."""
    tests_root = Path(__file__).resolve().parents[1]
    pattern = re.compile(r"^\s*(from|import)\s+src\.")
    failures: list[str] = []

    for path in tests_root.rglob("*.py"):
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            if pattern.search(line):
                failures.append(f"{path}:{lineno}: {line.strip()}")

    assert not failures, "src.* imports found in tests:\n" + "\n".join(failures)
