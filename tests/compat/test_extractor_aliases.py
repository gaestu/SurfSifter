"""Compatibility tests for legacy extractor aliases."""
from __future__ import annotations

import importlib

import pytest


pytestmark = pytest.mark.compat


ALIASES = [
    (
        "extractors.browser.chromium.cache",
        "ChromiumCacheExtractor",
        "CacheSimpleExtractor",
    ),
    (
        "extractors.browser.firefox.cache",
        "FirefoxCacheExtractor",
        "CacheFirefoxExtractor",
    ),
    (
        "extractors.browser.chromium.media_history",
        "ChromiumMediaHistoryExtractor",
        "MediaHistoryExtractor",
    ),
    (
        "extractors.system.file_list",
        "FileListImporterExtractor",
        "SystemFileListExtractor",
    ),
]


@pytest.mark.parametrize("module_path, alias_name, canonical_name", ALIASES)
def test_alias_points_to_canonical(module_path: str, alias_name: str, canonical_name: str) -> None:
    module = importlib.import_module(module_path)
    alias_cls = getattr(module, alias_name)
    canonical_cls = getattr(module, canonical_name)
    assert alias_cls is canonical_cls
