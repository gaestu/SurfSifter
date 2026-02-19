"""Centralized extractor registry discovery tests."""
from __future__ import annotations

import pkgutil
import sys

import pytest

from extractors import ExtractorRegistry
from extractors.browser.chromium.autofill import ChromiumAutofillExtractor
from extractors.browser.chromium.bookmarks import ChromiumBookmarksExtractor
from extractors.browser.chromium.cookies import ChromiumCookiesExtractor
from extractors.browser.chromium.downloads import ChromiumDownloadsExtractor
from extractors.browser.firefox.autofill import FirefoxAutofillExtractor
from extractors.browser.firefox.bookmarks import FirefoxBookmarksExtractor
from extractors.browser.firefox.cookies import FirefoxCookiesExtractor
from extractors.browser.firefox.downloads import FirefoxDownloadsExtractor
from extractors.browser.firefox.history import FirefoxHistoryExtractor
from extractors.browser.safari.bookmarks import SafariBookmarksExtractor
from extractors.browser.safari.cookies import SafariCookiesExtractor
from extractors.browser.safari.downloads import SafariDownloadsExtractor
from extractors.browser.safari.favicons import SafariFaviconsExtractor
from extractors.browser.safari.history import SafariHistoryExtractor
from extractors.carvers.bulk_extractor import BulkExtractorExtractor


EXPECTED_NAMES = {
    "bulk_extractor",
    "browser_carver",
    "cache_simple",
    "cache_firefox",
    "chromium_history",
    "firefox_history",
    "safari_history",
    "file_list",
    "filesystem_images",
    "foremost_carver",
    "scalpel",
    "system_registry",
    "system_jump_lists",
}

EXPECTED_FIREFOX = {
    "firefox_history",
    "firefox_cookies",
    "firefox_bookmarks",
    "firefox_downloads",
}

EXPECTED_SAFARI = {
    "safari_history",
    "safari_cookies",
    "safari_bookmarks",
    "safari_downloads",
    "safari_favicons",
}

REGISTRY_INSTANCES = [
    ("bulk_extractor", BulkExtractorExtractor),
    ("chromium_autofill", ChromiumAutofillExtractor),
    ("chromium_bookmarks", ChromiumBookmarksExtractor),
    ("chromium_cookies", ChromiumCookiesExtractor),
    ("chromium_downloads", ChromiumDownloadsExtractor),
    ("firefox_autofill", FirefoxAutofillExtractor),
    ("firefox_bookmarks", FirefoxBookmarksExtractor),
    ("firefox_cookies", FirefoxCookiesExtractor),
    ("firefox_downloads", FirefoxDownloadsExtractor),
    ("firefox_history", FirefoxHistoryExtractor),
    ("safari_bookmarks", SafariBookmarksExtractor),
    ("safari_cookies", SafariCookiesExtractor),
    ("safari_downloads", SafariDownloadsExtractor),
    ("safari_favicons", SafariFaviconsExtractor),
    ("safari_history", SafariHistoryExtractor),
]


def test_registry_discovers_extractors(extractor_registry_all):
    """Registry discovers at least some extractors."""
    assert len(extractor_registry_all) > 0, "Registry should discover extractors"


def test_registry_discovery_fallback_in_frozen_mode(monkeypatch):
    """Frozen-mode fallback still discovers extractors when iter_modules yields nothing."""
    monkeypatch.setattr(pkgutil, "iter_modules", lambda *args, **kwargs: iter(()))
    monkeypatch.setattr(sys, "frozen", True, raising=False)

    registry = ExtractorRegistry()

    assert registry.count() > 0
    assert "bulk_extractor" in registry.list_names()
    assert "file_list" in registry.list_names()


def test_registry_minimum_count(extractor_registry_all):
    """Registry discovers expected number of extractors."""
    assert len(extractor_registry_all) >= 35


def test_registry_has_expected_names(extractor_registry_names):
    """Core extractors are discoverable by name."""
    for name in EXPECTED_NAMES:
        assert name in extractor_registry_names


def test_registry_has_firefox_extractors(extractor_registry_names):
    """All Firefox extractors are discoverable."""
    for name in EXPECTED_FIREFOX:
        assert name in extractor_registry_names


def test_registry_has_safari_extractors(extractor_registry_names):
    """All Safari extractors are discoverable."""
    for name in EXPECTED_SAFARI:
        assert name in extractor_registry_names


def test_registry_no_duplicates(extractor_registry_names):
    """Registry does not return duplicate names."""
    assert len(extractor_registry_names) == len(set(extractor_registry_names)), (
        "Duplicates found: "
        f"{[name for name in extractor_registry_names if extractor_registry_names.count(name) > 1]}"
    )


@pytest.mark.parametrize("name,cls", REGISTRY_INSTANCES)
def test_registry_get_returns_instance(extractor_registry, name, cls):
    """Registry returns instances of expected extractor classes."""
    extractor = extractor_registry.get(name)
    assert extractor is not None
    assert isinstance(extractor, cls)


def test_registry_metadata_structure(extractor_registry_all):
    """All extractors have required metadata fields."""
    for extractor in extractor_registry_all:
        assert hasattr(extractor, "metadata"), f"{extractor} missing metadata"
        assert hasattr(extractor.metadata, "name"), f"{extractor} missing metadata.name"
        assert hasattr(extractor.metadata, "can_extract"), f"{extractor} missing can_extract"
        assert hasattr(extractor.metadata, "can_ingest"), f"{extractor} missing can_ingest"
