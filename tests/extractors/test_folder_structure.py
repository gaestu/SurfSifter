"""
Tests for the new extractor folder structure (Phase 6).

Verifies that the re-export modules work correctly.
"""

import pytest


class TestSystemFolder:
    """Test system/ folder re-exports."""

    def test_registry_extractor_import(self):
        """SystemRegistryExtractor can be imported from system/."""
        from extractors.system import SystemRegistryExtractor
        assert hasattr(SystemRegistryExtractor, 'metadata')

    def test_jump_lists_extractor_import(self):
        """SystemJumpListsExtractor can be imported from system/."""
        from extractors.system import SystemJumpListsExtractor
        assert hasattr(SystemJumpListsExtractor, 'metadata')

    def test_platform_fingerprint_extractor_import(self):
        """No platform fingerprint extractor is exported."""
        from extractors import system
        assert not hasattr(system, 'SystemPlatformFingerprintExtractor')

    def test_system_all_exports(self):
        """system/ __all__ contains expected exports."""
        from extractors import system
        assert 'SystemRegistryExtractor' in system.__all__
        assert 'SystemJumpListsExtractor' in system.__all__


class TestMediaFolder:
    """Test media/ folder re-exports."""

    def test_filesystem_images_extractor_import(self):
        """FilesystemImagesExtractor can be imported from media/."""
        from extractors.media import FilesystemImagesExtractor
        assert hasattr(FilesystemImagesExtractor, 'metadata')

    def test_foremost_carver_extractor_import(self):
        """ForemostCarverExtractor can be imported from media/."""
        from extractors.media import ForemostCarverExtractor
        assert hasattr(ForemostCarverExtractor, 'metadata')

    def test_scalpel_extractor_import(self):
        """ScalpelExtractor can be imported from media/."""
        from extractors.media import ScalpelExtractor
        assert hasattr(ScalpelExtractor, 'metadata')

    def test_media_all_exports(self):
        """media/ __all__ contains expected exports."""
        from extractors import media
        assert 'FilesystemImagesExtractor' in media.__all__
        assert 'ForemostCarverExtractor' in media.__all__
        assert 'ScalpelExtractor' in media.__all__


class TestCarversFolder:
    """Test carvers/ folder re-exports."""

    def test_bulk_extractor_import(self):
        """BulkExtractorExtractor can be imported from carvers/."""
        from extractors.carvers import BulkExtractorExtractor
        assert hasattr(BulkExtractorExtractor, 'metadata')

    def test_browser_carver_extractor_import(self):
        """BrowserCarverExtractor can be imported from carvers/."""
        from extractors.carvers import BrowserCarverExtractor
        assert hasattr(BrowserCarverExtractor, 'metadata')

    def test_carvers_all_exports(self):
        """carvers/ __all__ contains expected exports."""
        from extractors import carvers
        assert 'BulkExtractorExtractor' in carvers.__all__
        assert 'BrowserCarverExtractor' in carvers.__all__


# NOTE: TestCacheFolder removed - cache/ facade was deleted in cleanup refactoring


class TestSystemFileListFolder:
    """Test system/file_list folder exports."""

    def test_system_file_list_extractor_import(self):
        """SystemFileListExtractor can be imported from system/file_list/."""
        from extractors.system.file_list import SystemFileListExtractor
        assert hasattr(SystemFileListExtractor, 'metadata')

    def test_legacy_alias_import(self):
        """FileListImporterExtractor alias can be imported."""
        from extractors.system.file_list import FileListImporterExtractor, SystemFileListExtractor
        assert FileListImporterExtractor is SystemFileListExtractor


class TestBrowserFolderStillWorks:
    """Verify browser/ folder still works after Phase 6."""

    def test_chromium_imports(self):
        """Chromium extractors can still be imported."""
        from extractors.browser.chromium import (
            ChromiumHistoryExtractor,
            ChromiumCookiesExtractor,
            ChromiumBookmarksExtractor,
            ChromiumDownloadsExtractor,
        )
        assert hasattr(ChromiumHistoryExtractor, 'metadata')

    def test_firefox_imports(self):
        """Firefox extractors can still be imported."""
        from extractors.browser.firefox import (
            FirefoxHistoryExtractor,
            FirefoxCookiesExtractor,
            FirefoxBookmarksExtractor,
            FirefoxDownloadsExtractor,
        )
        assert hasattr(FirefoxHistoryExtractor, 'metadata')

    def test_safari_imports(self):
        """Safari extractors can still be imported."""
        from extractors.browser.safari import (
            SafariHistoryExtractor,
            SafariCookiesExtractor,
            SafariBookmarksExtractor,
            SafariDownloadsExtractor,
        )
        assert hasattr(SafariHistoryExtractor, 'metadata')


class TestMainExtractorsModuleExports:
    """Test that extractors exports the new folder structure."""

    def test_system_module_exported(self):
        """system module is exported from extractors."""
        from extractors import system
        assert hasattr(system, 'SystemRegistryExtractor')
        assert hasattr(system, 'SystemJumpListsExtractor')

    def test_media_module_exported(self):
        """media module is exported from extractors."""
        from extractors import media
        assert hasattr(media, 'FilesystemImagesExtractor')
        assert hasattr(media, 'ForemostCarverExtractor')
        assert hasattr(media, 'ScalpelExtractor')

    def test_carvers_module_exported(self):
        """carvers module is exported from extractors."""
        from extractors import carvers
        assert hasattr(carvers, 'BulkExtractorExtractor')
        assert hasattr(carvers, 'BrowserCarverExtractor')

    # NOTE: test_cache_module_exported removed - cache/ facade was deleted
    # NOTE: test_importers_module_exported removed - importers/ folder was deleted,
    #       file_list extractor is now in system/file_list/

    def test_browser_module_exported(self):
        """browser module is exported from extractors."""
        from extractors import browser
        assert hasattr(browser, 'chromium')
        assert hasattr(browser, 'firefox')
        assert hasattr(browser, 'safari')

    def test_browser_chromium_extractors(self):
        """Chromium extractors accessible via browser.chromium."""
        from extractors import browser
        assert hasattr(browser.chromium, 'ChromiumHistoryExtractor')
        assert hasattr(browser.chromium, 'ChromiumCookiesExtractor')
        assert hasattr(browser.chromium, 'ChromiumBookmarksExtractor')
        assert hasattr(browser.chromium, 'ChromiumDownloadsExtractor')

    def test_browser_firefox_extractors(self):
        """Firefox extractors accessible via browser.firefox."""
        from extractors import browser
        assert hasattr(browser.firefox, 'FirefoxHistoryExtractor')
        assert hasattr(browser.firefox, 'FirefoxCookiesExtractor')
        assert hasattr(browser.firefox, 'FirefoxBookmarksExtractor')
        assert hasattr(browser.firefox, 'FirefoxDownloadsExtractor')

    def test_browser_safari_extractors(self):
        """Safari extractors accessible via browser.safari."""
        from extractors import browser
        assert hasattr(browser.safari, 'SafariHistoryExtractor')
        assert hasattr(browser.safari, 'SafariCookiesExtractor')
        assert hasattr(browser.safari, 'SafariBookmarksExtractor')
        assert hasattr(browser.safari, 'SafariDownloadsExtractor')
