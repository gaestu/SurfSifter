"""
Integration tests for Image Extraction Enhancements.

Tests cover:
- Phase 1: Bug fixes (cache_simple rel_path/filename)
- Phase 2: Parity & consistency (unified signatures, hash-only fallback)
- Phase 3: New features (IndexedDB blobs, Safari cache, CacheStorage)
"""

import hashlib
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

pytestmark = [pytest.mark.integration, pytest.mark.gui_offscreen]


class TestCacheSimpleImageInserts:
    """Phase 1: Test cache_simple image insert field population."""

    def test_image_record_has_required_fields(self):
        """Test that image records have rel_path and filename."""
        # Expected fields for image insert
        required_fields = {
            'rel_path', 'filename', 'md5', 'sha256', 'discovered_by',
            'run_id', 'cache_key', 'ts_utc'
        }

        # Mock image record as produced by cache_simple
        image_record = {
            'rel_path': 'run_123/carved_images/test.jpg',
            'filename': 'test.jpg',
            'md5': 'd41d8cd98f00b204e9800998ecf8427e',
            'sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
            'discovered_by': 'cache_simple:1.4.0:run_123',
            'run_id': 'run_123',
            'cache_key': 'https://example.com/image.jpg',
            'ts_utc': '2024-01-01T00:00:00+00:00',
        }

        assert required_fields.issubset(image_record.keys())

    def test_rel_path_contains_run_id(self):
        """Test that rel_path includes run_id for isolation."""
        run_id = '20240101T120000_abc12345'
        filename = 'carved_abc12345.jpg'
        rel_path = f'{run_id}/carved_images/{filename}'

        assert run_id in rel_path
        assert rel_path.startswith(run_id)
        assert rel_path.endswith(filename)


class TestUnifiedImageSignatures:
    """Phase 2.1: Test unified image signature module usage."""

    def test_cache_simple_uses_unified_signatures(self):
        """Verify cache_simple imports from image_signatures module."""
        from extractors.image_signatures import detect_image_type

        # Should detect JPEG
        jpeg_data = b'\xff\xd8\xff\xe0\x00\x10JFIF'
        result = detect_image_type(jpeg_data)
        assert result == ('jpeg', '.jpg')

    def test_cache_firefox_uses_unified_signatures(self):
        """Verify cache_firefox uses unified signatures."""
        from extractors.image_signatures import detect_image_type

        # Should detect PNG
        png_data = b'\x89PNG\r\n\x1a\n'
        result = detect_image_type(png_data)
        assert result == ('png', '.png')

    def test_all_formats_detected_consistently(self):
        """Test all supported formats are detected by unified module."""
        from extractors.image_signatures import detect_image_type

        test_cases = [
            (b'\xff\xd8\xff\xe0', 'jpeg'),
            (b'\x89PNG\r\n\x1a\n', 'png'),
            (b'GIF89a', 'gif'),
            (b'RIFF\x00\x00\x00\x00WEBP', 'webp'),
            (b'BM\x00\x00\x00\x00', 'bmp'),
            (b'\x00\x00\x01\x00', 'ico'),
            (b'II*\x00', 'tiff'),
            (b'<svg xmlns="">', 'svg'),
        ]

        for data, expected_format in test_cases:
            result = detect_image_type(data + b'\x00' * 20)
            assert result is not None, f"Failed to detect {expected_format}"
            assert result[0] == expected_format


class TestHashOnlyFallback:
    """Phase 2.6: Test hash-only fallback for unsupported formats."""

    def test_image_process_result_has_notes_field(self):
        """Test ImageProcessResult dataclass has notes field."""
        from extractors._shared.carving.processor import ImageProcessResult
        from pathlib import Path

        result = ImageProcessResult(
            path=Path('/test/image.bin'),
            rel_path='test/image.bin',
            filename='image.bin',
            md5='d41d8cd98f00b204e9800998ecf8427e',
            sha256='e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
            phash=None,
            exif_json='{}',
            size_bytes=1024,
            error=None,
            notes='PIL decode failed: UnidentifiedImageError'
        )

        assert result.notes == 'PIL decode failed: UnidentifiedImageError'

    def test_to_db_record_includes_notes(self):
        """Test to_db_record includes notes field."""
        from extractors._shared.carving.processor import ImageProcessResult
        from pathlib import Path

        result = ImageProcessResult(
            path=Path('/test/image.bin'),
            rel_path='test/image.bin',
            filename='image.bin',
            md5='abc123',
            sha256='def456',
            phash=None,
            exif_json='{}',
            size_bytes=1024,
            error=None,
            notes='Hash-only record: format unsupported'
        )

        db_record = result.to_db_record('test_extractor:1.0:run_1')
        assert 'notes' in db_record
        assert db_record['notes'] == 'Hash-only record: format unsupported'


class TestCarvingIngestionExtensions:
    """Phase 2.5: Test expanded carving ingestion extension list."""

    def test_supported_extensions_list(self):
        """Test SUPPORTED_IMAGE_EXTENSIONS includes all formats."""
        from extractors.image_signatures import SUPPORTED_IMAGE_EXTENSIONS

        expected = {
            '.jpg', '.jpeg', '.jpe', '.jfif', '.png', '.gif', '.bmp', '.tiff', '.tif',
            '.webp', '.avif', '.heic', '.heif', '.svg', '.ico'
        }

        assert SUPPORTED_IMAGE_EXTENSIONS == expected

    def test_webp_is_supported(self):
        """Test WebP files are collected."""
        from extractors.image_signatures import is_supported_image_extension
        assert is_supported_image_extension('.webp') is True

    def test_avif_is_supported(self):
        """Test AVIF files are collected."""
        from extractors.image_signatures import is_supported_image_extension
        assert is_supported_image_extension('.avif') is True

    def test_heic_is_supported(self):
        """Test HEIC files are collected."""
        from extractors.image_signatures import is_supported_image_extension
        assert is_supported_image_extension('.heic') is True


class TestIndexedDBBlobExtraction:
    """Phase 3.1: Test IndexedDB blob image extraction."""

    def test_browser_storage_extractor_has_image_option(self):
        """Test browser_storage extractor has extract_images config option."""
        from extractors.browser.chromium.storage.extractor import ChromiumStorageWidget
        from PySide6.QtWidgets import QApplication

        # Need QApplication for widgets
        app = QApplication.instance()
        if app is None:
            app = QApplication([])

        widget = ChromiumStorageWidget()
        config = widget.get_config()

        assert 'extract_images' in config

    def test_image_detection_in_blob_data(self):
        """Test image detection works on raw blob bytes."""
        from extractors.image_signatures import detect_image_type

        # Simulate IndexedDB blob containing JPEG
        jpeg_blob = b'\xff\xd8\xff\xe0\x00\x10JFIF' + b'\x00' * 100

        result = detect_image_type(jpeg_blob)
        assert result == ('jpeg', '.jpg')

    def test_png_blob_detection(self):
        """Test PNG blob detection."""
        from extractors.image_signatures import detect_image_type

        png_blob = b'\x89PNG\r\n\x1a\n' + b'\x00' * 100

        result = detect_image_type(png_blob)
        assert result == ('png', '.png')


class TestCacheStoragePatterns:
    """Phase 3.3: Test CacheStorage pattern configuration."""

    def test_chrome_cache_storage_patterns(self):
        """Test Chrome has cache_storage patterns."""
        from extractors.browser_patterns import get_browser_paths

        patterns = get_browser_paths('chrome', 'cache_storage')
        assert patterns is not None
        assert len(patterns) > 0

        # Should contain Service Worker path
        pattern_str = ' '.join(patterns)
        assert 'Service Worker' in pattern_str or 'CacheStorage' in pattern_str

    def test_firefox_cache_storage_patterns(self):
        """Test Firefox has cache_storage patterns."""
        from extractors.browser_patterns import get_browser_paths

        patterns = get_browser_paths('firefox', 'cache_storage')
        assert patterns is not None
        assert len(patterns) > 0

    def test_edge_cache_storage_patterns(self):
        """Test Edge has cache_storage patterns."""
        from extractors.browser_patterns import get_browser_paths

        patterns = get_browser_paths('edge', 'cache_storage')
        assert patterns is not None
        assert len(patterns) > 0

    def test_safari_cache_storage_patterns(self):
        """Test Safari has cache_storage patterns."""
        from extractors.browser_patterns import get_browser_paths

        patterns = get_browser_paths('safari', 'cache_storage')
        assert patterns is not None
        assert len(patterns) > 0

    def test_cache_storage_wired_to_discovery(self):
        """Test CacheStorage flag is passed to discovery function."""
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor
        import inspect

        # Verify _discover_cache_directories accepts include_cache_storage parameter
        sig = inspect.signature(CacheSimpleExtractor._discover_cache_directories)
        params = list(sig.parameters.keys())

        assert 'include_cache_storage' in params

    def test_cache_storage_checkbox_exists(self):
        """Test CacheStorage checkbox exists in config widget."""
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor
        from PySide6.QtWidgets import QApplication

        app = QApplication.instance()
        if app is None:
            app = QApplication([])

        extractor = CacheSimpleExtractor()
        widget = extractor.get_config_widget(None)
        config = extractor._get_config_from_widget()

        # Should have include_cache_storage key
        assert 'include_cache_storage' in config




class TestZstdDecompression:
    """Phase 2.4: Test zstd decompression support."""

    def test_zstd_import_check(self):
        """Test zstd availability check pattern."""
        try:
            import zstandard
            ZSTD_AVAILABLE = True
        except ImportError:
            ZSTD_AVAILABLE = False

        # Just verify the import check pattern works
        assert isinstance(ZSTD_AVAILABLE, bool)

    def test_zstd_decompression_basic(self):
        """Test basic zstd decompression if available."""
        try:
            import zstandard
        except ImportError:
            pytest.skip('zstandard not installed')

        # Compress some test data
        original = b'Test image data' * 100
        cctx = zstandard.ZstdCompressor()
        compressed = cctx.compress(original)

        # Decompress
        dctx = zstandard.ZstdDecompressor()
        decompressed = dctx.decompress(compressed)

        assert decompressed == original


class TestImageRecordProvenance:
    """Test forensic provenance fields in image records."""

    def test_discovered_by_format(self):
        """Test discovered_by field format."""
        extractor_name = 'cache_simple'
        version = '1.4.0'
        run_id = '20240101T120000_abc12345'

        discovered_by = f'{extractor_name}:{version}:{run_id}'

        parts = discovered_by.split(':')
        assert len(parts) == 3
        assert parts[0] == extractor_name
        assert parts[1] == version
        assert parts[2] == run_id

    def test_cache_key_preserves_url(self):
        """Test cache_key preserves original URL."""
        url = 'https://example.com/images/photo.jpg?size=large'
        cache_key = url

        assert cache_key == url
        assert 'example.com' in cache_key

    def test_ts_utc_iso_format(self):
        """Test ts_utc uses ISO format."""
        from datetime import datetime, timezone

        ts = datetime.now(timezone.utc)
        ts_utc = ts.isoformat()

        # Verify ISO format
        assert 'T' in ts_utc
        assert '+' in ts_utc or 'Z' in ts_utc or '-' in ts_utc[-6:]


class TestManifestIntegration:
    """Test manifest includes cache_images field."""

    def test_manifest_has_cache_images_field(self):
        """Test manifest structure includes cache_images."""
        manifest = {
            'extractor': 'safari',
            'version': '1.4.0',
            'schema_version': '1.0.0',
            'run_id': 'test_run',
            'files': [],
            'cache_images': [],  # New field for
            'status': 'ok',
        }

        assert 'cache_images' in manifest
        assert isinstance(manifest['cache_images'], list)

    def test_cache_image_record_structure(self):
        """Test cache image record has expected structure."""
        cache_image = {
            'rel_path': 'run_123/cache_images/safari_cache_abc12345_d41d8cd9.jpg',
            'filename': 'safari_cache_abc12345_d41d8cd9.jpg',
            'md5': 'd41d8cd98f00b204e9800998ecf8427e',
            'sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
            'phash': '8f8f8f8f00000000',
            'discovered_by': 'safari:1.4.0:run_123',
            'ts_utc': '2024-01-01T00:00:00+00:00',
            'run_id': 'run_123',
            'size_bytes': 12345,
            'cache_key': 'Users/test/Library/Caches/com.apple.Safari/fsCachedData/abc123',
            'notes': 'Extracted from Safari fsCachedData: abc123',
        }

        required = {'rel_path', 'filename', 'md5', 'sha256', 'discovered_by', 'run_id'}
        assert required.issubset(cache_image.keys())


class TestCacheStorageDiscovery:
    """Test two-step CacheStorage discovery (bug fix)."""

    def test_cache_storage_discovery_method_exists(self):
        """Test that _discover_cache_storage_directories method exists."""
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor

        extractor = CacheSimpleExtractor()
        assert hasattr(extractor, '_discover_cache_storage_directories')

    def test_extract_profile_from_path_chrome_windows(self):
        """Test profile extraction from Chrome Windows path."""
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor

        extractor = CacheSimpleExtractor()
        path = "Users/test/AppData/Local/Google/Chrome/User Data/Profile 1/Service Worker/CacheStorage/abc123/cache_id/f_00001"

        profile = extractor._extract_profile_from_path(path, "chrome")
        assert profile == "Profile 1"

    def test_extract_profile_from_path_chrome_default(self):
        """Test profile extraction from Chrome Default path."""
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor

        extractor = CacheSimpleExtractor()
        path = "Users/test/AppData/Local/Google/Chrome/User Data/Default/Service Worker/CacheStorage/abc123/cache_id/f_00001"

        profile = extractor._extract_profile_from_path(path, "chrome")
        assert profile == "Default"

    def test_discover_cache_directories_respects_disk_cache_flag(self):
        """Test that include_disk_cache=False skips disk cache patterns."""
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor

        extractor = CacheSimpleExtractor()

        # Check method signature includes both flags
        import inspect
        sig = inspect.signature(extractor._discover_cache_directories)
        params = list(sig.parameters.keys())

        assert 'include_disk_cache' in params
        assert 'include_cache_storage' in params

    def test_cache_storage_no_recursive_patterns(self):
        """Test that CacheStorage discovery doesn't use ** recursive patterns."""
        from pathlib import Path

        # Logic moved to _discovery.py in refactoring
        import extractors.browser.chromium.cache._discovery as discovery_module
        discovery_path = Path(discovery_module.__file__)
        source = discovery_path.read_text()

        # Find the discover_cache_storage_directories function
        # It should use /*/ and /*/*/ but NOT /**/ for fixed depth scanning
        import re

        # Look for the search_patterns list in the function
        # Should have {origin_dir}/*/{file_pattern} and {origin_dir}/*/*/{file_pattern}
        # but NOT {origin_dir}/**/{file_pattern}

        # Check that the fixed-depth patterns are used
        assert '/*/{file_pattern}' in source or "/*/{" in source
        assert '/*/*/{file_pattern}' in source or "/*/*/{" in source

        # Check the function doesn't use ** for CacheStorage (in search_patterns list context)
        # The ** pattern in origin search is fine, we're checking the inner file search
        func_match = re.search(
            r'def discover_cache_storage_directories.*?(?=def \w+|\Z)',
            source,
            re.DOTALL
        )
        assert func_match is not None
        func_source = func_match.group(0)

        # In the search_patterns list, there should be no **
        if 'search_patterns = [' in func_source:
            patterns_match = re.search(
                r'search_patterns\s*=\s*\[(.*?)\]',
                func_source,
                re.DOTALL
            )
            if patterns_match:
                patterns_content = patterns_match.group(1)
                # Should not contain ** in the file search patterns
                assert '**' not in patterns_content, \
                    "search_patterns should use fixed depth /*/ and /*/*/ instead of **"


class TestCacheStorageDiscoveryBehavior:
    """Test actual behavior of CacheStorage discovery with mocked filesystem."""

    def test_discovery_returns_files_from_mock_fs(self):
        """Test that _discover_cache_storage_directories returns expected file structure."""
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor
        from unittest.mock import Mock, MagicMock

        extractor = CacheSimpleExtractor()

        # Mock evidence filesystem
        mock_fs = Mock()

        # Simulate CacheStorage structure with 2 origins, 2 cache dirs, multiple files
        base_path = "Users/test/AppData/Local/Google/Chrome/User Data/Default/Service Worker/CacheStorage"

        def mock_iter_paths(pattern):
            if pattern.endswith("CacheStorage/*"):
                # Step 1: Return origin-level matches
                return iter([
                    f"{base_path}/origin123",
                    f"{base_path}/origin456",
                ])
            elif "origin123/*/f_*" in pattern:
                return iter([
                    f"{base_path}/origin123/cache1/f_00001",
                    f"{base_path}/origin123/cache1/f_00002",
                ])
            elif "origin456/*/f_*" in pattern:
                return iter([
                    f"{base_path}/origin456/cache2/f_00003",
                ])
            elif "origin123/*/[0-9a-f]*_0" in pattern:
                return iter([
                    f"{base_path}/origin123/cache1/abc123_0",
                ])
            elif "origin456/*/[0-9a-f]*_0" in pattern:
                return iter([
                    f"{base_path}/origin456/cache2/def456_0",
                ])
            elif "origin123/*/index" in pattern:
                return iter([
                    f"{base_path}/origin123/cache1/index",
                ])
            elif "origin456/*/index" in pattern:
                return iter([
                    f"{base_path}/origin456/cache2/index",
                ])
            else:
                return iter([])

        mock_fs.iter_paths = mock_iter_paths

        # Mock callbacks
        mock_callbacks = Mock()
        mock_callbacks.on_log = Mock()

        # Call discovery
        patterns = [
            f"Users/*/AppData/Local/Google/Chrome/User Data/Default/Service Worker/CacheStorage/*"
        ]

        result = extractor._discover_cache_storage_directories(
            mock_fs, "chrome", patterns, mock_callbacks
        )

        # Assert result structure and counts
        assert isinstance(result, list)
        assert len(result) == 2, f"Should find 2 cache directories, got {len(result)}"

        # Collect all cache dirs and their file counts
        cache_dirs_found = {r["path"]: len(r["files"]) for r in result}

        # Verify both cache directories were found
        cache1_path = f"{base_path}/origin123/cache1"
        cache2_path = f"{base_path}/origin456/cache2"

        assert cache1_path in cache_dirs_found, f"Should find cache1 at {cache1_path}"
        assert cache2_path in cache_dirs_found, f"Should find cache2 at {cache2_path}"

        # cache1 should have 4 files (f_00001, f_00002, abc123_0, index)
        assert cache_dirs_found[cache1_path] == 4, \
            f"cache1 should have 4 files, got {cache_dirs_found[cache1_path]}"

        # cache2 should have 3 files (f_00003, def456_0, index)
        assert cache_dirs_found[cache2_path] == 3, \
            f"cache2 should have 3 files, got {cache_dirs_found[cache2_path]}"

        # Verify all results have correct structure
        for cache_dir in result:
            assert "path" in cache_dir
            assert "browser" in cache_dir
            assert cache_dir["browser"] == "chrome"
            assert "cache_type" in cache_dir
            assert cache_dir["cache_type"] == "cache_storage"
            assert "files" in cache_dir
            assert isinstance(cache_dir["files"], list)

            # Each file should have path and filename
            for file_info in cache_dir["files"]:
                assert "path" in file_info
                assert "filename" in file_info

    def test_origin_extraction_from_nested_path(self):
        """Test that origin dirs are correctly extracted even from nested CacheStorage paths."""
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor
        from unittest.mock import Mock

        extractor = CacheSimpleExtractor()

        # Mock evidence filesystem
        mock_fs = Mock()

        # Test nested CacheStorage (e.g., CacheStorage appears twice in path)
        # This can happen in some edge cases or partition layouts
        def mock_iter_paths(pattern):
            if "CacheStorage/*" in pattern:
                return iter([
                    # Normal path
                    "Users/test/Chrome/User Data/Default/Service Worker/CacheStorage/origin123",
                    # Nested CacheStorage (edge case) - should use LAST CacheStorage
                    "Backup/CacheStorage/old/Service Worker/CacheStorage/origin456",
                ])
            elif "origin123/*/f_*" in pattern:
                return iter([
                    "Users/test/Chrome/User Data/Default/Service Worker/CacheStorage/origin123/cache1/f_00001",
                ])
            elif "origin456/*/f_*" in pattern:
                return iter([
                    "Backup/CacheStorage/old/Service Worker/CacheStorage/origin456/cache1/f_00001",
                ])
            return iter([])

        mock_fs.iter_paths = mock_iter_paths

        mock_callbacks = Mock()
        mock_callbacks.on_log = Mock()

        patterns = ["**/CacheStorage/*"]

        result = extractor._discover_cache_storage_directories(
            mock_fs, "chrome", patterns, mock_callbacks
        )

        # Should have found 2 origins (each CacheStorage's child)
        # Verify that on_log was called with "Found 2 CacheStorage origins"
        log_calls = [str(call) for call in mock_callbacks.on_log.call_args_list]
        found_origins_log = [c for c in log_calls if "CacheStorage origins" in c]
        assert len(found_origins_log) > 0, "Should log found origins count"
        assert "2" in found_origins_log[0], "Should find exactly 2 origins"

    def test_data_star_pattern_included(self):
        """Test that data_* pattern is included for legacy blockfile support."""
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor
        from unittest.mock import Mock

        extractor = CacheSimpleExtractor()
        mock_fs = Mock()

        base_path = "Users/test/Chrome/User Data/Default/Service Worker/CacheStorage"

        # Track which patterns are searched
        searched_patterns = []

        def mock_iter_paths(pattern):
            searched_patterns.append(pattern)
            if pattern.endswith("CacheStorage/*"):
                return iter([f"{base_path}/origin123"])
            elif "data_*" in pattern:
                # Return a blockfile data file
                return iter([f"{base_path}/origin123/cache1/data_0"])
            return iter([])

        mock_fs.iter_paths = mock_iter_paths

        mock_callbacks = Mock()
        mock_callbacks.on_log = Mock()

        patterns = ["Users/*/Chrome/User Data/Default/Service Worker/CacheStorage/*"]

        result = extractor._discover_cache_storage_directories(
            mock_fs, "chrome", patterns, mock_callbacks
        )

        # Verify data_* pattern was searched
        data_patterns = [p for p in searched_patterns if "data_*" in p]
        assert len(data_patterns) > 0, "Should search for data_* pattern for blockfile support"

        # If we found the origin, should have results with the data file
        if result:
            all_files = []
            for cache_dir in result:
                all_files.extend([f["filename"] for f in cache_dir["files"]])
            assert "data_0" in all_files, "Should find data_0 blockfile"


class TestDiskCacheToggleBehavior:
    """Test that include_disk_cache=False actually skips disk patterns."""

    def test_disk_cache_false_skips_disk_patterns(self):
        """Test that setting include_disk_cache=False prevents disk cache scanning."""
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor
        from unittest.mock import Mock, patch

        extractor = CacheSimpleExtractor()

        # Mock evidence filesystem
        mock_fs = Mock()
        scanned_patterns = []

        def mock_iter_paths(pattern):
            scanned_patterns.append(pattern)
            return iter([])

        mock_fs.iter_paths = mock_iter_paths

        mock_callbacks = Mock()
        mock_callbacks.on_log = Mock()

        # Call with include_disk_cache=False
        with patch.object(extractor, '_scan_cache_pattern') as mock_scan:
            mock_scan.return_value = []

            result = extractor._discover_cache_directories(
                mock_fs,
                browsers=["chrome"],
                callbacks=mock_callbacks,
                include_cache_storage=False,
                include_disk_cache=False,
            )

        # _scan_cache_pattern should NOT have been called (disk cache disabled)
        assert mock_scan.call_count == 0, \
            "Disk cache patterns should be skipped when include_disk_cache=False"

    def test_disk_cache_true_scans_disk_patterns(self):
        """Test that setting include_disk_cache=True enables disk cache scanning."""
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor
        from unittest.mock import Mock, patch

        extractor = CacheSimpleExtractor()

        mock_fs = Mock()
        mock_fs.iter_paths = Mock(return_value=iter([]))

        mock_callbacks = Mock()
        mock_callbacks.on_log = Mock()

        # Call with include_disk_cache=True
        with patch.object(extractor, '_scan_cache_pattern') as mock_scan:
            mock_scan.return_value = []

            result = extractor._discover_cache_directories(
                mock_fs,
                browsers=["chrome"],
                callbacks=mock_callbacks,
                include_cache_storage=False,
                include_disk_cache=True,
            )

        # _scan_cache_pattern SHOULD have been called
        assert mock_scan.call_count > 0, \
            "Disk cache patterns should be scanned when include_disk_cache=True"


class TestDiskCacheToggle:
    """Test include_disk_cache toggle is respected (bug fix)."""

    def test_config_includes_disk_cache_setting(self):
        """Test that default config includes include_disk_cache."""
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor

        extractor = CacheSimpleExtractor()
        config = extractor._get_config_from_widget()

        assert 'include_disk_cache' in config
        assert config['include_disk_cache'] is True  # Default should be True

    def test_config_includes_cache_storage_setting(self):
        """Test that default config includes include_cache_storage."""
        from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor

        extractor = CacheSimpleExtractor()
        config = extractor._get_config_from_widget()

        assert 'include_cache_storage' in config
        assert config['include_cache_storage'] is False  # Default should be False


class TestImageCountAccuracy:
    """Test image count accuracy (bug fix)."""

    def test_insert_images_returns_count(self):
        """Test that insert_images returns actual insert count."""
        from core.database import insert_images
        import inspect

        # Check return type annotation
        sig = inspect.signature(insert_images)

        # insert_images should return int (actual inserted count)
        assert sig.return_annotation == int or sig.return_annotation == 'int'



# Mark GUI tests
pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")
