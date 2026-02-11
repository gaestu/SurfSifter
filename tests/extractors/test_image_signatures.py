"""
Tests for the unified image signature detection module.

Covers all supported image formats and edge cases.
"""

import pytest


class TestDetectImageType:
    """Tests for detect_image_type() function."""

    def test_jpeg_jfif(self):
        """Test JPEG/JFIF detection."""
        from extractors.image_signatures import detect_image_type

        data = b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01'
        result = detect_image_type(data)
        assert result == ('jpeg', '.jpg')

    def test_jpeg_exif(self):
        """Test JPEG/EXIF detection."""
        from extractors.image_signatures import detect_image_type

        data = b'\xff\xd8\xff\xe1\x00\x00Exif\x00\x00'
        result = detect_image_type(data)
        assert result == ('jpeg', '.jpg')

    def test_jpeg_icc(self):
        """Test JPEG with ICC profile detection."""
        from extractors.image_signatures import detect_image_type

        data = b'\xff\xd8\xff\xe2' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('jpeg', '.jpg')

    def test_jpeg_adobe(self):
        """Test Adobe JPEG detection."""
        from extractors.image_signatures import detect_image_type

        data = b'\xff\xd8\xff\xee' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('jpeg', '.jpg')

    def test_jpeg_raw(self):
        """Test raw JPEG detection (starts with DQT)."""
        from extractors.image_signatures import detect_image_type

        data = b'\xff\xd8\xff\xdb' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('jpeg', '.jpg')

    def test_jpeg_generic(self):
        """Test generic JPEG detection (3-byte prefix)."""
        from extractors.image_signatures import detect_image_type

        # Unknown marker after FFD8FF
        data = b'\xff\xd8\xff\xc0' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('jpeg', '.jpg')

    def test_png(self):
        """Test PNG detection."""
        from extractors.image_signatures import detect_image_type

        data = b'\x89PNG\r\n\x1a\n' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('png', '.png')

    def test_gif87a(self):
        """Test GIF87a detection."""
        from extractors.image_signatures import detect_image_type

        data = b'GIF87a' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('gif', '.gif')

    def test_gif89a(self):
        """Test GIF89a detection."""
        from extractors.image_signatures import detect_image_type

        data = b'GIF89a' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('gif', '.gif')

    def test_bmp(self):
        """Test BMP detection."""
        from extractors.image_signatures import detect_image_type

        data = b'BM' + b'\x00' * 50
        result = detect_image_type(data)
        assert result == ('bmp', '.bmp')

    def test_ico(self):
        """Test ICO (Windows icon) detection."""
        from extractors.image_signatures import detect_image_type

        data = b'\x00\x00\x01\x00' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('ico', '.ico')

    def test_tiff_little_endian(self):
        """Test little-endian TIFF detection."""
        from extractors.image_signatures import detect_image_type

        data = b'II*\x00' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('tiff', '.tif')

    def test_tiff_big_endian(self):
        """Test big-endian TIFF detection."""
        from extractors.image_signatures import detect_image_type

        data = b'MM\x00*' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('tiff', '.tif')

    def test_webp(self):
        """Test WebP detection via RIFF container."""
        from extractors.image_signatures import detect_image_type

        # RIFF header with WEBP marker
        data = b'RIFF\x00\x00\x00\x00WEBP' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('webp', '.webp')

    def test_webp_wrong_marker(self):
        """Test RIFF container that isn't WebP."""
        from extractors.image_signatures import detect_image_type

        # RIFF header with AVI marker (not an image)
        data = b'RIFF\x00\x00\x00\x00AVI ' + b'\x00' * 20
        result = detect_image_type(data)
        assert result is None

    def test_svg_direct(self):
        """Test SVG detection with <svg tag."""
        from extractors.image_signatures import detect_image_type

        data = b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"></svg>'
        result = detect_image_type(data)
        assert result == ('svg', '.svg')

    def test_svg_with_whitespace(self):
        """Test SVG detection with leading whitespace."""
        from extractors.image_signatures import detect_image_type

        data = b'   \n\t<svg xmlns="http://www.w3.org/2000/svg"></svg>'
        result = detect_image_type(data)
        assert result == ('svg', '.svg')

    def test_svg_with_xml_declaration(self):
        """Test SVG detection with XML declaration."""
        from extractors.image_signatures import detect_image_type

        data = b'<?xml version="1.0" encoding="UTF-8"?>\n<svg></svg>'
        result = detect_image_type(data)
        assert result == ('svg', '.svg')

    def test_svg_size_limit(self):
        """Test SVG detection respects size limit."""
        from extractors.image_signatures import detect_image_type

        # Large data exceeding default size limit
        large_data = b'<svg></svg>' + (b'\x00' * 300000)

        # Should not be detected as SVG due to size limit
        result = detect_image_type(large_data, svg_size_limit=1024)
        assert result is None

        # Should be detected with higher limit
        result = detect_image_type(large_data, svg_size_limit=500000)
        assert result == ('svg', '.svg')

    def test_avif(self):
        """Test AVIF detection via ftyp box."""
        from extractors.image_signatures import detect_image_type

        # ISO BMFF with avif brand
        data = b'\x00\x00\x00\x18ftypavis\x00\x00\x00\x00avis' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('avif', '.avif')

    def test_avif_avis_brand(self):
        """Test AVIF detection with avis brand."""
        from extractors.image_signatures import detect_image_type

        data = b'\x00\x00\x00\x18ftypavif\x00\x00\x00\x00mif1' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('avif', '.avif')

    def test_heic(self):
        """Test HEIC detection via ftyp box."""
        from extractors.image_signatures import detect_image_type

        data = b'\x00\x00\x00\x18ftypheic\x00\x00\x00\x00mif1' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('heic', '.heic')

    def test_heic_heix_brand(self):
        """Test HEIC detection with heix brand."""
        from extractors.image_signatures import detect_image_type

        data = b'\x00\x00\x00\x18ftypheix\x00\x00\x00\x00' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('heic', '.heic')

    def test_heic_mif1_brand(self):
        """Test HEIC detection with mif1 brand only."""
        from extractors.image_signatures import detect_image_type

        data = b'\x00\x00\x00\x18ftypmif1\x00\x00\x00\x00' + b'\x00' * 20
        result = detect_image_type(data)
        assert result == ('heic', '.heic')

    def test_not_an_image(self):
        """Test non-image data returns None."""
        from extractors.image_signatures import detect_image_type

        data = b'This is just plain text, not an image.'
        result = detect_image_type(data)
        assert result is None

    def test_empty_data(self):
        """Test empty data returns None."""
        from extractors.image_signatures import detect_image_type

        assert detect_image_type(b'') is None
        assert detect_image_type(b'\x00') is None

    def test_short_data(self):
        """Test data too short for detection returns None."""
        from extractors.image_signatures import detect_image_type

        # Only 1 byte
        assert detect_image_type(b'\xff') is None

    def test_partial_jpeg(self):
        """Test partial JPEG header (only 2 bytes) returns None."""
        from extractors.image_signatures import detect_image_type

        # Just FFD8 without third byte
        assert detect_image_type(b'\xff\xd8') is None


class TestGetExtensionForFormat:
    """Tests for get_extension_for_format() function."""

    def test_known_formats(self):
        """Test extension lookup for known formats."""
        from extractors.image_signatures import get_extension_for_format

        assert get_extension_for_format('jpeg') == '.jpg'
        assert get_extension_for_format('png') == '.png'
        assert get_extension_for_format('gif') == '.gif'
        assert get_extension_for_format('webp') == '.webp'
        assert get_extension_for_format('bmp') == '.bmp'
        assert get_extension_for_format('ico') == '.ico'
        assert get_extension_for_format('tiff') == '.tif'
        assert get_extension_for_format('svg') == '.svg'
        assert get_extension_for_format('avif') == '.avif'
        assert get_extension_for_format('heic') == '.heic'

    def test_unknown_format(self):
        """Test unknown format returns format name as extension."""
        from extractors.image_signatures import get_extension_for_format

        assert get_extension_for_format('unknown') == '.unknown'
        assert get_extension_for_format('xyz') == '.xyz'


class TestSupportedImageExtensions:
    """Tests for SUPPORTED_IMAGE_EXTENSIONS and is_supported_image_extension()."""

    def test_supported_extensions_set(self):
        """Test SUPPORTED_IMAGE_EXTENSIONS contains expected formats."""
        from extractors.image_signatures import SUPPORTED_IMAGE_EXTENSIONS

        expected = {
            '.jpg', '.jpeg', '.jpe', '.jfif', '.png', '.gif', '.bmp', '.tiff', '.tif',
            '.webp', '.avif', '.heic', '.heif', '.svg', '.ico'
        }
        assert SUPPORTED_IMAGE_EXTENSIONS == expected

    def test_is_supported_extension_with_dot(self):
        """Test is_supported_image_extension with extensions including dot."""
        from extractors.image_signatures import is_supported_image_extension

        assert is_supported_image_extension('.jpg') is True
        assert is_supported_image_extension('.PNG') is True
        assert is_supported_image_extension('.txt') is False

    def test_is_supported_extension_without_dot(self):
        """Test is_supported_image_extension with extensions without dot."""
        from extractors.image_signatures import is_supported_image_extension

        assert is_supported_image_extension('jpg') is True
        assert is_supported_image_extension('webp') is True
        assert is_supported_image_extension('doc') is False

    def test_is_supported_with_full_path(self):
        """Test is_supported_image_extension with full file paths."""
        from extractors.image_signatures import is_supported_image_extension

        assert is_supported_image_extension('/path/to/image.jpg') is True
        assert is_supported_image_extension('C:\\Users\\image.PNG') is True
        assert is_supported_image_extension('file.avif') is True
        assert is_supported_image_extension('/path/to/document.pdf') is False

    def test_is_supported_case_insensitive(self):
        """Test is_supported_image_extension is case-insensitive."""
        from extractors.image_signatures import is_supported_image_extension

        assert is_supported_image_extension('.JPG') is True
        assert is_supported_image_extension('.JpEg') is True
        assert is_supported_image_extension('PNG') is True
        assert is_supported_image_extension('.HEIC') is True


class TestImageSignatureIntegration:
    """Integration tests for image signature detection."""

    def test_detect_and_get_extension_roundtrip(self):
        """Test that detected format extension matches get_extension_for_format."""
        from extractors.image_signatures import detect_image_type, get_extension_for_format

        # Test data samples
        samples = [
            b'\xff\xd8\xff\xe0\x00\x10JFIF',  # JPEG
            b'\x89PNG\r\n\x1a\n',              # PNG
            b'GIF89a',                          # GIF
            b'RIFF\x00\x00\x00\x00WEBP',       # WebP
            b'BM' + b'\x00' * 10,              # BMP
            b'\x00\x00\x01\x00',                # ICO
            b'II*\x00',                         # TIFF LE
            b'<svg xmlns="">',                  # SVG
        ]

        for data in samples:
            result = detect_image_type(data)
            assert result is not None, f"Failed to detect: {data[:10]}"
            fmt, detected_ext = result
            expected_ext = get_extension_for_format(fmt)
            assert detected_ext == expected_ext, f"Extension mismatch for {fmt}"

    def test_jpeg_variants_all_return_same_format(self):
        """Test all JPEG variants return consistent format."""
        from extractors.image_signatures import detect_image_type

        jpeg_headers = [
            b'\xff\xd8\xff\xe0',  # JFIF
            b'\xff\xd8\xff\xe1',  # EXIF
            b'\xff\xd8\xff\xe2',  # ICC
            b'\xff\xd8\xff\xe8',  # SPIFF
            b'\xff\xd8\xff\xdb',  # Raw
            b'\xff\xd8\xff\xee',  # Adobe
        ]

        for header in jpeg_headers:
            data = header + b'\x00' * 20
            result = detect_image_type(data)
            assert result == ('jpeg', '.jpg'), f"Header {header.hex()} not detected as JPEG"
