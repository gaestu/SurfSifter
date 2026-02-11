"""
Unit tests for file_classifier utility module.

Tests for extension-to-type mapping and downloadability checks.
"""
from __future__ import annotations

import pytest

from core.file_classifier import (
    DOWNLOADABLE_EXTENSIONS,
    classify_file_type,
    get_extension,
    is_downloadable,
    get_extensions_for_type,
)


class TestGetExtension:
    """Tests for get_extension function."""

    def test_simple_extension(self):
        """Test extracting simple extensions."""
        assert get_extension("image.jpg") == ".jpg"
        assert get_extension("document.pdf") == ".pdf"
        assert get_extension("video.mp4") == ".mp4"

    def test_uppercase_extension(self):
        """Test that uppercase extensions are lowercased."""
        assert get_extension("IMAGE.JPG") == ".jpg"
        assert get_extension("PHOTO.PNG") == ".png"

    def test_url_with_extension(self):
        """Test extracting extension from URLs."""
        assert get_extension("https://example.com/path/image.jpg") == ".jpg"
        assert get_extension("https://example.com/path/to/file.pdf?query=1") == ".pdf"

    def test_no_extension(self):
        """Test files without extension."""
        assert get_extension("filename") == ""
        assert get_extension("path/to/file") == ""

    def test_path_object(self):
        """Test that Path objects work."""
        from pathlib import Path
        assert get_extension(Path("/home/user/photo.png")) == ".png"

    def test_double_extension(self):
        """Test files with double extensions."""
        # Returns the final extension only
        assert get_extension("archive.tar.gz") == ".gz"
        assert get_extension("data.json.bak") == ".bak"


class TestClassifyFileType:
    """Tests for classify_file_type function."""

    def test_image_types(self):
        """Test image file classification."""
        image_files = [
            "photo.jpg", "photo.jpeg", "logo.png", "animation.gif",
            "banner.webp", "icon.bmp", "diagram.svg", "scan.tiff"
        ]
        for f in image_files:
            assert classify_file_type(f) == "image", f"Expected {f} to be 'image'"

    def test_video_types(self):
        """Test video file classification."""
        video_files = ["movie.mp4", "clip.webm", "film.avi", "video.mov", "series.mkv"]
        for f in video_files:
            assert classify_file_type(f) == "video", f"Expected {f} to be 'video'"

    def test_audio_types(self):
        """Test audio file classification."""
        audio_files = ["song.mp3", "audio.wav", "podcast.ogg", "music.m4a", "track.flac"]
        for f in audio_files:
            assert classify_file_type(f) == "audio", f"Expected {f} to be 'audio'"

    def test_document_types(self):
        """Test document file classification."""
        doc_files = ["report.pdf", "letter.doc", "memo.docx", "budget.xls", "slides.pptx"]
        for f in doc_files:
            assert classify_file_type(f) == "document", f"Expected {f} to be 'document'"

    def test_archive_types(self):
        """Test archive file classification."""
        archive_files = ["data.zip", "backup.rar", "files.7z", "package.tar"]
        for f in archive_files:
            assert classify_file_type(f) == "archive", f"Expected {f} to be 'archive'"

    def test_other_types(self):
        """Test unknown extensions return 'other'."""
        other_files = ["script.py", "style.css", "page.html", "data.xml"]
        for f in other_files:
            assert classify_file_type(f) == "other", f"Expected {f} to be 'other'"

    def test_no_extension(self):
        """Test files without extension return 'other'."""
        assert classify_file_type("Makefile") == "other"
        assert classify_file_type("README") == "other"

    def test_url_classification(self):
        """Test classifying URLs."""
        assert classify_file_type("https://example.com/image.jpg") == "image"
        assert classify_file_type("https://example.com/document.pdf") == "document"


class TestIsDownloadable:
    """Tests for is_downloadable function."""

    def test_downloadable_extensions(self):
        """Test that known extensions are downloadable."""
        downloadable = [
            "image.jpg", "image.png", "video.mp4", "audio.mp3",
            "document.pdf", "archive.zip"
        ]
        for f in downloadable:
            assert is_downloadable(f) is True, f"Expected {f} to be downloadable"

    def test_non_downloadable_extensions(self):
        """Test that unknown extensions are not downloadable."""
        not_downloadable = [
            "script.py", "style.css", "page.html", "data.json", "config.ini"
        ]
        for f in not_downloadable:
            assert is_downloadable(f) is False, f"Expected {f} to NOT be downloadable"

    def test_no_extension_not_downloadable(self):
        """Test files without extension are not downloadable."""
        assert is_downloadable("README") is False
        assert is_downloadable("Makefile") is False

    def test_url_downloadability(self):
        """Test URL downloadability check."""
        assert is_downloadable("https://example.com/photo.jpg") is True
        assert is_downloadable("https://example.com/page") is False


class TestGetExtensionsForType:
    """Tests for get_extensions_for_type function."""

    def test_image_extensions(self):
        """Test getting image extensions."""
        exts = get_extensions_for_type("image")
        assert ".jpg" in exts
        assert ".jpeg" in exts
        assert ".png" in exts
        assert ".gif" in exts

    def test_video_extensions(self):
        """Test getting video extensions."""
        exts = get_extensions_for_type("video")
        assert ".mp4" in exts
        assert ".webm" in exts
        assert ".avi" in exts

    def test_audio_extensions(self):
        """Test getting audio extensions."""
        exts = get_extensions_for_type("audio")
        assert ".mp3" in exts
        assert ".wav" in exts

    def test_document_extensions(self):
        """Test getting document extensions."""
        exts = get_extensions_for_type("document")
        assert ".pdf" in exts
        assert ".doc" in exts
        assert ".docx" in exts

    def test_archive_extensions(self):
        """Test getting archive extensions."""
        exts = get_extensions_for_type("archive")
        assert ".zip" in exts
        assert ".rar" in exts
        assert ".7z" in exts

    def test_unknown_type_returns_empty(self):
        """Test that unknown type returns empty list."""
        assert get_extensions_for_type("unknown") == []
        assert get_extensions_for_type("nonsense") == []

    def test_other_type_returns_empty(self):
        """Test that 'other' type has no defined extensions."""
        assert get_extensions_for_type("other") == []


class TestDownloadableExtensionsConstant:
    """Tests for DOWNLOADABLE_EXTENSIONS set."""

    def test_contains_common_extensions(self):
        """Test that common downloadable extensions are included."""
        expected = {".jpg", ".jpeg", ".png", ".gif", ".mp4", ".mp3", ".pdf", ".zip"}
        for ext in expected:
            assert ext in DOWNLOADABLE_EXTENSIONS, f"Missing {ext}"

    def test_count_minimum(self):
        """Test that at least 40 extensions are defined."""
        assert len(DOWNLOADABLE_EXTENSIONS) >= 40


class TestDownloadableExtensionsMappings:
    """Tests for DOWNLOADABLE_EXTENSIONS dictionary mappings."""

    def test_all_extensions_have_types(self):
        """Test that all extensions map to valid types."""
        valid_types = {"image", "video", "audio", "document", "archive"}
        for ext, file_type in DOWNLOADABLE_EXTENSIONS.items():
            assert file_type in valid_types, f"Invalid type '{file_type}' for {ext}"

    def test_type_values_valid(self):
        """Test that all type values are valid categories."""
        valid_types = {"image", "video", "audio", "document", "archive"}
        for ext, file_type in DOWNLOADABLE_EXTENSIONS.items():
            assert file_type in valid_types, f"Invalid type '{file_type}' for {ext}"
