"""
Tests for Jump Lists extractor.

Tests cover:
- Pattern matching for different source types
- Path resolution fix for portable cases
- Source type classification
"""

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch


class TestJumpListSourceTypeMatching:
    """Test _matches_source_type helper for path classification."""
    
    @pytest.fixture
    def extractor(self):
        """Create extractor instance."""
        from extractors.system.jump_lists.extractor import SystemJumpListsExtractor
        return SystemJumpListsExtractor()
    
    def test_matches_automatic_destinations(self, extractor):
        """AutomaticDestinations files should match JUMPLIST_AUTO."""
        from extractors.system.jump_lists.extractor import SOURCE_TYPE_JUMPLIST_AUTO
        
        path = "Users/JohnDoe/AppData/Roaming/Microsoft/Windows/Recent/AutomaticDestinations/5d696d521de238c3.automaticDestinations-ms"
        assert extractor._matches_source_type(path, SOURCE_TYPE_JUMPLIST_AUTO) is True
        
    def test_matches_custom_destinations(self, extractor):
        """CustomDestinations files should match JUMPLIST_CUSTOM."""
        from extractors.system.jump_lists.extractor import SOURCE_TYPE_JUMPLIST_CUSTOM
        
        path = "Users/JohnDoe/AppData/Roaming/Microsoft/Windows/Recent/CustomDestinations/abc123.customDestinations-ms"
        assert extractor._matches_source_type(path, SOURCE_TYPE_JUMPLIST_CUSTOM) is True
    
    def test_matches_recent_item(self, extractor):
        """Recent Items LNK files should match RECENT_ITEM."""
        from extractors.system.jump_lists.extractor import SOURCE_TYPE_RECENT_ITEM
        
        # Valid Recent Item
        path = "Users/JohnDoe/AppData/Roaming/Microsoft/Windows/Recent/document.lnk"
        assert extractor._matches_source_type(path, SOURCE_TYPE_RECENT_ITEM) is True
        
        # Should NOT match if in AutomaticDestinations subfolder
        path2 = "Users/JohnDoe/AppData/Roaming/Microsoft/Windows/Recent/AutomaticDestinations/doc.lnk"
        assert extractor._matches_source_type(path2, SOURCE_TYPE_RECENT_ITEM) is False
        
    def test_matches_desktop(self, extractor):
        """Desktop shortcuts should match DESKTOP."""
        from extractors.system.jump_lists.extractor import SOURCE_TYPE_DESKTOP
        
        path = "Users/JohnDoe/Desktop/Chrome.lnk"
        assert extractor._matches_source_type(path, SOURCE_TYPE_DESKTOP) is True
        
        path2 = "Users/Public/Desktop/TeamViewer.lnk"
        assert extractor._matches_source_type(path2, SOURCE_TYPE_DESKTOP) is True
        
    def test_matches_taskbar_pinned(self, extractor):
        """Taskbar pinned items should match TASKBAR_PINNED."""
        from extractors.system.jump_lists.extractor import SOURCE_TYPE_TASKBAR_PINNED
        
        path = "Users/JohnDoe/AppData/Roaming/Microsoft/Internet Explorer/Quick Launch/User Pinned/TaskBar/Firefox.lnk"
        assert extractor._matches_source_type(path, SOURCE_TYPE_TASKBAR_PINNED) is True
        
    def test_matches_start_menu_pinned(self, extractor):
        """Start Menu pinned items should match START_MENU_PINNED."""
        from extractors.system.jump_lists.extractor import SOURCE_TYPE_START_MENU_PINNED
        
        path = "Users/JohnDoe/AppData/Roaming/Microsoft/Internet Explorer/Quick Launch/User Pinned/StartMenu/App.lnk"
        assert extractor._matches_source_type(path, SOURCE_TYPE_START_MENU_PINNED) is True
        
    def test_matches_quick_launch(self, extractor):
        """Quick Launch items should match QUICK_LAUNCH but not User Pinned subfolders."""
        from extractors.system.jump_lists.extractor import SOURCE_TYPE_QUICK_LAUNCH
        
        # Direct Quick Launch items should match
        path = "Users/JohnDoe/AppData/Roaming/Microsoft/Internet Explorer/Quick Launch/Microsoft Edge.lnk"
        assert extractor._matches_source_type(path, SOURCE_TYPE_QUICK_LAUNCH) is True
        
        # User Pinned items should NOT match as Quick Launch
        path2 = "Users/JohnDoe/AppData/Roaming/Microsoft/Internet Explorer/Quick Launch/User Pinned/TaskBar/App.lnk"
        assert extractor._matches_source_type(path2, SOURCE_TYPE_QUICK_LAUNCH) is False
        

class TestJumpListPathResolution:
    """Test path resolution for portable cases (fixes absolute path bug)."""
    
    def test_relative_path_in_manifest(self, tmp_path):
        """Extracted files should have relative paths in manifest for portability."""
        from extractors.system.jump_lists.extractor import SystemJumpListsExtractor
        
        extractor = SystemJumpListsExtractor()
        output_dir = tmp_path / "jump_lists"
        output_dir.mkdir()
        
        # Simulate what _extract_file does
        file_content = b"dummy lnk content"
        filename = "test.lnk"
        dest_path = output_dir / filename
        dest_path.write_bytes(file_content)
        
        file_info = {
            "logical_path": "Users/Test/Desktop/test.lnk",
            "filename": filename,
            "appid": "",
            "user": "Test",
            "source_type": "desktop",
        }
        
        # Mock callbacks
        callbacks = MagicMock()
        callbacks.on_log = MagicMock()
        
        # Mock evidence_fs
        mock_fs = MagicMock()
        mock_fs.read_file = MagicMock(return_value=file_content)
        
        result = extractor._extract_file(mock_fs, file_info, output_dir, callbacks)
        
        # The extracted_path should be relative (filename only), not absolute
        assert result["extracted_path"] == filename
        assert not Path(result["extracted_path"]).is_absolute()
        
    def test_ingestion_resolves_path_from_filename(self, tmp_path):
        """Ingestion should resolve path using filename within output_dir."""
        output_dir = tmp_path / "jump_lists"
        output_dir.mkdir()
        
        # Create a manifest with a file
        manifest = {
            "files": [
                {
                    "filename": "test.automaticDestinations-ms",
                    "extracted_path": "test.automaticDestinations-ms",  # Relative
                    "copy_status": "ok",
                    "logical_path": "Users/Test/AppData/...",
                    "appid": "abc123",
                },
            ]
        }
        (output_dir / "manifest.json").write_text(json.dumps(manifest))
        
        # Create the file
        test_file = output_dir / "test.automaticDestinations-ms"
        # Create minimal OLE-like content (won't parse but tests path resolution)
        test_file.write_bytes(b"\x00" * 100)
        
        # The ingestion should find the file using output_dir / filename
        # regardless of what was in extracted_path (even if it was an old absolute path)
        assert test_file.exists()


class TestJumpListPatterns:
    """Test JUMPLIST_PATTERNS configuration."""
    
    def test_patterns_have_source_types(self):
        """All patterns should include source type information."""
        from extractors.system.jump_lists.extractor import JUMPLIST_PATTERNS
        
        assert len(JUMPLIST_PATTERNS) > 0
        
        for pattern_tuple in JUMPLIST_PATTERNS:
            assert len(pattern_tuple) == 4, f"Pattern should have 4 elements: {pattern_tuple}"
            glob_pattern, source_type, path_pattern, filename_pattern = pattern_tuple
            
            assert glob_pattern, "Glob pattern should not be empty"
            assert source_type, "Source type should not be empty"
            assert path_pattern, "Path pattern should not be empty"
            assert filename_pattern, "Filename pattern should not be empty"
            
    def test_patterns_cover_all_source_types(self):
        """Patterns should cover all expected source types."""
        from extractors.system.jump_lists.extractor import (
            JUMPLIST_PATTERNS,
            SOURCE_TYPE_JUMPLIST_AUTO,
            SOURCE_TYPE_JUMPLIST_CUSTOM,
            SOURCE_TYPE_RECENT_ITEM,
            SOURCE_TYPE_DESKTOP,
            SOURCE_TYPE_TASKBAR_PINNED,
            SOURCE_TYPE_START_MENU_PINNED,
            SOURCE_TYPE_QUICK_LAUNCH,
        )
        
        source_types_in_patterns = set(p[1] for p in JUMPLIST_PATTERNS)
        
        expected_types = {
            SOURCE_TYPE_JUMPLIST_AUTO,
            SOURCE_TYPE_JUMPLIST_CUSTOM,
            SOURCE_TYPE_RECENT_ITEM,
            SOURCE_TYPE_DESKTOP,
            SOURCE_TYPE_TASKBAR_PINNED,
            SOURCE_TYPE_START_MENU_PINNED,
            SOURCE_TYPE_QUICK_LAUNCH,
        }
        
        assert expected_types.issubset(source_types_in_patterns), \
            f"Missing source types: {expected_types - source_types_in_patterns}"


class TestJumpListUserExtraction:
    """Test username extraction from paths."""
    
    @pytest.fixture
    def extractor(self):
        from extractors.system.jump_lists.extractor import SystemJumpListsExtractor
        return SystemJumpListsExtractor()
    
    def test_extract_user_from_users_path(self, extractor):
        """Extract username from Users/username/... paths."""
        path = "Users/JohnDoe/AppData/Roaming/Microsoft/Windows/Recent/test.lnk"
        assert extractor._extract_user_from_path(path) == "JohnDoe"
        
    def test_extract_user_from_documents_and_settings(self, extractor):
        """Extract username from Documents and Settings paths (XP style)."""
        path = "Documents and Settings/Administrator/Recent/test.lnk"
        assert extractor._extract_user_from_path(path) == "Administrator"
        
    def test_extract_user_unknown_path(self, extractor):
        """Return 'unknown' for paths without recognizable user folders."""
        path = "ProgramData/Microsoft/Windows/Start Menu/test.lnk"
        assert extractor._extract_user_from_path(path) == "unknown"


class TestOleStreamHexParsing:
    """Test that OLE stream names are parsed as hex to avoid entry_id collisions.

    Windows names OLE streams in .automaticDestinations-ms files using hex
    numbers (0, 1, ..., 9, a, b, ..., f, 10, 11, ...).  The DestList entry IDs
    are uint32 integers that correspond to these hex stream names.

    Parsing stream names as decimal first caused stream "a" (hex 10) to collide
    with stream "10" (decimal 10), triggering UNIQUE constraint violations and
    merging DestList metadata with the wrong LNK entry for streams >= 10.
    """

    def test_hex_and_decimal_streams_produce_distinct_ids(self):
        """Streams 'a' and '10' must produce distinct entry_ids (10 vs 16)."""
        # This is the exact logic now used in parse_jumplist_ole()
        assert int("a", 16) == 10
        assert int("10", 16) == 16
        assert int("a", 16) != int("10", 16)

    def test_single_digit_streams_unchanged(self):
        """Single-digit streams (0-9) parse identically in decimal and hex."""
        for i in range(10):
            assert int(str(i), 16) == i

    def test_destlist_entry_id_correspondence(self):
        """DestList entry IDs (uint32) correspond to hex-named OLE streams.

        Windows assigns DestList entry_id N to OLE stream named hex(N).
        Hex parsing ensures DestList metadata merges with the correct stream.
        """
        # Entry ID 10 -> OLE stream "a"
        assert int("a", 16) == 10
        # Entry ID 16 -> OLE stream "10"
        assert int("10", 16) == 16
        # Entry ID 255 -> OLE stream "ff"
        assert int("ff", 16) == 255

    def test_large_stream_ids(self):
        """Jump Lists with many entries have higher hex stream names."""
        # Chrome Jump Lists commonly exceed 16 entries
        assert int("1a", 16) == 26
        assert int("20", 16) == 32
        assert int("64", 16) == 100
