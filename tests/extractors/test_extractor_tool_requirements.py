"""
Tests for extractor tool requirements functionality.

NOTE: EXTRACTOR_TOOL_REQUIREMENTS was simplified:
- Legacy mappings removed (browser_history, cache_parsing, etc.)
- Modern extractors use Python packages, not external tools
- bulk_extractor_images removed (unified into bulk_extractor)
"""

import pytest
from core.extraction_orchestrator import get_extractor_tool_requirements, EXTRACTOR_TOOL_REQUIREMENTS


def test_extractor_tool_requirements_mapping_exists():
    """Test that EXTRACTOR_TOOL_REQUIREMENTS mapping is defined."""
    assert EXTRACTOR_TOOL_REQUIREMENTS is not None
    assert isinstance(EXTRACTOR_TOOL_REQUIREMENTS, dict)


def test_forensic_tools_have_requirements():
    """Test that forensic tool extractors declare their tool requirements."""
    # bulk_extractor should require bulk_extractor tool
    assert "bulk_extractor" in EXTRACTOR_TOOL_REQUIREMENTS
    assert EXTRACTOR_TOOL_REQUIREMENTS["bulk_extractor"] == ["bulk_extractor"]

    # foremost_carver should require foremost tool
    assert "foremost_carver" in EXTRACTOR_TOOL_REQUIREMENTS
    assert EXTRACTOR_TOOL_REQUIREMENTS["foremost_carver"] == ["foremost"]

    # scalpel should require scalpel tool
    assert "scalpel" in EXTRACTOR_TOOL_REQUIREMENTS
    assert EXTRACTOR_TOOL_REQUIREMENTS["scalpel"] == ["scalpel"]


def test_python_extractors_have_no_requirements():
    """Test that Python-based extractors have empty requirements."""
    python_extractors = [
        "sqlite_browser_history",
        "cache_simple",
        "cache_indexed",
        "cache_firefox",
        "registry_offline",
        "regex_text_scanner"
    ]

    for extractor in python_extractors:
        assert extractor in EXTRACTOR_TOOL_REQUIREMENTS
        assert EXTRACTOR_TOOL_REQUIREMENTS[extractor] == []


def test_get_extractor_tool_requirements_forensic():
    """Test get_extractor_tool_requirements for forensic tools."""
    # bulk_extractor
    requirements = get_extractor_tool_requirements("bulk_extractor")
    assert requirements == ["bulk_extractor"]

    # foremost_carver
    requirements = get_extractor_tool_requirements("foremost_carver")
    assert requirements == ["foremost"]

    # scalpel
    requirements = get_extractor_tool_requirements("scalpel")
    assert requirements == ["scalpel"]


def test_get_extractor_tool_requirements_python():
    """Test get_extractor_tool_requirements for Python extractors."""
    # SQLite browser history
    requirements = get_extractor_tool_requirements("sqlite_browser_history")
    assert requirements == []

    # Chrome cache
    requirements = get_extractor_tool_requirements("cache_simple")
    assert requirements == []


def test_get_extractor_tool_requirements_unknown():
    """Test get_extractor_tool_requirements for unknown extractor."""
    requirements = get_extractor_tool_requirements("nonexistent_extractor")
    assert requirements == []


def test_core_extractors_have_requirements_defined():
    """Test that core extractors have requirements defined."""
    core_extractors = [
        "bulk_extractor",
        "foremost_carver",
        "scalpel",
        "sqlite_browser_history",
        "cache_simple",
        "cache_indexed",
        "cache_firefox",
        "registry_offline",
        "regex_text_scanner"
    ]

    for extractor in core_extractors:
        assert extractor in EXTRACTOR_TOOL_REQUIREMENTS, \
            f"Extractor '{extractor}' missing from EXTRACTOR_TOOL_REQUIREMENTS"
