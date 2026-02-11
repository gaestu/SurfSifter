"""
Test extractor architecture.

Tests verify that:
- Modern extractors use Python-based patterns (not YAML rules)
- ExtractorRegistry discovers all extractors
- Browser patterns and registry rules are defined in Python

NOTE:
- rules_engine.py removed (YAML rule loading deprecated)
- Modern extractors are self-contained with Python patterns
"""


def test_registry_extractor_uses_python_rules():
    """Test that registry extractor uses Python rules, not YAML."""
    from extractors.system.registry.rules_util import load_registry_rules

    rules = load_registry_rules()

    # Should return rules from Python module
    assert len(rules.targets) >= 5

    # All targets should be registry-related
    for target in rules.targets:
        assert "paths" in target
        assert "actions" in target


def test_browser_patterns_module_exists():
    """Test that browser patterns are defined in Python module."""
    from extractors.browser_patterns import (
        BROWSER_PATTERNS,
        get_browser_display_name,
        get_browser_paths,
    )

    # Core browsers should be defined
    assert "chrome" in BROWSER_PATTERNS
    assert "firefox" in BROWSER_PATTERNS
    assert "edge" in BROWSER_PATTERNS
    assert "safari" in BROWSER_PATTERNS

    # Should have history paths
    chrome_history = get_browser_paths("chrome", "history")
    assert len(chrome_history) > 0
