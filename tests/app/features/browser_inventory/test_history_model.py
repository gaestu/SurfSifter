"""Tests for browser history model utilities."""

from app.features.browser_inventory.history.model import get_transition_label


class TestGetTransitionLabel:
    """Tests for get_transition_label handling different input types."""

    def test_none_returns_empty_string(self):
        assert get_transition_label(None) == ""

    def test_chromium_int_link(self):
        """Core type 0 → 'link'."""
        assert get_transition_label(0) == "link"

    def test_chromium_int_typed(self):
        """Core type 1 → 'typed'."""
        assert get_transition_label(1) == "typed"

    def test_chromium_int_bitmask(self):
        """High bits should be masked off; core type preserved."""
        # 805306368 = 0x30000000 | 0 → core type 0 → link
        assert get_transition_label(805306368) == "link"

    def test_chromium_int_reload(self):
        assert get_transition_label(8) == "reload"

    def test_string_label_passthrough(self):
        """Safari stores transition_type as plain string like 'link'."""
        assert get_transition_label("link") == "link"

    def test_string_numeric(self):
        """Numeric string should be converted to int and decoded."""
        assert get_transition_label("1") == "typed"

    def test_empty_string(self):
        """Empty string should be returned as-is."""
        assert get_transition_label("") == ""

    def test_unknown_string(self):
        """Unknown string label should be returned as-is."""
        assert get_transition_label("custom_type") == "custom_type"

    def test_unknown_int(self):
        """Unknown core type produces 'unknown_N'."""
        assert get_transition_label(255) == "unknown_255"
