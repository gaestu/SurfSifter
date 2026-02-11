"""Tests for CollapsibleGroupBox widget."""

import pytest

from PySide6.QtWidgets import QLabel
from PySide6.QtCore import Qt


class TestCollapsibleGroupBox:
    """Tests for CollapsibleGroupBox widget."""

    @pytest.mark.gui_offscreen
    def test_initial_expanded_state(self, qtbot):
        """Test initial expanded state."""
        from reports.ui.collapsible_group import CollapsibleGroupBox

        widget = CollapsibleGroupBox("Test Title", collapsed=False)
        qtbot.addWidget(widget)

        assert widget.is_collapsed() is False
        assert widget.title() == "Test Title"
        # Check that content container is not hidden (visibility depends on show())
        assert widget._content_container.isHidden() is False

    @pytest.mark.gui_offscreen
    def test_initial_collapsed_state(self, qtbot):
        """Test initial collapsed state."""
        from reports.ui.collapsible_group import CollapsibleGroupBox

        widget = CollapsibleGroupBox("Test Title", collapsed=True)
        qtbot.addWidget(widget)

        assert widget.is_collapsed() is True
        assert widget._content_container.isHidden() is True

    @pytest.mark.gui_offscreen
    def test_toggle_collapse(self, qtbot):
        """Test toggling collapse state."""
        from reports.ui.collapsible_group import CollapsibleGroupBox

        widget = CollapsibleGroupBox("Test", collapsed=False)
        qtbot.addWidget(widget)

        # Start expanded
        assert widget.is_collapsed() is False

        # Collapse
        widget.set_collapsed(True)
        assert widget.is_collapsed() is True
        assert widget._content_container.isHidden() is True

        # Expand
        widget.set_collapsed(False)
        assert widget.is_collapsed() is False
        assert widget._content_container.isHidden() is False

    @pytest.mark.gui_offscreen
    def test_collapsed_changed_signal(self, qtbot):
        """Test collapsed_changed signal is emitted."""
        from reports.ui.collapsible_group import CollapsibleGroupBox

        widget = CollapsibleGroupBox("Test", collapsed=False)
        qtbot.addWidget(widget)

        # Track signal emissions
        signal_values = []
        widget.collapsed_changed.connect(lambda v: signal_values.append(v))

        widget.set_collapsed(True)
        assert signal_values == [True]

        widget.set_collapsed(False)
        assert signal_values == [True, False]

    @pytest.mark.gui_offscreen
    def test_no_signal_when_same_state(self, qtbot):
        """Test no signal emitted when setting same state."""
        from reports.ui.collapsible_group import CollapsibleGroupBox

        widget = CollapsibleGroupBox("Test", collapsed=False)
        qtbot.addWidget(widget)

        signal_values = []
        widget.collapsed_changed.connect(lambda v: signal_values.append(v))

        # Setting to current state should not emit
        widget.set_collapsed(False)
        assert signal_values == []

    @pytest.mark.gui_offscreen
    def test_content_layout_returns_layout(self, qtbot):
        """Test content_layout returns the internal layout."""
        from reports.ui.collapsible_group import CollapsibleGroupBox

        widget = CollapsibleGroupBox("Test")
        qtbot.addWidget(widget)

        layout = widget.content_layout()
        assert layout is not None

        # Can add widgets to it
        label = QLabel("Test content")
        layout.addWidget(label)
        assert layout.count() == 1

    @pytest.mark.gui_offscreen
    def test_set_title(self, qtbot):
        """Test set_title updates the title."""
        from reports.ui.collapsible_group import CollapsibleGroupBox

        widget = CollapsibleGroupBox("Original")
        qtbot.addWidget(widget)

        widget.set_title("Updated Title")
        assert widget.title() == "Updated Title"
        assert widget._title_label.text() == "Updated Title"

    @pytest.mark.gui_offscreen
    def test_indicator_updates_on_collapse(self, qtbot):
        """Test collapse indicator updates correctly."""
        from reports.ui.collapsible_group import CollapsibleGroupBox

        widget = CollapsibleGroupBox("Test", collapsed=False)
        qtbot.addWidget(widget)

        # Expanded shows down arrow
        assert widget._indicator.text() == "▼"

        widget.set_collapsed(True)
        # Collapsed shows right arrow
        assert widget._indicator.text() == "▶"

        widget.set_collapsed(False)
        assert widget._indicator.text() == "▼"
