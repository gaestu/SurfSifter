"""GUI tests for registry extractor configuration."""

from __future__ import annotations

import pytest
from PySide6.QtWidgets import QGroupBox

from extractors.system.registry.ui import RegistryConfigWidget


@pytest.mark.gui_offscreen
def test_registry_config_widget_no_custom_rules_ui(qtbot) -> None:
    widget = RegistryConfigWidget()
    qtbot.addWidget(widget)

    group_titles = {group.title() for group in widget.findChildren(QGroupBox)}

    assert "Detector Rules (Optional)" not in group_titles
    assert "rules_path" not in widget.get_config()