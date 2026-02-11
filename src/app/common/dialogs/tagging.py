"""
Tagging dialog for artifacts (URLs, Images, File List entries).
"""
from __future__ import annotations

from typing import List, Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QCompleter,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)


class TagArtifactsDialog(QDialog):
    """
    Unified dialog for tagging artifacts (URLs, Images, File List entries).

    Part of the unified tagging system.
    Provides autocomplete, tag creation, and multi-artifact tagging support.
    """

    tags_changed = Signal()  # Emitted when tags are applied

    def __init__(
        self,
        case_data,  # CaseDataAccess
        evidence_id: int,
        artifact_type: str,
        artifact_ids: List[int],
        parent: Optional[QWidget] = None,
    ):
        """
        Initialize the tagging dialog.

        Args:
            case_data: Case data access object
            evidence_id: Evidence ID
            artifact_type: Type of artifact ('url', 'image', 'file_list')
            artifact_ids: List of artifact IDs to tag
            parent: Parent widget
        """
        super().__init__(parent)
        self.case_data = case_data
        self.evidence_id = evidence_id
        self.artifact_type = artifact_type
        self.artifact_ids = artifact_ids

        self.setWindowTitle("Tag Artifacts")
        self.resize(500, 400)

        self._init_ui()
        self._load_existing_tags()
        self._load_artifact_tags()

    def _init_ui(self):
        """Initialize the UI."""
        layout = QVBoxLayout(self)

        # Info label
        count = len(self.artifact_ids)
        artifact_name = {
            'url': 'URL',
            'image': 'Image',
            'file_list': 'File',
            'download': 'Download',
            'cookie': 'Cookie',
            'bookmark': 'Bookmark',
            'browser_download': 'Browser Download',
            'autofill': 'Autofill Entry',
            'credential': 'Credential',
            'session_tab': 'Session Tab',
            'site_permission': 'Site Permission',
            'media_playback': 'Media Playback',
            'local_storage': 'Local Storage Key',
            'session_storage': 'Session Storage Key',
        }.get(self.artifact_type, 'Artifact')

        if count == 1:
            info_text = f"Tagging 1 {artifact_name.lower()}"
        else:
            info_text = f"Tagging {count} {artifact_name.lower()}s"

        info_label = QLabel(info_text)
        layout.addWidget(info_label)

        # Tag input with autocomplete
        input_layout = QHBoxLayout()
        self.tag_input = QLineEdit()
        self.tag_input.setPlaceholderText("Type tag name...")
        self.tag_input.returnPressed.connect(self._add_tag)
        input_layout.addWidget(self.tag_input)

        self.add_button = QPushButton("Add")
        self.add_button.clicked.connect(self._add_tag)
        input_layout.addWidget(self.add_button)

        layout.addLayout(input_layout)

        # Selected tags list
        layout.addWidget(QLabel("Selected Tags:"))
        self.selected_tags_list = QListWidget()
        self.selected_tags_list.setMaximumHeight(150)
        layout.addWidget(self.selected_tags_list)

        # Remove button
        remove_layout = QHBoxLayout()
        remove_layout.addStretch()
        self.remove_button = QPushButton("Remove Selected")
        self.remove_button.clicked.connect(self._remove_selected_tags)
        self.remove_button.setEnabled(False)
        remove_layout.addWidget(self.remove_button)
        layout.addLayout(remove_layout)

        # Existing tags (suggestions)
        layout.addWidget(QLabel("All Tags (double-click to add):"))
        self.all_tags_list = QListWidget()
        self.all_tags_list.itemDoubleClicked.connect(self._add_tag_from_list)
        layout.addWidget(self.all_tags_list)

        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        # Connect selection change
        self.selected_tags_list.itemSelectionChanged.connect(self._on_selection_changed)

    def _load_existing_tags(self):
        """Load all existing tags for autocomplete and display."""
        all_tags = self.case_data.list_tags(self.evidence_id)

        # Set up autocomplete
        tag_names = [tag['name'] for tag in all_tags]
        completer = QCompleter(tag_names)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        self.tag_input.setCompleter(completer)

        # Populate all tags list
        self.all_tags_list.clear()
        for tag in all_tags:
            item = QListWidgetItem(tag['name'])
            # Add color indicator if available
            if tag.get('color'):
                item.setForeground(QColor(tag['color']))
            # Show usage count in tooltip
            item.setToolTip(
                f"Used {tag['usage_count']} times"
            )
            self.all_tags_list.addItem(item)

    def _load_artifact_tags(self):
        """Load existing tags for the selected artifacts."""
        if len(self.artifact_ids) == 1:
            # Single artifact - show its current tags
            artifact_id = self.artifact_ids[0]
            tags = self.case_data.get_artifact_tags(
                self.evidence_id, self.artifact_type, artifact_id
            )
            for tag in tags:
                self._add_tag_to_selected(tag['name'], color=tag.get('color'))
        else:
            # Multiple artifacts - show intersection of tags
            common_tags = self._get_common_tags()
            for tag_name in common_tags:
                # Get color from tag registry
                tag = self.case_data.get_tag(self.evidence_id, tag_name)
                color = tag.get('color') if tag else None
                self._add_tag_to_selected(tag_name, color=color)

    def _get_common_tags(self):
        """Get tags that are common to all selected artifacts."""
        if not self.artifact_ids:
            return set()

        # Get tags for first artifact
        first_artifact_tags = self.case_data.get_artifact_tags(
            self.evidence_id, self.artifact_type, self.artifact_ids[0]
        )
        common = {tag['name'] for tag in first_artifact_tags}

        # Intersect with tags from remaining artifacts
        for artifact_id in self.artifact_ids[1:]:
            artifact_tags = self.case_data.get_artifact_tags(
                self.evidence_id, self.artifact_type, artifact_id
            )
            artifact_tag_names = {tag['name'] for tag in artifact_tags}
            common &= artifact_tag_names

        return common

    def _add_tag(self):
        """Add tag from input field."""
        tag_name = self.tag_input.text().strip()
        if not tag_name:
            return

        # Check if tag already selected
        for i in range(self.selected_tags_list.count()):
            if self.selected_tags_list.item(i).text() == tag_name:
                self.tag_input.clear()
                return

        # Get or create tag
        tag = self.case_data.get_tag(self.evidence_id, tag_name)
        if not tag:
            # Tag doesn't exist - create it
            try:
                self.case_data.create_tag(self.evidence_id, tag_name)
                tag = self.case_data.get_tag(self.evidence_id, tag_name)
                # Refresh all tags list
                self._load_existing_tags()
            except Exception as e:
                QMessageBox.warning(
                    self,
                    "Error",
                    f"Failed to create tag: {str(e)}",
                )
                return

        # Add to selected list
        color = tag.get('color') if tag else None
        self._add_tag_to_selected(tag_name, color=color)
        self.tag_input.clear()

    def _add_tag_from_list(self, item: QListWidgetItem):
        """Add tag from the all tags list."""
        tag_name = item.text()

        # Check if already selected
        for i in range(self.selected_tags_list.count()):
            if self.selected_tags_list.item(i).text() == tag_name:
                return

        # Add to selected
        color = item.foreground().color().name() if item.foreground() else None
        self._add_tag_to_selected(tag_name, color=color)

    def _add_tag_to_selected(self, tag_name: str, color: Optional[str] = None):
        """Add a tag to the selected tags list."""
        item = QListWidgetItem(tag_name)
        if color:
            item.setForeground(QColor(color))
        self.selected_tags_list.addItem(item)

    def _remove_selected_tags(self):
        """Remove selected tags from the list."""
        for item in self.selected_tags_list.selectedItems():
            self.selected_tags_list.takeItem(
                self.selected_tags_list.row(item)
            )

    def _on_selection_changed(self):
        """Update remove button state based on selection."""
        has_selection = len(self.selected_tags_list.selectedItems()) > 0
        self.remove_button.setEnabled(has_selection)

    def get_selected_tags(self) -> List[str]:
        """Get list of selected tag names."""
        tags = []
        for i in range(self.selected_tags_list.count()):
            tags.append(self.selected_tags_list.item(i).text())
        return tags

    def accept(self):
        """Apply tags to artifacts and close dialog."""
        selected_tags = self.get_selected_tags()

        try:
            # For each artifact, sync its tags with the selected list
            for artifact_id in self.artifact_ids:
                # Get current tags
                current_tags = self.case_data.get_artifact_tags(
                    self.evidence_id, self.artifact_type, artifact_id
                )
                current_tag_names = {tag['name'] for tag in current_tags}
                selected_tag_set = set(selected_tags)

                # Remove tags that are no longer selected
                for tag_name in current_tag_names - selected_tag_set:
                    self.case_data.untag_artifact(
                        self.evidence_id, tag_name, self.artifact_type, artifact_id
                    )

                # Add new tags
                for tag_name in selected_tag_set - current_tag_names:
                    self.case_data.tag_artifact(
                        self.evidence_id, tag_name, self.artifact_type, artifact_id
                    )

            self.tags_changed.emit()
            super().accept()

        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                f"Failed to apply tags: {str(e)}",
            )
