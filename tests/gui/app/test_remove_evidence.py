"""
Tests for Remove Evidence functionality.

Tests the RemoveEvidenceDialog and evidence removal logic.
"""
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

from PySide6.QtWidgets import QApplication, QDialog

from app.common.dialogs.remove_evidence import RemoveEvidenceDialog


# Ensure QApplication exists for Qt widgets
@pytest.fixture(scope="module")
def qapp():
    """Create QApplication for the test module."""
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app


class TestRemoveEvidenceDialog:
    """Tests for RemoveEvidenceDialog."""

    def test_dialog_creation_with_evidences(self, qapp):
        """Test dialog creates successfully with evidence list."""
        evidences = [
            {"id": 1, "label": "Test Evidence 1", "source_path": "/path/to/evidence1.E01"},
            {"id": 2, "label": "Test Evidence 2", "source_path": "/path/to/evidence2.E01"},
        ]

        def mock_get_counts(ev_id):
            return {"urls": 100, "images": 50, "browser_history": 25}

        dialog = RemoveEvidenceDialog(evidences, mock_get_counts)

        assert dialog is not None
        assert dialog.windowTitle() == "Remove Evidence"
        assert len(dialog.radio_buttons) == 2
        assert dialog.selected_evidence_id is None
        assert dialog.remove_button.isEnabled() is False

    def test_dialog_creation_no_evidences(self, qapp):
        """Test dialog handles empty evidence list."""
        dialog = RemoveEvidenceDialog([], lambda x: {})

        assert dialog is not None
        assert not hasattr(dialog, 'radio_buttons') or len(getattr(dialog, 'radio_buttons', {})) == 0
        assert dialog.remove_button.isEnabled() is False

    def test_selection_enables_remove_button(self, qapp):
        """Test selecting an evidence enables the remove button."""
        evidences = [
            {"id": 1, "label": "Test Evidence", "source_path": "/path/to/evidence.E01"},
        ]

        counts_called = []
        def mock_get_counts(ev_id):
            counts_called.append(ev_id)
            return {"urls": 10}

        dialog = RemoveEvidenceDialog(evidences, mock_get_counts)

        # Simulate clicking the radio button
        radio = dialog.radio_buttons[1]
        radio.setChecked(True)
        dialog._on_selection_changed(radio)

        assert dialog.selected_evidence_id == 1
        assert dialog.remove_button.isEnabled() is True
        assert 1 in counts_called

    def test_get_selected_evidence(self, qapp):
        """Test get_selected_evidence returns correct evidence dict."""
        evidences = [
            {"id": 1, "label": "Evidence A", "source_path": "/path/a.E01"},
            {"id": 2, "label": "Evidence B", "source_path": "/path/b.E01"},
        ]

        dialog = RemoveEvidenceDialog(evidences, lambda x: {})

        # No selection
        assert dialog.get_selected_evidence() is None

        # Select evidence 2
        radio = dialog.radio_buttons[2]
        radio.setChecked(True)
        dialog._on_selection_changed(radio)

        selected = dialog.get_selected_evidence()
        assert selected is not None
        assert selected["id"] == 2
        assert selected["label"] == "Evidence B"

    def test_summary_display(self, qapp):
        """Test summary updates with correct data counts."""
        evidences = [
            {"id": 1, "label": "Test", "source_path": "/test.E01"},
        ]

        def mock_get_counts(ev_id):
            return {
                "urls": 500,
                "images": 200,
                "browser_history": 1000,
                "cookies": 50,
                "other_table": 25,
            }

        dialog = RemoveEvidenceDialog(evidences, mock_get_counts)

        # Select and trigger summary update
        radio = dialog.radio_buttons[1]
        radio.setChecked(True)
        dialog._on_selection_changed(radio)

        # Check summary label contains expected counts
        summary_text = dialog.summary_label.text()
        assert "500" in summary_text  # URLs
        assert "200" in summary_text  # Images
        assert "1,000" in summary_text  # Browser history (formatted with comma)


class TestEvidenceRemovalIntegration:
    """Integration tests for evidence removal (requires file system)."""

    def test_evidence_folder_structure(self, tmp_path):
        """Test we understand the evidence folder structure correctly."""
        from core.database.manager import slugify_label

        evidence_id = 1
        evidence_label = "Test Evidence"

        # Create expected folder structure
        slug = slugify_label(evidence_label, evidence_id)
        assert slug == "test-evidence"

        case_folder = tmp_path / "test_case"
        case_folder.mkdir()

        evidence_folder = case_folder / "evidences" / slug
        evidence_folder.mkdir(parents=True)

        # Create evidence database
        evidence_db = evidence_folder / f"evidence_{slug}.sqlite"
        evidence_db.touch()

        # Create thumbnails folder
        thumbs = evidence_folder / "thumbnails"
        thumbs.mkdir()
        (thumbs / "test.jpg").touch()

        # Create downloads folder
        downloads = evidence_folder / "_downloads"
        downloads.mkdir()
        (downloads / "file.html").touch()

        # Create evidence log
        logs_dir = case_folder / "logs"
        logs_dir.mkdir()
        evidence_log = logs_dir / f"evidence_{evidence_id}.log"
        evidence_log.touch()

        # Verify structure exists
        assert evidence_folder.exists()
        assert evidence_db.exists()
        assert evidence_log.exists()

        # Simulate removal
        shutil.rmtree(evidence_folder)
        evidence_log.unlink()

        # Verify deleted
        assert not evidence_folder.exists()
        assert not evidence_log.exists()

    def test_slugify_label_function(self):
        """Test slugify_label produces expected slugs."""
        from core.database.manager import slugify_label

        # Basic label
        assert slugify_label("My Evidence", 1) == "my-evidence"

        # Label with special characters - parentheses become dashes, duplicates collapsed
        assert slugify_label("Test (Copy)", 2) == "test-copy"

        # Label starting with number gets 'ev-' prefix
        assert slugify_label("4Dell Latitude CPi", 3) == "ev-4dell-latitude-cpi"
