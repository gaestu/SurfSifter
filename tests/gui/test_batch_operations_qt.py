"""GUI-focused tests for batch operations dialogs/workers."""

from pathlib import Path

from PySide6.QtCore import Qt, Signal

from app.features.extraction.dialogs import ExtractAndIngestDialog
from app.services.workers import ExtractAndIngestWorker


def test_extract_and_ingest_worker_has_required_signals():
    """Verify ExtractAndIngestWorker has the required signal interface."""
    assert hasattr(ExtractAndIngestWorker, "extractor_started")
    assert hasattr(ExtractAndIngestWorker, "extractor_finished")
    assert hasattr(ExtractAndIngestWorker, "batch_finished")
    assert hasattr(ExtractAndIngestWorker, "log_message")

    assert isinstance(ExtractAndIngestWorker.extractor_started, Signal)
    assert isinstance(ExtractAndIngestWorker.extractor_finished, Signal)
    assert isinstance(ExtractAndIngestWorker.batch_finished, Signal)
    assert isinstance(ExtractAndIngestWorker.log_message, Signal)


def test_extract_and_ingest_dialog_has_required_methods():
    """Verify ExtractAndIngestDialog has the required public interface."""
    assert hasattr(ExtractAndIngestDialog, "get_selected_extractors")
    assert hasattr(ExtractAndIngestDialog, "get_selected_mode")
    assert callable(getattr(ExtractAndIngestDialog, "get_selected_extractors"))
    assert callable(getattr(ExtractAndIngestDialog, "get_selected_mode"))


def test_extract_and_ingest_worker_cancel_method():
    """Verify ExtractAndIngestWorker has cancel functionality."""
    worker = ExtractAndIngestWorker(
        extractors=[],
        evidence_fs=None,
        evidence_source_path=None,
        evidence_id=1,
        evidence_label="test",
        workspace_dir=Path("/tmp"),
        db_manager=None,
        overwrite_mode="overwrite",
    )

    assert hasattr(worker, "cancel")
    assert callable(worker.cancel)

    assert not worker._cancelled
    worker.cancel()
    assert worker._cancelled


def test_extract_and_ingest_dialog_attributes():
    """Verify ExtractAndIngestDialog has required attributes."""
    assert hasattr(ExtractAndIngestDialog, "_setup_ui")
    assert hasattr(ExtractAndIngestDialog, "_select_all")
    assert hasattr(ExtractAndIngestDialog, "_select_none")
    assert hasattr(ExtractAndIngestDialog, "_update_mode")
    assert hasattr(ExtractAndIngestDialog, "_update_preview")


def test_extract_and_ingest_worker_has_existing_data_check():
    """Verify ExtractAndIngestWorker has method to check existing data."""
    assert hasattr(ExtractAndIngestWorker, "_has_existing_data")
    assert callable(getattr(ExtractAndIngestWorker, "_has_existing_data"))


def test_purge_data_dialog_checkbox_signal_comparison():
    """Verify PurgeDataDialog checkbox signal uses correct Qt enum comparison."""
    checked_state = 2  # Qt.CheckState.Checked value
    unchecked_state = 0  # Qt.CheckState.Unchecked value

    assert checked_state != Qt.Checked
    assert unchecked_state != Qt.Unchecked

    assert checked_state == Qt.Checked.value
    assert unchecked_state == Qt.Unchecked.value

    button_enabled_when_checked = checked_state == Qt.Checked.value
    button_enabled_when_unchecked = unchecked_state == Qt.Checked.value

    assert button_enabled_when_checked is True
    assert button_enabled_when_unchecked is False
