"""Tests for JumpListsTableModel."""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock

from PySide6.QtCore import Qt

from app.features.os_artifacts.models.jump_lists_model import JumpListsTableModel


def _create_jump_list_db(db_path) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE jump_list_entries (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER,
            appid TEXT,
            browser TEXT,
            jumplist_path TEXT,
            entry_id TEXT,
            target_path TEXT,
            arguments TEXT,
            url TEXT,
            title TEXT,
            lnk_creation_time TEXT,
            lnk_modification_time TEXT,
            lnk_access_time TEXT,
            access_count INTEGER,
            pin_status TEXT,
            source_path TEXT,
            run_id TEXT,
            discovered_by TEXT,
            partition_index INTEGER,
            fs_type TEXT,
            logical_path TEXT,
            forensic_path TEXT,
            tags TEXT,
            notes TEXT,
            created_at_utc TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO jump_list_entries (
            id,
            evidence_id,
            appid,
            browser,
            jumplist_path,
            entry_id,
            target_path,
            arguments,
            url,
            title,
            lnk_creation_time,
            lnk_modification_time,
            lnk_access_time,
            access_count,
            pin_status,
            source_path,
            run_id,
            discovered_by,
            partition_index,
            fs_type,
            logical_path,
            forensic_path,
            tags,
            notes,
            created_at_utc
        )
        VALUES (
            1,
            1,
            'test.appid',
            'chrome',
            'C:\\Users\\test\\Recent\\abc.automaticDestinations-ms',
            'entry-1',
            'C:\\Users\\test\\Downloads\\sample.pdf',
            '',
            'https://example.com',
            'Example',
            '2026-01-01T10:00:00',
            '2026-01-01T10:00:00',
            '2026-01-01T11:00:00',
            3,
            'pinned',
            '',
            'run-1',
            'jump_lists',
            0,
            'ntfs',
            '/Users/test/Recent/abc.automaticDestinations-ms',
            '',
            NULL,
            '',
            '2026-01-01T11:05:00'
        )
        """
    )
    conn.commit()
    conn.close()


def test_jump_lists_model_columns_include_tags():
    assert "tags" in JumpListsTableModel.COLUMNS
    assert "Tags" in JumpListsTableModel.HEADERS


def test_jump_lists_model_uses_unified_tag_strings(tmp_path, qtbot):
    db_path = tmp_path / "test_evidence.sqlite"
    _create_jump_list_db(db_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    db_manager = MagicMock()
    db_manager.get_evidence_conn.return_value = conn

    case_data = MagicMock()
    case_data.get_tag_strings_for_artifacts.return_value = {1: "important, review"}

    model = JumpListsTableModel(
        db_manager=db_manager,
        evidence_id=1,
        evidence_label="test_evidence",
        case_data=case_data,
    )

    assert model.rowCount() == 1
    target_path_index = model.index(0, JumpListsTableModel.COL_TARGET_PATH)
    assert model.data(target_path_index, Qt.DisplayRole) == "C:\\Users\\test\\Downloads\\sample.pdf"
    tag_index = model.index(0, JumpListsTableModel.COL_TAGS)
    assert model.data(tag_index, Qt.DisplayRole) == "important, review"
    assert model.data(tag_index, Qt.ToolTipRole) == "important, review"
    case_data.get_tag_strings_for_artifacts.assert_called_once_with(1, "jump_list", [1])

    conn.close()
