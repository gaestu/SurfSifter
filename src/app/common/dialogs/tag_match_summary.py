"""
Tag & Match Summary inspector panel.

Non-modal floating dialog that shows per-tag artifact counts and
per-reference-list match counts for the current evidence.  Designed to
stay open while the investigator works in other tabs so they can see
at a glance what has been tagged and matched before building a report.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from app.common.constants import ARTIFACT_DISPLAY_NAMES
from core.database.helpers.tags import get_tag_artifact_summary, get_match_summary

logger = logging.getLogger(__name__)


class TagMatchSummaryDialog(QDialog):
    """Non-modal floating panel showing tag/match statistics.

    Use ``show()`` (not ``exec()``) to open.  The dialog uses the
    ``Qt.Tool`` window flag so it floats above the main window without
    blocking interaction.
    """

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)

        self.setWindowTitle("Tag & Match Summary")
        self.setWindowFlags(Qt.Window | Qt.Tool)
        self.setAttribute(Qt.WA_DeleteOnClose, False)  # reuse across open/close
        self.resize(520, 480)

        self._db_conn: Optional[sqlite3.Connection] = None
        self._evidence_id: Optional[int] = None

        self._setup_ui()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        # Header row (title + refresh button)
        header = QHBoxLayout()
        title_label = QLabel("Overview of tagged artifacts and reference-list matches.")
        title_label.setStyleSheet("color: palette(mid);")
        header.addWidget(title_label)
        header.addStretch()

        self._refresh_btn = QPushButton("🔄 Refresh")
        self._refresh_btn.setToolTip("Re-query the database for current counts")
        self._refresh_btn.clicked.connect(self.refresh)
        header.addWidget(self._refresh_btn)
        layout.addLayout(header)

        # ── Tags tree ──
        tags_label = QLabel("Tags")
        tags_label.setStyleSheet("font-weight: 600; font-size: 10pt;")
        layout.addWidget(tags_label)

        self._tag_tree = QTreeWidget()
        self._tag_tree.setHeaderLabels(["Name", "Count"])
        self._tag_tree.setRootIsDecorated(True)
        self._tag_tree.setAlternatingRowColors(True)
        self._tag_tree.header().setStretchLastSection(False)
        self._tag_tree.header().setSectionResizeMode(0, QHeaderView.Stretch)
        self._tag_tree.header().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        layout.addWidget(self._tag_tree, stretch=1)

        # ── Matches tree ──
        matches_label = QLabel("Reference-List Matches")
        matches_label.setStyleSheet("font-weight: 600; font-size: 10pt;")
        layout.addWidget(matches_label)

        self._match_tree = QTreeWidget()
        self._match_tree.setHeaderLabels(["List Name", "URLs", "Images", "Files"])
        self._match_tree.setRootIsDecorated(False)
        self._match_tree.setAlternatingRowColors(True)
        self._match_tree.header().setStretchLastSection(False)
        self._match_tree.header().setSectionResizeMode(0, QHeaderView.Stretch)
        for col in (1, 2, 3):
            self._match_tree.header().setSectionResizeMode(
                col, QHeaderView.ResizeToContents
            )
        layout.addWidget(self._match_tree, stretch=1)

        # Empty-state labels (shown when no data)
        self._tag_empty = QLabel("No tagged artifacts.")
        self._tag_empty.setStyleSheet("color: palette(mid); font-style: italic;")
        self._tag_empty.setAlignment(Qt.AlignCenter)
        self._tag_empty.setVisible(False)

        self._match_empty = QLabel("No reference-list matches.")
        self._match_empty.setStyleSheet("color: palette(mid); font-style: italic;")
        self._match_empty.setAlignment(Qt.AlignCenter)
        self._match_empty.setVisible(False)

        # Insert empty labels into the layout (before stretch widgets)
        layout.insertWidget(layout.indexOf(self._tag_tree) + 1, self._tag_empty)
        layout.insertWidget(layout.indexOf(self._match_tree) + 1, self._match_empty)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_db_connection(
        self,
        conn: Optional[sqlite3.Connection],
        evidence_id: Optional[int] = None,
    ) -> None:
        """Set / update the evidence database connection.

        Args:
            conn: Evidence DB connection (or None to clear).
            evidence_id: Evidence ID for the queries.
        """
        self._db_conn = conn
        self._evidence_id = evidence_id
        if self.isVisible():
            self.refresh()

    def refresh(self) -> None:
        """Re-query the database and repopulate both trees."""
        self._populate_tags()
        self._populate_matches()

    # ------------------------------------------------------------------
    # Override show to auto-refresh
    # ------------------------------------------------------------------

    def show(self) -> None:  # noqa: D401
        self.refresh()
        super().show()
        self.raise_()
        self.activateWindow()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _populate_tags(self) -> None:
        """Fetch tag-artifact summary and fill the tag tree."""
        self._tag_tree.clear()

        if self._db_conn is None or self._evidence_id is None:
            self._tag_tree.setVisible(False)
            self._tag_empty.setText("No evidence selected.")
            self._tag_empty.setVisible(True)
            return

        try:
            rows: List[Dict[str, Any]] = get_tag_artifact_summary(
                self._db_conn, self._evidence_id
            )
        except Exception:
            logger.exception("Failed to load tag artifact summary")
            self._tag_tree.setVisible(False)
            self._tag_empty.setText("Error loading tag data.")
            self._tag_empty.setVisible(True)
            return

        if not rows:
            self._tag_tree.setVisible(False)
            self._tag_empty.setText("No tagged artifacts.")
            self._tag_empty.setVisible(True)
            return

        self._tag_tree.setVisible(True)
        self._tag_empty.setVisible(False)

        # Group rows by tag_name
        tags: Dict[str, List[Dict[str, Any]]] = {}
        tag_totals: Dict[str, int] = {}
        for row in rows:
            name = row["tag_name"]
            tags.setdefault(name, []).append(row)
            tag_totals[name] = tag_totals.get(name, 0) + row["count"]

        for tag_name, children in tags.items():
            total = tag_totals[tag_name]
            parent_item = QTreeWidgetItem([tag_name, str(total)])
            parent_item.setToolTip(0, f"Tag: {tag_name} ({total} total associations)")
            for child in children:
                display = ARTIFACT_DISPLAY_NAMES.get(
                    child["artifact_type"], child["artifact_type"]
                )
                # Pluralise simple names when count > 1
                label = f"{display}s" if child["count"] != 1 else display
                child_item = QTreeWidgetItem([label, str(child["count"])])
                parent_item.addChild(child_item)
            self._tag_tree.addTopLevelItem(parent_item)

        self._tag_tree.expandAll()

    def _populate_matches(self) -> None:
        """Fetch match summary and fill the match tree."""
        self._match_tree.clear()

        if self._db_conn is None or self._evidence_id is None:
            self._match_tree.setVisible(False)
            self._match_empty.setText("No evidence selected.")
            self._match_empty.setVisible(True)
            return

        try:
            rows: List[Dict[str, Any]] = get_match_summary(
                self._db_conn, self._evidence_id
            )
        except Exception:
            logger.exception("Failed to load match summary")
            self._match_tree.setVisible(False)
            self._match_empty.setText("Error loading match data.")
            self._match_empty.setVisible(True)
            return

        if not rows:
            self._match_tree.setVisible(False)
            self._match_empty.setText("No reference-list matches.")
            self._match_empty.setVisible(True)
            return

        self._match_tree.setVisible(True)
        self._match_empty.setVisible(False)

        for row in rows:
            item = QTreeWidgetItem([
                row["list_name"],
                str(row["url_count"]),
                str(row["image_count"]),
                str(row["file_count"]),
            ])
            self._match_tree.addTopLevelItem(item)
