"""
UI widgets for File List Importer configuration and status display.

v2.0: Dual-path UI - Generate from E01 (primary) + Import CSV (fallback)
"""

from pathlib import Path
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFileDialog, QGroupBox, QComboBox, QFormLayout
)
from PySide6.QtCore import Signal

from .sleuthkit_utils import get_sleuthkit_bin

class FileListConfigWidget(QWidget):
    """
    Configuration widget for file list importer.

    Dual-path UI:
    - Generate from E01 (primary) - uses SleuthKitFileListGenerator
    - Import External CSV (fallback) - for FTK/EnCase exports
    """

    configChanged = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._selected_csv_path = None
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # --- Generate from E01 section (Primary) ---
        generate_group = QGroupBox("⚡ Generate from E01 (Recommended)")
        generate_layout = QVBoxLayout()

        # fls status
        self._fls_status = QLabel()
        self._update_fls_status()
        generate_layout.addWidget(self._fls_status)

        # Description
        desc_label = QLabel(
            "Uses SleuthKit fls to enumerate all files in the evidence image.\n"
            "This is the fastest method and preserves partition context."
        )
        desc_label.setWordWrap(True)
        desc_label.setStyleSheet("color: gray; font-size: 9pt;")
        generate_layout.addWidget(desc_label)

        # Generate button (action happens on extraction)
        self._generate_mode = False
        self._generate_btn = QPushButton("⚡ Use fls Generation")
        self._generate_btn.setCheckable(True)
        self._generate_btn.clicked.connect(self._on_generate_toggled)
        generate_layout.addWidget(self._generate_btn)

        generate_group.setLayout(generate_layout)
        layout.addWidget(generate_group)

        # --- Import External CSV section (Fallback) ---
        import_group = QGroupBox("📂 Import External CSV (Fallback)")
        import_layout = QVBoxLayout()

        # Description
        import_desc = QLabel(
            "For investigations with only FTK/EnCase exports (no original image)."
        )
        import_desc.setWordWrap(True)
        import_desc.setStyleSheet("color: gray; font-size: 9pt;")
        import_layout.addWidget(import_desc)

        # Source type selector
        source_form = QFormLayout()
        self._source_combo = QComboBox()
        self._source_combo.addItems([
            "Auto-detect",
            "FTK",
            "EnCase",
            "Generic CSV"
        ])
        self._source_combo.currentIndexChanged.connect(lambda: self.configChanged.emit())
        source_form.addRow("Source Type:", self._source_combo)
        import_layout.addLayout(source_form)

        # File selection
        file_layout = QHBoxLayout()
        self._csv_path_label = QLabel("No file selected")
        self._csv_path_label.setStyleSheet("color: gray;")
        file_layout.addWidget(self._csv_path_label, stretch=1)

        self._import_btn = QPushButton("📂 Select CSV...")
        self._import_btn.clicked.connect(self._on_import_clicked)
        file_layout.addWidget(self._import_btn)
        import_layout.addLayout(file_layout)

        import_group.setLayout(import_layout)
        layout.addWidget(import_group)

        layout.addStretch()

    def _update_fls_status(self):
        """Update fls availability status."""
        fls_path = get_sleuthkit_bin("fls")
        if fls_path:
            self._fls_status.setText(f"✓ SleuthKit available: {fls_path}")
            self._fls_status.setStyleSheet("color: green;")
        else:
            self._fls_status.setText("✗ SleuthKit not found (bundled or PATH)")
            self._fls_status.setStyleSheet("color: red;")

    def _on_generate_toggled(self, checked: bool):
        """Handle generate button toggle."""
        self._generate_mode = checked
        if checked:
            self._generate_btn.setText("✓ fls Generation Selected")
            # Clear CSV selection when switching to generate mode
            self._selected_csv_path = None
            self._csv_path_label.setText("No file selected")
            self._csv_path_label.setStyleSheet("color: gray;")
        else:
            self._generate_btn.setText("⚡ Use fls Generation")
        self.configChanged.emit()

    def _on_import_clicked(self):
        """Handle import button click - open file dialog."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select File List CSV",
            "",
            "CSV Files (*.csv);;All Files (*)"
        )
        if file_path:
            self._selected_csv_path = file_path
            self._csv_path_label.setText(Path(file_path).name)
            self._csv_path_label.setStyleSheet("color: green;")
            # Uncheck generate mode when CSV is selected
            self._generate_mode = False
            self._generate_btn.setChecked(False)
            self._generate_btn.setText("⚡ Use fls Generation")
            self.configChanged.emit()

    @property
    def fls_available(self) -> bool:
        """Check if fls is available."""
        return get_sleuthkit_bin("fls") is not None

    def get_config(self) -> dict:
        """
        Get current configuration.

        Returns:
            Dictionary with mode and settings
        """
        # Map combo box index to import source
        source_map = {
            0: "auto",
            1: "ftk",
            2: "encase",
            3: "generic"
        }

        return {
            "generate_from_e01": self._generate_mode,
            "imported_csv_path": self._selected_csv_path,
            "import_source": source_map.get(self._source_combo.currentIndex(), "auto")
        }

    def set_config(self, config: dict):
        """
        Set configuration from dictionary.

        Args:
            config: Dictionary with mode and settings
        """
        # Map import source to combo box index
        source_map = {
            "auto": 0,
            "ftk": 1,
            "encase": 2,
            "generic": 3
        }

        import_source = config.get("import_source", "auto")
        index = source_map.get(import_source, 0)
        self._source_combo.setCurrentIndex(index)

        # Set generate mode
        self._generate_mode = config.get("generate_from_e01", False)
        self._generate_btn.setChecked(self._generate_mode)
        if self._generate_mode:
            self._generate_btn.setText("✓ fls Generation Selected")

        # Set CSV path
        csv_path = config.get("imported_csv_path")
        if csv_path:
            self._selected_csv_path = csv_path
            self._csv_path_label.setText(Path(csv_path).name)
            self._csv_path_label.setStyleSheet("color: green;")


class FileListStatusWidget(QWidget):
    """
    Status widget showing file list import results.

    Displays:
    - CSV file name (if found)
    - Number of entries in database
    - Import timestamp (if available)
    """

    def __init__(
        self,
        parent,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ):
        super().__init__(parent)
        self.output_dir = output_dir
        self.evidence_conn = evidence_conn
        self.evidence_id = evidence_id
        self._setup_ui()
        self._load_status()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self._status_label = QLabel("Loading...")
        self._status_label.setWordWrap(True)
        layout.addWidget(self._status_label)

    def _load_status(self):
        """Load and display current import status."""
        # Check for file_list.csv in output directory
        csv_file = self.output_dir / "file_list.csv"
        csv_exists = csv_file.exists()

        # Query database for file list entries
        cursor = self.evidence_conn.cursor()
        cursor.execute(
            '''
            SELECT
                COUNT(*) as total,
                import_source,
                import_timestamp
            FROM file_list
            WHERE evidence_id = ?
            ORDER BY import_timestamp DESC
            LIMIT 1
            ''',
            (self.evidence_id,)
        )
        row = cursor.fetchone()

        # Build status message
        status_parts = []

        if csv_exists:
            try:
                size_kb = csv_file.stat().st_size / 1024
                status_parts.append(f"CSV: file_list.csv ({size_kb:.1f} KB)")
            except OSError:
                status_parts.append("CSV: file_list.csv")
        else:
            status_parts.append("CSV: Not imported (click Import button)")

        if row and row[0] > 0:
            count = row[0]
            source = row[1] or "unknown"
            timestamp = row[2] or "unknown"

            status_parts.append(f"Database: {count:,} entries")
            status_parts.append(f"Source: {source}")
            if timestamp != "unknown":
                # Simplify timestamp display
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    timestamp = dt.strftime("%Y-%m-%d %H:%M")
                except (ValueError, AttributeError):
                    pass
            status_parts.append(f"Last import: {timestamp}")
        else:
            status_parts.append("Database: No entries")

        self._status_label.setText("\n".join(status_parts))

    def refresh(self):
        """Refresh status display."""
        self._load_status()
