from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, TYPE_CHECKING

from PySide6.QtCore import QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from app.config.settings import AppSettings, GeneralSettings, HashSettings, NetworkSettings, ToolPaths, ReportSettings
from core.tool_discovery import get_tool_version
import json
import shutil
import subprocess
import yaml
from jsonschema import Draft202012Validator, ValidationError

if TYPE_CHECKING:
    from core.tool_registry import ToolRegistry


class PreferencesDialog(QDialog):
    """Preferences window with settings grouped by category."""

    def __init__(
        self,
        settings: AppSettings,
        config_dir: Optional[Path] = None,
        rules_dir: Optional[Path] = None,
        tool_registry: Optional['ToolRegistry'] = None,  #
        initial_tab: Optional[str] = None,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Preferences")
        self._original = settings
        self._config_dir = Path(config_dir) if config_dir else None
        self._rules_dir = Path(rules_dir) if rules_dir else None
        self._tool_registry = tool_registry  #
        self._initial_tab = initial_tab
        self._tool_line_edits: Dict[str, QLineEdit] = {}
        self.result_settings: Optional[AppSettings] = None

        self.tabs = QTabWidget()
        self._build_general_tab(settings)
        self._build_tools_tab(settings)
        self._build_network_tab(settings)
        self._build_hash_tab(settings)
        self._build_file_lists_tab()  #
        self._build_hash_lists_tab()  #
        self._build_url_lists_tab()  #
        self._build_rules_tab()
        self._build_reports_tab(settings)  #
        self._build_text_blocks_tab()  #

        self.error_label = QLabel()
        self.error_label.setStyleSheet("color: #c62828;")
        self.error_label.setWordWrap(True)
        self.restore_button = QPushButton("Restore defaults")
        self.restore_button.clicked.connect(self._restore_defaults)

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        self.save_button = buttons.button(QDialogButtonBox.Save)
        self.save_button.setEnabled(False)
        buttons.accepted.connect(self._on_save)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addWidget(self.tabs)
        layout.addWidget(self.restore_button)
        layout.addWidget(self.error_label)
        layout.addWidget(buttons)
        self.setLayout(layout)
        self.resize(520, self.sizeHint().height())

        self._select_initial_tab()
        self._validate()

    def _select_initial_tab(self) -> None:
        """Select the configured initial tab if it exists."""
        if not self._initial_tab:
            return

        target = self._initial_tab.strip().lower()
        if not target:
            return

        for index in range(self.tabs.count()):
            if self.tabs.tabText(index).strip().lower() == target:
                self.tabs.setCurrentIndex(index)
                return

    # General -------------------------------------------------------------

    def _build_general_tab(self, settings: AppSettings) -> None:
        widget = QWidget()
        form = QFormLayout()

        self.thumbnail_spin = QSpinBox()
        self.thumbnail_spin.setRange(64, 512)
        self.thumbnail_spin.setSingleStep(16)
        self.thumbnail_spin.setValue(settings.general.thumbnail_size)
        self.thumbnail_spin.valueChanged.connect(self._validate)
        form.addRow("Thumbnail size (px)", self.thumbnail_spin)

        self.open_config_button = QPushButton("Open config directory")
        self.open_config_button.setEnabled(self._config_dir is not None)
        self.open_config_button.clicked.connect(self._open_config_dir)
        form.addRow("", self.open_config_button)

        widget.setLayout(form)
        self.tabs.addTab(widget, "General")

    # Tools ---------------------------------------------------------------

    def _build_tools_tab(self, settings: AppSettings) -> None:
        # Use enhanced ToolsTab if tool_registry available
        if self._tool_registry:
            from app.common.widgets import ToolsTab
            widget = ToolsTab(self._tool_registry, self)
            self.tabs.addTab(widget, "Tools")
            return

        # Fallback to legacy simple tools tab
        widget = QWidget()
        form = QFormLayout()

        self.bulk_edit = self._create_path_row("bulk_extractor", settings.tools.bulk_extractor, form)
        self.foremost_edit = self._create_path_row("foremost / scalpel", settings.tools.foremost, form)
        self.exiftool_edit = self._create_path_row("exiftool", settings.tools.exiftool, form)
        self.ewfmount_edit = self._create_path_row("ewfmount", settings.tools.ewfmount, form)

        widget.setLayout(form)
        self.tabs.addTab(widget, "Tools")

    def _create_path_row(self, name: str, value: str, form: QFormLayout) -> QLineEdit:
        line = QLineEdit(value)
        browse = QPushButton("Browse…")
        browse.clicked.connect(lambda: self._pick_file(line))
        test = QPushButton("Test")
        test.clicked.connect(lambda: self._test_tool_path(name, line))
        line.textChanged.connect(self._validate)
        container = QHBoxLayout()
        container.addWidget(line)
        container.addWidget(browse)
        container.addWidget(test)
        row_widget = QWidget()
        row_widget.setLayout(container)
        form.addRow(name.replace("_", " ").title(), row_widget)
        self._tool_line_edits[name] = line
        return line

    # Network -------------------------------------------------------------

    def _build_network_tab(self, settings: AppSettings) -> None:
        widget = QWidget()
        form = QFormLayout()

        self.concurrency_spin = QSpinBox()
        self.concurrency_spin.setRange(1, 4)
        self.concurrency_spin.setValue(settings.network.concurrency)
        self.concurrency_spin.valueChanged.connect(self._validate)
        form.addRow("Concurrency", self.concurrency_spin)

        self.timeout_spin = QSpinBox()
        self.timeout_spin.setRange(5, 60)
        self.timeout_spin.setValue(settings.network.timeout_s)
        self.timeout_spin.valueChanged.connect(self._validate)
        form.addRow("Timeout (s)", self.timeout_spin)

        self.retries_spin = QSpinBox()
        self.retries_spin.setRange(0, 5)
        self.retries_spin.setValue(settings.network.retries)
        self.retries_spin.valueChanged.connect(self._validate)
        form.addRow("Retries", self.retries_spin)

        self.max_bytes_spin = QSpinBox()
        self.max_bytes_spin.setRange(1, 2048)
        self.max_bytes_spin.setSuffix(" MB")
        self.max_bytes_spin.setValue(max(1, settings.network.max_bytes // (1024 * 1024)))
        self.max_bytes_spin.valueChanged.connect(self._validate)
        form.addRow("Max download size", self.max_bytes_spin)

        self.allowed_types_edit = QLineEdit(",".join(settings.network.allowed_content_types))
        self.allowed_types_edit.setPlaceholderText("image/*,video/*")
        self.allowed_types_edit.textChanged.connect(self._validate)
        form.addRow("Allowed content types", self.allowed_types_edit)

        widget.setLayout(form)
        self.tabs.addTab(widget, "Network")

    # Hash ----------------------------------------------------------------

    def _build_hash_tab(self, settings: AppSettings) -> None:
        widget = QWidget()
        layout = QGridLayout()
        self.hash_path_edit = QLineEdit(settings.hash.db_path)
        self.hash_path_edit.setPlaceholderText("Select SQLite hash database…")
        self.hash_path_edit.textChanged.connect(self._validate)
        browse = QPushButton("Browse…")
        browse.clicked.connect(lambda: self._pick_file(self.hash_path_edit))
        layout.addWidget(QLabel("SQLite database"), 0, 0)
        layout.addWidget(self.hash_path_edit, 0, 1)
        layout.addWidget(browse, 0, 2)
        widget.setLayout(layout)
        self.tabs.addTab(widget, "Hash DB")

    # Rules ---------------------------------------------------------------

    def _build_rules_tab(self) -> None:
        """Build the Rules tab showing loaded rule files with validation."""
        widget = QWidget()
        layout = QVBoxLayout()

        # Instructions
        info_label = QLabel(
            "Rule files define extraction targets, carvers, and detectors. "
            "Select a rule to validate or open in your editor."
        )
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        # List of rule files
        self.rules_list = QListWidget()
        self.rules_list.currentItemChanged.connect(self._on_rule_selected)
        layout.addWidget(self.rules_list)

        # Buttons
        buttons_layout = QHBoxLayout()

        self.validate_rule_button = QPushButton("Validate Selected")
        self.validate_rule_button.setEnabled(False)
        self.validate_rule_button.clicked.connect(self._validate_selected_rule)
        buttons_layout.addWidget(self.validate_rule_button)

        self.validate_all_button = QPushButton("Validate All")
        self.validate_all_button.clicked.connect(self._validate_all_rules)
        buttons_layout.addWidget(self.validate_all_button)

        self.open_editor_button = QPushButton("Open in Editor")
        self.open_editor_button.setEnabled(False)
        self.open_editor_button.clicked.connect(self._open_rule_in_editor)
        buttons_layout.addWidget(self.open_editor_button)

        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.clicked.connect(self._refresh_rules_list)
        buttons_layout.addWidget(self.refresh_button)

        buttons_layout.addStretch()
        layout.addLayout(buttons_layout)

        # Validation output
        validation_label = QLabel("Validation Output:")
        layout.addWidget(validation_label)

        self.validation_output = QTextEdit()
        self.validation_output.setReadOnly(True)
        self.validation_output.setMaximumHeight(150)
        layout.addWidget(self.validation_output)

        widget.setLayout(layout)
        self.tabs.addTab(widget, "Rules")

        # Load rules list initially
        self._refresh_rules_list()

    def _refresh_rules_list(self) -> None:
        """Refresh the list of rule files."""
        self.rules_list.clear()
        self.validation_output.clear()

        if not self._rules_dir or not self._rules_dir.exists():
            self.validation_output.setPlainText(
                "Rules directory not found: {path}".format(
                    path=str(self._rules_dir) if self._rules_dir else "None"
                )
            )
            return

        # Find all YAML rule files
        rule_files = sorted(
            {path for suffix in ("*.yml", "*.yaml") for path in self._rules_dir.rglob(suffix)}
        )

        if not rule_files:
            self.validation_output.setPlainText(
                "No rule files found in {path}".format(path=str(self._rules_dir))
            )
            return

        for rule_file in rule_files:
            relative_path = rule_file.relative_to(self._rules_dir)
            item = QListWidgetItem(str(relative_path))
            item.setData(1, rule_file)  # Store full path in user role
            self.rules_list.addItem(item)

        self.validation_output.setPlainText(
            "Found {count} rule file(s).".format(count=len(rule_files))
        )

    def _on_rule_selected(self, current: Optional[QListWidgetItem], previous: Optional[QListWidgetItem]) -> None:  # noqa: ARG002
        """Enable/disable buttons based on selection."""
        has_selection = current is not None
        self.validate_rule_button.setEnabled(has_selection)
        self.open_editor_button.setEnabled(has_selection)

    def _validate_selected_rule(self) -> None:
        """Validate the selected rule file against the JSON schema."""
        current_item = self.rules_list.currentItem()
        if not current_item:
            return

        rule_file = Path(current_item.data(1))
        self._validate_rule_file(rule_file)

    def _validate_all_rules(self) -> None:
        """Validate all loaded rule files against the JSON schema."""
        if not self._rules_dir or not self._rules_dir.exists():
            self.validation_output.setPlainText(
                "Rules directory not found."
            )
            return

        self.validation_output.clear()
        output_lines = ["Validating all rules...\n"]

        # Load schema
        schema_path = self._rules_dir.parent / "docs" / "rules.schema.json"
        if not schema_path.exists():
            output_lines.append(
                "ERROR: Schema file not found at {path}".format(path=str(schema_path))
            )
            self.validation_output.setPlainText("\n".join(output_lines))
            return

        try:
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            validator = Draft202012Validator(schema)
        except Exception as exc:
            output_lines.append(
                "ERROR: Failed to load schema: {error}".format(error=str(exc))
            )
            self.validation_output.setPlainText("\n".join(output_lines))
            return

        # Validate each rule file
        rule_files = sorted(
            {path for suffix in ("*.yml", "*.yaml") for path in self._rules_dir.rglob(suffix)}
        )

        errors_found = 0
        for rule_file in rule_files:
            relative_path = rule_file.relative_to(self._rules_dir)
            try:
                raw_text = rule_file.read_text(encoding="utf-8")
                document = yaml.safe_load(raw_text) or {}

                # Collect all validation errors
                validation_errors = list(validator.iter_errors(document))

                if validation_errors:
                    errors_found += 1
                    output_lines.append(f"\n❌ {relative_path}: {len(validation_errors)} error(s)")
                    for error in validation_errors[:3]:  # Show first 3 errors
                        path_str = " → ".join(str(p) for p in error.path) if error.path else "root"
                        output_lines.append(f"  • {path_str}: {error.message}")
                    if len(validation_errors) > 3:
                        output_lines.append(f"  ... and {len(validation_errors) - 3} more error(s)")
                else:
                    output_lines.append(f"✓ {relative_path}")

            except yaml.YAMLError as exc:
                errors_found += 1
                output_lines.append(f"\n❌ {relative_path}: YAML parsing error")
                output_lines.append(f"  {str(exc)}")
            except Exception as exc:
                errors_found += 1
                output_lines.append(f"\n❌ {relative_path}: {str(exc)}")

        # Summary
        output_lines.append(f"\n{'-' * 50}")
        if errors_found == 0:
            output_lines.append("✓ All {count} rule file(s) validated successfully!".format(count=len(rule_files)))
        else:
            output_lines.append(
                "❌ {errors} file(s) with errors out of {total}".format(
                    errors=errors_found, total=len(rule_files)
                )
            )

        self.validation_output.setPlainText("\n".join(output_lines))

    def _validate_rule_file(self, rule_file: Path) -> None:
        """Validate a single rule file and display results."""
        self.validation_output.clear()

        if not rule_file.exists():
            self.validation_output.setPlainText(
                "ERROR: Rule file not found: {path}".format(path=str(rule_file))
            )
            return

        # Load schema
        schema_path = self._rules_dir.parent / "docs" / "rules.schema.json" if self._rules_dir else None
        if not schema_path or not schema_path.exists():
            self.validation_output.setPlainText(
                "ERROR: Schema file not found at {path}".format(
                    path=str(schema_path) if schema_path else "Unknown"
                )
            )
            return

        try:
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            validator = Draft202012Validator(schema)
        except Exception as exc:
            self.validation_output.setPlainText(
                "ERROR: Failed to load schema:\n{error}".format(error=str(exc))
            )
            return

        # Validate rule file
        try:
            raw_text = rule_file.read_text(encoding="utf-8")
            document = yaml.safe_load(raw_text) or {}

            # Collect all validation errors
            validation_errors = list(validator.iter_errors(document))

            output_lines = [
                "Validating: {path}".format(
                    path=rule_file.relative_to(self._rules_dir) if self._rules_dir else rule_file.name
                ),
                ""
            ]

            if validation_errors:
                output_lines.append("❌ {count} validation error(s) found:\n".format(count=len(validation_errors)))
                for idx, error in enumerate(validation_errors, 1):
                    path_str = " → ".join(str(p) for p in error.path) if error.path else "root"
                    output_lines.append(f"{idx}. Path: {path_str}")
                    output_lines.append(f"   Error: {error.message}")
                    if error.validator:
                        output_lines.append(f"   Validator: {error.validator}")
                    output_lines.append("")
            else:
                output_lines.append("✓ Rule file is valid!")

                # Show summary
                targets_count = len(document.get("targets", []))
                detectors_count = len(document.get("detectors", []))
                signatures_count = len(document.get("signatures", []))
                output_lines.append("")
                output_lines.append("Summary:")
                output_lines.append(f"  • {targets_count} target(s)")
                output_lines.append(f"  • {detectors_count} detector(s)")
                output_lines.append(f"  • {signatures_count} signature(s)")

            self.validation_output.setPlainText("\n".join(output_lines))

        except yaml.YAMLError as exc:
            self.validation_output.setPlainText(
                "❌ YAML parsing error:\n{error}".format(error=str(exc))
            )
        except Exception as exc:
            self.validation_output.setPlainText(
                "❌ Error:\n{error}".format(error=str(exc))
            )

    def _open_rule_in_editor(self) -> None:
        """Open the selected rule file in the system's default editor."""
        current_item = self.rules_list.currentItem()
        if not current_item:
            return

        rule_file = Path(current_item.data(1))

        if not rule_file.exists():
            QMessageBox.warning(
                self,
                "File Not Found",
                "Rule file not found: {path}".format(path=str(rule_file))
            )
            return

        # Try to open with system default editor
        try:
            # Use xdg-open on Linux, open on macOS, start on Windows
            import sys
            if sys.platform.startswith('linux'):
                subprocess.Popen(['xdg-open', str(rule_file)])
            elif sys.platform == 'darwin':
                subprocess.Popen(['open', str(rule_file)])
            elif sys.platform == 'win32':
                subprocess.Popen(['start', '', str(rule_file)], shell=True)
            else:
                # Fallback: use Qt's openUrl
                QDesktopServices.openUrl(QUrl.fromLocalFile(str(rule_file)))
        except Exception as exc:
            QMessageBox.warning(
                self,
                "Error Opening File",
                "Failed to open file in editor:\n{error}".format(error=str(exc))
            )

    # Reports --------------------------------------------------

    def _build_reports_tab(self, settings: AppSettings) -> None:
        """Build the default report branding settings tab."""
        widget = QWidget()
        layout = QVBoxLayout()
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        # Info label
        info_label = QLabel(
            "Set default branding values for reports. These will be used as defaults\n"
            "when opening a new case that has no per-case settings saved yet."
        )
        info_label.setWordWrap(True)
        info_label.setStyleSheet("color: palette(mid); margin-bottom: 8px;")
        layout.addWidget(info_label)

        form = QFormLayout()
        form.setSpacing(8)

        # Author Function
        self.report_author_function = QLineEdit(settings.reports.default_author_function)
        self.report_author_function.setPlaceholderText("e.g., Forensic Analyst")
        self.report_author_function.textChanged.connect(self._validate)
        form.addRow("Default Function:", self.report_author_function)

        # Author Name
        self.report_author_name = QLineEdit(settings.reports.default_author_name)
        self.report_author_name.setPlaceholderText("Your name for reports...")
        self.report_author_name.textChanged.connect(self._validate)
        form.addRow("Default Name:", self.report_author_name)

        # Organization
        self.report_org_name = QLineEdit(settings.reports.default_org_name)
        self.report_org_name.setPlaceholderText("Organization name (title page)...")
        self.report_org_name.textChanged.connect(self._validate)
        form.addRow("Organization:", self.report_org_name)

        # Department
        self.report_department = QLineEdit(settings.reports.default_department)
        self.report_department.setPlaceholderText("Department / unit (below organization)...")
        self.report_department.textChanged.connect(self._validate)
        form.addRow("Department:", self.report_department)

        # Footer text
        self.report_footer_text = QLineEdit(settings.reports.default_footer_text)
        self.report_footer_text.setPlaceholderText("Footer text (all pages)...")
        self.report_footer_text.textChanged.connect(self._validate)
        form.addRow("Footer Text:", self.report_footer_text)

        layout.addLayout(form)

        # Logo section
        logo_layout = QHBoxLayout()
        logo_label = QLabel("Logo:")
        logo_label.setFixedWidth(100)
        logo_layout.addWidget(logo_label)

        self.report_logo_path = QLineEdit()
        self.report_logo_path.setReadOnly(True)
        self.report_logo_path.setPlaceholderText("No logo selected...")
        # Display current logo path
        if settings.reports.default_logo_path:
            if self._config_dir:
                full_path = self._config_dir / settings.reports.default_logo_path
                if full_path.exists():
                    self.report_logo_path.setText(str(full_path))
        logo_layout.addWidget(self.report_logo_path)

        self.report_logo_browse = QPushButton("Browse...")
        self.report_logo_browse.setFixedWidth(80)
        self.report_logo_browse.clicked.connect(self._on_report_logo_browse)
        logo_layout.addWidget(self.report_logo_browse)

        self.report_logo_clear = QPushButton("Clear")
        self.report_logo_clear.setFixedWidth(60)
        self.report_logo_clear.clicked.connect(self._on_report_logo_clear)
        logo_layout.addWidget(self.report_logo_clear)

        layout.addLayout(logo_layout)

        # Language and Date format row
        prefs_form = QFormLayout()
        prefs_form.setSpacing(8)

        # Default locale
        self.report_locale = QComboBox()
        self.report_locale.addItem("English", "en")
        self.report_locale.addItem("Deutsch", "de")
        locale_index = self.report_locale.findData(settings.reports.default_locale)
        if locale_index >= 0:
            self.report_locale.setCurrentIndex(locale_index)
        self.report_locale.currentIndexChanged.connect(self._validate)
        prefs_form.addRow("Default Language:", self.report_locale)

        # Default date format
        self.report_date_format = QComboBox()
        self.report_date_format.addItem("European (dd.mm.yyyy)", "eu")
        self.report_date_format.addItem("US (mm/dd/yyyy)", "us")
        date_fmt_index = self.report_date_format.findData(settings.reports.default_date_format)
        if date_fmt_index >= 0:
            self.report_date_format.setCurrentIndex(date_fmt_index)
        self.report_date_format.currentIndexChanged.connect(self._validate)
        prefs_form.addRow("Default Date Format:", self.report_date_format)

        layout.addLayout(prefs_form)

        # --- Options separator ---
        options_sep = QFrame()
        options_sep.setFrameShape(QFrame.Shape.HLine)
        options_sep.setFrameShadow(QFrame.Shadow.Sunken)
        layout.addWidget(options_sep)

        options_label = QLabel("Default Title-Page & Footer Options")
        options_label.setStyleSheet("font-weight: bold; margin-top: 4px;")
        layout.addWidget(options_label)

        options_hint = QLabel(
            "These defaults are applied when a new evidence has no saved report settings."
        )
        options_hint.setWordWrap(True)
        options_hint.setStyleSheet("color: palette(mid); margin-bottom: 4px;")
        layout.addWidget(options_hint)

        # Title page visibility checkboxes
        self.report_show_case_number = QCheckBox("Show Case Number on title page")
        self.report_show_case_number.setChecked(settings.reports.default_show_title_case_number)
        layout.addWidget(self.report_show_case_number)

        self.report_show_evidence = QCheckBox("Show Evidence on title page")
        self.report_show_evidence.setChecked(settings.reports.default_show_title_evidence)
        layout.addWidget(self.report_show_evidence)

        self.report_show_investigator = QCheckBox("Show Investigator on title page")
        self.report_show_investigator.setChecked(settings.reports.default_show_title_investigator)
        layout.addWidget(self.report_show_investigator)

        self.report_show_date = QCheckBox("Show Date on title page")
        self.report_show_date.setChecked(settings.reports.default_show_title_date)
        layout.addWidget(self.report_show_date)

        self.report_show_footer_date = QCheckBox("Show creation date in footer")
        self.report_show_footer_date.setChecked(settings.reports.default_show_footer_date)
        layout.addWidget(self.report_show_footer_date)

        self.report_hide_appendix_pg = QCheckBox("Hide page numbers in appendix")
        self.report_hide_appendix_pg.setChecked(settings.reports.default_hide_appendix_page_numbers)
        layout.addWidget(self.report_hide_appendix_pg)

        layout.addStretch()

        widget.setLayout(layout)
        self.tabs.addTab(widget, "Reports")

    def _build_text_blocks_tab(self) -> None:
        """Build the text blocks management tab."""
        from .text_blocks_tab import TextBlocksTab

        # Text blocks are global user-level settings, independent from case workspace.
        widget = TextBlocksTab(None, self)
        self.tabs.addTab(widget, "Text Blocks")

    def _on_report_logo_browse(self) -> None:
        """Handle logo browse button - copy to config/branding/."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Logo Image",
            "",
            "Images (*.png *.jpg *.jpeg *.gif *.svg);;All Files (*)",
        )
        if not file_path:
            return

        if not self._config_dir:
            # No config dir, just show the path
            self.report_logo_path.setText(file_path)
            return

        # Copy to config/branding/
        branding_dir = self._config_dir / "branding"
        branding_dir.mkdir(parents=True, exist_ok=True)

        source = Path(file_path)
        dest = branding_dir / f"logo{source.suffix.lower()}"

        try:
            shutil.copy2(source, dest)
            self.report_logo_path.setText(str(dest))
            self._validate()
        except Exception as exc:
            QMessageBox.warning(
                self,
                "Error Copying Logo",
                f"Failed to copy logo to config directory:\n{exc}",
            )

    def _on_report_logo_clear(self) -> None:
        """Clear the logo path."""
        self.report_logo_path.clear()
        self._validate()

    def _get_report_logo_relative_path(self) -> str:
        """Get the logo path relative to config dir, or empty string."""
        logo_text = self.report_logo_path.text().strip()
        if not logo_text or not self._config_dir:
            return ""

        logo_path = Path(logo_text)
        try:
            rel_path = logo_path.relative_to(self._config_dir)
            return str(rel_path)
        except ValueError:
            # Not relative to config dir, return empty (won't be portable)
            return ""

    # File Lists -----------------------------------------------

    def _build_file_lists_tab(self) -> None:
        """Build the file lists management tab."""
        from core.matching import ReferenceListManager

        widget = QWidget()
        layout = QVBoxLayout()

        # Initialize reference list manager (shared with hash lists)
        if not hasattr(self, 'ref_manager'):
            self.ref_manager = ReferenceListManager()

        # Info label
        info_text = (
            "File lists contain filename patterns (wildcards or regex) for matching files.\n"
            f"Location: {self.ref_manager.filelists_dir}"
        )
        info_label = QLabel(info_text)
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        # File list widget
        self.filelist_widget = QListWidget()
        self.filelist_widget.currentItemChanged.connect(self._on_filelist_selection_changed)
        layout.addWidget(self.filelist_widget)

        # File list buttons
        buttons_layout = QHBoxLayout()

        self.add_filelist_btn = QPushButton("Add File List")
        self.add_filelist_btn.clicked.connect(self._add_filelist)
        buttons_layout.addWidget(self.add_filelist_btn)

        self.view_filelist_btn = QPushButton("View")
        self.view_filelist_btn.setEnabled(False)
        self.view_filelist_btn.clicked.connect(self._view_filelist)
        buttons_layout.addWidget(self.view_filelist_btn)

        self.delete_filelist_btn = QPushButton("Delete")
        self.delete_filelist_btn.setEnabled(False)
        self.delete_filelist_btn.clicked.connect(self._delete_filelist)
        buttons_layout.addWidget(self.delete_filelist_btn)

        buttons_layout.addStretch()
        layout.addLayout(buttons_layout)

        # Install predefined lists button
        self.install_predefined_btn = QPushButton("Install Predefined Lists")
        self.install_predefined_btn.setToolTip(
            "Copy predefined file lists and hash lists from the application to your config directory"
        )
        self.install_predefined_btn.clicked.connect(self._install_predefined_lists)
        layout.addWidget(self.install_predefined_btn)

        widget.setLayout(layout)
        self.tabs.addTab(widget, "File Lists")

        # Load file lists
        self._refresh_file_lists()

    def _build_hash_lists_tab(self) -> None:
        """Build the hash lists management tab."""
        from core.matching import ReferenceListManager

        widget = QWidget()
        layout = QVBoxLayout()

        # Initialize reference list manager (shared with file lists)
        if not hasattr(self, 'ref_manager'):
            self.ref_manager = ReferenceListManager()

        # Info label
        info_text = (
            "Hash lists contain MD5, SHA1, or SHA256 hashes for exact file matching.\n"
            f"Location: {self.ref_manager.hashlists_dir}"
        )
        info_label = QLabel(info_text)
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        # Hash list widget
        self.hashlist_widget = QListWidget()
        self.hashlist_widget.currentItemChanged.connect(self._on_hashlist_selection_changed)
        layout.addWidget(self.hashlist_widget)

        # Hash list buttons
        buttons_layout = QHBoxLayout()

        self.add_hashlist_btn = QPushButton("Add Hash List")
        self.add_hashlist_btn.clicked.connect(self._add_hashlist)
        buttons_layout.addWidget(self.add_hashlist_btn)

        self.import_folder_btn = QPushButton("📁 Import Folder")
        self.import_folder_btn.setToolTip("Import all .txt hash lists from a folder")
        self.import_folder_btn.clicked.connect(self._import_hashlist_folder)
        buttons_layout.addWidget(self.import_folder_btn)

        self.view_hashlist_btn = QPushButton("View")
        self.view_hashlist_btn.setEnabled(False)
        self.view_hashlist_btn.clicked.connect(self._view_hashlist)
        buttons_layout.addWidget(self.view_hashlist_btn)

        self.delete_hashlist_btn = QPushButton("Delete")
        self.delete_hashlist_btn.setEnabled(False)
        self.delete_hashlist_btn.clicked.connect(self._delete_hashlist)
        buttons_layout.addWidget(self.delete_hashlist_btn)

        buttons_layout.addStretch()
        layout.addLayout(buttons_layout)

        # Rebuild Hash DB section (Phase 4)
        hash_db_layout = QHBoxLayout()

        self.rebuild_hashdb_btn = QPushButton("Rebuild Hash DB")
        self.rebuild_hashdb_btn.setToolTip(
            "Rebuild the SQLite hash database from all .txt hash lists.\n"
            "This is required for fast hash matching in the Images tab."
        )
        self.rebuild_hashdb_btn.clicked.connect(self._rebuild_hash_db)
        hash_db_layout.addWidget(self.rebuild_hashdb_btn)

        self.hash_db_status_label = QLabel()
        self.hash_db_status_label.setStyleSheet("color: gray; font-style: italic;")
        hash_db_layout.addWidget(self.hash_db_status_label)
        hash_db_layout.addStretch()

        layout.addLayout(hash_db_layout)

        widget.setLayout(layout)
        self.tabs.addTab(widget, "Hash Lists")

        # Load hash lists
        self._refresh_hash_lists()
        self._update_hash_db_status()

    def _build_url_lists_tab(self) -> None:
        """Build the URL lists management tab."""
        from core.matching import ReferenceListManager

        widget = QWidget()
        layout = QVBoxLayout()

        # Initialize reference list manager (shared with file lists and hash lists)
        if not hasattr(self, 'ref_manager'):
            self.ref_manager = ReferenceListManager()

        # Info label
        info_text = (
            "URL lists contain URL patterns (domains, wildcards, or regex) for matching discovered URLs.\n"
            f"Location: {self.ref_manager.urllists_dir}"
        )
        info_label = QLabel(info_text)
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        # URL list widget
        self.urllist_widget = QListWidget()
        self.urllist_widget.currentItemChanged.connect(self._on_urllist_selection_changed)
        layout.addWidget(self.urllist_widget)

        # URL list buttons
        buttons_layout = QHBoxLayout()

        self.add_urllist_btn = QPushButton("Add URL List")
        self.add_urllist_btn.clicked.connect(self._add_urllist)
        buttons_layout.addWidget(self.add_urllist_btn)

        self.view_urllist_btn = QPushButton("View")
        self.view_urllist_btn.setEnabled(False)
        self.view_urllist_btn.clicked.connect(self._view_urllist)
        buttons_layout.addWidget(self.view_urllist_btn)

        self.delete_urllist_btn = QPushButton("Delete")
        self.delete_urllist_btn.setEnabled(False)
        self.delete_urllist_btn.clicked.connect(self._delete_urllist)
        buttons_layout.addWidget(self.delete_urllist_btn)

        buttons_layout.addStretch()
        layout.addLayout(buttons_layout)

        # Reuse install predefined lists button (already installs URL lists too)
        # No need to add another button here - it's in the File Lists tab

        widget.setLayout(layout)
        self.tabs.addTab(widget, "URL Lists")

        # Load URL lists
        self._refresh_url_lists()

    def _refresh_file_lists(self) -> None:
        """Refresh the file lists display."""
        self.filelist_widget.clear()

        available = self.ref_manager.list_available()

        # Populate file lists
        for filelist in available["filelists"]:
            item = QListWidgetItem(filelist)
            self.filelist_widget.addItem(item)

    def _refresh_hash_lists(self) -> None:
        """Refresh the hash lists display."""
        self.hashlist_widget.clear()

        available = self.ref_manager.list_available()

        # Populate hash lists
        for hashlist in available["hashlists"]:
            item = QListWidgetItem(hashlist)
            self.hashlist_widget.addItem(item)

    def _refresh_reference_lists(self) -> None:
        """Refresh file lists, hash lists, and URL lists."""
        self._refresh_file_lists()
        self._refresh_hash_lists()
        self._refresh_url_lists()

    def _on_hashlist_selection_changed(self, current, previous) -> None:
        """Handle hash list selection change."""
        has_selection = current is not None
        self.view_hashlist_btn.setEnabled(has_selection)
        self.delete_hashlist_btn.setEnabled(has_selection)

    def _on_filelist_selection_changed(self, current, previous) -> None:
        """Handle file list selection change."""
        has_selection = current is not None
        self.view_filelist_btn.setEnabled(has_selection)
        self.delete_filelist_btn.setEnabled(has_selection)

    def _add_hashlist(self) -> None:
        """Add a new hash list from file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Hash List File",
            "",
            "Text Files (*.txt);;All Files (*)"
        )

        if not file_path:
            return

        # Get name from user
        from PySide6.QtWidgets import QInputDialog
        name, ok = QInputDialog.getText(
            self,
            "Hash List Name",
            "Enter a name for this hash list (without .txt):",
            text=Path(file_path).stem
        )

        if not ok or not name:
            return

        # Copy file to hashlists directory
        try:
            import shutil
            dest_path = self.ref_manager.hashlists_dir / f"{name}.txt"
            if dest_path.exists():
                reply = QMessageBox.question(
                    self,
                    "Overwrite?",
                    "Hash list '{name}' already exists. Overwrite?".format(name=name),
                    QMessageBox.Yes | QMessageBox.No
                )
                if reply != QMessageBox.Yes:
                    return

            shutil.copy2(file_path, dest_path)
            QMessageBox.information(
                self,
                "Success",
                "Hash list '{name}' added successfully".format(name=name)
            )
            self._refresh_reference_lists()
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                "Failed to add hash list: {error}".format(error=str(e))
            )

    def _import_hashlist_folder(self) -> None:
        """Import all hash lists from a selected folder."""
        from PySide6.QtCore import QThreadPool
        from app.common.dialogs import BatchHashListImportDialog, BatchImportProgressDialog
        from app.services.workers import BatchHashListImportTask, BatchHashListImportConfig

        # Step 1: Select folder
        folder = QFileDialog.getExistingDirectory(
            self,
            "Select Folder with Hash Lists",
            "",
            QFileDialog.ShowDirsOnly
        )

        if not folder:
            return

        # Step 2: Scan for .txt files
        folder_path = Path(folder)
        files = sorted(folder_path.glob("*.txt"))

        if not files:
            QMessageBox.information(
                self,
                "No Files Found",
                "No .txt files found in the selected folder."
            )
            return

        # Step 3: Get existing names for conflict detection
        existing = set(self.ref_manager.list_available()["hashlists"])

        # Step 4: Show preview dialog
        preview_dialog = BatchHashListImportDialog(files, existing, self)
        if preview_dialog.exec() != QDialog.Accepted:
            return

        # Step 5: Get selected files and options
        selected_files = preview_dialog.get_selected_files()
        if not selected_files:
            return

        conflict_policy = preview_dialog.get_conflict_policy()
        rebuild_db = preview_dialog.should_rebuild_db()

        # Step 6: Show progress dialog and run import
        progress_dialog = BatchImportProgressDialog(len(selected_files), self)

        # Create task config
        config = BatchHashListImportConfig(
            files=tuple(selected_files),
            conflict_policy=conflict_policy,
            rebuild_db=rebuild_db,
        )

        # Create and configure task
        self._import_task = BatchHashListImportTask(config)

        def on_progress(percent: int, message: str) -> None:
            pass  # Handled by file_progress signal

        def on_file_progress(current: int, total: int, filename: str) -> None:
            progress_dialog.update_progress(current, filename)

        def on_result(result: dict) -> None:
            progress_dialog.set_complete()

            # Show summary
            imported = result.get("imported", 0)
            skipped = result.get("skipped", 0)
            errors = result.get("errors", 0)
            cancelled = result.get("cancelled", 0)
            rebuild_success = result.get("rebuild_success", False)
            rebuild_count = result.get("rebuild_count", 0)

            summary_parts = []
            if imported > 0:
                summary_parts.append(f"{imported} imported")
            if skipped > 0:
                summary_parts.append(f"{skipped} skipped")
            if errors > 0:
                summary_parts.append(f"{errors} errors")
            if cancelled > 0:
                summary_parts.append(f"{cancelled} cancelled")

            summary = ", ".join(summary_parts) if summary_parts else "No files processed"

            if rebuild_success:
                summary += f"\n\nHash database rebuilt with {rebuild_count} entries."
            elif rebuild_db and imported > 0:
                summary += "\n\n⚠ Hash database rebuild failed."

            QMessageBox.information(
                self,
                "Import Complete",
                summary
            )

            # Refresh the list
            self._refresh_reference_lists()
            self._update_hash_db_status()

        def on_error(error: str, tb: str) -> None:
            progress_dialog.close()
            QMessageBox.critical(
                self,
                "Import Error",
                f"Import failed: {error}"
            )

        def on_finished() -> None:
            self._import_task = None

        def on_cancelled() -> None:
            self._import_task.cancel()

        # Connect signals
        self._import_task.signals.progress.connect(on_progress)
        self._import_task.signals.file_progress.connect(on_file_progress)
        self._import_task.signals.result.connect(on_result)
        self._import_task.signals.error.connect(on_error)
        self._import_task.signals.finished.connect(on_finished)
        progress_dialog.cancelled.connect(on_cancelled)

        # Start task
        QThreadPool.globalInstance().start(self._import_task)

        # Show progress dialog (non-blocking)
        progress_dialog.exec()

    def _add_filelist(self) -> None:
        """Add a new file list from file with metadata configuration."""
        from datetime import datetime
        from app.common.dialogs.add_filelist import AddFileListDialog

        # Step 1: Select file
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Pattern File",
            "",
            "Text Files (*.txt);;All Files (*)"
        )

        if not file_path:
            return

        # Step 2: Read and process patterns
        try:
            patterns = []
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    # Skip empty lines and comments
                    if line and not line.startswith("#"):
                        patterns.append(line)
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                "Failed to read file: {error}".format(error=str(e))
            )
            return

        # Step 3: Validate not empty
        if not patterns:
            QMessageBox.critical(
                self,
                "Invalid File",
                "No valid patterns found in file. "
                "File must contain at least one non-empty, non-comment line."
            )
            return

        # Step 4: Show metadata configuration dialog
        dialog = AddFileListDialog(
            patterns=patterns,
            suggested_name=Path(file_path).stem,
            parent=self
        )

        if dialog.exec() != QDialog.Accepted:
            return

        # Step 5: Check if file list already exists
        dest_path = self.ref_manager.filelists_dir / f"{dialog.name}.txt"
        if dest_path.exists():
            reply = QMessageBox.question(
                self,
                "Overwrite?",
                "File list '{name}' already exists. Overwrite?".format(name=dialog.name),
                QMessageBox.Yes | QMessageBox.No
            )
            if reply != QMessageBox.Yes:
                return

        # Step 6: Generate metadata and write file
        try:
            with open(dest_path, "w", encoding="utf-8") as f:
                # Write metadata header
                f.write(f"# NAME: {dialog.name}\n")
                f.write(f"# CATEGORY: {dialog.category}\n")
                f.write(f"# DESCRIPTION: {dialog.description}\n")
                f.write(f"# UPDATED: {datetime.now().strftime('%Y-%m-%d')}\n")
                f.write(f"# TYPE: filelist\n")
                f.write(f"# REGEX: {'true' if dialog.is_regex else 'false'}\n")
                f.write("\n")

                # Write patterns
                for pattern in patterns:
                    f.write(f"{pattern}\n")

            QMessageBox.information(
                self,
                "Success",
                "File list '{name}' added successfully with {count} patterns.".format(
                    name=dialog.name,
                    count=len(patterns)
                )
            )
            self._refresh_reference_lists()

        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                "Failed to create file list: {error}".format(error=str(e))
            )

    def _view_hashlist(self) -> None:
        """View the selected hash list."""
        current_item = self.hashlist_widget.currentItem()
        if not current_item:
            return

        name = current_item.text()
        try:
            list_path = self.ref_manager.hashlists_dir / f"{name}.txt"
            content = list_path.read_text(encoding="utf-8")

            # Show in dialog
            dialog = QDialog(self)
            dialog.setWindowTitle("Hash List: {name}".format(name=name))
            layout = QVBoxLayout()

            text_edit = QTextEdit()
            text_edit.setPlainText(content)
            text_edit.setReadOnly(True)
            layout.addWidget(text_edit)

            close_btn = QPushButton("Close")
            close_btn.clicked.connect(dialog.accept)
            layout.addWidget(close_btn)

            dialog.setLayout(layout)
            dialog.resize(600, 400)
            dialog.exec()
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                "Failed to load hash list: {error}".format(error=str(e))
            )

    def _view_filelist(self) -> None:
        """View the selected file list."""
        current_item = self.filelist_widget.currentItem()
        if not current_item:
            return

        name = current_item.text()
        try:
            list_path = self.ref_manager.filelists_dir / f"{name}.txt"
            content = list_path.read_text(encoding="utf-8")

            # Show in dialog
            dialog = QDialog(self)
            dialog.setWindowTitle("File List: {name}".format(name=name))
            layout = QVBoxLayout()

            text_edit = QTextEdit()
            text_edit.setPlainText(content)
            text_edit.setReadOnly(True)
            layout.addWidget(text_edit)

            close_btn = QPushButton("Close")
            close_btn.clicked.connect(dialog.accept)
            layout.addWidget(close_btn)

            dialog.setLayout(layout)
            dialog.resize(600, 400)
            dialog.exec()
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                "Failed to load file list: {error}".format(error=str(e))
            )

    def _delete_hashlist(self) -> None:
        """Delete the selected hash list."""
        current_item = self.hashlist_widget.currentItem()
        if not current_item:
            return

        name = current_item.text()
        reply = QMessageBox.question(
            self,
            "Confirm Delete",
            "Are you sure you want to delete hash list '{name}'?".format(name=name),
            QMessageBox.Yes | QMessageBox.No
        )

        if reply != QMessageBox.Yes:
            return

        try:
            list_path = self.ref_manager.hashlists_dir / f"{name}.txt"
            list_path.unlink()
            QMessageBox.information(
                self,
                "Success",
                "Hash list '{name}' deleted".format(name=name)
            )
            self._refresh_reference_lists()
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                "Failed to delete hash list: {error}".format(error=str(e))
            )

    def _rebuild_hash_db(self) -> None:
        """Rebuild the SQLite hash database from all .txt hash lists (Phase 4)."""
        from core.matching import rebuild_hash_db

        hashlists_dir = self.ref_manager.hashlists_dir
        if not hashlists_dir.exists():
            QMessageBox.warning(
                self,
                "No Hash Lists",
                "Hash lists directory does not exist: {path}".format(path=str(hashlists_dir))
            )
            return

        # Check if there are any hash list files
        txt_files = list(hashlists_dir.glob("*.txt"))
        if not txt_files:
            QMessageBox.information(
                self,
                "No Hash Lists",
                f"No .txt hash list files found in:\n{hashlists_dir}\n\n"
                "Add hash lists first, then rebuild the database."
            )
            return

        # Confirm rebuild
        reply = QMessageBox.question(
            self,
            "Rebuild Hash Database",
            f"This will rebuild the hash database from {len(txt_files)} .txt file(s).\n\n"
            f"Location: {self._get_hash_db_path()}\n\n"
            "Continue?",
            QMessageBox.Yes | QMessageBox.No
        )

        if reply != QMessageBox.Yes:
            return

        # Rebuild
        try:
            db_path = self._get_hash_db_path()
            total = rebuild_hash_db(hashlists_dir, db_path)

            QMessageBox.information(
                self,
                "Success",
                f"Hash database rebuilt successfully!\n\n"
                f"Total entries: {total}\n"
                f"Location: {db_path}"
            )
            self._update_hash_db_status()

        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                "Failed to rebuild hash database:\n{error}".format(error=str(e))
            )

    def _get_hash_db_path(self) -> Path:
        """Get the path to the hash database file."""
        # Use ~/.config/surfsifter/hash_lists.db
        import os
        config_dir = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
        app_config_dir = config_dir / "surfsifter"
        app_config_dir.mkdir(parents=True, exist_ok=True)
        return app_config_dir / "hash_lists.db"

    def _update_hash_db_status(self) -> None:
        """Update the hash database status label."""
        from core.matching import list_hash_lists

        db_path = self._get_hash_db_path()

        if not db_path.exists():
            self.hash_db_status_label.setText(
                "No hash database. Click 'Rebuild Hash DB' to create one."
            )
            return

        try:
            lists = list_hash_lists(db_path)
            total_entries = sum(lst.get("entry_count", 0) for lst in lists)

            # Get last modified time
            import datetime
            mtime = datetime.datetime.fromtimestamp(db_path.stat().st_mtime)
            mtime_str = mtime.strftime("%Y-%m-%d %H:%M")

            self.hash_db_status_label.setText(
                "DB: {lists} list(s), {entries} entries (updated: {time})".format(
                    lists=len(lists),
                    entries=total_entries,
                    time=mtime_str
                )
            )
        except Exception as e:
            self.hash_db_status_label.setText(
                "Error reading hash database: {error}".format(error=str(e))
            )

    def _delete_filelist(self) -> None:
        """Delete the selected file list."""
        current_item = self.filelist_widget.currentItem()
        if not current_item:
            return

        name = current_item.text()
        reply = QMessageBox.question(
            self,
            "Confirm Delete",
            "Are you sure you want to delete file list '{name}'?".format(name=name),
            QMessageBox.Yes | QMessageBox.No
        )

        if reply != QMessageBox.Yes:
            return

        try:
            list_path = self.ref_manager.filelists_dir / f"{name}.txt"
            list_path.unlink()
            QMessageBox.information(
                self,
                "Success",
                "File list '{name}' deleted".format(name=name)
            )
            self._refresh_reference_lists()
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                "Failed to delete file list: {error}".format(error=str(e))
            )

    def _refresh_url_lists(self) -> None:
        """Refresh the URL lists display."""
        self.urllist_widget.clear()

        available = self.ref_manager.list_available()

        # Populate URL lists
        for urllist in available.get("urllists", []):
            item = QListWidgetItem(urllist)
            self.urllist_widget.addItem(item)

    def _on_urllist_selection_changed(self, current, previous) -> None:
        """Handle URL list selection change."""
        has_selection = current is not None
        self.view_urllist_btn.setEnabled(has_selection)
        self.delete_urllist_btn.setEnabled(has_selection)

    def _add_urllist(self) -> None:
        """Add a new URL list from file."""
        from datetime import datetime
        from app.common.dialogs.add_urllist import AddUrlListDialog

        # Step 1: Select file
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select URL List File",
            "",
            "Text Files (*.txt);;All Files (*)"
        )

        if not file_path:
            return

        # Step 2: Read and process patterns
        try:
            patterns = []
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    # Skip empty lines and comments
                    if line and not line.startswith("#"):
                        patterns.append(line)
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                "Failed to read file: {error}".format(error=str(e))
            )
            return

        # Step 3: Validate not empty
        if not patterns:
            QMessageBox.critical(
                self,
                "Invalid File",
                "No valid URL patterns found in file. "
                "File must contain at least one non-empty, non-comment line."
            )
            return

        # Step 4: Show metadata configuration dialog
        dialog = AddUrlListDialog(
            patterns=patterns,
            suggested_name=Path(file_path).stem,
            parent=self
        )

        if dialog.exec() != QDialog.Accepted:
            return

        # Step 5: Check if URL list already exists
        dest_path = self.ref_manager.urllists_dir / f"{dialog.name}.txt"
        if dest_path.exists():
            reply = QMessageBox.question(
                self,
                "Overwrite?",
                "URL list '{name}' already exists. Overwrite?".format(name=dialog.name),
                QMessageBox.Yes | QMessageBox.No
            )
            if reply != QMessageBox.Yes:
                return

        # Step 6: Generate metadata and write file
        try:
            with open(dest_path, "w", encoding="utf-8") as f:
                # Write metadata header
                f.write(f"# NAME: {dialog.name}\n")
                f.write(f"# CATEGORY: {dialog.category}\n")
                f.write(f"# DESCRIPTION: {dialog.description}\n")
                f.write(f"# UPDATED: {datetime.now().strftime('%Y-%m-%d')}\n")
                f.write(f"# TYPE: urllist\n")
                f.write(f"# REGEX: {'true' if dialog.is_regex else 'false'}\n")
                f.write("\n")

                # Write patterns
                for pattern in patterns:
                    f.write(f"{pattern}\n")

            QMessageBox.information(
                self,
                "Success",
                "URL list '{name}' added successfully with {count} patterns.".format(
                    name=dialog.name,
                    count=len(patterns)
                )
            )
            self._refresh_url_lists()

        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                "Failed to create URL list: {error}".format(error=str(e))
            )

    def _view_urllist(self) -> None:
        """View the selected URL list."""
        current_item = self.urllist_widget.currentItem()
        if not current_item:
            return

        name = current_item.text()
        try:
            list_path = self.ref_manager.urllists_dir / f"{name}.txt"
            content = list_path.read_text(encoding="utf-8")

            # Show in dialog
            dialog = QDialog(self)
            dialog.setWindowTitle("URL List: {name}".format(name=name))
            layout = QVBoxLayout()

            text_edit = QTextEdit()
            text_edit.setPlainText(content)
            text_edit.setReadOnly(True)
            layout.addWidget(text_edit)

            close_btn = QPushButton("Close")
            close_btn.clicked.connect(dialog.accept)
            layout.addWidget(close_btn)

            dialog.setLayout(layout)
            dialog.resize(600, 400)
            dialog.exec()
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                "Failed to load URL list: {error}".format(error=str(e))
            )

    def _delete_urllist(self) -> None:
        """Delete the selected URL list."""
        current_item = self.urllist_widget.currentItem()
        if not current_item:
            return

        name = current_item.text()
        reply = QMessageBox.question(
            self,
            "Confirm Delete",
            "Are you sure you want to delete URL list '{name}'?".format(name=name),
            QMessageBox.Yes | QMessageBox.No
        )

        if reply != QMessageBox.Yes:
            return

        try:
            list_path = self.ref_manager.urllists_dir / f"{name}.txt"
            list_path.unlink()
            QMessageBox.information(
                self,
                "Success",
                "URL list '{name}' deleted".format(name=name)
            )
            self._refresh_url_lists()
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                "Failed to delete URL list: {error}".format(error=str(e))
            )

    def _install_predefined_lists(self) -> None:
        """Install predefined reference lists from the application."""
        try:
            from core.matching import install_predefined_lists
            installed = install_predefined_lists(self.ref_manager.base_path)

            if installed:
                msg = "Installed {count} predefined reference list(s):\n\n{lists}".format(
                    count=len(installed),
                    lists="\n".join(f"• {name}" for name in installed)
                )
                QMessageBox.information(self, "Success", msg)
                self._refresh_reference_lists()
            else:
                QMessageBox.information(
                    self,
                    "Already Installed",
                    "All predefined reference lists are already installed."
                )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                "Failed to install predefined lists: {error}".format(error=str(e))
            )

    # Helpers -------------------------------------------------------------

    def _pick_file(self, line_edit: QLineEdit) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Select file",
            line_edit.text() or "",
        )
        if path:
            line_edit.setText(path)

    def _open_config_dir(self) -> None:
        if not self._config_dir:
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(self._config_dir)))

    def _test_tool_path(self, tool_name: str, line_edit: QLineEdit) -> None:
        path_text = line_edit.text().strip()
        if not path_text:
            QMessageBox.information(
                self,
                "Test Tool",
                "Enter a tool path before testing.",
            )
            return
        candidate = Path(path_text)
        if not candidate.exists():
            QMessageBox.warning(
                self,
                "Test Tool",
                "Path not found: {path}".format(path=candidate),
            )
            return
        version = get_tool_version([str(candidate)])
        if version:
            QMessageBox.information(
                self,
                "Test Tool",
                "{tool} responded with:\n{version}".format(tool=tool_name, version=version),
            )
        else:
            QMessageBox.warning(
                self,
                "Test Tool",
                "Unable to determine version for {tool}.".format(tool=tool_name),
            )

    def _parse_content_types(self) -> List[str]:
        return [value.strip() for value in self.allowed_types_edit.text().split(",") if value.strip()]

    def _restore_defaults(self) -> None:
        defaults = AppSettings()
        self.thumbnail_spin.setValue(defaults.general.thumbnail_size)
        self.concurrency_spin.setValue(defaults.network.concurrency)
        self.timeout_spin.setValue(defaults.network.timeout_s)
        self.retries_spin.setValue(defaults.network.retries)
        self.max_bytes_spin.setValue(max(1, defaults.network.max_bytes // (1024 * 1024)))
        self.allowed_types_edit.setText(",".join(defaults.network.allowed_content_types))
        for name, editor in self._tool_line_edits.items():
            editor.setText(getattr(defaults.tools, name, ""))
        self.hash_path_edit.setText(defaults.hash.db_path)
        # Report defaults
        self.report_author_function.setText(defaults.reports.default_author_function)
        self.report_author_name.setText(defaults.reports.default_author_name)
        self.report_org_name.setText(defaults.reports.default_org_name)
        self.report_department.setText(defaults.reports.default_department)
        self.report_footer_text.setText(defaults.reports.default_footer_text)
        self.report_logo_path.clear()
        self.report_show_case_number.setChecked(defaults.reports.default_show_title_case_number)
        self.report_show_evidence.setChecked(defaults.reports.default_show_title_evidence)
        self.report_show_investigator.setChecked(defaults.reports.default_show_title_investigator)
        self.report_show_date.setChecked(defaults.reports.default_show_title_date)
        self.report_show_footer_date.setChecked(defaults.reports.default_show_footer_date)
        self.report_hide_appendix_pg.setChecked(defaults.reports.default_hide_appendix_page_numbers)
        self._validate()

    def _validate(self) -> bool:
        errors: List[str] = []

        if not self._parse_content_types():
            errors.append("Provide at least one allowed content type.")

        # Only validate legacy tool paths if not using enhanced tools tab
        if not self._tool_registry:
            for name, widget in [
                ("bulk_extractor", self.bulk_edit),
                ("foremost / scalpel", self.foremost_edit),
                ("exiftool", self.exiftool_edit),
                ("ewfmount", self.ewfmount_edit),
            ]:
                path = widget.text().strip()
                if path and not Path(path).exists():
                    errors.append("{tool} path not found.".format(tool=name.title()))
                    break

        hash_path = self.hash_path_edit.text().strip()
        if hash_path and not Path(hash_path).exists():
            errors.append("Hash database path not found.")

        if errors:
            self.error_label.setText(errors[0])
            self.save_button.setEnabled(False)
            return False

        self.error_label.clear()
        self.save_button.setEnabled(True)
        return True

    def _on_save(self) -> None:
        if not self._validate():
            return

        general = GeneralSettings(
            thumbnail_size=self.thumbnail_spin.value(),
        )

        # Tool paths are managed by ToolRegistry if available
        # For now, keep empty strings in settings (ToolRegistry is the source of truth)
        if self._tool_registry:
            tools = ToolPaths(
                bulk_extractor="",
                foremost="",
                exiftool="",
                ewfmount="",
            )
        else:
            tools = ToolPaths(
                bulk_extractor=self.bulk_edit.text().strip(),
                foremost=self.foremost_edit.text().strip(),
                exiftool=self.exiftool_edit.text().strip(),
                ewfmount=self.ewfmount_edit.text().strip(),
            )

        network = NetworkSettings(
            concurrency=self.concurrency_spin.value(),
            timeout_s=self.timeout_spin.value(),
            retries=self.retries_spin.value(),
            max_bytes=self.max_bytes_spin.value() * 1024 * 1024,
            allowed_content_types=self._parse_content_types(),
        )
        hash_cfg = HashSettings(db_path=self.hash_path_edit.text().strip())

        # Report branding defaults
        reports = ReportSettings(
            default_author_function=self.report_author_function.text().strip(),
            default_author_name=self.report_author_name.text().strip(),
            default_org_name=self.report_org_name.text().strip(),
            default_department=self.report_department.text().strip(),
            default_footer_text=self.report_footer_text.text().strip(),
            default_logo_path=self._get_report_logo_relative_path(),
            default_locale=self.report_locale.currentData() or "en",
            default_date_format=self.report_date_format.currentData() or "eu",
            default_show_title_case_number=self.report_show_case_number.isChecked(),
            default_show_title_evidence=self.report_show_evidence.isChecked(),
            default_show_title_investigator=self.report_show_investigator.isChecked(),
            default_show_title_date=self.report_show_date.isChecked(),
            default_show_footer_date=self.report_show_footer_date.isChecked(),
            default_hide_appendix_page_numbers=self.report_hide_appendix_pg.isChecked(),
        )

        self.result_settings = AppSettings(
            general=general, tools=tools, network=network, hash=hash_cfg, reports=reports
        )
        self.accept()
