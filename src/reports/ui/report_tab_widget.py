"""Main Reports tab widget - self-contained UI for report generation."""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import shutil
import sqlite3

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QGroupBox,
    QPushButton,
    QScrollArea,
    QMessageBox,
    QFrame,
    QDialog,
    QFileDialog,
    QDateEdit,
    QListWidget,
    QListWidgetItem,
    QComboBox,
    QCheckBox,
    QSizePolicy,
)
from PySide6.QtCore import Qt, QDate, QUrl, Signal
from PySide6.QtGui import QDesktopServices

from .collapsible_group import CollapsibleGroupBox
from .section_editor import SectionEditorDialog
from .module_picker import ModulePickerDialog
from .section_card import SectionCard
from ..locales import SUPPORTED_LOCALES, LOCALE_NAMES, DEFAULT_LOCALE
from ..database import (
    insert_custom_section,
    update_custom_section,
    delete_custom_section,
    get_custom_sections,
    get_custom_section_by_id,
    reorder_custom_section,
    # Module helpers
    insert_section_module,
    update_section_module,
    delete_section_module,
    get_section_modules,
    delete_modules_by_section,
    # Appendix helpers
    insert_appendix_module,
    update_appendix_module,
    delete_appendix_module,
    get_appendix_modules,
    reorder_appendix_module,
    # Settings helpers
    get_report_settings,
    save_report_settings,
)
from ..generator import ReportBuilder, ReportGenerator, ReportMode
from ..appendix import AppendixRegistry


class ReportTabWidget(QWidget):
    """Self-contained report tab widget.

    This widget contains all the UI logic for report generation.
    The tab.py shim in app/features/reports just wraps this.
    """

    manage_text_blocks_requested = Signal()

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)

        # State
        self._case_number: Optional[str] = None
        self._evidence_label: Optional[str] = None
        self._evidence_id: Optional[int] = None
        self._db_conn: Optional[sqlite3.Connection] = None
        self._workspace_path: Optional[Path] = None  # For default save location
        self._investigator: Optional[str] = None
        self._config_dir: Optional[Path] = None  # For resolving global logo path

        # Global default settings (from Preferences)
        self._default_settings: Optional[Dict[str, Any]] = None

        # Flag to prevent auto-save during settings load
        self._loading_settings = False

        # Section cards cache
        self._section_cards: List[SectionCard] = []
        self._appendix_items: List[Dict[str, Any]] = []
        self._appendix_registry = AppendixRegistry()

        # Report generator
        self._generator = ReportGenerator()

        self._setup_ui()
        self._connect_auto_save_signals()

    def _setup_ui(self) -> None:
        """Setup the report tab UI.

        Three sections:
        1. Report Settings (collapsible) — title, language, branding, author
        2. Custom Sections (not collapsible) — main work area
        3. Appendix (collapsible) — appendix modules
        """
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(8)

        # Report Settings section (collapsible, expanded by default)
        self._settings_group = CollapsibleGroupBox("Report Settings", collapsed=False)
        self._build_settings_section(self._settings_group.content_layout())
        self._settings_group.collapsed_changed.connect(self._on_collapse_changed)
        layout.addWidget(self._settings_group)

        # Custom sections area (NOT collapsible - main work area)
        sections_group = self._build_sections_area()
        sections_group.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        layout.addWidget(sections_group, 1)  # Give it stretch

        # Appendix section (collapsible, collapsed by default)
        self._appendix_group = CollapsibleGroupBox("Appendix", collapsed=True)
        self._build_appendix_section(self._appendix_group.content_layout())
        self._appendix_group.collapsed_changed.connect(self._on_collapse_changed)
        layout.addWidget(self._appendix_group)

        # Report generation buttons at the bottom
        buttons_layout = self._build_generation_buttons()
        layout.addLayout(buttons_layout)

    def _make_sub_heading(self, text: str) -> QLabel:
        """Create a styled sub-heading label for section groups."""
        label = QLabel(text)
        label.setStyleSheet("font-weight: 600; font-size: 10pt; margin-top: 4px;")
        return label

    def _make_separator(self) -> QFrame:
        """Create a horizontal separator line."""
        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setFrameShadow(QFrame.Sunken)
        return sep

    def _build_settings_section(self, group_layout: QVBoxLayout) -> None:
        """Build the unified report settings section.

        Contains sub-groups separated by horizontal lines:
        - Title & Language
        - Branding (org, department, logo, footer)
        - Report Created By (author info)
        - Options (title page field visibility, footer/appendix toggles)

        Args:
            group_layout: Layout to add widgets to
        """
        group_layout.setSpacing(8)

        # ── Title & Language ──
        group_layout.addWidget(self._make_sub_heading("Title & Language"))

        # Title row
        title_row = QHBoxLayout()
        label = QLabel("Title:")
        label.setFixedWidth(90)
        title_row.addWidget(label)

        self._title_input = QLineEdit()
        self._title_input.setPlaceholderText("Enter report title...")
        title_row.addWidget(self._title_input)
        group_layout.addLayout(title_row)

        # Language row
        lang_row = QHBoxLayout()
        lang_label = QLabel("Language:")
        lang_label.setFixedWidth(90)
        lang_row.addWidget(lang_label)

        self._locale_combo = QComboBox()
        for locale_code in SUPPORTED_LOCALES:
            display_name = LOCALE_NAMES.get(locale_code, locale_code)
            self._locale_combo.addItem(display_name, locale_code)
        default_index = self._locale_combo.findData(DEFAULT_LOCALE)
        if default_index >= 0:
            self._locale_combo.setCurrentIndex(default_index)
        self._locale_combo.setToolTip("Select report output language")
        self._locale_combo.setFixedWidth(150)
        lang_row.addWidget(self._locale_combo)
        lang_row.addStretch()
        group_layout.addLayout(lang_row)

        # Date format row
        date_fmt_row = QHBoxLayout()
        date_fmt_label = QLabel("Date Format:")
        date_fmt_label.setFixedWidth(90)
        date_fmt_row.addWidget(date_fmt_label)

        self._date_format_combo = QComboBox()
        self._date_format_combo.addItem("European (dd.mm.yyyy)", "eu")
        self._date_format_combo.addItem("US (mm/dd/yyyy)", "us")
        self._date_format_combo.setCurrentIndex(0)
        self._date_format_combo.setToolTip("Select date format for report")
        self._date_format_combo.setFixedWidth(150)
        date_fmt_row.addWidget(self._date_format_combo)
        date_fmt_row.addStretch()
        group_layout.addLayout(date_fmt_row)

        # ── Branding ──
        group_layout.addWidget(self._make_separator())
        group_layout.addWidget(self._make_sub_heading("Branding"))

        # Org name row
        org_layout = QHBoxLayout()
        org_label = QLabel("Organization:")
        org_label.setFixedWidth(90)
        org_layout.addWidget(org_label)

        self._branding_org_input = QLineEdit()
        self._branding_org_input.setPlaceholderText("Organization name (appears on title page, bold)...")
        org_layout.addWidget(self._branding_org_input)
        group_layout.addLayout(org_layout)

        # Department row
        dept_layout = QHBoxLayout()
        dept_label = QLabel("Department:")
        dept_label.setFixedWidth(90)
        dept_layout.addWidget(dept_label)

        self._branding_dept_input = QLineEdit()
        self._branding_dept_input.setPlaceholderText("Department name (appears below org, not bold)...")
        dept_layout.addWidget(self._branding_dept_input)
        group_layout.addLayout(dept_layout)

        # Logo path row
        logo_layout = QHBoxLayout()
        logo_label = QLabel("Logo:")
        logo_label.setFixedWidth(90)
        logo_layout.addWidget(logo_label)

        self._branding_logo_input = QLineEdit()
        self._branding_logo_input.setPlaceholderText("Path to logo image...")
        self._branding_logo_input.setReadOnly(True)
        logo_layout.addWidget(self._branding_logo_input)

        self._branding_logo_btn = QPushButton("Browse...")
        self._branding_logo_btn.setFixedWidth(80)
        self._branding_logo_btn.clicked.connect(self._on_browse_logo)
        logo_layout.addWidget(self._branding_logo_btn)

        self._branding_logo_clear_btn = QPushButton("Clear")
        self._branding_logo_clear_btn.setFixedWidth(60)
        self._branding_logo_clear_btn.clicked.connect(self._on_clear_logo)
        logo_layout.addWidget(self._branding_logo_clear_btn)

        group_layout.addLayout(logo_layout)

        # Footer text row
        footer_layout = QHBoxLayout()
        footer_label = QLabel("Footer Text:")
        footer_label.setFixedWidth(90)
        footer_layout.addWidget(footer_label)

        self._branding_footer_input = QLineEdit()
        self._branding_footer_input.setPlaceholderText("Custom footer text (appears on all pages)...")
        footer_layout.addWidget(self._branding_footer_input)
        group_layout.addLayout(footer_layout)

        # ── Report Created By ──
        group_layout.addWidget(self._make_separator())
        group_layout.addWidget(self._make_sub_heading("Report Created By"))

        # Function row
        func_layout = QHBoxLayout()
        func_label = QLabel("Function:")
        func_label.setFixedWidth(90)
        func_layout.addWidget(func_label)

        self._author_function_input = QLineEdit()
        self._author_function_input.setText("Forensic Analyst")
        self._author_function_input.setPlaceholderText("e.g., Forensic Analyst")
        func_layout.addWidget(self._author_function_input)
        group_layout.addLayout(func_layout)

        # Name row
        name_layout = QHBoxLayout()
        name_label = QLabel("Name:")
        name_label.setFixedWidth(90)
        name_layout.addWidget(name_label)

        self._author_name_input = QLineEdit()
        self._author_name_input.setPlaceholderText("Enter name...")
        name_layout.addWidget(self._author_name_input)
        group_layout.addLayout(name_layout)

        # Date row
        date_layout = QHBoxLayout()
        date_label = QLabel("Date:")
        date_label.setFixedWidth(90)
        date_layout.addWidget(date_label)

        self._author_date_input = QDateEdit()
        self._author_date_input.setDate(QDate.currentDate())
        self._author_date_input.setCalendarPopup(True)
        self._author_date_input.setDisplayFormat("dd.MM.yyyy")
        date_layout.addWidget(self._author_date_input)
        date_layout.addStretch()
        group_layout.addLayout(date_layout)

        # ── Options ──
        group_layout.addWidget(self._make_separator())
        group_layout.addWidget(self._make_sub_heading("Options"))

        # Title page field visibility
        tp_label = QLabel("Title Page Fields:")
        tp_label.setStyleSheet("color: palette(mid); margin-left: 2px;")
        group_layout.addWidget(tp_label)

        tp_row = QHBoxLayout()
        self._show_case_number_cb = QCheckBox("Case Number")
        self._show_case_number_cb.setChecked(True)
        tp_row.addWidget(self._show_case_number_cb)

        self._show_evidence_cb = QCheckBox("Evidence")
        self._show_evidence_cb.setChecked(True)
        tp_row.addWidget(self._show_evidence_cb)

        self._show_investigator_cb = QCheckBox("Investigator")
        self._show_investigator_cb.setChecked(True)
        tp_row.addWidget(self._show_investigator_cb)

        self._show_date_cb = QCheckBox("Date")
        self._show_date_cb.setChecked(True)
        tp_row.addWidget(self._show_date_cb)
        tp_row.addStretch()
        group_layout.addLayout(tp_row)

        # Footer options
        opts_row = QHBoxLayout()
        self._show_footer_date_cb = QCheckBox("Show creation date in footer")
        self._show_footer_date_cb.setChecked(True)
        opts_row.addWidget(self._show_footer_date_cb)
        opts_row.addStretch()
        group_layout.addLayout(opts_row)

        # Footer evidence label override
        ev_layout = QHBoxLayout()
        ev_label = QLabel("Evidence label\n(header):")
        ev_label.setFixedWidth(90)
        ev_layout.addWidget(ev_label)

        self._footer_evidence_input = QLineEdit()
        self._footer_evidence_input.setPlaceholderText("Override evidence label in page header (optional)...")
        ev_layout.addWidget(self._footer_evidence_input)
        group_layout.addLayout(ev_layout)

    def _on_browse_logo(self) -> None:
        """Handle logo browse button click - copy logo to workspace."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Logo Image",
            "",
            "Images (*.png *.jpg *.jpeg *.gif *.svg);;All Files (*)",
        )
        if file_path:
            # Copy logo to workspace if possible
            copied_path = self._copy_logo_to_workspace(file_path)
            if copied_path and self._workspace_path:
                # Store relative path but display absolute for user clarity
                abs_path = self._workspace_path / copied_path
                self._branding_logo_input.setText(str(abs_path))
            else:
                self._branding_logo_input.setText(file_path)
            # Trigger save
            self._on_settings_changed()

    def _on_clear_logo(self) -> None:
        """Clear the logo path."""
        self._branding_logo_input.clear()
        self._on_settings_changed()

    def _build_appendix_section(self, group_layout: QVBoxLayout) -> None:
        """Build the appendix management area.

        Args:
            group_layout: Layout to add widgets to
        """
        group_layout.setSpacing(8)

        header_layout = QHBoxLayout()
        info_label = QLabel("Add appendix modules (tables and lists) to the report.")
        info_label.setStyleSheet("color: palette(mid);")
        header_layout.addWidget(info_label)
        header_layout.addStretch()

        self._add_appendix_btn = QPushButton("➕ Add Appendix")
        self._add_appendix_btn.setToolTip("Add a new appendix module")
        self._add_appendix_btn.clicked.connect(self._on_add_appendix)
        header_layout.addWidget(self._add_appendix_btn)
        group_layout.addLayout(header_layout)

        self._appendix_list = QListWidget()
        self._appendix_list.setMinimumHeight(60)
        self._appendix_list.setMaximumHeight(150)
        self._appendix_list.itemDoubleClicked.connect(self._on_edit_appendix)
        self._appendix_list.currentRowChanged.connect(self._update_appendix_buttons)
        group_layout.addWidget(self._appendix_list)

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        self._appendix_move_up_btn = QPushButton("↑")
        self._appendix_move_up_btn.setFixedWidth(30)
        self._appendix_move_up_btn.setToolTip("Move appendix up")
        self._appendix_move_up_btn.clicked.connect(self._on_move_appendix_up)
        btn_layout.addWidget(self._appendix_move_up_btn)

        self._appendix_move_down_btn = QPushButton("↓")
        self._appendix_move_down_btn.setFixedWidth(30)
        self._appendix_move_down_btn.setToolTip("Move appendix down")
        self._appendix_move_down_btn.clicked.connect(self._on_move_appendix_down)
        btn_layout.addWidget(self._appendix_move_down_btn)

        self._appendix_edit_btn = QPushButton("Edit")
        self._appendix_edit_btn.clicked.connect(self._on_edit_appendix)
        btn_layout.addWidget(self._appendix_edit_btn)

        self._appendix_remove_btn = QPushButton("Remove")
        self._appendix_remove_btn.clicked.connect(self._on_remove_appendix)
        btn_layout.addWidget(self._appendix_remove_btn)

        group_layout.addLayout(btn_layout)

        self._update_appendix_buttons()

    def _build_sections_area(self) -> QGroupBox:
        """Build the custom sections management area."""
        group = QGroupBox("Custom Sections")
        group_layout = QVBoxLayout(group)
        group_layout.setContentsMargins(12, 12, 12, 12)
        group_layout.setSpacing(12)

        # Header with Add button
        header_layout = QHBoxLayout()

        info_label = QLabel("Add custom sections to include in your report.")
        info_label.setStyleSheet("color: palette(mid);")
        header_layout.addWidget(info_label)

        header_layout.addStretch()

        self._manage_text_blocks_btn = QPushButton("📝 Manage Text Blocks")
        self._manage_text_blocks_btn.setToolTip("Open Preferences → Text Blocks")
        self._manage_text_blocks_btn.clicked.connect(self._on_manage_text_blocks)
        header_layout.addWidget(self._manage_text_blocks_btn)

        self._add_section_btn = QPushButton("➕ Add Section")
        self._add_section_btn.setToolTip("Add a new custom section")
        self._add_section_btn.clicked.connect(self._on_add_section)
        header_layout.addWidget(self._add_section_btn)

        group_layout.addLayout(header_layout)

        # Scroll area for section cards
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        # Container widget for cards
        self._sections_container = QWidget()
        self._sections_layout = QVBoxLayout(self._sections_container)
        self._sections_layout.setContentsMargins(0, 0, 0, 0)
        self._sections_layout.setSpacing(8)
        self._sections_layout.addStretch()  # Push cards to top

        scroll.setWidget(self._sections_container)
        group_layout.addWidget(scroll, 1)

        # Empty state label (shown when no sections)
        self._empty_label = QLabel("No custom sections yet. Click 'Add Section' to create one.")
        self._empty_label.setStyleSheet("color: palette(mid); font-style: italic;")
        self._empty_label.setAlignment(Qt.AlignCenter)
        self._sections_layout.insertWidget(0, self._empty_label)

        return group

    def _on_manage_text_blocks(self) -> None:
        """Request opening text blocks management in Preferences."""
        self.manage_text_blocks_requested.emit()

    def _build_generation_buttons(self) -> QHBoxLayout:
        """Build the report generation buttons (Preview and PDF variants)."""
        layout = QHBoxLayout()
        layout.setSpacing(12)

        # Add stretch to push buttons to the right
        layout.addStretch()

        # Preview button
        self._preview_btn = QPushButton("👁️ Preview")
        self._preview_btn.setToolTip("Preview report in web browser")
        self._preview_btn.setMinimumWidth(120)
        self._preview_btn.clicked.connect(self._on_preview)
        layout.addWidget(self._preview_btn)

        # Report-only PDF button
        self._create_report_pdf_btn = QPushButton("📄 Report PDF")
        self._create_report_pdf_btn.setToolTip("Generate report PDF (without appendix)")
        self._create_report_pdf_btn.setMinimumWidth(120)
        self._create_report_pdf_btn.clicked.connect(self._on_create_report_pdf)
        layout.addWidget(self._create_report_pdf_btn)

        # Appendix-only PDF button
        self._create_appendix_pdf_btn = QPushButton("📎 Appendix PDF")
        self._create_appendix_pdf_btn.setToolTip("Generate appendix PDF only")
        self._create_appendix_pdf_btn.setMinimumWidth(120)
        self._create_appendix_pdf_btn.clicked.connect(self._on_create_appendix_pdf)
        layout.addWidget(self._create_appendix_pdf_btn)

        # Complete PDF button (both files)
        self._create_complete_pdf_btn = QPushButton("📄📎 Complete PDF")
        self._create_complete_pdf_btn.setToolTip(
            "Generate both report and appendix PDFs"
        )
        self._create_complete_pdf_btn.setMinimumWidth(140)
        self._create_complete_pdf_btn.clicked.connect(self._on_create_complete_pdf)
        layout.addWidget(self._create_complete_pdf_btn)

        return layout

    def _build_report_html(self, mode: ReportMode = ReportMode.REPORT_ONLY):
        """Build the report HTML from current state.

        Args:
            mode: Which parts to render.

        Returns:
            For REPORT_ONLY / APPENDIX_ONLY: HTML string, or None on error.
            For COMPLETE: tuple of (report_html, appendix_html), or None on error.
        """
        if self._db_conn is None or self._evidence_id is None:
            QMessageBox.warning(
                self,
                "No Evidence Selected",
                "Please select an evidence before generating a report."
            )
            return None

        title = self.get_title()
        if not title:
            QMessageBox.warning(
                self,
                "Missing Title",
                "Please enter a report title."
            )
            return None

        # Get selected locale
        locale = self._locale_combo.currentData() or DEFAULT_LOCALE

        # Get selected date format
        date_format = self._date_format_combo.currentData() or "eu"

        # Build the report
        builder = ReportBuilder(
            self._db_conn,
            self._evidence_id,
            case_folder=self._workspace_path,
            locale=locale,
        )
        builder.set_title(title)
        builder.set_date_format(date_format)
        builder.set_case_info(
            case_number=self._case_number,
            evidence_label=self._evidence_label,
            investigator=self._investigator,
        )

        # Set author info from UI fields
        author_function = self._author_function_input.text().strip()
        author_name = self._author_name_input.text().strip()
        author_date = self._author_date_input.date().toString("dd.MM.yyyy")

        builder.set_author_info(
            function=author_function if author_function else None,
            name=author_name if author_name else None,
            date=author_date if author_date else None,
        )

        # Set branding info from UI fields
        branding_org = self._branding_org_input.text().strip()
        branding_dept = self._branding_dept_input.text().strip()
        branding_footer = self._branding_footer_input.text().strip()
        branding_logo = self._branding_logo_input.text().strip()

        builder.set_branding(
            org_name=branding_org if branding_org else None,
            department=branding_dept if branding_dept else None,
            footer_text=branding_footer if branding_footer else None,
            logo_path=branding_logo if branding_logo else None,
        )

        # Set title page field visibility
        builder.set_title_page_options(
            show_case_number=self._show_case_number_cb.isChecked(),
            show_evidence=self._show_evidence_cb.isChecked(),
            show_investigator=self._show_investigator_cb.isChecked(),
            show_date=self._show_date_cb.isChecked(),
        )

        # Set footer options
        footer_ev = self._footer_evidence_input.text().strip()
        builder.set_footer_options(
            show_footer_date=self._show_footer_date_cb.isChecked(),
            footer_evidence_label=footer_ev if footer_ev else None,
        )

        # Load data based on mode
        if mode != ReportMode.APPENDIX_ONLY:
            builder.load_sections_from_db()
        if mode != ReportMode.REPORT_ONLY:
            builder.load_appendix_from_db()

        return builder.render_html(mode)

    def _on_preview(self) -> None:
        """Handle Preview button click - open report in browser."""
        html = self._build_report_html(ReportMode.REPORT_ONLY)
        if html is None:
            return

        try:
            self._generator.preview_in_browser(html)
        except Exception as e:
            QMessageBox.critical(
                self,
                "Preview Error",
                f"Failed to open preview: {e}"
            )

    # ── Helpers for PDF generation ────────────────────────────────────

    def _check_weasyprint(self) -> bool:
        """Check WeasyPrint availability and show warning if missing.

        Returns:
            True if WeasyPrint is available.
        """
        if not self._generator.can_generate_pdf:
            QMessageBox.warning(
                self,
                "PDF Generation Unavailable",
                "WeasyPrint is not installed. Please install it with:\n\n"
                "pip install weasyprint\n\n"
                "Note: WeasyPrint requires additional system dependencies."
            )
            return False
        return True

    def _default_pdf_path(self, suffix: str = "") -> str:
        """Build a default save path for a PDF file.

        Args:
            suffix: Optional suffix appended before the timestamp
                    (e.g. ``"_Appendix"``).

        Returns:
            Absolute path string (or bare filename when no workspace).
        """
        title = self.get_title() or "Report"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_title = "".join(
            c if c.isalnum() or c in " -_" else "_" for c in title
        ).replace(" ", "_")
        filename = f"{safe_title}{suffix}_{timestamp}.pdf"

        if self._workspace_path:
            reports_dir = self._workspace_path / "reports"
            reports_dir.mkdir(exist_ok=True)
            return str(reports_dir / filename)
        return filename

    def _ask_save_path(self, dialog_title: str, default_path: str) -> Optional[str]:
        """Show a save-file dialog and return the chosen path.

        Returns:
            Chosen file path with ``.pdf`` extension, or ``None`` if cancelled.
        """
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            dialog_title,
            default_path,
            "PDF Files (*.pdf);;All Files (*)",
        )
        if not file_path:
            return None
        if not file_path.lower().endswith(".pdf"):
            file_path += ".pdf"
        return file_path

    # ── PDF creation slots ────────────────────────────────────────────

    def _on_create_report_pdf(self) -> None:
        """Generate report-only PDF (no appendix)."""
        html = self._build_report_html(ReportMode.REPORT_ONLY)
        if html is None or not self._check_weasyprint():
            return

        file_path = self._ask_save_path(
            "Save Report PDF", self._default_pdf_path()
        )
        if not file_path:
            return

        try:
            self._generator.generate_pdf(html, file_path)
            QMessageBox.information(
                self, "PDF Created", f"Report saved to:\n{file_path}"
            )
            QDesktopServices.openUrl(QUrl.fromLocalFile(file_path))
        except Exception as e:
            QMessageBox.critical(
                self, "PDF Error", f"Failed to create PDF: {e}"
            )

    def _on_create_appendix_pdf(self) -> None:
        """Generate appendix-only PDF."""
        html = self._build_report_html(ReportMode.APPENDIX_ONLY)
        if html is None or not self._check_weasyprint():
            return

        file_path = self._ask_save_path(
            "Save Appendix PDF", self._default_pdf_path("_Appendix")
        )
        if not file_path:
            return

        try:
            self._generator.generate_pdf(html, file_path)
            QMessageBox.information(
                self, "PDF Created", f"Appendix saved to:\n{file_path}"
            )
            QDesktopServices.openUrl(QUrl.fromLocalFile(file_path))
        except Exception as e:
            QMessageBox.critical(
                self, "PDF Error", f"Failed to create appendix PDF: {e}"
            )

    def _on_create_complete_pdf(self) -> None:
        """Generate both report and appendix PDFs at once."""
        result = self._build_report_html(ReportMode.COMPLETE)
        if result is None or not self._check_weasyprint():
            return

        report_html, appendix_html = result

        # Ask for report path — derive appendix path from it
        report_path = self._ask_save_path(
            "Save Report PDF (appendix will be saved alongside)",
            self._default_pdf_path(),
        )
        if not report_path:
            return

        # Derive appendix path: insert _Appendix before .pdf
        rp = Path(report_path)
        appendix_path = str(rp.with_name(f"{rp.stem}_Appendix{rp.suffix}"))

        try:
            self._generator.generate_pdf_pair(
                report_html, appendix_html, report_path, appendix_path
            )
            QMessageBox.information(
                self,
                "PDFs Created",
                f"Report saved to:\n{report_path}\n\n"
                f"Appendix saved to:\n{appendix_path}",
            )
            QDesktopServices.openUrl(QUrl.fromLocalFile(report_path))
            QDesktopServices.openUrl(QUrl.fromLocalFile(appendix_path))
        except Exception as e:
            QMessageBox.critical(
                self, "PDF Error", f"Failed to create PDFs: {e}"
            )

    def _update_default_title(self) -> None:
        """Update the title input with default value based on case/evidence."""
        parts = []

        if self._case_number:
            parts.append(f"Case: {self._case_number}")

        if self._evidence_label:
            parts.append(f"Evidence: {self._evidence_label}")

        if parts:
            default_title = ", ".join(parts)
            self._title_input.setText(default_title)

    def _load_sections(self) -> None:
        """Load sections from database and refresh the UI."""
        # Clear existing cards
        for card in self._section_cards:
            self._sections_layout.removeWidget(card)
            card.deleteLater()
        self._section_cards.clear()

        # Check if we can load
        if self._db_conn is None or self._evidence_id is None:
            self._empty_label.setVisible(True)
            return

        # Load from database
        sections = get_custom_sections(self._db_conn, self._evidence_id)

        # Show/hide empty state
        self._empty_label.setVisible(len(sections) == 0)

        # Create cards
        for i, section in enumerate(sections):
            is_first = (i == 0)
            is_last = (i == len(sections) - 1)

            # Load modules for this section
            section_modules = get_section_modules(self._db_conn, section["id"])

            card = SectionCard(
                section_id=section["id"],
                title=section["title"],
                content=section.get("content", ""),
                modules=section_modules,
                is_first=is_first,
                is_last=is_last,
                parent=self._sections_container,
            )

            # Connect signals
            card.edit_requested.connect(self._on_edit_section)
            card.delete_requested.connect(self._on_delete_section)
            card.move_up_requested.connect(self._on_move_up)
            card.move_down_requested.connect(self._on_move_down)

            # Insert before the stretch
            self._sections_layout.insertWidget(
                self._sections_layout.count() - 1,
                card
            )
            self._section_cards.append(card)

    def _load_appendix_modules(self) -> None:
        """Load appendix modules from database and refresh the UI."""
        self._appendix_list.clear()
        self._appendix_items.clear()

        if self._db_conn is None or self._evidence_id is None:
            self._update_appendix_buttons()
            return

        self._appendix_items = get_appendix_modules(self._db_conn, self._evidence_id)
        self._refresh_appendix_list()

    def _refresh_appendix_list(self) -> None:
        """Refresh the appendix list widget."""
        self._appendix_list.clear()

        for mod in self._appendix_items:
            module_id = mod.get("module_id", "")
            config = mod.get("config", {})
            title = (mod.get("title") or "").strip()

            module = self._appendix_registry.get_module(module_id)
            if module:
                display_title = title or module.metadata.name
                summary = module.format_config_summary(config)
                name = f"{module.metadata.icon} {module.metadata.name}"
            else:
                display_title = title or module_id
                summary = "(unknown module)"
                name = f"❓ {module_id}"

            item = QListWidgetItem(f"{display_title}\n    {name}: {summary}")
            item.setData(Qt.UserRole, mod.get("id"))
            self._appendix_list.addItem(item)

        self._update_appendix_buttons()

    def _update_appendix_buttons(self) -> None:
        row = self._appendix_list.currentRow()
        has_selection = row >= 0
        self._appendix_edit_btn.setEnabled(has_selection)
        self._appendix_remove_btn.setEnabled(has_selection)
        self._appendix_move_up_btn.setEnabled(has_selection and row > 0)
        self._appendix_move_down_btn.setEnabled(
            has_selection and row < len(self._appendix_items) - 1
        )
        self._add_appendix_btn.setEnabled(
            self._db_conn is not None and self._evidence_id is not None
        )

    def _on_add_appendix(self) -> None:
        if self._db_conn is None or self._evidence_id is None:
            QMessageBox.warning(
                self,
                "No Evidence Selected",
                "Please select an evidence before adding appendix items.",
            )
            return

        dialog = ModulePickerDialog(
            self,
            edit_mode=False,
            db_conn=self._db_conn,
            registry=self._appendix_registry,
            show_title_input=True,
        )
        if dialog.exec() == QDialog.Accepted:
            module_id = dialog.get_module_id()
            config = dialog.get_config()
            title = dialog.get_title()
            if module_id:
                insert_appendix_module(
                    self._db_conn,
                    self._evidence_id,
                    module_id,
                    title=title or None,
                    config=config,
                )
                self._load_appendix_modules()

    def _on_edit_appendix(self) -> None:
        row = self._appendix_list.currentRow()
        if row < 0 or row >= len(self._appendix_items):
            return

        mod = self._appendix_items[row]
        dialog = ModulePickerDialog(
            self,
            module_id=mod.get("module_id"),
            config=mod.get("config", {}),
            edit_mode=True,
            db_conn=self._db_conn,
            registry=self._appendix_registry,
            show_title_input=True,
            title=mod.get("title", ""),
        )

        if dialog.exec() == QDialog.Accepted:
            update_appendix_module(
                self._db_conn,
                mod["id"],
                title=dialog.get_title() or None,
                config=dialog.get_config(),
            )
            self._load_appendix_modules()

    def _on_remove_appendix(self) -> None:
        row = self._appendix_list.currentRow()
        if row < 0 or row >= len(self._appendix_items):
            return

        reply = QMessageBox.question(
            self,
            "Remove Appendix",
            "Are you sure you want to remove this appendix item?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if reply == QMessageBox.Yes:
            delete_appendix_module(self._db_conn, self._appendix_items[row]["id"])
            self._load_appendix_modules()

    def _on_move_appendix_up(self) -> None:
        row = self._appendix_list.currentRow()
        if row <= 0:
            return
        mod = self._appendix_items[row]
        reorder_appendix_module(self._db_conn, mod["id"], mod["sort_order"] - 1)
        self._load_appendix_modules()
        self._appendix_list.setCurrentRow(row - 1)

    def _on_move_appendix_down(self) -> None:
        row = self._appendix_list.currentRow()
        if row < 0 or row >= len(self._appendix_items) - 1:
            return
        mod = self._appendix_items[row]
        reorder_appendix_module(self._db_conn, mod["id"], mod["sort_order"] + 1)
        self._load_appendix_modules()
        self._appendix_list.setCurrentRow(row + 1)

    def _get_available_text_blocks(self) -> List[Dict[str, Any]]:
        """Load global text blocks for section prefill."""
        try:
            from app.config.text_blocks import TextBlockStore
            store = TextBlockStore()
            blocks = store.load_blocks()
        except Exception:
            return []

        return [
            {
                "id": block.id,
                "title": block.title,
                "content": block.content,
                "tags": list(block.tags),
            }
            for block in blocks
        ]

    def _on_add_section(self) -> None:
        """Handle Add Section button click."""
        if self._db_conn is None or self._evidence_id is None:
            QMessageBox.warning(
                self,
                "No Evidence Selected",
                "Please select an evidence before adding sections."
            )
            return

        dialog = SectionEditorDialog(
            self,
            edit_mode=False,
            db_conn=self._db_conn,
            text_blocks=self._get_available_text_blocks(),
        )
        if dialog.exec() == QDialog.Accepted:
            title = dialog.get_title()
            content = dialog.get_content()
            modules = dialog.get_modules()

            # Insert section
            section_id = insert_custom_section(
                self._db_conn,
                self._evidence_id,
                title,
                content,
            )

            # Insert modules for this section
            for mod_data in modules:
                insert_section_module(
                    self._db_conn,
                    section_id,
                    mod_data.get("module_id", ""),
                    mod_data.get("config", {}),
                )

            self._load_sections()

    def _on_edit_section(self, section_id: int) -> None:
        """Handle edit request for a section."""
        if self._db_conn is None:
            return

        section = get_custom_section_by_id(self._db_conn, section_id)
        if section is None:
            return

        # Load existing modules
        existing_modules = get_section_modules(self._db_conn, section_id)

        dialog = SectionEditorDialog(
            self,
            title=section["title"],
            content=section.get("content", ""),
            modules=existing_modules,
            edit_mode=True,
            db_conn=self._db_conn,
            text_blocks=self._get_available_text_blocks(),
        )

        if dialog.exec() == QDialog.Accepted:
            # Update section
            update_custom_section(
                self._db_conn,
                section_id,
                title=dialog.get_title(),
                content=dialog.get_content(),
            )

            # Replace modules: delete old, insert new
            delete_modules_by_section(self._db_conn, section_id)

            for mod_data in dialog.get_modules():
                insert_section_module(
                    self._db_conn,
                    section_id,
                    mod_data.get("module_id", ""),
                    mod_data.get("config", {}),
                )

            self._load_sections()

    def _on_delete_section(self, section_id: int) -> None:
        """Handle delete request for a section."""
        if self._db_conn is None:
            return

        reply = QMessageBox.question(
            self,
            "Delete Section",
            "Are you sure you want to delete this section?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if reply == QMessageBox.Yes:
            delete_custom_section(self._db_conn, section_id)
            self._load_sections()

    def _on_move_up(self, section_id: int) -> None:
        """Handle move up request for a section."""
        if self._db_conn is None:
            return

        section = get_custom_section_by_id(self._db_conn, section_id)
        if section is None or section["sort_order"] == 0:
            return

        reorder_custom_section(
            self._db_conn,
            section_id,
            section["sort_order"] - 1,
        )
        self._load_sections()

    def _on_move_down(self, section_id: int) -> None:
        """Handle move down request for a section."""
        if self._db_conn is None or self._evidence_id is None:
            return

        section = get_custom_section_by_id(self._db_conn, section_id)
        if section is None:
            return

        # Get max order
        sections = get_custom_sections(self._db_conn, self._evidence_id)
        max_order = len(sections) - 1

        if section["sort_order"] >= max_order:
            return

        reorder_custom_section(
            self._db_conn,
            section_id,
            section["sort_order"] + 1,
        )
        self._load_sections()

    # --- Public API (called from shim) ---

    def set_db_connection(self, conn: Optional[sqlite3.Connection]) -> None:
        """Set the database connection for section persistence.

        Args:
            conn: SQLite connection to evidence database
        """
        self._db_conn = conn
        self._load_sections()
        self._load_appendix_modules()

    def set_case_data(self, case_data) -> None:
        """Set case metadata.

        Args:
            case_data: CaseDataAccess object or dict with case metadata
        """
        if case_data is None:
            self._case_number = None
        elif hasattr(case_data, 'get_case_metadata'):
            # CaseDataAccess object - call method to get dict
            metadata = case_data.get_case_metadata()
            self._case_number = metadata.get("case_number") or metadata.get("case_id")
        elif isinstance(case_data, dict):
            # Plain dict
            self._case_number = case_data.get("case_number") or case_data.get("case_id")
        else:
            self._case_number = None

        self._update_default_title()

    def set_evidence(self, evidence_id: Optional[int], evidence_label: Optional[str] = None) -> None:
        """Set the current evidence.

        Args:
            evidence_id: Evidence ID
            evidence_label: Human-readable evidence label
        """
        self._evidence_id = evidence_id
        self._evidence_label = evidence_label

        # Prevent auto-save during evidence switch
        self._loading_settings = True
        try:
            self._load_sections()
            self._load_appendix_modules()
            self._load_report_settings_or_defaults()
        finally:
            self._loading_settings = False

    def get_title(self) -> str:
        """Get the current report title."""
        return self._title_input.text().strip()

    def set_title(self, title: str) -> None:
        """Set the report title."""
        self._title_input.setText(title)

    def get_custom_sections(self) -> List[Dict[str, Any]]:
        """Get all custom sections for report generation.

        Returns:
            List of section dictionaries with title and content
        """
        if self._db_conn is None or self._evidence_id is None:
            return []
        return get_custom_sections(self._db_conn, self._evidence_id)

    def set_workspace_path(self, path: Optional[Path]) -> None:
        """Set the workspace path for default PDF save location.

        Args:
            path: Path to case workspace directory (will save to path/reports/)
        """
        self._workspace_path = path

    def set_default_settings(
        self,
        defaults: Dict[str, Any],
        config_dir: Optional[Path] = None,
    ) -> None:
        """Set global default settings from app preferences.

        These defaults are used when opening a new case that has no
        per-evidence settings saved yet.

        Args:
            defaults: Dictionary with keys matching ReportSettings dataclass:
                - default_author_function
                - default_author_name
                - default_org_name
                - default_footer_text
                - default_logo_path (relative to config_dir)
                - default_locale
                - default_date_format
            config_dir: Path to config directory for resolving logo path
        """
        self._default_settings = defaults
        self._config_dir = config_dir

    def set_investigator(self, investigator: Optional[str]) -> None:
        """Set the investigator name for the report.

        Also prefills the author name field if it's empty.

        Args:
            investigator: Investigator name
        """
        self._investigator = investigator

        # Prefill author name if field is empty
        if investigator and not self._author_name_input.text().strip():
            self._author_name_input.setText(investigator)

    def _connect_auto_save_signals(self) -> None:
        """Connect signals for auto-saving report settings."""
        # Title
        self._title_input.textChanged.connect(self._on_settings_changed)

        # Author
        self._author_function_input.textChanged.connect(self._on_settings_changed)
        self._author_name_input.textChanged.connect(self._on_settings_changed)
        self._author_date_input.dateChanged.connect(self._on_settings_changed)

        # Branding
        self._branding_org_input.textChanged.connect(self._on_settings_changed)
        self._branding_dept_input.textChanged.connect(self._on_settings_changed)
        self._branding_footer_input.textChanged.connect(self._on_settings_changed)
        # Logo path changes are handled in _on_browse_logo and _on_clear_logo

        # Title page field visibility checkboxes
        self._show_case_number_cb.stateChanged.connect(self._on_settings_changed)
        self._show_evidence_cb.stateChanged.connect(self._on_settings_changed)
        self._show_investigator_cb.stateChanged.connect(self._on_settings_changed)
        self._show_date_cb.stateChanged.connect(self._on_settings_changed)

        # Footer options
        self._show_footer_date_cb.stateChanged.connect(self._on_settings_changed)
        self._footer_evidence_input.textChanged.connect(self._on_settings_changed)

        # Preferences
        self._locale_combo.currentIndexChanged.connect(self._on_settings_changed)
        self._date_format_combo.currentIndexChanged.connect(self._on_settings_changed)

    def _on_settings_changed(self) -> None:
        """Handle settings change - auto-save to database."""
        if self._loading_settings:
            return
        self._save_report_settings()

    def _on_collapse_changed(self, is_collapsed: bool) -> None:
        """Handle collapse state change - auto-save to database."""
        if self._loading_settings:
            return
        self._save_report_settings()

    def _load_report_settings_or_defaults(self) -> None:
        """Load report settings from database, or set defaults if none exist."""
        if self._db_conn is None or self._evidence_id is None:
            # No database - just set the default title
            self._update_default_title()
            return

        settings = get_report_settings(self._db_conn, self._evidence_id)
        if settings is None:
            # No saved settings - set default title and apply global defaults
            self._update_default_title()
            self._apply_global_defaults()
            return

        # Title - use saved title, or fall back to default if empty
        if settings.get("title"):
            self._title_input.setText(settings["title"])
        else:
            self._update_default_title()

        # Author section - fall back to global defaults if empty
        defaults = self._default_settings or {}

        if settings.get("author_function"):
            self._author_function_input.setText(settings["author_function"])
        elif defaults.get("default_author_function"):
            self._author_function_input.setText(defaults["default_author_function"])

        if settings.get("author_name"):
            self._author_name_input.setText(settings["author_name"])
        elif defaults.get("default_author_name"):
            self._author_name_input.setText(defaults["default_author_name"])

        if settings.get("author_date"):
            # Parse date and set to QDateEdit
            date_str = settings["author_date"]
            # Try parsing dd.mm.yyyy format
            try:
                parts = date_str.split(".")
                if len(parts) == 3:
                    day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                    self._author_date_input.setDate(QDate(year, month, day))
            except (ValueError, IndexError):
                pass

        # Branding section - fall back to global defaults if empty
        if settings.get("branding_org_name"):
            self._branding_org_input.setText(settings["branding_org_name"])
        elif defaults.get("default_org_name"):
            self._branding_org_input.setText(defaults["default_org_name"])

        if settings.get("branding_department"):
            self._branding_dept_input.setText(settings["branding_department"])
        elif defaults.get("default_department"):
            self._branding_dept_input.setText(defaults["default_department"])

        if settings.get("branding_footer_text"):
            self._branding_footer_input.setText(settings["branding_footer_text"])
        elif defaults.get("default_footer_text"):
            self._branding_footer_input.setText(defaults["default_footer_text"])

        if settings.get("branding_logo_path"):
            # Convert relative path to absolute for display
            logo_path = settings["branding_logo_path"]
            if self._workspace_path and not Path(logo_path).is_absolute():
                logo_path = str(self._workspace_path / logo_path)
            self._branding_logo_input.setText(logo_path)
        elif defaults.get("default_logo_path"):
            # Resolve relative path from config dir
            logo_path = defaults["default_logo_path"]
            if self._config_dir and not Path(logo_path).is_absolute():
                full_path = self._config_dir / logo_path
                if full_path.exists():
                    self._branding_logo_input.setText(str(full_path))
            elif Path(logo_path).is_absolute() and Path(logo_path).exists():
                self._branding_logo_input.setText(logo_path)

        # Title page field visibility
        self._show_case_number_cb.setChecked(settings.get("show_title_case_number", True))
        self._show_evidence_cb.setChecked(settings.get("show_title_evidence", True))
        self._show_investigator_cb.setChecked(settings.get("show_title_investigator", True))
        self._show_date_cb.setChecked(settings.get("show_title_date", True))

        # Footer options
        self._show_footer_date_cb.setChecked(settings.get("show_footer_date", True))
        if settings.get("footer_evidence_label"):
            self._footer_evidence_input.setText(settings["footer_evidence_label"])

        # Preferences
        locale = settings.get("locale", DEFAULT_LOCALE)
        locale_index = self._locale_combo.findData(locale)
        if locale_index >= 0:
            self._locale_combo.setCurrentIndex(locale_index)

        date_format = settings.get("date_format", "eu")
        date_fmt_index = self._date_format_combo.findData(date_format)
        if date_fmt_index >= 0:
            self._date_format_combo.setCurrentIndex(date_fmt_index)

        # Collapsed states
        self._settings_group.set_collapsed(settings.get("collapsed_settings", False))
        self._appendix_group.set_collapsed(settings.get("collapsed_appendix", True))

    def _apply_global_defaults(self) -> None:
        """Apply global default settings from app preferences.

        Only called when no per-evidence settings exist.
        """
        if not self._default_settings:
            return

        defaults = self._default_settings

        # Author section
        if defaults.get("default_author_function"):
            self._author_function_input.setText(defaults["default_author_function"])
        if defaults.get("default_author_name"):
            self._author_name_input.setText(defaults["default_author_name"])

        # Branding section
        if defaults.get("default_org_name"):
            self._branding_org_input.setText(defaults["default_org_name"])
        if defaults.get("default_department"):
            self._branding_dept_input.setText(defaults["default_department"])
        if defaults.get("default_footer_text"):
            self._branding_footer_input.setText(defaults["default_footer_text"])
        if defaults.get("default_logo_path"):
            # Resolve relative path from config dir
            logo_path = defaults["default_logo_path"]
            if self._config_dir and not Path(logo_path).is_absolute():
                full_path = self._config_dir / logo_path
                if full_path.exists():
                    self._branding_logo_input.setText(str(full_path))

        # Preferences
        locale = defaults.get("default_locale", DEFAULT_LOCALE)
        locale_index = self._locale_combo.findData(locale)
        if locale_index >= 0:
            self._locale_combo.setCurrentIndex(locale_index)

        date_format = defaults.get("default_date_format", "eu")
        date_fmt_index = self._date_format_combo.findData(date_format)
        if date_fmt_index >= 0:
            self._date_format_combo.setCurrentIndex(date_fmt_index)

        # Title page visibility defaults
        if "default_show_title_case_number" in defaults:
            self._show_case_number_cb.setChecked(bool(defaults["default_show_title_case_number"]))
        if "default_show_title_evidence" in defaults:
            self._show_evidence_cb.setChecked(bool(defaults["default_show_title_evidence"]))
        if "default_show_title_investigator" in defaults:
            self._show_investigator_cb.setChecked(bool(defaults["default_show_title_investigator"]))
        if "default_show_title_date" in defaults:
            self._show_date_cb.setChecked(bool(defaults["default_show_title_date"]))

        # Footer defaults
        if "default_show_footer_date" in defaults:
            self._show_footer_date_cb.setChecked(bool(defaults["default_show_footer_date"]))

    def _save_report_settings(self) -> None:
        """Save report settings to database."""
        if self._db_conn is None or self._evidence_id is None:
            return

        # Get author date as string
        author_date = self._author_date_input.date().toString("dd.MM.yyyy")

        # Get logo path - convert to relative if in workspace
        logo_path = self._branding_logo_input.text().strip()
        if logo_path and self._workspace_path:
            logo_path_obj = Path(logo_path)
            try:
                rel_path = logo_path_obj.relative_to(self._workspace_path)
                logo_path = str(rel_path)
            except ValueError:
                # Not relative to workspace, keep absolute
                pass

        save_report_settings(
            self._db_conn,
            self._evidence_id,
            title=self._title_input.text().strip() or None,
            author_function=self._author_function_input.text().strip() or None,
            author_name=self._author_name_input.text().strip() or None,
            author_date=author_date if author_date else None,
            branding_org_name=self._branding_org_input.text().strip() or None,
            branding_department=self._branding_dept_input.text().strip() or None,
            branding_footer_text=self._branding_footer_input.text().strip() or None,
            branding_logo_path=logo_path or None,
            locale=self._locale_combo.currentData() or DEFAULT_LOCALE,
            date_format=self._date_format_combo.currentData() or "eu",
            collapsed_settings=self._settings_group.is_collapsed(),
            collapsed_appendix=self._appendix_group.is_collapsed(),
            show_title_case_number=self._show_case_number_cb.isChecked(),
            show_title_evidence=self._show_evidence_cb.isChecked(),
            show_title_investigator=self._show_investigator_cb.isChecked(),
            show_title_date=self._show_date_cb.isChecked(),
            show_footer_date=self._show_footer_date_cb.isChecked(),
            footer_evidence_label=self._footer_evidence_input.text().strip() or None,
        )

    def _copy_logo_to_workspace(self, source_path: str) -> Optional[str]:
        """Copy logo file to workspace/reports/assets/ folder.

        Args:
            source_path: Absolute path to source logo file

        Returns:
            Relative path to copied logo, or None if copy failed
        """
        if not self._workspace_path:
            return source_path  # No workspace, keep absolute path

        source = Path(source_path)
        if not source.exists():
            return None

        # Create assets folder in reports directory
        assets_dir = self._workspace_path / "reports" / "assets"
        assets_dir.mkdir(parents=True, exist_ok=True)

        # Copy with original filename (add suffix if exists)
        dest = assets_dir / source.name
        if dest.exists():
            # Add timestamp to avoid overwrite
            stem = source.stem
            suffix = source.suffix
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            dest = assets_dir / f"{stem}_{timestamp}{suffix}"

        try:
            shutil.copy2(source, dest)
            # Return relative path
            return str(dest.relative_to(self._workspace_path))
        except Exception:
            return None
