"""Module picker dialog for selecting and configuring report modules."""

import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QComboBox,
    QCheckBox,
    QSpinBox,
    QDateEdit,
    QPushButton,
    QGroupBox,
    QScrollArea,
    QWidget,
    QListWidget,
    QListWidgetItem,
    QStackedWidget,
    QFormLayout,
    QFrame,
    QMessageBox,
    QSplitter,
)
from PySide6.QtCore import Qt, QDate

from ..modules import ModuleRegistry, BaseReportModule, FilterType, FilterField


class ModulePickerDialog(QDialog):
    """Dialog for selecting a module and configuring its filters.

    Shows available modules in a list on the left, and filter configuration
    on the right when a module is selected.
    """

    def __init__(
        self,
        parent: Optional[QWidget] = None,
        *,
        registry: Optional[ModuleRegistry] = None,
        module_id: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        edit_mode: bool = False,
        db_conn: Optional[sqlite3.Connection] = None,
        show_title_input: bool = False,
        title: Optional[str] = None,
    ):
        """Initialize the module picker dialog.

        Args:
            parent: Parent widget
            module_id: Pre-selected module ID (for editing)
            config: Pre-filled configuration (for editing)
            edit_mode: True if editing existing module, False for new
            db_conn: Optional SQLite connection for dynamic filter options
        """
        super().__init__(parent)

        self._registry = registry or ModuleRegistry()
        self._selected_module_id: Optional[str] = module_id
        self._config = config or {}
        self._edit_mode = edit_mode
        self._db_conn = db_conn
        self._show_title_input = show_title_input
        self._module_title = title or ""
        self._filter_widgets: Dict[str, QWidget] = {}

        self.setWindowTitle("Edit Module" if edit_mode else "Add Module")
        self.setMinimumSize(600, 450)
        self.resize(700, 500)

        self._setup_ui()

        # Pre-select module if editing
        if module_id:
            self._select_module_by_id(module_id)

    def _setup_ui(self) -> None:
        """Setup the dialog UI."""
        layout = QVBoxLayout(self)
        layout.setSpacing(12)

        # Main content: module list + config panel
        splitter = QSplitter(Qt.Horizontal)

        # Left panel: Module selection
        left_panel = self._build_module_list_panel()
        splitter.addWidget(left_panel)

        # Right panel: Filter configuration
        right_panel = self._build_config_panel()
        splitter.addWidget(right_panel)

        splitter.setSizes([250, 450])
        layout.addWidget(splitter, 1)

        # Button row
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(cancel_btn)

        self._save_btn = QPushButton("Add Module" if not self._edit_mode else "Save")
        self._save_btn.setDefault(True)
        self._save_btn.clicked.connect(self._on_save)
        self._save_btn.setEnabled(False)  # Disabled until module selected
        button_layout.addWidget(self._save_btn)

        layout.addLayout(button_layout)

    def _build_module_list_panel(self) -> QWidget:
        """Build the module selection panel."""
        panel = QGroupBox("Select Module")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(8, 12, 8, 8)

        # Module list
        self._module_list = QListWidget()
        self._module_list.currentItemChanged.connect(self._on_module_selected)

        # Populate with available modules
        modules_by_category = self._registry.list_modules_by_category()

        if not modules_by_category:
            # No modules available
            item = QListWidgetItem("No modules available")
            item.setFlags(Qt.NoItemFlags)
            self._module_list.addItem(item)
        else:
            for category, modules in modules_by_category.items():
                # Category header
                header = QListWidgetItem(f"── {category} ──")
                header.setFlags(Qt.NoItemFlags)
                header.setForeground(Qt.gray)
                self._module_list.addItem(header)

                # Modules in category
                for meta in modules:
                    item = QListWidgetItem(f"  {meta.icon} {meta.name}")
                    item.setData(Qt.UserRole, meta.module_id)
                    item.setToolTip(meta.description)
                    self._module_list.addItem(item)

        layout.addWidget(self._module_list)
        return panel

    def _build_config_panel(self) -> QWidget:
        """Build the filter configuration panel."""
        panel = QGroupBox("Configure Filters")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(8, 12, 8, 8)

        # Stacked widget: empty state vs config form
        self._config_stack = QStackedWidget()

        # Empty state
        empty_widget = QWidget()
        empty_layout = QVBoxLayout(empty_widget)
        empty_label = QLabel("Select a module from the list to configure its filters.")
        empty_label.setStyleSheet("color: palette(mid); font-style: italic;")
        empty_label.setAlignment(Qt.AlignCenter)
        empty_layout.addWidget(empty_label)
        self._config_stack.addWidget(empty_widget)

        # Config form (will be populated when module selected)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)

        self._config_form_widget = QWidget()
        self._config_form_layout = QFormLayout(self._config_form_widget)
        self._config_form_layout.setSpacing(12)
        self._config_form_layout.setContentsMargins(4, 4, 4, 4)

        scroll.setWidget(self._config_form_widget)
        self._config_stack.addWidget(scroll)

        layout.addWidget(self._config_stack)
        return panel

    def _select_module_by_id(self, module_id: str) -> None:
        """Select a module in the list by its ID."""
        for i in range(self._module_list.count()):
            item = self._module_list.item(i)
            if item.data(Qt.UserRole) == module_id:
                self._module_list.setCurrentItem(item)
                break

    def _on_module_selected(self, current: QListWidgetItem, previous: QListWidgetItem) -> None:
        """Handle module selection change."""
        if current is None:
            self._selected_module_id = None
            self._config_stack.setCurrentIndex(0)
            self._save_btn.setEnabled(False)
            return

        module_id = current.data(Qt.UserRole)
        if module_id is None:
            # Category header selected
            self._selected_module_id = None
            self._config_stack.setCurrentIndex(0)
            self._save_btn.setEnabled(False)
            return

        self._selected_module_id = module_id
        self._build_filter_form(module_id)
        self._config_stack.setCurrentIndex(1)
        self._save_btn.setEnabled(True)

    def _build_filter_form(self, module_id: str) -> None:
        """Build the filter configuration form for a module."""
        # Clear existing form
        self._filter_widgets.clear()
        while self._config_form_layout.count():
            item = self._config_form_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        # Get module and its filter fields
        module = self._registry.get_module(module_id)
        if module is None:
            return

        # Optional title input (for appendix items)
        if self._show_title_input:
            self._title_input = QLineEdit()
            self._title_input.setPlaceholderText("Enter title...")
            if self._module_title:
                self._title_input.setText(self._module_title)
            self._config_form_layout.addRow("Title", self._title_input)

        # Add module description
        desc_label = QLabel(module.metadata.description)
        desc_label.setStyleSheet("color: palette(mid); margin-bottom: 8px;")
        desc_label.setWordWrap(True)
        self._config_form_layout.addRow(desc_label)

        # Add separator
        separator = QFrame()
        separator.setFrameShape(QFrame.HLine)
        separator.setFrameShadow(QFrame.Sunken)
        self._config_form_layout.addRow(separator)

        # Default title to module name if not set
        if self._show_title_input and not self._title_input.text().strip():
            self._title_input.setText(module.metadata.name)

        # Build filter widgets
        filter_fields = module.get_filter_fields()

        if not filter_fields:
            no_filters = QLabel("This module has no configurable filters.")
            no_filters.setStyleSheet("color: palette(mid); font-style: italic;")
            self._config_form_layout.addRow(no_filters)
            return

        for field in filter_fields:
            # Check for dynamic options
            options = field.options
            if self._db_conn is not None:
                dynamic_opts = module.get_dynamic_options(field.key, self._db_conn)
                if dynamic_opts is not None:
                    options = dynamic_opts

            # Create widget with resolved options
            widget = self._create_filter_widget(field, options)
            self._filter_widgets[field.key] = widget

            # Apply existing config value
            if field.key in self._config:
                self._set_widget_value(widget, field, self._config[field.key])
            elif field.default is not None:
                self._set_widget_value(widget, field, field.default)

            # Create label with required indicator
            label_text = field.label
            if field.required:
                label_text += " *"

            self._config_form_layout.addRow(label_text, widget)

            # Add help text if provided
            if field.help_text:
                help_label = QLabel(field.help_text)
                help_label.setStyleSheet("color: palette(mid); font-size: 11px; margin-left: 4px;")
                help_label.setWordWrap(True)
                self._config_form_layout.addRow("", help_label)

    def _create_filter_widget(
        self, field: FilterField, options: Optional[List[tuple]] = None
    ) -> QWidget:
        """Create the appropriate widget for a filter field.

        Args:
            field: Filter field definition
            options: Resolved options (may be dynamic), overrides field.options
        """
        # Use provided options or fall back to field.options
        resolved_options = options if options is not None else field.options

        if field.filter_type == FilterType.TEXT:
            widget = QLineEdit()
            if field.placeholder:
                widget.setPlaceholderText(field.placeholder)
            return widget

        elif field.filter_type == FilterType.NUMBER:
            widget = QSpinBox()
            widget.setRange(0, 999999)
            return widget

        elif field.filter_type == FilterType.CHECKBOX:
            widget = QCheckBox()
            return widget

        elif field.filter_type in (FilterType.DROPDOWN, FilterType.BROWSER_SELECT, FilterType.SOURCE_SELECT):
            widget = QComboBox()
            if resolved_options:
                for value, label in resolved_options:
                    widget.addItem(label, value)
            return widget

        elif field.filter_type in (FilterType.MULTI_SELECT, FilterType.TAG_SELECT):
            # Use a list widget with checkboxes
            widget = QListWidget()
            widget.setMaximumHeight(120)
            if resolved_options:
                for value, label in resolved_options:
                    item = QListWidgetItem(label)
                    item.setData(Qt.UserRole, value)
                    item.setCheckState(Qt.Unchecked)
                    widget.addItem(item)
            return widget

        elif field.filter_type == FilterType.TAG_SELECT_SINGLE:
            widget = QComboBox()
            widget.addItem("(Any tag)", None)
            if resolved_options:
                for value, label in resolved_options:
                    widget.addItem(label, value)
            return widget

        elif field.filter_type == FilterType.DATE_RANGE:
            # Container with two date edits
            container = QWidget()
            layout = QHBoxLayout(container)
            layout.setContentsMargins(0, 0, 0, 0)

            start_date = QDateEdit()
            start_date.setCalendarPopup(True)
            start_date.setDate(QDate.currentDate().addMonths(-1))
            start_date.setObjectName("start")

            layout.addWidget(QLabel("From:"))
            layout.addWidget(start_date)

            end_date = QDateEdit()
            end_date.setCalendarPopup(True)
            end_date.setDate(QDate.currentDate())
            end_date.setObjectName("end")

            layout.addWidget(QLabel("To:"))
            layout.addWidget(end_date)

            return container

        elif field.filter_type == FilterType.NUMBER_RANGE:
            # Container with two spinboxes
            container = QWidget()
            layout = QHBoxLayout(container)
            layout.setContentsMargins(0, 0, 0, 0)

            min_spin = QSpinBox()
            min_spin.setRange(0, 999999)
            min_spin.setObjectName("min")

            layout.addWidget(QLabel("Min:"))
            layout.addWidget(min_spin)

            max_spin = QSpinBox()
            max_spin.setRange(0, 999999)
            max_spin.setObjectName("max")

            layout.addWidget(QLabel("Max:"))
            layout.addWidget(max_spin)

            return container

        # Fallback: text input
        return QLineEdit()

    def _set_widget_value(self, widget: QWidget, field: FilterField, value: Any) -> None:
        """Set the value of a filter widget."""
        if isinstance(widget, QLineEdit):
            widget.setText(str(value) if value else "")

        elif isinstance(widget, QSpinBox):
            widget.setValue(int(value) if value else 0)

        elif isinstance(widget, QCheckBox):
            widget.setChecked(bool(value))

        elif isinstance(widget, QComboBox):
            index = widget.findData(value)
            if index >= 0:
                widget.setCurrentIndex(index)

        elif isinstance(widget, QListWidget):
            # Multi-select: check items that match values
            values = value if isinstance(value, list) else [value]
            for i in range(widget.count()):
                item = widget.item(i)
                if item.data(Qt.UserRole) in values:
                    item.setCheckState(Qt.Checked)

        elif field.filter_type == FilterType.DATE_RANGE and isinstance(value, dict):
            start_edit = widget.findChild(QDateEdit, "start")
            end_edit = widget.findChild(QDateEdit, "end")
            if start_edit and value.get("start"):
                start_edit.setDate(QDate.fromString(value["start"], Qt.ISODate))
            if end_edit and value.get("end"):
                end_edit.setDate(QDate.fromString(value["end"], Qt.ISODate))

        elif field.filter_type == FilterType.NUMBER_RANGE and isinstance(value, dict):
            min_spin = widget.findChild(QSpinBox, "min")
            max_spin = widget.findChild(QSpinBox, "max")
            if min_spin and value.get("min") is not None:
                min_spin.setValue(value["min"])
            if max_spin and value.get("max") is not None:
                max_spin.setValue(value["max"])

    def _get_widget_value(self, widget: QWidget, field: FilterField) -> Any:
        """Get the current value from a filter widget."""
        if isinstance(widget, QLineEdit):
            return widget.text().strip() or None

        elif isinstance(widget, QSpinBox):
            return widget.value()

        elif isinstance(widget, QCheckBox):
            return widget.isChecked()

        elif isinstance(widget, QComboBox):
            return widget.currentData()

        elif isinstance(widget, QListWidget):
            # Multi-select: return list of checked values
            values = []
            for i in range(widget.count()):
                item = widget.item(i)
                if item.checkState() == Qt.Checked:
                    values.append(item.data(Qt.UserRole))
            return values if values else None

        elif field.filter_type == FilterType.DATE_RANGE:
            start_edit = widget.findChild(QDateEdit, "start")
            end_edit = widget.findChild(QDateEdit, "end")
            return {
                "start": start_edit.date().toString(Qt.ISODate) if start_edit else None,
                "end": end_edit.date().toString(Qt.ISODate) if end_edit else None,
            }

        elif field.filter_type == FilterType.NUMBER_RANGE:
            min_spin = widget.findChild(QSpinBox, "min")
            max_spin = widget.findChild(QSpinBox, "max")
            return {
                "min": min_spin.value() if min_spin else None,
                "max": max_spin.value() if max_spin else None,
            }

        return None

    def _on_save(self) -> None:
        """Validate and accept the dialog."""
        if not self._selected_module_id:
            QMessageBox.warning(self, "No Module Selected", "Please select a module.")
            return

        # Build config from widgets
        module = self._registry.get_module(self._selected_module_id)
        if module is None:
            return

        config = {}
        for field in module.get_filter_fields():
            widget = self._filter_widgets.get(field.key)
            if widget:
                config[field.key] = self._get_widget_value(widget, field)

        # Validate
        errors = module.validate_config(config)
        if errors:
            QMessageBox.warning(
                self,
                "Validation Error",
                "\n".join(errors)
            )
            return

        self._config = config
        if self._show_title_input and hasattr(self, "_title_input"):
            self._module_title = self._title_input.text().strip()
        self.accept()

    def get_module_id(self) -> Optional[str]:
        """Get the selected module ID."""
        return self._selected_module_id

    def get_config(self) -> Dict[str, Any]:
        """Get the configured filter values."""
        return self._config

    def get_title(self) -> str:
        """Get the configured module title (if enabled)."""
        return self._module_title
