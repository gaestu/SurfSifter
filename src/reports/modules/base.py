"""
Base classes for report modules.

Each report module must extend BaseReportModule and implement:
- metadata: Module info (id, name, description, icon)
- get_filter_fields(): Define configurable filters
- render(): Generate HTML output
"""

from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.app_version import get_app_version


class FilterType(Enum):
    """Types of filter fields that modules can expose."""

    # Single/multi select from predefined options
    TAG_SELECT = "tag_select"  # Select tags (multi)
    TAG_SELECT_SINGLE = "tag_select_single"  # Select single tag

    # Date/time filters
    DATE_RANGE = "date_range"  # Start and end date

    # Numeric filters
    NUMBER = "number"  # Single number input
    NUMBER_RANGE = "number_range"  # Min/max range

    # Text filters
    TEXT = "text"  # Free text input

    # Selection filters
    DROPDOWN = "dropdown"  # Single select from options
    MULTI_SELECT = "multi_select"  # Multi select from options
    CHECKBOX = "checkbox"  # Boolean toggle

    # Source filters
    SOURCE_SELECT = "source_select"  # Select discovered_by sources
    BROWSER_SELECT = "browser_select"  # Select browsers


@dataclass
class FilterField:
    """Definition of a configurable filter field for a module.

    Attributes:
        key: Unique identifier for this filter (used in config dict)
        label: Human-readable label shown in UI
        filter_type: Type of filter control to render
        required: Whether this filter must have a value
        default: Default value if not specified
        options: For dropdown/multi_select - list of (value, label) tuples
        help_text: Optional tooltip/help text
        placeholder: Placeholder text for text inputs
    """

    key: str
    label: str
    filter_type: FilterType
    required: bool = False
    default: Any = None
    options: Optional[List[tuple]] = None  # [(value, label), ...]
    help_text: Optional[str] = None
    placeholder: Optional[str] = None


@dataclass
class ModuleMetadata:
    """Metadata describing a report module.

    Attributes:
        module_id: Unique identifier (e.g., "tagged_urls")
        name: Human-readable name (e.g., "Tagged URLs")
        description: Short description of what the module does
        icon: Emoji or icon string for UI display
        category: Grouping category (e.g., "URLs", "Images", "Timeline")
        version: Module version string
    """

    module_id: str
    name: str
    description: str
    icon: str = "📦"
    category: str = "General"
    version: str = field(default_factory=get_app_version)


class BaseReportModule(ABC):
    """Abstract base class for all report modules.

    To create a new module:
    1. Create a folder in src/reports/modules/ (e.g., tagged_urls/)
    2. Create module.py with a class extending BaseReportModule
    3. Create template.html with Jinja2 template for rendering
    4. Implement metadata property, get_filter_fields(), and render()

    Example:
        class TaggedUrlsModule(BaseReportModule):
            @property
            def metadata(self) -> ModuleMetadata:
                return ModuleMetadata(
                    module_id="tagged_urls",
                    name="Tagged URLs",
                    description="Display URLs matching selected tags",
                    icon="🔗",
                    category="URLs",
                )

            def get_filter_fields(self) -> List[FilterField]:
                return [
                    FilterField(
                        key="tags",
                        label="Tags",
                        filter_type=FilterType.TAG_SELECT,
                        help_text="Select tags to filter URLs",
                    ),
                ]

            def render(self, db_conn, evidence_id, config) -> str:
                # Query data and render template
                ...
    """

    @property
    @abstractmethod
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""
        ...

    @abstractmethod
    def get_filter_fields(self) -> List[FilterField]:
        """Return list of configurable filter fields.

        Returns:
            List of FilterField definitions that will be rendered in the UI
            for the user to configure.
        """
        ...

    @abstractmethod
    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        """Render the module content as HTML.

        Args:
            db_conn: SQLite connection to the evidence database
            evidence_id: Current evidence ID
            config: Dictionary of filter values from user configuration

        Returns:
            HTML string to be included in the report section
        """
        ...

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        """Get dynamic options for a filter field from the database.

        Override this method to load options dynamically from the database
        instead of using static options in FilterField. This is useful for
        filters that depend on data (e.g., available tags, reference lists).

        Args:
            key: The filter field key
            db_conn: SQLite connection to evidence database

        Returns:
            List of (value, label) tuples, or None if this field doesn't
            have dynamic options (uses static options from FilterField instead)
        """
        return None

    def get_template_path(self) -> Optional[Path]:
        """Get path to the module's HTML template.

        Default implementation looks for template.html in the module's folder.
        Override if your template is named differently.

        Returns:
            Path to template file, or None if no template
        """
        module_file = Path(__file__).parent
        # Get the actual module's directory (subclass location)
        import inspect
        subclass_file = inspect.getfile(self.__class__)
        module_dir = Path(subclass_file).parent
        template_path = module_dir / "template.html"

        if template_path.exists():
            return template_path
        return None

    def format_config_summary(self, config: Dict[str, Any]) -> str:
        """Format a human-readable summary of the current configuration.

        Used to display in the section card what filters are active.

        Args:
            config: Current configuration dictionary

        Returns:
            Short summary string (e.g., "Tags: important, review | Date: 2024-01-01 to 2024-12-31")
        """
        if not config:
            return "No filters configured"

        parts = []
        for filter_field in self.get_filter_fields():
            value = config.get(filter_field.key)
            if value is not None and value != "" and value != []:
                if isinstance(value, list):
                    value_str = ", ".join(str(v) for v in value)
                else:
                    value_str = str(value)
                parts.append(f"{filter_field.label}: {value_str}")

        return " | ".join(parts) if parts else "No filters configured"

    def validate_config(self, config: Dict[str, Any]) -> List[str]:
        """Validate the configuration and return any errors.

        Args:
            config: Configuration to validate

        Returns:
            List of error messages (empty if valid)
        """
        errors = []
        for filter_field in self.get_filter_fields():
            if filter_field.required:
                value = config.get(filter_field.key)
                if value is None or value == "" or value == []:
                    errors.append(f"{filter_field.label} is required")
        return errors

    def get_default_config(self) -> Dict[str, Any]:
        """Get default configuration from filter field defaults.

        Returns:
            Dictionary with default values for all filters
        """
        return {
            f.key: f.default
            for f in self.get_filter_fields()
            if f.default is not None
        }
