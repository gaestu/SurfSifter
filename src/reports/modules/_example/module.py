"""
Example report module — demonstrates the standardized module architecture.

This module is prefixed with underscore so it's NOT auto-discovered.
Use it as a template when creating new modules.

See MODULE_GUIDE.md for the full specification of standard fields,
field ordering, template structure, and CSS scoping conventions.

To create a new module:
1. Copy this folder to src/reports/modules/<your_module>/  (no underscore)
2. Rename the class, update metadata, and implement your logic
3. Create template.html following the HTML pattern below
4. Export the class in src/reports/modules/__init__.py
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ...paths import get_module_template_dir
from ..base import BaseReportModule, FilterField, FilterType, ModuleMetadata


class ExampleModule(BaseReportModule):
    """Example module following the standard field layout.

    Standard field order (see MODULE_GUIDE.md):
      1. Title group       — show_title, custom_title
      2. Description group — show_description, custom_description
      3. Data filters      — tag_filter, date range, etc. (module-specific)
      4. Display options   — show_* column toggles (module-specific)
      5. Sort & limit      — sort_by, limit
      6. Footer            — show_filter_info
    """

    # Sentinel values for limit dropdown
    UNLIMITED = "unlimited"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="example",
            name="Example Module",
            description="A sample module demonstrating the architecture",
            icon="📋",
            category="Examples",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Standard fields first, then module-specific fields."""
        return [
            # ── 1. Title group ──────────────────────────────────────
            FilterField(
                key="show_title",
                label="Show Title",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display a title at the top of this section",
            ),
            FilterField(
                key="custom_title",
                label="Custom Title",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Custom title (leave empty for default)",
            ),
            # ── 2. Description group ────────────────────────────────
            FilterField(
                key="show_description",
                label="Show Description",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display a short description below the title",
            ),
            FilterField(
                key="custom_description",
                label="Custom Description",
                filter_type=FilterType.TEXT,
                default="",
                help_text="Custom description (leave empty for default)",
            ),
            # ── 3. Data filters (module-specific) ───────────────────
            # Multi-select checkbox list. Empty selection = no tag filter
            # (include all items). Tag values are loaded dynamically from
            # the database via get_dynamic_options().
            FilterField(
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.MULTI_SELECT,
                default=[],
                options=[],
                help_text="Select one or more tags to filter by (leave all unchecked to include everything)",
            ),
            # ── 4. Display options (module-specific) ────────────────
            FilterField(
                key="show_details",
                label="Show Details",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Include additional detail rows",
            ),
            # ── 5. Sort & limit ─────────────────────────────────────
            FilterField(
                key="sort_by",
                label="Sort By",
                filter_type=FilterType.DROPDOWN,
                default="name_asc",
                options=[
                    ("name_asc", "Name (A-Z)"),
                    ("name_desc", "Name (Z-A)"),
                    ("date_desc", "Date (Newest First)"),
                    ("date_asc", "Date (Oldest First)"),
                ],
                help_text="Sort order for the items",
            ),
            FilterField(
                key="limit",
                label="Limit",
                filter_type=FilterType.DROPDOWN,
                default="100",
                options=[
                    ("10", "10"),
                    ("25", "25"),
                    ("50", "50"),
                    ("100", "100"),
                    ("250", "250"),
                    ("500", "500"),
                    (self.UNLIMITED, "Unlimited"),
                ],
                help_text="Maximum number of items to show",
            ),
            # ── 6. Footer ──────────────────────────────────────────
            FilterField(
                key="show_filter_info",
                label="Show Filter Info",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display filter criteria below the content",
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        """Load dynamic options for filter fields.

        For MULTI_SELECT tag filters, return one (value, label) tuple per
        tag. The UI renders these as a checkbox list so the user can pick
        any combination.
        """
        if key == "tag_filter":
            options: List[tuple] = []
            # Example: load tags from database
            # cursor = db_conn.execute(
            #     "SELECT DISTINCT t.name FROM tags t ORDER BY t.name"
            # )
            # for (tag_name,) in cursor.fetchall():
            #     options.append((tag_name, tag_name))
            return options
        return None

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        """Render the module content as HTML via Jinja2 template.

        Args:
            db_conn: SQLite connection to the evidence database
            evidence_id: Current evidence ID
            config: Dictionary of filter values

        Returns:
            HTML string to be included in the report section
        """
        # ── Internal config keys (injected by report generator) ─────
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        date_format = config.get("_date_format", "eu")
        t = translations

        # ── Standard fields ─────────────────────────────────────────
        show_title = config.get("show_title", True)
        custom_title = config.get("custom_title", "")
        title_text = custom_title or t.get("example_title", self.metadata.name)

        show_description = config.get("show_description", True)
        custom_description = config.get("custom_description", "")
        description_text = custom_description or t.get(
            "example_description", self.metadata.description
        )

        show_filter_info = config.get("show_filter_info", False)

        # ── Module-specific fields ──────────────────────────────────
        # MULTI_SELECT values arrive as a list of selected tag names.
        # An empty list means "no tag filter applied".
        selected_tags: List[str] = list(config.get("tag_filter", []) or [])
        show_details = config.get("show_details", True)
        sort_by = config.get("sort_by", "name_asc")
        limit = config.get("limit", "100")

        # ── Query data ──────────────────────────────────────────────
        # Replace this with your actual DB query
        items: List[Dict[str, Any]] = []
        total_count = 0

        # ── Truncation ──────────────────────────────────────────────
        shown_count = len(items)
        is_truncated = shown_count < total_count

        # ── Filter description (for show_filter_info) ───────────────
        filter_description = self._build_filter_description(selected_tags, sort_by, t)

        # ── Render template ─────────────────────────────────────────
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            # Standard
            show_title=show_title,
            title_text=title_text,
            show_description=show_description,
            description_text=description_text,
            show_filter_info=show_filter_info,
            filter_description=filter_description,
            total_count=total_count,
            shown_count=shown_count,
            is_truncated=is_truncated,
            # Module-specific
            items=items,
            show_details=show_details,
            # Translation / locale
            t=translations,
            locale=locale,
        )

    def _build_filter_description(
        self, selected_tags: List[str], sort_by: str, t: Dict[str, str]
    ) -> str:
        """Build human-readable filter description for the footer."""
        parts = []

        if not selected_tags:
            parts.append(t.get("filter_all_tags", "all tags"))
        else:
            joined = ", ".join(selected_tags)
            parts.append(
                t.get("filter_tagged", 'tagged "{tag}"').replace("{tag}", joined)
            )

        sort_labels = {
            "name_asc": t.get("sort_name_az", "name A-Z"),
            "name_desc": t.get("sort_name_za", "name Z-A"),
            "date_desc": t.get("sort_newest_first", "newest first"),
            "date_asc": t.get("sort_oldest_first", "oldest first"),
        }
        sort_label = sort_labels.get(sort_by, sort_by)
        parts.append(t.get("filter_sorted_by", "sorted by {sort}").replace("{sort}", sort_label))

        return ", ".join(parts)
