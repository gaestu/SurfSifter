"""
Example report module - demonstrates the module architecture.

This module is prefixed with underscore so it's NOT auto-discovered.
Use it as a template when creating new modules.

To create a new module:
1. Create a folder in src/reports/modules/ (e.g., tagged_urls/)
2. Copy this file as module.py
3. Create template.html with your Jinja2 template
4. Modify the class to implement your logic
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List

from ..base import BaseReportModule, FilterField, FilterType, ModuleMetadata


class ExampleModule(BaseReportModule):
    """Example module showing how to implement a report module.

    This module demonstrates:
    - Defining metadata
    - Declaring filter fields
    - Rendering HTML output
    """

    @property
    def metadata(self) -> ModuleMetadata:
        """Return module metadata."""
        return ModuleMetadata(
            module_id="example",
            name="Example Module",
            description="A sample module demonstrating the architecture",
            icon="📋",
            category="Examples",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return list of configurable filter fields."""
        return [
            FilterField(
                key="sample_text",
                label="Sample Text",
                filter_type=FilterType.TEXT,
                placeholder="Enter some text...",
                help_text="This is an example text filter",
            ),
            FilterField(
                key="include_details",
                label="Include Details",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Check to include additional details",
            ),
            FilterField(
                key="limit",
                label="Max Items",
                filter_type=FilterType.NUMBER,
                default=10,
                help_text="Maximum number of items to show",
            ),
        ]

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
            config: Dictionary of filter values

        Returns:
            HTML string to be included in the report section
        """
        sample_text = config.get("sample_text", "")
        include_details = config.get("include_details", True)
        limit = config.get("limit", 10)

        html_parts = [
            "<div class='example-module'>",
            "<h4>Example Module Output</h4>",
            f"<p>Sample text: {sample_text or '(none)'}</p>",
            f"<p>Include details: {include_details}</p>",
            f"<p>Limit: {limit}</p>",
        ]

        if include_details:
            html_parts.append("<p><em>Details would go here...</em></p>")

        html_parts.append("</div>")

        return "\n".join(html_parts)
