"""
Example appendix module — demonstrates the standardized appendix architecture.

This module is prefixed with underscore so it's NOT auto-discovered by the
appendix registry (see ``appendix/registry.py``). Use it as a template when
creating new appendix modules.

Conventions (compare with ``appendix/url_list/`` and ``appendix/file_list/``):
  - Extend ``BaseAppendixModule`` (not ``BaseReportModule`` directly).
  - ``ModuleMetadata.category`` is "Appendix".
  - Title / description chrome is rendered by the appendix wrapper, so
    appendix modules do NOT expose ``show_title`` / ``custom_title`` /
    ``show_description`` fields the way regular report modules do.
  - Multi-select filters use ``FilterType.MULTI_SELECT`` (or ``TAG_SELECT``
    for tags) and load their options dynamically via ``get_dynamic_options``.
    The UI renders both as a checkbox list — see
    ``reports/ui/module_picker.py``.
  - Empty multi-select == "no filter applied" (include everything).

To create a new appendix module:
1. Copy this folder to ``src/reports/appendix/<your_module>/`` (no underscore)
2. Rename the class, update metadata, and implement the query logic
3. Create ``template.html`` following the HTML pattern below
4. Re-export the class from ``src/reports/appendix/__init__.py`` so the
   PyInstaller frozen-bundle fallback can still find it
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...paths import get_module_template_dir


class AppendixExampleModule(BaseAppendixModule):
    """Example appendix module following the standard field layout.

    Standard appendix field order:
      1. Data filters    — tag_filter, match_filter, … (multi-select)
      2. Filter mode     — and/or combination of the multi-selects
      3. Display options — grouping / column toggles
    """

    # Filter mode sentinels
    MODE_OR = "or"
    MODE_AND = "and"

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_example",
            name="Example Appendix",
            description="Sample appendix module demonstrating the architecture",
            icon="📎",
            category="Appendix",
        )

    def get_filter_fields(self) -> List[FilterField]:
        return [
            # ── 1. Data filters (multi-select checkbox lists) ──────────
            # Empty selection = no filter applied for that field.
            FilterField(
                key="tag_filter",
                label="Tags",
                filter_type=FilterType.MULTI_SELECT,
                default=[],
                options=[],
                help_text=(
                    "Select one or more tags to filter by "
                    "(leave all unchecked to include everything)"
                ),
                required=False,
            ),
            FilterField(
                key="match_filter",
                label="Reference List Matches",
                filter_type=FilterType.MULTI_SELECT,
                default=[],
                options=[],
                help_text=(
                    "Select one or more reference lists to filter by "
                    "(leave all unchecked to include everything)"
                ),
                required=False,
            ),
            # ── 2. Filter combination mode ─────────────────────────────
            FilterField(
                key="filter_mode",
                label="Filter Mode",
                filter_type=FilterType.DROPDOWN,
                default=self.MODE_OR,
                options=[
                    (self.MODE_OR, "OR — Any tag or any match"),
                    (self.MODE_AND, "AND — Must have a selected tag AND a selected match"),
                ],
                help_text=(
                    "How to combine tag and match filters when both have "
                    "selections."
                ),
                required=False,
            ),
            # ── 3. Display options (module-specific) ───────────────────
            FilterField(
                key="show_count",
                label="Show Count",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Display the total number of items in the heading",
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        """Load checkbox-list options from the evidence database.

        Each entry is a ``(value, label)`` tuple. The UI renders one
        checkbox per entry. Return an empty list if there are no options
        yet (the field still renders, just with nothing to check).
        """
        if key == "tag_filter":
            options: List[tuple] = []
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT t.name
                    FROM tags t
                    ORDER BY t.name
                    """
                )
                for (tag_name,) in cursor.fetchall():
                    options.append((tag_name, tag_name))
            except Exception:
                pass
            return options

        if key == "match_filter":
            options: List[tuple] = []
            # Replace with the relevant reference-list table for your
            # appendix (e.g., ``url_matches``, ``file_matches``, …).
            # try:
            #     cursor = db_conn.execute(
            #         "SELECT DISTINCT list_name FROM url_matches "
            #         "ORDER BY list_name"
            #     )
            #     for (list_name,) in cursor.fetchall():
            #         options.append((list_name, list_name))
            # except Exception:
            #     pass
            return options

        return None

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        """Render the appendix content as HTML via Jinja2 template."""
        # ── Internal config keys (injected by report generator) ────────
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})
        t = translations

        # ── Filter values ─────────────────────────────────────────────
        # MULTI_SELECT values arrive as a list. Empty == no filter.
        selected_tags: List[str] = list(config.get("tag_filter", []) or [])
        selected_matches: List[str] = list(config.get("match_filter", []) or [])
        filter_mode = config.get("filter_mode", self.MODE_OR)
        show_count = bool(config.get("show_count", True))

        # ── Query data ────────────────────────────────────────────────
        # Replace with your real query. The helper below shows the
        # idiomatic AND/OR combination used by other appendix modules.
        items: List[Dict[str, Any]] = []
        try:
            query, params = self._build_query(
                evidence_id, selected_tags, selected_matches, filter_mode
            )
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(query, params)
            for row in cursor.fetchall():
                items.append(dict(row))
        except Exception as exc:
            return f'<div class="module-error">Error loading data: {exc}</div>'

        # ── Render template ───────────────────────────────────────────
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            items=items,
            total_count=len(items),
            show_count=show_count,
            selected_tags=selected_tags,
            selected_matches=selected_matches,
            filter_mode=filter_mode,
            t=translations,
            locale=locale,
        )

    # ──────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────

    def _build_query(
        self,
        evidence_id: int,
        tag_filter: List[str],
        match_filter: List[str],
        filter_mode: str,
    ) -> tuple[str, list[Any]]:
        """Example AND/OR combination of multi-select filters.

        This skeleton assumes a hypothetical ``items`` table scoped by
        ``evidence_id``. Adapt the table / join names to your appendix.
        """
        params: list[Any] = [evidence_id]
        conditions: list[str] = ["i.evidence_id = ?"]

        tag_condition: Optional[str] = None
        match_condition: Optional[str] = None

        if tag_filter:
            placeholders = ", ".join(["?"] * len(tag_filter))
            tag_condition = f"""
                EXISTS (
                    SELECT 1
                    FROM tag_associations ta
                    JOIN tags t ON t.id = ta.tag_id
                    WHERE ta.artifact_id = i.id
                      AND ta.evidence_id = i.evidence_id
                      AND t.name IN ({placeholders})
                )
            """

        if match_filter:
            placeholders = ", ".join(["?"] * len(match_filter))
            match_condition = f"""
                EXISTS (
                    SELECT 1
                    FROM example_matches m
                    WHERE m.item_id = i.id
                      AND m.evidence_id = i.evidence_id
                      AND m.list_name IN ({placeholders})
                )
            """

        if filter_mode == self.MODE_AND and tag_filter and match_filter:
            conditions.append(f"({tag_condition})")
            params.extend(tag_filter)
            conditions.append(f"({match_condition})")
            params.extend(match_filter)
        elif tag_condition or match_condition:
            or_parts: list[str] = []
            if tag_condition:
                or_parts.append(tag_condition)
                params.extend(tag_filter)
            if match_condition:
                or_parts.append(match_condition)
                params.extend(match_filter)
            conditions.append(f"({' OR '.join(or_parts)})")

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT DISTINCT i.id, i.name
            FROM items i
            WHERE {where_clause}
            ORDER BY i.name
        """
        return query, params
