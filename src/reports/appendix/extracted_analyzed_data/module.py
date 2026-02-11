"""Appendix Extracted & Analyzed Data Module.

Displays files extracted and analyzed from evidence, with provenance details.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ..base import BaseAppendixModule, FilterField, FilterType, ModuleMetadata
from ...paths import get_module_template_dir


class AppendixExtractedAnalyzedDataModule(BaseAppendixModule):
    """Appendix module for listing extracted and analyzed files."""

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="appendix_extracted_analyzed_data",
            name="Extracted & Analyzed Data",
            description="Lists files extracted and analyzed with hashes and provenance",
            category="Appendix",
            icon="FILE",
        )

    def get_filter_fields(self) -> List[FilterField]:
        return [
            FilterField(
                key="extractor_filter",
                label="Extractors",
                filter_type=FilterType.MULTI_SELECT,
                help_text="Filter by one or more extractors",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> Optional[List[tuple]]:
        if key == "extractor_filter":
            options: List[tuple] = []
            try:
                cursor = db_conn.execute(
                    """
                    SELECT DISTINCT extractor_name
                    FROM extracted_files
                    ORDER BY extractor_name
                    """
                )
                for (extractor_name,) in cursor.fetchall():
                    if extractor_name:
                        options.append((extractor_name, extractor_name))
            except Exception:
                pass
            return options

        return None

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        # Extract locale and translations from config
        locale = config.get("_locale", "en")
        translations = config.get("_translations", {})

        extractor_filter = config.get("extractor_filter") or []

        query = """
            SELECT
                extractor_name,
                dest_filename,
                md5,
                source_inode,
                source_path
            FROM extracted_files
            WHERE evidence_id = ?
            {extractor_clause}
            ORDER BY extractor_name, dest_filename, source_path
        """

        rows: List[Dict[str, str]] = []
        try:
            db_conn.row_factory = sqlite3.Row
            params: list[Any] = [evidence_id]
            extractor_clause = ""
            if extractor_filter:
                placeholders = ", ".join(["?"] * len(extractor_filter))
                extractor_clause = f"AND extractor_name IN ({placeholders})"
                params.extend(extractor_filter)

            cursor = db_conn.execute(query.format(extractor_clause=extractor_clause), params)
            for row in cursor.fetchall():
                rows.append(
                    {
                        "extractor": row["extractor_name"],
                        "filename": row["dest_filename"],
                        "md5": row["md5"] or "",
                        "inode": row["source_inode"] or "",
                        "source_path": row["source_path"] or "",
                    }
                )
        except Exception as exc:
            return f'<div class="module-error">Error loading extracted files: {exc}</div>'

        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            files=rows,
            total_count=len(rows),
            t=translations,
            locale=locale,
        )
