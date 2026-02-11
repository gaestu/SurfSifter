"""Screenshots Report Module.

Displays investigator-captured screenshots with titles, captions, and optional grouping by sequence.

Initial implementation.
"""

from __future__ import annotations

import base64
import sqlite3
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Template

from ..base import (
    BaseReportModule,
    FilterField,
    FilterType,
    ModuleMetadata,
)
from ...dates import format_datetime
from ...locales import get_translations, DEFAULT_LOCALE
from core.database.helpers import get_screenshots, get_sequences, get_screenshot_count
from core.database.manager import slugify_label
from reports.paths import get_module_template_dir

# Try to import PIL for thumbnail generation
try:
    from PIL import Image as PILImage
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

# Module directory for template resolution
_MODULE_DIR = get_module_template_dir(__file__)


class ScreenshotsModule(BaseReportModule):
    """Module for displaying investigator screenshots in reports."""

    # Special filter values
    ALL = "all"

    # Thumbnail size
    THUMB_SIZE = (400, 400)

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="screenshots",
            name="Screenshots",
            description="Displays investigator-captured screenshots with captions",
            category="Documentation",
            icon="📷",
        )

    def get_template_path(self) -> Optional[Path]:
        """Get path to the module's HTML template."""
        template_path = _MODULE_DIR / "template.html"
        if template_path.exists():
            return template_path
        return None

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for sequence and notes inclusion."""
        return [
            FilterField(
                key="sequence_filter",
                label="Sequence",
                filter_type=FilterType.DROPDOWN,
                default=self.ALL,
                options=[
                    (self.ALL, "All Screenshots"),
                ],
                help_text="Filter by sequence group",
                required=False,
            ),
            FilterField(
                key="include_notes",
                label="Include Notes",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Include internal investigator notes in report (normally hidden)",
                required=False,
            ),
            FilterField(
                key="include_url",
                label="Include URLs",
                filter_type=FilterType.CHECKBOX,
                default=True,
                help_text="Show captured URL under each screenshot",
                required=False,
            ),
            FilterField(
                key="show_total",
                label="Show Total Count",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Display total screenshot count at the bottom",
                required=False,
            ),
        ]

    def get_dynamic_options(
        self, key: str, db_conn: sqlite3.Connection
    ) -> List[tuple] | None:
        """Load dynamic options for sequence filter.

        Args:
            key: The filter field key
            db_conn: SQLite connection to evidence database

        Returns:
            List of (value, label) tuples or None if not a dynamic field
        """
        if key == "sequence_filter":
            options = [
                (self.ALL, "All Screenshots"),
            ]
            try:
                # Get evidence_id from context
                cursor = db_conn.execute(
                    "SELECT DISTINCT evidence_id FROM screenshots LIMIT 1"
                )
                row = cursor.fetchone()
                if row:
                    evidence_id = row[0]
                    sequences = get_sequences(db_conn, evidence_id)
                    for seq in sequences:
                        options.append((seq, seq))

                    # Add ungrouped option if there are screenshots without sequence
                    cursor = db_conn.execute(
                        """
                        SELECT COUNT(*) FROM screenshots
                        WHERE evidence_id = ? AND (sequence_name IS NULL OR sequence_name = '')
                        """,
                        (evidence_id,)
                    )
                    ungrouped_count = cursor.fetchone()[0]
                    if ungrouped_count > 0:
                        options.append(("_ungrouped", f"Ungrouped ({ungrouped_count})"))

            except Exception:
                pass
            return options

        return None

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
        workspace_path: Optional[Path] = None,
        evidence_label: Optional[str] = None,
        **kwargs,
    ) -> str:
        """Render the screenshots module as HTML.

        Args:
            db_conn: SQLite connection to evidence database
            evidence_id: Evidence ID
            config: Module configuration dict
            workspace_path: Path to case workspace (for loading images)
            evidence_label: Evidence label (for folder path)
            **kwargs: Additional context (t=translations dict)

        Returns:
            Rendered HTML string
        """
        # Get workspace_path and evidence_label from config if not passed directly
        # (report generator passes them as _case_folder and _evidence_label in config)
        if workspace_path is None:
            workspace_path = config.get("_case_folder")
        if evidence_label is None:
            evidence_label = config.get("_evidence_label")

        # Get locale and translations
        locale = config.get("_locale", DEFAULT_LOCALE)
        t = config.get("_translations") or get_translations(locale)
        date_format = config.get("_date_format", "eu")

        # Get filter settings
        sequence_filter = config.get("sequence_filter", self.ALL)
        include_notes = config.get("include_notes", False)
        include_url = config.get("include_url", True)
        show_total = config.get("show_total", False)

        # Build query filters
        sequence_name = None
        if sequence_filter == "_ungrouped":
            # Special case: filter for NULL/empty sequence
            # We'll handle this in the query
            sequence_name = ""
        elif sequence_filter != self.ALL:
            sequence_name = sequence_filter

        # Get screenshots
        screenshots = get_screenshots(
            db_conn,
            evidence_id,
            sequence_name=sequence_name if sequence_filter != "_ungrouped" else None,
            limit=10000,
        )

        # If ungrouped filter, filter manually
        if sequence_filter == "_ungrouped":
            screenshots = [
                s for s in screenshots
                if not s.get("sequence_name")
            ]

        # Group by sequence
        sequences_dict: Dict[str, List[Dict]] = {}
        ungrouped: List[Dict] = []

        for screenshot in screenshots:
            seq = screenshot.get("sequence_name")
            if seq:
                if seq not in sequences_dict:
                    sequences_dict[seq] = []
                sequences_dict[seq].append(screenshot)
            else:
                ungrouped.append(screenshot)

        # Sort sequences by name
        sorted_sequences = sorted(sequences_dict.items(), key=lambda x: x[0])

        # Process screenshots with thumbnails
        processed_sequences = []
        for seq_name, seq_screenshots in sorted_sequences:
            processed_screenshots = self._process_screenshots(
                seq_screenshots,
                workspace_path,
                evidence_label,
                evidence_id,
                date_format,
                t,
            )
            processed_sequences.append({
                "name": seq_name,
                "screenshots": processed_screenshots,
            })

        processed_ungrouped = self._process_screenshots(
            ungrouped,
            workspace_path,
            evidence_label,
            evidence_id,
            date_format,
            t,
        )

        # Load template
        template_path = self.get_template_path()
        if template_path and template_path.exists():
            template_str = template_path.read_text(encoding="utf-8")
        else:
            template_str = self._get_default_template()

        template = Template(template_str)

        return template.render(
            sequences=processed_sequences,
            ungrouped=processed_ungrouped,
            total_count=len(screenshots),
            include_notes=include_notes,
            include_url=include_url,
            show_total=show_total,
            t=t,
        )

    def _process_screenshots(
        self,
        screenshots: List[Dict],
        workspace_path: Optional[Path],
        evidence_label: Optional[str],
        evidence_id: int,
        date_format: str = "eu",
        t: Optional[Dict[str, str]] = None,
    ) -> List[Dict]:
        """Process screenshots and generate thumbnails."""
        processed = []
        t = t or {}
        untitled_label = t.get("untitled", "Untitled")

        for screenshot in screenshots:
            # Format the timestamp using the shared date helper
            captured_at = format_datetime(
                screenshot.get("captured_at_utc"),
                date_format=date_format,
                include_seconds=False,
            )

            item = {
                "id": screenshot.get("id"),
                "title": screenshot.get("title") or untitled_label,
                "caption": screenshot.get("caption") or "",
                "notes": screenshot.get("notes") or "",
                "url": screenshot.get("captured_url") or "",
                "source": screenshot.get("source", "unknown"),
                "captured_at": captured_at,
                "thumbnail_b64": None,
            }

            # Try to load thumbnail
            if workspace_path and evidence_label:
                try:
                    slug = slugify_label(evidence_label, evidence_id)
                    image_path = workspace_path / "evidences" / slug / screenshot["dest_path"]

                    if image_path.exists():
                        item["thumbnail_b64"] = self._generate_thumbnail(image_path)
                except Exception:
                    pass

            processed.append(item)

        return processed

    def _generate_thumbnail(self, image_path: Path) -> Optional[str]:
        """Generate base64 encoded thumbnail."""
        if not HAS_PIL:
            return None

        try:
            with PILImage.open(image_path) as img:
                img.thumbnail(self.THUMB_SIZE, PILImage.Resampling.LANCZOS)

                # Convert to RGB if necessary
                if img.mode in ("RGBA", "P"):
                    img = img.convert("RGB")

                # Save to buffer
                buffer = BytesIO()
                img.save(buffer, format="JPEG", quality=85)
                buffer.seek(0)

                # Encode to base64
                b64 = base64.b64encode(buffer.read()).decode("ascii")
                return f"data:image/jpeg;base64,{b64}"

        except Exception:
            return None

    def _get_default_template(self) -> str:
        """Return default template if file not found."""
        return """
<div class="module-screenshots">
    {% if sequences or ungrouped %}
        {% for seq in sequences %}
        <div class="screenshot-sequence">
            <h3>{{ seq.name }}</h3>
            {% for screenshot in seq.screenshots %}
            <div class="screenshot-item">
                {% if screenshot.thumbnail_b64 %}
                <img src="{{ screenshot.thumbnail_b64 }}" alt="{{ screenshot.title }}" class="screenshot-image"/>
                {% else %}
                <div class="no-thumbnail">📷 {{ t.no_preview | default('No Preview') }}</div>
                {% endif %}
                <div class="screenshot-meta">
                    <strong>{{ screenshot.title }}</strong>
                    <p>{{ screenshot.caption }}</p>
                    {% if include_url and screenshot.url %}
                    <small>{{ t.url | default('URL') }}: {{ screenshot.url }}</small>
                    {% endif %}
                    {% if include_notes and screenshot.notes %}
                    <p class="screenshot-notes"><em>{{ t.notes | default('Notes') }}: {{ screenshot.notes }}</em></p>
                    {% endif %}
                    <small>{{ t.screenshot_captured | default('Captured') }}: {{ screenshot.captured_at }}</small>
                </div>
            </div>
            {% endfor %}
        </div>
        {% endfor %}

        {% if ungrouped %}
        <div class="screenshot-sequence">
            <h3>{{ t.screenshot_additional | default('Additional Screenshots') }}</h3>
            {% for screenshot in ungrouped %}
            <div class="screenshot-item">
                {% if screenshot.thumbnail_b64 %}
                <img src="{{ screenshot.thumbnail_b64 }}" alt="{{ screenshot.title }}" class="screenshot-image"/>
                {% else %}
                <div class="no-thumbnail">📷 {{ t.no_preview | default('No Preview') }}</div>
                {% endif %}
                <div class="screenshot-meta">
                    <strong>{{ screenshot.title }}</strong>
                    <p>{{ screenshot.caption }}</p>
                    {% if include_url and screenshot.url %}
                    <small>{{ t.url | default('URL') }}: {{ screenshot.url }}</small>
                    {% endif %}
                    {% if include_notes and screenshot.notes %}
                    <p class="screenshot-notes"><em>{{ t.notes | default('Notes') }}: {{ screenshot.notes }}</em></p>
                    {% endif %}
                    <small>{{ t.screenshot_captured | default('Captured') }}: {{ screenshot.captured_at }}</small>
                </div>
            </div>
            {% endfor %}
        </div>
        {% endif %}

        {% if show_total %}
        <p class="screenshot-count">{{ t.total | default('Total') }}: {{ total_count }} {{ t.screenshots | default('screenshot(s)') }}</p>
        {% endif %}
    {% else %}
    <p class="empty-message">{{ t.no_screenshots_found | default('No screenshots found.') }}</p>
    {% endif %}
</div>
"""
