"""
Report generator - HTML rendering and PDF generation.

This module provides:
- ReportBuilder: Collects data and renders Jinja2 templates to HTML
- ReportGenerator: Converts HTML to PDF using WeasyPrint
- Preview functionality: Opens HTML in default browser

Usage:
    from reports.generator import ReportBuilder, ReportGenerator

    # Build report data
    builder = ReportBuilder(db_conn, evidence_id)
    builder.set_title("Forensic Report")
    builder.set_case_info(case_number="2024-001", investigator="John Doe")
    html = builder.render_html()

    # Generate PDF
    generator = ReportGenerator()
    generator.generate_pdf(html, "/path/to/output.pdf")

    # Or preview in browser
    generator.preview_in_browser(html)
"""

from __future__ import annotations

import logging
import sqlite3
import tempfile
import webbrowser
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from .modules import ModuleRegistry
from .appendix import AppendixRegistry
from .database import get_custom_sections, get_section_modules, get_appendix_modules
from .locales import get_translations, DEFAULT_LOCALE, TranslationDict
from .paths import get_templates_dir

logger = logging.getLogger(__name__)


# Template directory
TEMPLATES_DIR = get_templates_dir()


@dataclass
class SectionData:
    """Data for a single report section."""

    title: str
    content: str = ""
    modules: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ReportData:
    """Complete data for report generation."""

    title: str = "Forensic Report"
    case_number: Optional[str] = None
    evidence_label: Optional[str] = None
    investigator: Optional[str] = None
    notes: Optional[str] = None
    generation_date: str = ""
    sections: List[SectionData] = field(default_factory=list)
    appendix_modules: List[Dict[str, Any]] = field(default_factory=list)
    # Author signature fields
    author_function: Optional[str] = None
    author_name: Optional[str] = None
    author_date: Optional[str] = None
    # Localization
    locale: str = "en"
    date_format: str = "eu"  # "eu" for dd.mm.yyyy, "us" for mm/dd/yyyy
    # Branding fields
    branding_org_name: Optional[str] = None
    branding_footer_text: Optional[str] = None
    branding_logo_path: Optional[str] = None

    def __post_init__(self):
        if not self.generation_date:
            self.generation_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


class ReportBuilder:
    """Builds report data and renders to HTML using Jinja2 templates.

    The builder collects all report data, renders module content,
    and produces the final HTML output.
    """

    def __init__(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        template_name: str = "base_report.html",
        case_folder: Optional[Path] = None,
        locale: str = DEFAULT_LOCALE,
    ):
        """Initialize the report builder.

        Args:
            db_conn: SQLite connection to evidence database
            evidence_id: Evidence ID to generate report for
            template_name: Name of the Jinja2 template to use
            case_folder: Path to the case folder for resolving image paths
            locale: Locale for report text (e.g., "en", "de")
        """
        self._db_conn = db_conn
        self._evidence_id = evidence_id
        self._template_name = template_name
        self._case_folder = case_folder
        self._locale = locale
        self._translations = get_translations(locale)
        self._registry = ModuleRegistry()
        self._appendix_registry = AppendixRegistry()

        # Report data
        self._data = ReportData(locale=locale)

        # Jinja2 environment
        self._env = Environment(
            loader=FileSystemLoader(TEMPLATES_DIR),
            autoescape=True,
        )

    def set_locale(self, locale: str) -> "ReportBuilder":
        """Set the report locale.

        Args:
            locale: Locale code (e.g., "en", "de")

        Returns:
            self for method chaining
        """
        self._locale = locale
        self._translations = get_translations(locale)
        self._data.locale = locale
        return self

    def set_branding(
        self,
        org_name: Optional[str] = None,
        footer_text: Optional[str] = None,
        logo_path: Optional[str] = None,
    ) -> "ReportBuilder":
        """Set branding fields for the report.

        Args:
            org_name: Organization name (displayed on title page)
            footer_text: Footer text (displayed on all pages)
            logo_path: Path to logo image file

        Returns:
            self for method chaining
        """
        if org_name is not None:
            self._data.branding_org_name = org_name
        if footer_text is not None:
            self._data.branding_footer_text = footer_text
        if logo_path is not None:
            self._data.branding_logo_path = logo_path
        return self

    def set_date_format(self, date_format: str) -> "ReportBuilder":
        """Set the date format for the report.

        Args:
            date_format: "eu" for dd.mm.yyyy, "us" for mm/dd/yyyy

        Returns:
            self for method chaining
        """
        self._data.date_format = date_format
        return self

    def _format_author_date(self) -> str:
        """Format author_date according to date_format setting.

        Returns:
            Formatted date string, or empty string if no author_date
        """
        if not self._data.author_date:
            return ""

        # Try to parse the date (could be various formats)
        date_str = self._data.author_date.strip()
        if not date_str:
            return ""

        # If already in target format, return as-is
        if self._data.date_format == "eu":
            # Check if already dd.mm.yyyy
            import re
            if re.match(r'^\d{2}\.\d{2}\.\d{4}$', date_str):
                return date_str

        # Try parsing common formats
        parsed_date = None
        formats_to_try = [
            "%Y-%m-%d",           # ISO format
            "%d.%m.%Y",           # EU format
            "%m/%d/%Y",           # US format
            "%Y-%m-%d %H:%M",     # ISO with time
            "%d.%m.%Y %H:%M",     # EU with time
            "%m/%d/%Y %H:%M",     # US with time
        ]

        for fmt in formats_to_try:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue

        if not parsed_date:
            # Could not parse, return original
            return date_str

        # Format according to preference
        if self._data.date_format == "eu":
            return parsed_date.strftime("%d.%m.%Y")
        else:  # us
            return parsed_date.strftime("%m/%d/%Y")

    def set_title(self, title: str) -> "ReportBuilder":
        """Set the report title.

        Args:
            title: Report title

        Returns:
            self for method chaining
        """
        self._data.title = title
        return self

    def set_case_info(
        self,
        case_number: Optional[str] = None,
        evidence_label: Optional[str] = None,
        investigator: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> "ReportBuilder":
        """Set case metadata for the report.

        Args:
            case_number: Case identifier
            evidence_label: Evidence label/description
            investigator: Investigator name
            notes: Additional notes

        Returns:
            self for method chaining
        """
        if case_number is not None:
            self._data.case_number = case_number
        if evidence_label is not None:
            self._data.evidence_label = evidence_label
        if investigator is not None:
            self._data.investigator = investigator
        if notes is not None:
            self._data.notes = notes
        return self

    def set_author_info(
        self,
        function: Optional[str] = None,
        name: Optional[str] = None,
        date: Optional[str] = None,
    ) -> "ReportBuilder":
        """Set author/signature info for the report footer.

        Args:
            function: Author's function/role (e.g., "Forensic Analyst")
            name: Author's name
            date: Date of report creation (formatted string)

        Returns:
            self for method chaining
        """
        if function is not None:
            self._data.author_function = function
        if name is not None:
            self._data.author_name = name
        if date is not None:
            self._data.author_date = date
        return self

    def load_sections_from_db(self) -> "ReportBuilder":
        """Load sections and modules from the database.

        Returns:
            self for method chaining
        """
        self._data.sections.clear()

        # Get all sections for this evidence
        sections = get_custom_sections(self._db_conn, self._evidence_id)

        for section in sections:
            section_data = SectionData(
                title=section["title"],
                content=section.get("content", "") or "",
            )

            # Load and render modules for this section
            modules = get_section_modules(self._db_conn, section["id"])

            for mod in modules:
                module_id = mod.get("module_id", "")
                config = mod.get("config", {})

                # Inject case_folder and locale into config for modules that need it
                render_config = dict(config)
                if self._case_folder is not None:
                    render_config["_case_folder"] = self._case_folder
                if self._data.evidence_label:
                    render_config["_evidence_label"] = self._data.evidence_label
                render_config["_evidence_id"] = self._evidence_id
                render_config["_locale"] = self._locale
                render_config["_translations"] = self._translations
                render_config["_date_format"] = self._data.date_format

                # Get module instance and render
                module_instance = self._registry.get_module(module_id)
                if module_instance is not None:
                    try:
                        rendered_html = module_instance.render(
                            self._db_conn,
                            self._evidence_id,
                            render_config,
                        )
                        section_data.modules.append({
                            "module_id": module_id,
                            "config": config,
                            "rendered_html": rendered_html,
                        })
                    except Exception as e:
                        logger.error(f"Failed to render module {module_id}: {e}")
                        section_data.modules.append({
                            "module_id": module_id,
                            "config": config,
                            "rendered_html": f'<p class="text-muted">Error rendering module: {e}</p>',
                        })
                else:
                    logger.warning(f"Module not found: {module_id}")
                    section_data.modules.append({
                        "module_id": module_id,
                        "config": config,
                        "rendered_html": f'<p class="text-muted">Module not found: {module_id}</p>',
                    })

            self._data.sections.append(section_data)

        return self

    def load_appendix_from_db(self) -> "ReportBuilder":
        """Load appendix modules from the database.

        Returns:
            self for method chaining
        """
        self._data.appendix_modules.clear()

        modules = get_appendix_modules(self._db_conn, self._evidence_id)
        for mod in modules:
            module_id = mod.get("module_id", "")
            config = mod.get("config", {})
            title = (mod.get("title") or "").strip()

            render_config = dict(config)
            if self._case_folder is not None:
                render_config["_case_folder"] = self._case_folder
            if self._data.evidence_label:
                render_config["_evidence_label"] = self._data.evidence_label
            render_config["_evidence_id"] = self._evidence_id
            render_config["_locale"] = self._locale
            render_config["_translations"] = self._translations
            render_config["_date_format"] = self._data.date_format

            module_instance = self._appendix_registry.get_module(module_id)
            if module_instance is not None:
                try:
                    rendered_html = module_instance.render(
                        self._db_conn,
                        self._evidence_id,
                        render_config,
                    )
                    module_title = title or getattr(module_instance, "get_default_title", lambda: module_instance.metadata.name)()
                    self._data.appendix_modules.append(
                        {
                            "module_id": module_id,
                            "config": config,
                            "title": module_title,
                            "rendered_html": rendered_html,
                        }
                    )
                except Exception as e:
                    logger.error(f"Failed to render appendix module {module_id}: {e}")
                    self._data.appendix_modules.append(
                        {
                            "module_id": module_id,
                            "config": config,
                            "title": title or module_id,
                            "rendered_html": f'<p class="text-muted">Error rendering appendix module: {e}</p>',
                        }
                    )
            else:
                logger.warning(f"Appendix module not found: {module_id}")
                self._data.appendix_modules.append(
                    {
                        "module_id": module_id,
                        "config": config,
                        "title": title or module_id,
                        "rendered_html": f'<p class="text-muted">Appendix module not found: {module_id}</p>',
                    }
                )

        return self

    def add_section(
        self,
        title: str,
        content: str = "",
        modules: Optional[List[Dict[str, Any]]] = None,
    ) -> "ReportBuilder":
        """Add a section programmatically (without loading from DB).

        Args:
            title: Section title
            content: Section text content (HTML allowed)
            modules: List of module dicts with rendered_html

        Returns:
            self for method chaining
        """
        self._data.sections.append(SectionData(
            title=title,
            content=content,
            modules=modules or [],
        ))
        return self

    def render_html(self) -> str:
        """Render the report to HTML.

        Returns:
            Complete HTML string
        """
        template = self._env.get_template(self._template_name)

        # Convert dataclasses to dicts for Jinja2
        sections_data = []
        for section in self._data.sections:
            sections_data.append({
                "title": section.title,
                "content": section.content,
                "modules": section.modules,
            })

        return template.render(
            # Translations
            t=self._translations,
            locale=self._locale,
            # Report metadata
            report_title=self._data.title,
            case_number=self._data.case_number,
            evidence_label=self._data.evidence_label,
            investigator=self._data.investigator,
            notes=self._data.notes,
            generation_date=self._data.generation_date,
            sections=sections_data,
            appendix_modules=self._data.appendix_modules,
            author_function=self._data.author_function,
            author_name=self._data.author_name,
            author_date=self._data.author_date,
            author_date_formatted=self._format_author_date(),
            # Branding
            branding_org_name=self._data.branding_org_name,
            branding_footer_text=self._data.branding_footer_text,
            branding_logo_path=self._data.branding_logo_path,
        )

    def get_data(self) -> ReportData:
        """Get the current report data.

        Returns:
            ReportData instance
        """
        return self._data


class ReportGenerator:
    """Generates PDF reports and provides preview functionality."""

    def __init__(self):
        """Initialize the report generator."""
        self._weasyprint_available = False
        self._check_weasyprint()

    def _check_weasyprint(self) -> None:
        """Check if WeasyPrint is available."""
        try:
            import weasyprint  # noqa: F401
            self._weasyprint_available = True
        except ImportError:
            logger.warning("WeasyPrint not available. PDF generation will be disabled.")
            self._weasyprint_available = False

    @property
    def can_generate_pdf(self) -> bool:
        """Check if PDF generation is available.

        Returns:
            True if WeasyPrint is available
        """
        return self._weasyprint_available

    def generate_pdf(self, html_content: str, output_path: Path | str) -> bool:
        """Generate a PDF from HTML content.

        Args:
            html_content: Complete HTML string to convert
            output_path: Path where PDF will be saved

        Returns:
            True if successful, False otherwise

        Raises:
            ImportError: If WeasyPrint is not available
            IOError: If PDF cannot be written
        """
        if not self._weasyprint_available:
            raise ImportError("WeasyPrint is not installed. Please install it with: pip install weasyprint")

        import weasyprint

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Create PDF with WeasyPrint
            doc = weasyprint.HTML(string=html_content, base_url=str(TEMPLATES_DIR))
            doc.write_pdf(str(output_path))

            logger.info(f"PDF generated: {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to generate PDF: {e}")
            raise

    def preview_in_browser(self, html_content: str) -> Path:
        """Open HTML preview in the default web browser.

        Creates a temporary HTML file and opens it in the browser.

        Args:
            html_content: Complete HTML string to preview

        Returns:
            Path to the temporary HTML file
        """
        # Create a temporary file that won't be auto-deleted
        # (browser needs time to open it)
        temp_dir = Path(tempfile.gettempdir()) / "web_analyzer_reports"
        temp_dir.mkdir(exist_ok=True)

        # Use timestamp for unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_file = temp_dir / f"report_preview_{timestamp}.html"

        # Write HTML to file
        temp_file.write_text(html_content, encoding="utf-8")

        # Open in default browser
        webbrowser.open(f"file://{temp_file}")

        logger.info(f"Preview opened in browser: {temp_file}")
        return temp_file


def build_report(
    db_conn: sqlite3.Connection,
    evidence_id: int,
    title: str,
    case_number: Optional[str] = None,
    evidence_label: Optional[str] = None,
    investigator: Optional[str] = None,
) -> str:
    """Convenience function to build a complete report HTML.

    Args:
        db_conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        title: Report title
        case_number: Case identifier
        evidence_label: Evidence label
        investigator: Investigator name

    Returns:
        Complete HTML string
    """
    builder = ReportBuilder(db_conn, evidence_id)
    builder.set_title(title)
    builder.set_case_info(
        case_number=case_number,
        evidence_label=evidence_label,
        investigator=investigator,
    )
    builder.load_sections_from_db()
    builder.load_appendix_from_db()
    return builder.render_html()
