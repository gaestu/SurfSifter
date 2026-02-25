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

import enum
import logging
import sqlite3
import tempfile
import webbrowser
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from jinja2 import Environment, FileSystemLoader

from .modules import ModuleRegistry
from .appendix import AppendixRegistry
from .database import get_custom_sections, get_section_modules, get_appendix_modules
from .locales import get_translations, DEFAULT_LOCALE, TranslationDict
from .paths import get_templates_dir

logger = logging.getLogger(__name__)


# Template directory
TEMPLATES_DIR = get_templates_dir()


class ReportMode(enum.Enum):
    """Report generation mode.

    Controls which parts of the report are rendered and output as PDF.
    """

    COMPLETE = "complete"       # Both report and appendix (two separate PDFs)
    REPORT_ONLY = "report_only" # Report sections + author signature only
    APPENDIX_ONLY = "appendix_only"  # Appendix content only


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
    branding_department: Optional[str] = None
    branding_footer_text: Optional[str] = None
    branding_logo_path: Optional[str] = None
    # Title page field visibility
    show_title_case_number: bool = True
    show_title_evidence: bool = True
    show_title_investigator: bool = True
    show_title_date: bool = True
    # Footer options
    show_footer_date: bool = True
    footer_evidence_label: Optional[str] = None  # Override evidence label in footer
    # Appendix options
    hide_appendix_page_numbers: bool = False

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
        department: Optional[str] = None,
        footer_text: Optional[str] = None,
        logo_path: Optional[str] = None,
    ) -> "ReportBuilder":
        """Set branding fields for the report.

        Args:
            org_name: Organization name (displayed on title page, bold)
            department: Department name (displayed on title page below org, not bold)
            footer_text: Footer text (displayed on all pages)
            logo_path: Path to logo image file

        Returns:
            self for method chaining
        """
        if org_name is not None:
            self._data.branding_org_name = org_name
        if department is not None:
            self._data.branding_department = department
        if footer_text is not None:
            self._data.branding_footer_text = footer_text
        if logo_path is not None:
            self._data.branding_logo_path = logo_path
        return self

    def set_title_page_options(
        self,
        show_case_number: bool = True,
        show_evidence: bool = True,
        show_investigator: bool = True,
        show_date: bool = True,
    ) -> "ReportBuilder":
        """Set which metadata fields appear on the title page.

        Args:
            show_case_number: Show case number on title page
            show_evidence: Show evidence label on title page
            show_investigator: Show investigator on title page
            show_date: Show date on title page

        Returns:
            self for method chaining
        """
        self._data.show_title_case_number = show_case_number
        self._data.show_title_evidence = show_evidence
        self._data.show_title_investigator = show_investigator
        self._data.show_title_date = show_date
        return self

    def set_footer_options(
        self,
        show_footer_date: bool = True,
        footer_evidence_label: Optional[str] = None,
    ) -> "ReportBuilder":
        """Set footer display options.

        Args:
            show_footer_date: Whether to show the generation date in the footer
            footer_evidence_label: Custom evidence label for footer (overrides default)

        Returns:
            self for method chaining
        """
        self._data.show_footer_date = show_footer_date
        if footer_evidence_label is not None:
            self._data.footer_evidence_label = footer_evidence_label
        return self

    def set_appendix_options(
        self,
        hide_page_numbers: bool = False,
    ) -> "ReportBuilder":
        """Set appendix display options.

        Args:
            hide_page_numbers: Hide page numbers on appendix pages

        Returns:
            self for method chaining
        """
        self._data.hide_appendix_page_numbers = hide_page_numbers
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

    def _get_common_template_context(self) -> Dict[str, Any]:
        """Build the template context dict shared by report and appendix templates.

        Returns:
            Dict of Jinja2 template variables
        """
        return {
            # Translations
            "t": self._translations,
            "locale": self._locale,
            # Report metadata
            "report_title": self._data.title,
            "case_number": self._data.case_number,
            "evidence_label": self._data.evidence_label,
            "investigator": self._data.investigator,
            "notes": self._data.notes,
            "generation_date": self._data.generation_date,
            "author_date_formatted": self._format_author_date(),
            # Branding
            "branding_org_name": self._data.branding_org_name,
            "branding_department": self._data.branding_department,
            "branding_footer_text": self._data.branding_footer_text,
            "branding_logo_path": self._data.branding_logo_path,
            # Title page field visibility
            "show_title_case_number": self._data.show_title_case_number,
            "show_title_evidence": self._data.show_title_evidence,
            "show_title_investigator": self._data.show_title_investigator,
            "show_title_date": self._data.show_title_date,
            # Footer options
            "show_footer_date": self._data.show_footer_date,
            "footer_evidence_label": self._data.footer_evidence_label,
        }

    def render_report_html(self, include_appendix: bool = False) -> str:
        """Render the main report (sections + author signature) to HTML.

        Args:
            include_appendix: If True, include the appendix section in the
                report document (legacy/complete-in-one-doc behaviour).

        Returns:
            Complete HTML string for the report document
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

        ctx = self._get_common_template_context()
        ctx.update({
            "sections": sections_data,
            "appendix_modules": self._data.appendix_modules,
            "author_function": self._data.author_function,
            "author_name": self._data.author_name,
            "author_date": self._data.author_date,
            # Control whether the appendix block is rendered inside this document
            "render_appendix": include_appendix,
        })
        return template.render(**ctx)

    def render_appendix_html(self) -> str:
        """Render the appendix as a standalone HTML document.

        The appendix document has its own title page, TOC, and page numbering
        starting at 1 with the format "Appendix — Page X of Y".

        Returns:
            Complete HTML string for the appendix document
        """
        template = self._env.get_template("appendix_report.html")

        ctx = self._get_common_template_context()
        ctx.update({
            "appendix_modules": self._data.appendix_modules,
        })
        return template.render(**ctx)

    def render_html(self, mode: ReportMode = ReportMode.REPORT_ONLY) -> str | Tuple[str, str]:
        """Render the report to HTML in the specified mode.

        Args:
            mode: Which parts to render.
                - REPORT_ONLY: report sections + author signature (no appendix)
                - APPENDIX_ONLY: standalone appendix document
                - COMPLETE: returns a tuple of (report_html, appendix_html)

        Returns:
            For REPORT_ONLY / APPENDIX_ONLY: a single HTML string.
            For COMPLETE: a tuple ``(report_html, appendix_html)``.
        """
        if mode == ReportMode.REPORT_ONLY:
            return self.render_report_html(include_appendix=False)
        elif mode == ReportMode.APPENDIX_ONLY:
            return self.render_appendix_html()
        else:  # COMPLETE
            report_html = self.render_report_html(include_appendix=False)
            appendix_html = self.render_appendix_html()
            return (report_html, appendix_html)

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

    def generate_pdf_pair(
        self,
        report_html: str,
        appendix_html: str,
        report_path: Path | str,
        appendix_path: Path | str,
    ) -> Tuple[bool, bool]:
        """Generate both report and appendix PDFs.

        Args:
            report_html: HTML for the report document
            appendix_html: HTML for the appendix document
            report_path: Output path for report PDF
            appendix_path: Output path for appendix PDF

        Returns:
            Tuple of (report_success, appendix_success)

        Raises:
            ImportError: If WeasyPrint is not available
        """
        report_ok = self.generate_pdf(report_html, report_path)
        appendix_ok = self.generate_pdf(appendix_html, appendix_path)
        return (report_ok, appendix_ok)

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
    mode: ReportMode = ReportMode.REPORT_ONLY,
) -> str | Tuple[str, str]:
    """Convenience function to build report HTML.

    Args:
        db_conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        title: Report title
        case_number: Case identifier
        evidence_label: Evidence label
        investigator: Investigator name
        mode: Report generation mode

    Returns:
        For REPORT_ONLY / APPENDIX_ONLY: a single HTML string.
        For COMPLETE: a tuple ``(report_html, appendix_html)``.
    """
    builder = ReportBuilder(db_conn, evidence_id)
    builder.set_title(title)
    builder.set_case_info(
        case_number=case_number,
        evidence_label=evidence_label,
        investigator=investigator,
    )
    builder.load_sections_from_db()
    if mode != ReportMode.REPORT_ONLY:
        builder.load_appendix_from_db()
    return builder.render_html(mode)
