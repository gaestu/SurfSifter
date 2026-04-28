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
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import webbrowser
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from jinja2 import Environment, FileSystemLoader

from .modules import ModuleRegistry
from .appendix import AppendixRegistry
from .database import get_custom_sections, get_section_modules, get_appendix_modules
from .locales import get_translations, DEFAULT_LOCALE, TranslationDict
from .paths import get_templates_dir

logger = logging.getLogger(__name__)


# Template directory
TEMPLATES_DIR = get_templates_dir()
# Chromium 131 is the minimum version we rely on for the print-margin behavior
# used by investigator-facing appendix page numbering.
MIN_CHROMIUM_MARGIN_BOX_VERSION = 131


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


@dataclass(frozen=True)
class ExternalInvocation:
    """Captured external tool invocation for later audit logging."""

    task: str
    command: str
    exit_code: int
    stdout: Optional[str] = None
    stderr: Optional[str] = None


@dataclass(frozen=True)
class ChromiumBinary:
    """Detected Chromium-family executable with a supported version."""

    executable: Path
    display_name: str
    major_version: int


@dataclass(frozen=True)
class AppendixRenderPlan:
    """Resolved renderer choice for appendix PDF generation."""

    renderer: str
    chromium_binary: Optional[ChromiumBinary] = None
    note: Optional[str] = None
    audit_events: Tuple[ExternalInvocation, ...] = ()


@dataclass(frozen=True)
class PdfRenderResult:
    """Structured result for a completed PDF render."""

    renderer: str
    output_path: Path
    used_fallback: bool = False
    note: Optional[str] = None
    audit_events: Tuple[ExternalInvocation, ...] = ()


class AppendixRenderError(RuntimeError):
    """Appendix render failure carrying subprocess audit events."""

    def __init__(
        self,
        message: str,
        *,
        audit_events: Tuple[ExternalInvocation, ...] = (),
    ) -> None:
        super().__init__(message)
        self.audit_events = audit_events


AuditStartCallback = Callable[[str, str], Any]
AuditFinishCallback = Callable[[Any, ExternalInvocation], None]


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
        self._owns_db_conn = False

        # Report data
        self._data = ReportData(locale=locale)

        # Jinja2 environment
        self._env = Environment(
            loader=FileSystemLoader(TEMPLATES_DIR),
            autoescape=True,
        )

    def take_db_connection_ownership(self) -> "ReportBuilder":
        """Mark the builder's database connection for worker-owned cleanup."""
        self._owns_db_conn = True
        return self

    def close_owned_db_connection(self) -> None:
        """Close the builder-owned database connection when applicable."""
        if not self._owns_db_conn:
            return
        self._db_conn.close()
        self._owns_db_conn = False

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

    def load_appendix_from_db(
        self,
        progress_callback: Optional[callable] = None,
        cancelled_fn: Optional[callable] = None,
    ) -> "ReportBuilder":
        """Load appendix modules from the database.

        Args:
            progress_callback: Optional ``(percent, message)`` callable for
                reporting progress to the caller.
            cancelled_fn: Optional callable returning ``True`` when the
                operation should be aborted.

        Returns:
            self for method chaining
        """
        self._data.appendix_modules.clear()

        modules = get_appendix_modules(self._db_conn, self._evidence_id)
        total = len(modules) or 1
        for idx, mod in enumerate(modules):
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
            # Forward progress / cancellation hooks to modules
            if progress_callback is not None:
                render_config["_progress_callback"] = progress_callback
            if cancelled_fn is not None:
                render_config["_cancelled_fn"] = cancelled_fn

            # Check for cancellation between modules
            if cancelled_fn and cancelled_fn():
                logger.info("Appendix build cancelled by user")
                break

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

            # Report per-module progress
            if progress_callback:
                pct = int(80 * (idx + 1) / total)
                progress_callback(pct, f"Rendered appendix module: {title or module_id}")

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

    def render_appendix_html(self, renderer: str = "weasyprint") -> str:
        """Render the appendix as a standalone HTML document.

        The appendix document has its own title page, TOC, and page numbering
        starting at 1 with the format "Appendix — Page X of Y".

        Args:
            renderer: Target PDF renderer, used for renderer-specific appendix
                features such as TOC page numbers.

        Returns:
            Complete HTML string for the appendix document
        """
        template = self._env.get_template("appendix_report.html")

        ctx = self._get_common_template_context()
        ctx.update({
            "appendix_modules": self._data.appendix_modules,
            "appendix_renderer": renderer,
            "appendix_toc_page_numbers": renderer == "weasyprint",
            "hide_appendix_page_numbers": self._data.hide_appendix_page_numbers,
        })
        return template.render(**ctx)

    def render_html(
        self,
        mode: ReportMode = ReportMode.REPORT_ONLY,
        appendix_renderer: str = "weasyprint",
    ) -> str | Tuple[str, str]:
        """Render the report to HTML in the specified mode.

        Args:
            mode: Which parts to render.
                - REPORT_ONLY: report sections + author signature (no appendix)
                - APPENDIX_ONLY: standalone appendix document
                - COMPLETE: returns a tuple of (report_html, appendix_html)
            appendix_renderer: Renderer to target for appendix HTML.

        Returns:
            For REPORT_ONLY / APPENDIX_ONLY: a single HTML string.
            For COMPLETE: a tuple ``(report_html, appendix_html)``.
        """
        if mode == ReportMode.REPORT_ONLY:
            return self.render_report_html(include_appendix=False)
        elif mode == ReportMode.APPENDIX_ONLY:
            return self.render_appendix_html(renderer=appendix_renderer)
        else:  # COMPLETE
            report_html = self.render_report_html(include_appendix=False)
            appendix_html = self.render_appendix_html(renderer=appendix_renderer)
            return (report_html, appendix_html)

    def get_data(self) -> ReportData:
        """Get the current report data.

        Returns:
            ReportData instance
        """
        return self._data


class ReportGenerator:
    """Generates PDF reports and appendix PDFs with renderer fallback."""

    def __init__(self):
        """Initialize the report generator."""
        self._weasyprint_available = False
        self._preview_root: Optional[Path] = None
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

    def set_preview_root(self, preview_root: Optional[Path]) -> None:
        """Set the workspace-backed directory used for HTML previews."""
        self._preview_root = preview_root

    def _get_preview_dir(self) -> Path:
        """Return the workspace-backed directory used for preview/render HTML."""
        if self._preview_root is None:
            raise ValueError("Preview output directory is not configured.")

        preview_dir = self._preview_root / "reports" / "previews"
        preview_dir.mkdir(parents=True, exist_ok=True)
        return preview_dir

    def _write_workspace_html(self, html_content: str, prefix: str) -> Path:
        """Write HTML to the workspace-backed preview directory."""
        preview_dir = self._get_preview_dir()
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".html",
            prefix=prefix,
            dir=preview_dir,
            delete=False,
        ) as temp_handle:
            temp_handle.write(html_content)
            return Path(temp_handle.name)

    def _find_chromium_candidates(self) -> List[Path]:
        """Discover likely Chromium-family executables without invoking them."""
        candidates: List[Path] = []
        seen: set[Path] = set()

        def _add_candidate(path_str: str) -> None:
            if not path_str:
                return
            path = Path(path_str)
            if not path.exists():
                return
            resolved = path.resolve()
            if resolved in seen:
                return
            seen.add(resolved)
            candidates.append(resolved)

        for name in (
            "chromium",
            "chromium-browser",
            "google-chrome",
            "google-chrome-stable",
            "brave-browser",
            "microsoft-edge",
            "msedge",
        ):
            resolved = shutil.which(name)
            if resolved:
                _add_candidate(resolved)

        if sys.platform == "darwin":
            for path_str in (
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                "/Applications/Chromium.app/Contents/MacOS/Chromium",
                "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
                "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
            ):
                _add_candidate(path_str)
        elif sys.platform.startswith("win"):
            program_files = [
                os.environ.get("ProgramFiles", ""),
                os.environ.get("ProgramFiles(x86)", ""),
                os.environ.get("LocalAppData", ""),
            ]
            suffixes = (
                "Google/Chrome/Application/chrome.exe",
                "Chromium/Application/chrome.exe",
                "BraveSoftware/Brave-Browser/Application/brave.exe",
                "Microsoft/Edge/Application/msedge.exe",
            )
            for base_dir in program_files:
                for suffix in suffixes:
                    _add_candidate(str(Path(base_dir) / suffix))

        return candidates

    def _run_external_command(
        self,
        task: str,
        command: List[str],
        *,
        timeout_s: int = 30,
        audit_start_cb: Optional[AuditStartCallback] = None,
        audit_finish_cb: Optional[AuditFinishCallback] = None,
    ) -> ExternalInvocation:
        """Run an external command and capture the result for audit logging."""
        command_str = shlex.join(command)
        audit_token = audit_start_cb(task, command_str) if audit_start_cb is not None else None
        event: Optional[ExternalInvocation] = None
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                errors="replace",
                timeout=timeout_s,
                check=False,
            )
            event = ExternalInvocation(
                task=task,
                command=command_str,
                exit_code=completed.returncode,
                stdout=(completed.stdout or "").strip() or None,
                stderr=(completed.stderr or "").strip() or None,
            )
            return event
        except subprocess.TimeoutExpired as exc:
            event = ExternalInvocation(
                task=task,
                command=command_str,
                exit_code=-1,
                stdout=(exc.stdout or "").strip() or None,
                stderr=(exc.stderr or "").strip() or f"Timed out after {timeout_s}s",
            )
            return event
        except OSError as exc:
            event = ExternalInvocation(
                task=task,
                command=command_str,
                exit_code=-1,
                stdout=None,
                stderr=str(exc),
            )
            return event
        finally:
            if audit_finish_cb is not None and event is not None:
                audit_finish_cb(audit_token, event)

    def _probe_chromium_binary(
        self,
        executable: Path,
        *,
        audit_start_cb: Optional[AuditStartCallback] = None,
        audit_finish_cb: Optional[AuditFinishCallback] = None,
    ) -> Tuple[Optional[ChromiumBinary], ExternalInvocation, Optional[str]]:
        """Return a supported Chromium binary, its probe audit event, and failure reason."""
        event = self._run_external_command(
            "chromium_probe",
            [str(executable), "--version"],
            timeout_s=10,
            audit_start_cb=audit_start_cb,
            audit_finish_cb=audit_finish_cb,
        )
        output = " ".join(
            part for part in (event.stdout, event.stderr) if part
        ).strip()
        if event.exit_code != 0:
            reason = output or f"Failed to probe Chromium executable at {executable}"
            return None, event, reason

        version_match = re.search(r"(\d+)(?:\.\d+){1,3}", output)
        if not version_match:
            return None, event, f"Could not determine Chromium version from: {output or executable.name}"

        major_version = int(version_match.group(1))
        display_name = re.sub(r"\s+\d+(?:\.\d+){1,3}.*$", "", output).strip() or executable.name
        if major_version < MIN_CHROMIUM_MARGIN_BOX_VERSION:
            reason = (
                f"{display_name} {major_version} is too old for appendix page numbering "
                f"(requires Chromium {MIN_CHROMIUM_MARGIN_BOX_VERSION}+)"
            )
            return None, event, reason

        return (
            ChromiumBinary(
                executable=executable,
                display_name=display_name,
                major_version=major_version,
            ),
            event,
            None,
        )

    def plan_appendix_render(
        self,
        *,
        audit_start_cb: Optional[AuditStartCallback] = None,
        audit_finish_cb: Optional[AuditFinishCallback] = None,
    ) -> AppendixRenderPlan:
        """Resolve the renderer for appendix PDFs.

        Prefers Chromium when a supported executable is available, otherwise
        falls back to WeasyPrint when installed.
        """
        audit_events: List[ExternalInvocation] = []
        unsupported_reasons: List[str] = []

        for candidate in self._find_chromium_candidates():
            chromium_binary, event, reason = self._probe_chromium_binary(
                candidate,
                audit_start_cb=audit_start_cb,
                audit_finish_cb=audit_finish_cb,
            )
            audit_events.append(event)
            if chromium_binary is not None:
                return AppendixRenderPlan(
                    renderer="chromium",
                    chromium_binary=chromium_binary,
                    audit_events=tuple(audit_events),
                )
            if reason:
                unsupported_reasons.append(reason)

        fallback_note = None
        if self._weasyprint_available:
            if unsupported_reasons:
                fallback_note = (
                    f"{unsupported_reasons[0]}; using slower WeasyPrint fallback for the appendix."
                )
            else:
                fallback_note = "Chromium renderer unavailable; using slower WeasyPrint fallback for the appendix."
            return AppendixRenderPlan(
                renderer="weasyprint",
                note=fallback_note,
                audit_events=tuple(audit_events),
            )

        if unsupported_reasons:
            return AppendixRenderPlan(
                renderer="unavailable",
                note=unsupported_reasons[0],
                audit_events=tuple(audit_events),
            )
        return AppendixRenderPlan(
            renderer="unavailable",
            note=(
                "No supported appendix PDF renderer is available. Install WeasyPrint or a Chromium-family browser "
                f"{MIN_CHROMIUM_MARGIN_BOX_VERSION}+."
            ),
            audit_events=tuple(audit_events),
        )

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

    def generate_appendix_pdf(
        self,
        html_content: str,
        output_path: Path | str,
        plan: Optional[AppendixRenderPlan] = None,
        fallback_html_content: Optional[str] = None,
        *,
        audit_start_cb: Optional[AuditStartCallback] = None,
        audit_finish_cb: Optional[AuditFinishCallback] = None,
    ) -> PdfRenderResult:
        """Generate an appendix PDF, preferring Chromium when available."""
        output_path = Path(output_path).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        generated_plan = plan is None
        plan = plan or self.plan_appendix_render(
            audit_start_cb=audit_start_cb,
            audit_finish_cb=audit_finish_cb,
        )
        inherited_audit_events = plan.audit_events if generated_plan else ()

        if plan.renderer == "unavailable":
            raise ImportError(plan.note or "Appendix PDF generation is unavailable.")

        if plan.renderer != "chromium":
            self.generate_pdf(html_content, output_path)
            return PdfRenderResult(
                renderer="weasyprint",
                output_path=output_path,
                used_fallback=bool(plan.note),
                note=plan.note,
                audit_events=inherited_audit_events,
            )

        html_path = self._write_workspace_html(html_content, "appendix_render_")
        preview_dir = self._get_preview_dir()
        render_event: Optional[ExternalInvocation] = None
        try:
            with tempfile.TemporaryDirectory(dir=preview_dir, prefix="chromium_profile_") as profile_dir:
                staging_output = Path(profile_dir) / "appendix.pdf"
                command = [
                    str(plan.chromium_binary.executable),
                    "--headless=new",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--disable-extensions",
                    "--no-pdf-header-footer",
                    '--host-resolver-rules=MAP * ~NOTFOUND',
                    f"--user-data-dir={profile_dir}",
                    f"--print-to-pdf={staging_output}",
                    "--virtual-time-budget=10000",
                    html_path.as_uri(),
                ]
                render_event = self._run_external_command(
                    "chromium_appendix_pdf",
                    command,
                    timeout_s=60,
                    audit_start_cb=audit_start_cb,
                    audit_finish_cb=audit_finish_cb,
                )

                if render_event.exit_code == 0 and staging_output.exists() and staging_output.stat().st_size > 0:
                    staging_output.replace(output_path)
                    logger.info(
                        "Appendix PDF generated with %s %s: %s",
                        plan.chromium_binary.display_name,
                        plan.chromium_binary.major_version,
                        output_path,
                    )
                    return PdfRenderResult(
                        renderer="chromium",
                        output_path=output_path,
                        note=(
                            f"Appendix rendered with {plan.chromium_binary.display_name} "
                            f"{plan.chromium_binary.major_version}."
                        ),
                        audit_events=inherited_audit_events + (render_event,),
                    )
        finally:
            html_path.unlink(missing_ok=True)

        assert render_event is not None
        failure_reason = render_event.stderr or render_event.stdout or (
            f"Chromium appendix rendering failed for {plan.chromium_binary.executable}"
        )
        audit_events = inherited_audit_events + (render_event,)
        if not self._weasyprint_available:
            raise AppendixRenderError(
                failure_reason,
                audit_events=audit_events,
            )

        try:
            self.generate_pdf(fallback_html_content or html_content, output_path)
        except Exception as exc:
            raise AppendixRenderError(
                f"{failure_reason}; WeasyPrint fallback also failed: {exc}",
                audit_events=audit_events,
            ) from exc
        fallback_note = (
            f"Chromium appendix rendering failed ({failure_reason}); using slower WeasyPrint fallback."
        )
        return PdfRenderResult(
            renderer="weasyprint",
            output_path=output_path,
            used_fallback=True,
            note=fallback_note,
            audit_events=audit_events,
        )

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
        temp_file = self._write_workspace_html(html_content, "report_preview_")

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
