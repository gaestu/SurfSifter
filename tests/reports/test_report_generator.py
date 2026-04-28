"""Tests for report generation (builder, generator, templates)."""

import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from reports.generator import (
    AppendixRenderPlan,
    ChromiumBinary,
    ExternalInvocation,
    MIN_CHROMIUM_MARGIN_BOX_VERSION,
    ReportBuilder,
    ReportGenerator,
    ReportData,
    ReportMode,
    SectionData,
    build_report,
    TEMPLATES_DIR,
)


class TestReportData:
    """Test ReportData dataclass."""

    def test_defaults(self):
        """Test default values."""
        data = ReportData()
        assert data.title == "Forensic Report"
        assert data.case_number is None
        assert data.evidence_label is None
        assert data.investigator is None
        assert data.notes is None
        assert data.sections == []
        assert data.generation_date  # Should be auto-generated
        # Author info defaults
        assert data.author_function is None
        assert data.author_name is None
        assert data.author_date is None

    def test_custom_values(self):
        """Test with custom values."""
        data = ReportData(
            title="Test Report",
            case_number="2024-001",
            evidence_label="HDD-001",
            investigator="John Doe",
        )
        assert data.title == "Test Report"
        assert data.case_number == "2024-001"
        assert data.evidence_label == "HDD-001"
        assert data.investigator == "John Doe"

    def test_author_info_values(self):
        """Test author info fields."""
        data = ReportData(
            title="Test Report",
            author_function="Forensic Analyst",
            author_name="Max Mustermann",
            author_date="17.01.2026",
        )
        assert data.author_function == "Forensic Analyst"
        assert data.author_name == "Max Mustermann"
        assert data.author_date == "17.01.2026"


class TestSectionData:
    """Test SectionData dataclass."""

    def test_defaults(self):
        """Test default values."""
        section = SectionData(title="Test Section")
        assert section.title == "Test Section"
        assert section.content == ""
        assert section.modules == []

    def test_with_content_and_modules(self):
        """Test with content and modules."""
        section = SectionData(
            title="URLs",
            content="<p>Found URLs</p>",
            modules=[{"module_id": "tagged_urls", "rendered_html": "<ul></ul>"}],
        )
        assert section.title == "URLs"
        assert section.content == "<p>Found URLs</p>"
        assert len(section.modules) == 1


class TestTemplatesDir:
    """Test templates directory configuration."""

    def test_templates_dir_exists(self):
        """Test that templates directory exists."""
        assert TEMPLATES_DIR.exists()
        assert TEMPLATES_DIR.is_dir()

    def test_base_template_exists(self):
        """Test that base template file exists."""
        base_template = TEMPLATES_DIR / "base_report.html"
        assert base_template.exists()


class TestReportBuilder:
    """Test ReportBuilder class."""

    @pytest.fixture
    def db_conn(self):
        """Create in-memory database with required tables."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row

        # Create custom_report_sections table
        conn.execute("""
            CREATE TABLE custom_report_sections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                content TEXT,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            )
        """)

        # Create section_modules table
        conn.execute("""
            CREATE TABLE section_modules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                section_id INTEGER NOT NULL,
                module_id TEXT NOT NULL,
                config TEXT DEFAULT '{}',
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                FOREIGN KEY (section_id) REFERENCES custom_report_sections(id)
            )
        """)

        conn.commit()
        yield conn
        conn.close()

    def test_init(self, db_conn):
        """Test builder initialization."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        assert builder._evidence_id == 1
        assert builder._db_conn is db_conn

    def test_set_title(self, db_conn):
        """Test setting report title."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        result = builder.set_title("My Report")

        assert result is builder  # Method chaining
        assert builder._data.title == "My Report"

    def test_close_owned_db_connection_only_closes_owned_connections(self, db_conn):
        """Owned builder connections should close; borrowed ones should remain open."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.close_owned_db_connection()
        db_conn.execute("SELECT 1").fetchone()

        owned_conn = sqlite3.connect(":memory:")
        owned_builder = ReportBuilder(owned_conn, evidence_id=1).take_db_connection_ownership()
        owned_builder.close_owned_db_connection()

        with pytest.raises(sqlite3.ProgrammingError):
            owned_conn.execute("SELECT 1")

    def test_set_case_info(self, db_conn):
        """Test setting case metadata."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_case_info(
            case_number="2024-001",
            evidence_label="Evidence 1",
            investigator="Jane Doe",
            notes="Test notes",
        )

        assert builder._data.case_number == "2024-001"
        assert builder._data.evidence_label == "Evidence 1"
        assert builder._data.investigator == "Jane Doe"
        assert builder._data.notes == "Test notes"

    def test_set_author_info(self, db_conn):
        """Test setting author info for report footer."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        result = builder.set_author_info(
            function="Forensic Analyst",
            name="Max Mustermann",
            date="17.01.2026",
        )

        assert result is builder  # Method chaining
        assert builder._data.author_function == "Forensic Analyst"
        assert builder._data.author_name == "Max Mustermann"
        assert builder._data.author_date == "17.01.2026"

    def test_set_author_info_partial(self, db_conn):
        """Test setting only some author info fields."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_author_info(function="Analyst")

        assert builder._data.author_function == "Analyst"
        assert builder._data.author_name is None
        assert builder._data.author_date is None

    def test_add_section(self, db_conn):
        """Test adding section programmatically."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.add_section(
            title="Test Section",
            content="<p>Section content</p>",
        )

        assert len(builder._data.sections) == 1
        assert builder._data.sections[0].title == "Test Section"
        assert builder._data.sections[0].content == "<p>Section content</p>"

    def test_render_html_empty(self, db_conn):
        """Test rendering empty report."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("Empty Report")

        html = builder.render_html()

        assert "Empty Report" in html
        assert "No Content" in html or "No sections" in html.lower()

    def test_render_html_with_sections(self, db_conn):
        """Test rendering report with sections."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("Full Report")
        builder.set_case_info(case_number="2024-001")
        builder.add_section("Section 1", "<p>Content 1</p>")
        builder.add_section("Section 2", "<p>Content 2</p>")

        html = builder.render_html()

        assert "Full Report" in html
        assert "2024-001" in html
        assert "Section 1" in html
        assert "Section 2" in html
        assert "Content 1" in html
        assert "Content 2" in html

    def test_render_html_has_toc(self, db_conn):
        """Test that rendered HTML includes table of contents."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("Report with TOC")
        builder.add_section("Introduction", "Intro text")
        builder.add_section("Findings", "Findings text")

        html = builder.render_html()

        assert "Table of Contents" in html
        # TOC should link to sections
        assert 'href="#section-1"' in html
        assert 'href="#section-2"' in html

    def test_load_sections_from_db_empty(self, db_conn):
        """Test loading from empty database."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.load_sections_from_db()

        assert builder._data.sections == []

    def test_load_sections_from_db(self, db_conn):
        """Test loading sections from database."""
        # Insert a section
        now = datetime.now(timezone.utc).isoformat()
        db_conn.execute(
            """
            INSERT INTO custom_report_sections
            (evidence_id, title, content, sort_order, created_at_utc, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (1, "DB Section", "<p>From DB</p>", 0, now, now)
        )
        db_conn.commit()

        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.load_sections_from_db()

        assert len(builder._data.sections) == 1
        assert builder._data.sections[0].title == "DB Section"
        assert builder._data.sections[0].content == "<p>From DB</p>"

    def test_get_data(self, db_conn):
        """Test getting report data."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("Test")

        data = builder.get_data()

        assert isinstance(data, ReportData)
        assert data.title == "Test"

    def test_render_appendix_html_hides_toc_page_numbers_for_chromium(self, db_conn):
        """Chromium appendix HTML should drop TOC page counters."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder._data.appendix_modules.append(
            {"title": "Images", "rendered_html": "<p>Body</p>"}
        )

        html = builder.render_appendix_html(renderer="chromium")

        assert "target-counter(" not in html
        assert 'colspan="3"' in html

    def test_render_appendix_html_keeps_toc_page_numbers_for_weasyprint(self, db_conn):
        """WeasyPrint appendix HTML should keep TOC page counters."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder._data.appendix_modules.append(
            {"title": "Images", "rendered_html": "<p>Body</p>"}
        )

        html = builder.render_appendix_html(renderer="weasyprint")

        assert "target-counter(" in html
        assert 'class="toc-page-num"' in html

    def test_render_appendix_html_honors_hide_page_numbers_option(self, db_conn):
        """Appendix footer page numbers can be disabled in the template."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder._data.appendix_modules.append(
            {"title": "Images", "rendered_html": "<p>Body</p>"}
        )
        builder.set_appendix_options(hide_page_numbers=True)

        html = builder.render_appendix_html(renderer="chromium")

        assert "@bottom-center" not in html


class TestReportGenerator:
    """Test ReportGenerator class."""

    def test_init(self):
        """Test generator initialization."""
        generator = ReportGenerator()
        assert hasattr(generator, '_weasyprint_available')

    def test_can_generate_pdf(self):
        """Test PDF capability check."""
        generator = ReportGenerator()
        # Should be True if weasyprint is installed
        assert isinstance(generator.can_generate_pdf, bool)

    def test_plan_appendix_render_reprobes_each_time(self, monkeypatch):
        """Appendix render planning should emit a fresh probe on each call."""
        generator = ReportGenerator()
        probe_calls = {"count": 0}
        candidate = Path("/tmp/chromium")
        event = ExternalInvocation(
            task="chromium_probe",
            command="chromium --version",
            exit_code=0,
            stdout=f"Chromium {MIN_CHROMIUM_MARGIN_BOX_VERSION}.0.0.0",
            stderr=None,
        )
        binary = ChromiumBinary(
            executable=candidate,
            display_name="Chromium",
            major_version=MIN_CHROMIUM_MARGIN_BOX_VERSION,
        )
        monkeypatch.setattr(generator, "_find_chromium_candidates", lambda: [candidate])

        def fake_probe(path, **kwargs):
            probe_calls["count"] += 1
            return binary, event, None

        monkeypatch.setattr(generator, "_probe_chromium_binary", fake_probe)

        first = generator.plan_appendix_render()
        second = generator.plan_appendix_render()

        assert first.renderer == "chromium"
        assert first.audit_events == (event,)
        assert second.renderer == "chromium"
        assert second.audit_events == (event,)
        assert probe_calls["count"] == 2

    def test_preview_in_browser(self, tmp_path):
        """Test preview creates temp file and opens browser."""
        generator = ReportGenerator()
        generator.set_preview_root(tmp_path)
        html = "<html><body><h1>Test</h1></body></html>"

        with patch('webbrowser.open') as mock_open:
            result = generator.preview_in_browser(html)

            # Should return path to temp file
            assert isinstance(result, Path)
            assert result.exists()
            assert result.suffix == ".html"

            # Should open in browser
            mock_open.assert_called_once()
            call_args = mock_open.call_args[0][0]
            assert call_args.startswith("file://")

    def test_preview_in_browser_uses_distinct_temp_files(self, tmp_path):
        """Back-to-back previews should not overwrite the same temp file."""
        generator = ReportGenerator()
        generator.set_preview_root(tmp_path)

        with patch('webbrowser.open'):
            first = generator.preview_in_browser("<html><body>report</body></html>")
            second = generator.preview_in_browser("<html><body>appendix</body></html>")

        assert first != second
        assert first.read_text(encoding="utf-8") == "<html><body>report</body></html>"
        assert second.read_text(encoding="utf-8") == "<html><body>appendix</body></html>"

    def test_preview_in_browser_requires_workspace_root(self):
        """Preview output must be rooted in the case workspace."""
        generator = ReportGenerator()

        with pytest.raises(ValueError, match="Preview output directory is not configured"):
            generator.preview_in_browser("<html><body>report</body></html>")

    def test_run_external_command_calls_audit_hooks(self):
        """External command audit hooks should bracket subprocess execution."""
        generator = ReportGenerator()
        audit = {}

        def start_cb(task, command):
            audit["started"] = (task, command)
            return 42

        def finish_cb(token, event):
            audit["finished"] = (token, event.task, event.exit_code)

        event = generator._run_external_command(
            "chromium_probe",
            ["/bin/echo", "Chromium 131.0.0.0"],
            audit_start_cb=start_cb,
            audit_finish_cb=finish_cb,
        )

        assert event.exit_code == 0
        assert audit["started"][0] == "chromium_probe"
        assert audit["finished"] == (42, "chromium_probe", 0)

    def test_plan_appendix_render_prefers_supported_chromium(self, monkeypatch):
        """Supported Chromium should be preferred over WeasyPrint for appendices."""
        generator = ReportGenerator()
        candidate = Path("/tmp/chromium")
        event = ExternalInvocation(
            task="chromium_probe",
            command="chromium --version",
            exit_code=0,
            stdout=f"Chromium {MIN_CHROMIUM_MARGIN_BOX_VERSION}.0.0.0",
            stderr=None,
        )
        binary = ChromiumBinary(
            executable=candidate,
            display_name="Chromium",
            major_version=MIN_CHROMIUM_MARGIN_BOX_VERSION,
        )
        monkeypatch.setattr(generator, "_find_chromium_candidates", lambda: [candidate])
        monkeypatch.setattr(
            generator,
            "_probe_chromium_binary",
            lambda path, **kwargs: (binary, event, None),
        )

        plan = generator.plan_appendix_render()

        assert plan.renderer == "chromium"
        assert plan.chromium_binary == binary
        assert plan.note is None
        assert plan.audit_events == (event,)

    def test_plan_appendix_render_falls_back_to_weasyprint(self, monkeypatch):
        """Unsupported Chromium should produce a WeasyPrint fallback note."""
        generator = ReportGenerator()
        generator._weasyprint_available = True
        candidate = Path("/tmp/chromium")
        event = ExternalInvocation(
            task="chromium_probe",
            command="chromium --version",
            exit_code=0,
            stdout="Chromium 130.0.0.0",
            stderr=None,
        )
        monkeypatch.setattr(generator, "_find_chromium_candidates", lambda: [candidate])
        monkeypatch.setattr(
            generator,
            "_probe_chromium_binary",
            lambda path, **kwargs: (None, event, "Chromium 130 is too old"),
        )

        plan = generator.plan_appendix_render()

        assert plan.renderer == "weasyprint"
        assert "Chromium 130 is too old" in (plan.note or "")
        assert "WeasyPrint fallback" in (plan.note or "")
        assert plan.audit_events == (event,)

    def test_generate_appendix_pdf_with_chromium(self, tmp_path, monkeypatch):
        """Chromium appendix rendering should reuse the workspace HTML temp path."""
        generator = ReportGenerator()
        generator.set_preview_root(tmp_path)
        plan = AppendixRenderPlan(
            renderer="chromium",
            chromium_binary=ChromiumBinary(
                executable=Path("/usr/bin/chromium"),
                display_name="Chromium",
                major_version=MIN_CHROMIUM_MARGIN_BOX_VERSION,
            ),
        )

        def fake_run(task, command, timeout_s=30, **kwargs):
            for arg in command:
                if arg.startswith("--print-to-pdf="):
                    Path(arg.split("=", 1)[1]).write_bytes(b"%PDF-1.7 test")
            return ExternalInvocation(
                task=task,
                command=" ".join(command),
                exit_code=0,
                stdout=None,
                stderr=None,
            )

        monkeypatch.setattr(generator, "_run_external_command", fake_run)
        output_path = tmp_path / "appendix.pdf"

        result = generator.generate_appendix_pdf("<html><body>Appendix</body></html>", output_path, plan=plan)

        assert result.renderer == "chromium"
        assert result.used_fallback is False
        assert output_path.exists()
        html_files = list((tmp_path / "reports" / "previews").glob("appendix_render_*.html"))
        assert html_files == []
        assert len(result.audit_events) == 1

    def test_generate_appendix_pdf_includes_internally_computed_plan_audit_events(self, tmp_path, monkeypatch):
        """Implicit plan generation should preserve probe audit events."""
        generator = ReportGenerator()
        generator.set_preview_root(tmp_path)
        probe_event = ExternalInvocation(
            task="chromium_probe",
            command="chromium --version",
            exit_code=0,
            stdout=f"Chromium {MIN_CHROMIUM_MARGIN_BOX_VERSION}.0.0.0",
            stderr=None,
        )
        render_event = ExternalInvocation(
            task="chromium_appendix_pdf",
            command="chromium --headless",
            exit_code=0,
            stdout=None,
            stderr=None,
        )
        plan = AppendixRenderPlan(
            renderer="chromium",
            chromium_binary=ChromiumBinary(
                executable=Path("/usr/bin/chromium"),
                display_name="Chromium",
                major_version=MIN_CHROMIUM_MARGIN_BOX_VERSION,
            ),
            audit_events=(probe_event,),
        )

        def fake_plan(**kwargs):
            return plan

        def fake_run(task, command, timeout_s=30, **kwargs):
            for arg in command:
                if arg.startswith("--print-to-pdf="):
                    Path(arg.split("=", 1)[1]).write_bytes(b"%PDF-1.7 test")
            return render_event

        monkeypatch.setattr(generator, "plan_appendix_render", fake_plan)
        monkeypatch.setattr(generator, "_run_external_command", fake_run)

        result = generator.generate_appendix_pdf("<html><body>Appendix</body></html>", tmp_path / "appendix.pdf")

        assert result.audit_events == (probe_event, render_event)

    def test_generate_appendix_pdf_falls_back_after_chromium_failure(self, tmp_path, monkeypatch):
        """Chromium runtime failures should fall back to WeasyPrint when available."""
        generator = ReportGenerator()
        generator._weasyprint_available = True
        generator.set_preview_root(tmp_path)
        plan = AppendixRenderPlan(
            renderer="chromium",
            chromium_binary=ChromiumBinary(
                executable=Path("/usr/bin/chromium"),
                display_name="Chromium",
                major_version=MIN_CHROMIUM_MARGIN_BOX_VERSION,
            ),
        )

        def fake_run(task, command, timeout_s=30, **kwargs):
            return ExternalInvocation(
                task=task,
                command=" ".join(command),
                exit_code=1,
                stdout=None,
                stderr="render failed",
            )

        fallback_html = "<html><body>Fallback Appendix</body></html>"

        def fake_generate_pdf(html_content, output_path):
            assert html_content == fallback_html
            Path(output_path).write_bytes(b"%PDF-1.7 fallback")
            return True

        monkeypatch.setattr(generator, "_run_external_command", fake_run)
        monkeypatch.setattr(generator, "generate_pdf", fake_generate_pdf)
        output_path = tmp_path / "appendix.pdf"

        result = generator.generate_appendix_pdf(
            "<html><body>Appendix</body></html>",
            output_path,
            plan=plan,
            fallback_html_content=fallback_html,
        )

        assert result.renderer == "weasyprint"
        assert result.used_fallback is True
        assert "render failed" in (result.note or "")
        assert output_path.exists()
        assert len(result.audit_events) == 1

    def test_generate_appendix_pdf_raises_with_audit_events_when_fallback_fails(self, tmp_path, monkeypatch):
        """Chromium failure plus fallback failure should still preserve audit events."""
        generator = ReportGenerator()
        generator._weasyprint_available = True
        generator.set_preview_root(tmp_path)
        plan = AppendixRenderPlan(
            renderer="chromium",
            chromium_binary=ChromiumBinary(
                executable=Path("/usr/bin/chromium"),
                display_name="Chromium",
                major_version=MIN_CHROMIUM_MARGIN_BOX_VERSION,
            ),
        )
        render_event = ExternalInvocation(
            task="chromium_appendix_pdf",
            command="chromium --headless",
            exit_code=1,
            stdout=None,
            stderr="render failed",
        )

        monkeypatch.setattr(generator, "_run_external_command", lambda *args, **kwargs: render_event)

        def fake_generate_pdf(html_content, output_path):
            raise RuntimeError("fallback failed")

        monkeypatch.setattr(generator, "generate_pdf", fake_generate_pdf)

        with pytest.raises(Exception, match="fallback also failed") as exc_info:
            generator.generate_appendix_pdf("<html><body>Appendix</body></html>", tmp_path / "appendix.pdf", plan=plan)

        assert getattr(exc_info.value, "audit_events", ()) == (render_event,)

    @pytest.mark.skipif(
        not ReportGenerator()._weasyprint_available,
        reason="WeasyPrint not available"
    )
    def test_generate_pdf(self):
        """Test PDF generation."""
        generator = ReportGenerator()
        html = """
        <!DOCTYPE html>
        <html><head><title>Test</title></head>
        <body><h1>Test PDF</h1></body></html>
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_report.pdf"

            try:
                result = generator.generate_pdf(html, output_path)

                assert result is True
                assert output_path.exists()
                # PDF should have some content
                assert output_path.stat().st_size > 0
            except TypeError as e:
                # Known pydyf compatibility issue with some WeasyPrint versions
                if "PDF.__init__" in str(e):
                    pytest.skip("WeasyPrint/pydyf version incompatibility")
                raise

    @pytest.mark.skipif(
        not ReportGenerator()._weasyprint_available,
        reason="WeasyPrint not available"
    )
    def test_generate_pdf_creates_directory(self):
        """Test PDF generation creates parent directory."""
        generator = ReportGenerator()
        html = "<html><body>Test</body></html>"

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "subdir" / "test.pdf"

            try:
                generator.generate_pdf(html, output_path)
                assert output_path.exists()
            except TypeError as e:
                # Known pydyf compatibility issue
                if "PDF.__init__" in str(e):
                    pytest.skip("WeasyPrint/pydyf version incompatibility")
                raise

    def test_generate_pdf_raises_without_weasyprint(self):
        """Test PDF generation raises error without WeasyPrint."""
        generator = ReportGenerator()
        generator._weasyprint_available = False

        with pytest.raises(ImportError):
            generator.generate_pdf("<html></html>", "/tmp/test.pdf")


class TestBuildReportHelper:
    """Test the convenience build_report function."""

    @pytest.fixture
    def db_conn(self):
        """Create in-memory database with required tables."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE custom_report_sections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                content TEXT,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE section_modules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                section_id INTEGER NOT NULL,
                module_id TEXT NOT NULL,
                config TEXT DEFAULT '{}',
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            )
        """)
        conn.commit()
        yield conn
        conn.close()

    def test_build_report(self, db_conn):
        """Test build_report convenience function."""
        html = build_report(
            db_conn,
            evidence_id=1,
            title="Quick Report",
            case_number="2024-123",
        )

        assert "Quick Report" in html
        assert "2024-123" in html


class TestTemplateRendering:
    """Test template rendering features."""

    @pytest.fixture
    def db_conn(self):
        """Create in-memory database."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE custom_report_sections (
                id INTEGER PRIMARY KEY, evidence_id INTEGER,
                title TEXT, content TEXT, sort_order INTEGER,
                created_at_utc TEXT, updated_at_utc TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE section_modules (
                id INTEGER PRIMARY KEY, section_id INTEGER,
                module_id TEXT, config TEXT, sort_order INTEGER,
                created_at_utc TEXT, updated_at_utc TEXT
            )
        """)
        conn.commit()
        yield conn
        conn.close()

    def test_html_escaping(self, db_conn):
        """Test that HTML in content is properly escaped when needed."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        # Content with safe filter should not be escaped
        builder.add_section("Test", "<p>Safe HTML</p>")

        html = builder.render_html()

        # The <p> tag should be preserved (not escaped)
        assert "<p>Safe HTML</p>" in html

    def test_special_chars_in_title(self, db_conn):
        """Test special characters in title are escaped."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("Report: Test & Analysis <2024>")

        html = builder.render_html()

        # Title should be escaped in HTML context
        assert "Report: Test &amp; Analysis &lt;2024&gt;" in html

    def test_metadata_in_output(self, db_conn):
        """Test all metadata appears in output."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("Full Metadata Test")
        builder.set_case_info(
            case_number="CASE-001",
            evidence_label="Evidence A",
            investigator="Detective Smith",
            notes="Important investigation",
        )

        html = builder.render_html()

        assert "CASE-001" in html
        assert "Evidence A" in html
        assert "Detective Smith" in html
        assert "Important investigation" in html

    def test_section_numbering(self, db_conn):
        """Test sections are numbered correctly."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("Numbered Report")
        builder.add_section("First", "1st content")
        builder.add_section("Second", "2nd content")
        builder.add_section("Third", "3rd content")

        html = builder.render_html()

        # Sections should be numbered 1, 2, 3
        assert "1. First" in html
        assert "2. Second" in html
        assert "3. Third" in html

    def test_section_ids_for_toc(self, db_conn):
        """Test sections have proper IDs for TOC links."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.add_section("Section A", "")
        builder.add_section("Section B", "")

        html = builder.render_html()

        assert 'id="section-1"' in html
        assert 'id="section-2"' in html

    def test_author_section_in_output(self, db_conn):
        """Test author/signature section appears in output."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("Report with Author")
        builder.set_author_info(
            function="Forensic Analyst",
            name="Max Mustermann",
            date="17.01.2026",
        )
        builder.add_section("Findings", "Test findings")

        html = builder.render_html()

        assert "Report Created By" in html
        assert "Forensic Analyst" in html
        assert "Max Mustermann" in html
        assert "17.01.2026" in html

    def test_author_section_partial(self, db_conn):
        """Test author section with only some fields."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("Partial Author Report")
        builder.set_author_info(function="Analyst")

        html = builder.render_html()

        assert "Report Created By" in html
        assert "Analyst" in html
        # Name and Date fields should not appear if not set
        # (only Function row should be present)

    def test_author_section_not_shown_when_empty(self, db_conn):
        """Test author section is not shown when no author info."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("No Author Report")

        html = builder.render_html()

        # Author section should not appear
        assert "Report Created By" not in html


class TestReportMode:
    """Test ReportMode enum and mode-aware rendering."""

    def test_enum_values(self):
        """Test ReportMode enum has expected members."""
        assert ReportMode.COMPLETE.value == "complete"
        assert ReportMode.REPORT_ONLY.value == "report_only"
        assert ReportMode.APPENDIX_ONLY.value == "appendix_only"

    @pytest.fixture
    def db_conn(self):
        """Create in-memory database with required tables."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE custom_report_sections (
                id INTEGER PRIMARY KEY, evidence_id INTEGER,
                title TEXT, content TEXT, sort_order INTEGER,
                created_at_utc TEXT, updated_at_utc TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE section_modules (
                id INTEGER PRIMARY KEY, section_id INTEGER,
                module_id TEXT, config TEXT, sort_order INTEGER,
                created_at_utc TEXT, updated_at_utc TEXT
            )
        """)
        conn.commit()
        yield conn
        conn.close()

    def test_render_report_only_excludes_appendix(self, db_conn):
        """Test REPORT_ONLY mode does not include appendix block."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("Report Only Test")
        builder.add_section("Findings", "<p>Some findings</p>")
        # Manually add appendix data to verify it is excluded
        builder._data.appendix_modules.append({
            "module_id": "test_appendix",
            "config": {},
            "title": "Appendix Module A",
            "rendered_html": "<p>Appendix content</p>",
        })

        html = builder.render_html(ReportMode.REPORT_ONLY)

        assert isinstance(html, str)
        assert "Findings" in html
        # The appendix content block should NOT be rendered
        assert "Appendix content" not in html
        assert "Appendix Module A" not in html

    def test_render_appendix_only(self, db_conn):
        """Test APPENDIX_ONLY mode renders a standalone appendix document."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("My Report")
        builder._data.appendix_modules.append({
            "module_id": "test_appendix",
            "config": {},
            "title": "URL List",
            "rendered_html": "<p>URLs here</p>",
        })

        html = builder.render_html(ReportMode.APPENDIX_ONLY)

        assert isinstance(html, str)
        # Should have appendix in the title
        assert "Appendix" in html
        assert "URL List" in html
        assert "URLs here" in html
        # Should NOT have report sections or author signature
        assert "Report Created By" not in html

    def test_render_complete_returns_tuple(self, db_conn):
        """Test COMPLETE mode returns a tuple of two HTML strings."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("Complete Report")
        builder.add_section("Section 1", "<p>Content</p>")
        builder._data.appendix_modules.append({
            "module_id": "test",
            "config": {},
            "title": "Appendix A",
            "rendered_html": "<p>Appendix data</p>",
        })

        result = builder.render_html(ReportMode.COMPLETE)

        assert isinstance(result, tuple)
        assert len(result) == 2
        report_html, appendix_html = result
        assert isinstance(report_html, str)
        assert isinstance(appendix_html, str)
        # Report should have sections but no appendix content
        assert "Section 1" in report_html
        assert "Appendix data" not in report_html
        assert "Appendix A" not in report_html
        # Appendix should have appendix content
        assert "Appendix A" in appendix_html
        assert "Appendix data" in appendix_html

    def test_render_report_html_with_appendix_flag(self, db_conn):
        """Test render_report_html with include_appendix=True (legacy)."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("Legacy Complete")
        builder.add_section("Findings", "<p>Findings</p>")
        builder._data.appendix_modules.append({
            "module_id": "test",
            "config": {},
            "title": "Appendix B",
            "rendered_html": "<p>Data B</p>",
        })

        html = builder.render_report_html(include_appendix=True)

        assert "Findings" in html
        assert "appendix-section" in html
        assert "Appendix B" in html

    def test_report_only_has_no_appendix_in_toc(self, db_conn):
        """Test REPORT_ONLY mode TOC does not reference appendix."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("TOC Test")
        builder.add_section("Section A", "")
        builder._data.appendix_modules.append({
            "module_id": "test",
            "config": {},
            "title": "Appendix Z",
            "rendered_html": "<p>Z</p>",
        })

        html = builder.render_html(ReportMode.REPORT_ONLY)

        assert 'href="#section-1"' in html
        assert 'href="#appendix"' not in html

    def test_appendix_template_exists(self):
        """Test that the appendix template file exists."""
        appendix_template = TEMPLATES_DIR / "appendix_report.html"
        assert appendix_template.exists()

    def test_appendix_template_has_appendix_page_footer(self, db_conn):
        """Test appendix template renders with appendix-prefixed page counter."""
        builder = ReportBuilder(db_conn, evidence_id=1)
        builder.set_title("Footer Test")

        html = builder.render_appendix_html()

        # The CSS should contain the appendix-prefixed counter string
        # e.g. "Appendix — Page " counter(page) ...
        assert "Appendix" in html


class TestReportModeWithBuildReport:
    """Test build_report convenience function with modes."""

    @pytest.fixture
    def db_conn(self):
        """Create in-memory database with required tables."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE custom_report_sections (
                id INTEGER PRIMARY KEY, evidence_id INTEGER,
                title TEXT, content TEXT, sort_order INTEGER,
                created_at_utc TEXT, updated_at_utc TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE section_modules (
                id INTEGER PRIMARY KEY, section_id INTEGER,
                module_id TEXT, config TEXT, sort_order INTEGER,
                created_at_utc TEXT, updated_at_utc TEXT
            )
        """)
        conn.commit()
        yield conn
        conn.close()

    def test_build_report_default_mode(self, db_conn):
        """Test build_report defaults to REPORT_ONLY."""
        html = build_report(db_conn, evidence_id=1, title="Default Mode")
        assert isinstance(html, str)
        assert "Default Mode" in html


class TestGeneratePdfPair:
    """Test ReportGenerator.generate_pdf_pair method."""

    @pytest.mark.skipif(
        not ReportGenerator()._weasyprint_available,
        reason="WeasyPrint not available",
    )
    def test_generate_pdf_pair(self):
        """Test generating two PDFs at once."""
        generator = ReportGenerator()
        report_html = "<html><body><h1>Report</h1></body></html>"
        appendix_html = "<html><body><h1>Appendix</h1></body></html>"

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "report.pdf"
            appendix_path = Path(tmpdir) / "report_Appendix.pdf"

            try:
                r_ok, a_ok = generator.generate_pdf_pair(
                    report_html, appendix_html, report_path, appendix_path,
                )
                assert r_ok is True
                assert a_ok is True
                assert report_path.exists()
                assert appendix_path.exists()
            except TypeError as e:
                if "PDF.__init__" in str(e):
                    pytest.skip("WeasyPrint/pydyf version incompatibility")
                raise
