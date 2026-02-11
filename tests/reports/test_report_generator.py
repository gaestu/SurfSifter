"""Tests for report generation (builder, generator, templates)."""

import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from reports.generator import (
    ReportBuilder,
    ReportGenerator,
    ReportData,
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

    def test_preview_in_browser(self):
        """Test preview creates temp file and opens browser."""
        generator = ReportGenerator()
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
