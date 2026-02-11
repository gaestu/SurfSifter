"""Tests for report settings persistence."""

import json
import sqlite3
import pytest

from reports.database.settings_helpers import (
    get_report_settings,
    save_report_settings,
    delete_report_settings,
)


@pytest.fixture
def db_conn():
    """Create an in-memory database connection."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


class TestReportSettingsDataclass:
    """Tests for ReportSettings dataclass in app settings."""

    def test_app_settings_includes_reports(self):
        """Test AppSettings includes reports settings."""
        from app.config.settings import AppSettings, ReportSettings

        settings = AppSettings()

        assert hasattr(settings, "reports")
        assert isinstance(settings.reports, ReportSettings)

    def test_report_settings_defaults(self):
        """Test ReportSettings has correct defaults."""
        from app.config.settings import ReportSettings

        settings = ReportSettings()

        assert settings.default_author_function == ""
        assert settings.default_author_name == ""
        assert settings.default_org_name == ""
        assert settings.default_footer_text == ""
        assert settings.default_logo_path == ""
        assert settings.default_locale == "en"
        assert settings.default_date_format == "eu"

    def test_app_settings_save_load_reports(self, tmp_path):
        """Test report settings persist through save/load cycle."""
        from app.config.settings import AppSettings, ReportSettings

        settings = AppSettings()
        settings.reports.default_author_function = "Forensic Analyst"
        settings.reports.default_author_name = "Max Mustermann"
        settings.reports.default_org_name = "Test Organization"
        settings.reports.default_footer_text = "Confidential"
        settings.reports.default_logo_path = "branding/logo.png"
        settings.reports.default_locale = "de"
        settings.reports.default_date_format = "us"

        settings_file = tmp_path / "settings.json"
        settings.save(settings_file)

        loaded = AppSettings.load(settings_file)

        assert loaded.reports.default_author_function == "Forensic Analyst"
        assert loaded.reports.default_author_name == "Max Mustermann"
        assert loaded.reports.default_org_name == "Test Organization"
        assert loaded.reports.default_footer_text == "Confidential"
        assert loaded.reports.default_logo_path == "branding/logo.png"
        assert loaded.reports.default_locale == "de"
        assert loaded.reports.default_date_format == "us"

    def test_app_settings_load_without_reports_uses_defaults(self, tmp_path):
        """Test loading old config without reports section uses defaults."""
        # Simulate old config without reports
        old_config = {
            "general": {"thumbnail_size": 180},
            "tools": {},
            "network": {},
            "hash": {},
        }

        settings_file = tmp_path / "settings.json"
        settings_file.write_text(json.dumps(old_config))

        from app.config.settings import AppSettings
        loaded = AppSettings.load(settings_file)

        # Should have default report settings
        assert loaded.reports.default_author_function == ""
        assert loaded.reports.default_locale == "en"
        assert loaded.reports.default_date_format == "eu"

    def test_settings_json_structure(self, tmp_path):
        """Test saved settings.json has correct structure."""
        from app.config.settings import AppSettings

        settings = AppSettings()
        settings.reports.default_org_name = "Test Org"

        settings_file = tmp_path / "settings.json"
        settings.save(settings_file)

        data = json.loads(settings_file.read_text())

        assert "reports" in data
        assert data["reports"]["default_org_name"] == "Test Org"
        assert data["reports"]["default_locale"] == "en"


class TestGetReportSettings:
    """Test get_report_settings function."""

    def test_returns_none_for_missing_settings(self, db_conn):
        """Returns None when no settings exist for evidence."""
        result = get_report_settings(db_conn, evidence_id=1)
        assert result is None

    def test_returns_settings_dict(self, db_conn):
        """Returns settings dict when settings exist."""
        save_report_settings(
            db_conn,
            evidence_id=1,
            author_name="Test Author",
            locale="de",
        )

        result = get_report_settings(db_conn, evidence_id=1)
        assert result is not None
        assert result["author_name"] == "Test Author"
        assert result["locale"] == "de"

    def test_settings_scoped_to_evidence(self, db_conn):
        """Settings are scoped per-evidence."""
        save_report_settings(db_conn, evidence_id=1, author_name="Author 1")
        save_report_settings(db_conn, evidence_id=2, author_name="Author 2")

        result1 = get_report_settings(db_conn, evidence_id=1)
        result2 = get_report_settings(db_conn, evidence_id=2)

        assert result1["author_name"] == "Author 1"
        assert result2["author_name"] == "Author 2"


class TestSaveReportSettings:
    """Test save_report_settings function."""

    def test_saves_author_info(self, db_conn):
        """Saves author function, name, and date."""
        save_report_settings(
            db_conn,
            evidence_id=1,
            author_function="Forensic Analyst",
            author_name="John Doe",
            author_date="22.01.2026",
        )

        result = get_report_settings(db_conn, evidence_id=1)
        assert result["author_function"] == "Forensic Analyst"
        assert result["author_name"] == "John Doe"
        assert result["author_date"] == "22.01.2026"

    def test_saves_branding_info(self, db_conn):
        """Saves branding org name, footer, and logo path."""
        save_report_settings(
            db_conn,
            evidence_id=1,
            branding_org_name="Test Org",
            branding_footer_text="Confidential",
            branding_logo_path="reports/assets/logo.png",
        )

        result = get_report_settings(db_conn, evidence_id=1)
        assert result["branding_org_name"] == "Test Org"
        assert result["branding_footer_text"] == "Confidential"
        assert result["branding_logo_path"] == "reports/assets/logo.png"

    def test_saves_preferences(self, db_conn):
        """Saves locale and date format preferences."""
        save_report_settings(
            db_conn,
            evidence_id=1,
            locale="de",
            date_format="us",
        )

        result = get_report_settings(db_conn, evidence_id=1)
        assert result["locale"] == "de"
        assert result["date_format"] == "us"

    def test_defaults_for_locale_and_date_format(self, db_conn):
        """Defaults to 'en' locale and 'eu' date format."""
        save_report_settings(db_conn, evidence_id=1)

        result = get_report_settings(db_conn, evidence_id=1)
        assert result["locale"] == "en"
        assert result["date_format"] == "eu"

    def test_upsert_overwrites_existing(self, db_conn):
        """Save overwrites existing settings (upsert behavior)."""
        save_report_settings(db_conn, evidence_id=1, author_name="First")
        save_report_settings(db_conn, evidence_id=1, author_name="Second")

        result = get_report_settings(db_conn, evidence_id=1)
        assert result["author_name"] == "Second"

    def test_empty_strings_stored_as_none(self, db_conn):
        """Empty strings are stored as NULL."""
        save_report_settings(
            db_conn,
            evidence_id=1,
            author_name="",
            branding_org_name="",
        )

        result = get_report_settings(db_conn, evidence_id=1)
        assert result["author_name"] is None
        assert result["branding_org_name"] is None

    def test_sets_updated_at_utc(self, db_conn):
        """Sets updated_at_utc timestamp."""
        save_report_settings(db_conn, evidence_id=1)

        result = get_report_settings(db_conn, evidence_id=1)
        assert result["updated_at_utc"] is not None
        assert "T" in result["updated_at_utc"]  # ISO format

    def test_saves_collapsed_states(self, db_conn):
        """Saves collapsed state for each section."""
        save_report_settings(
            db_conn,
            evidence_id=1,
            collapsed_title=True,
            collapsed_author=False,
            collapsed_branding=True,
            collapsed_appendix=False,
        )

        result = get_report_settings(db_conn, evidence_id=1)
        assert result["collapsed_title"] is True
        assert result["collapsed_author"] is False
        assert result["collapsed_branding"] is True
        assert result["collapsed_appendix"] is False

    def test_defaults_for_collapsed_states(self, db_conn):
        """Defaults: title expanded, others collapsed."""
        save_report_settings(db_conn, evidence_id=1)

        result = get_report_settings(db_conn, evidence_id=1)
        assert result["collapsed_title"] is False  # expanded by default
        assert result["collapsed_author"] is True  # collapsed by default
        assert result["collapsed_branding"] is True  # collapsed by default
        assert result["collapsed_appendix"] is True  # collapsed by default

    def test_collapsed_states_are_booleans(self, db_conn):
        """Collapsed states are returned as Python booleans."""
        save_report_settings(
            db_conn,
            evidence_id=1,
            collapsed_title=True,
            collapsed_author=False,
        )

        result = get_report_settings(db_conn, evidence_id=1)
        assert isinstance(result["collapsed_title"], bool)
        assert isinstance(result["collapsed_author"], bool)


class TestDeleteReportSettings:
    """Test delete_report_settings function."""

    def test_deletes_existing_settings(self, db_conn):
        """Deletes settings and returns True."""
        save_report_settings(db_conn, evidence_id=1, author_name="Test")

        result = delete_report_settings(db_conn, evidence_id=1)

        assert result is True
        assert get_report_settings(db_conn, evidence_id=1) is None

    def test_returns_false_for_missing(self, db_conn):
        """Returns False when no settings exist."""
        result = delete_report_settings(db_conn, evidence_id=999)
        assert result is False

    def test_only_deletes_specified_evidence(self, db_conn):
        """Only deletes settings for specified evidence."""
        save_report_settings(db_conn, evidence_id=1, author_name="Keep")
        save_report_settings(db_conn, evidence_id=2, author_name="Delete")

        delete_report_settings(db_conn, evidence_id=2)

        assert get_report_settings(db_conn, evidence_id=1) is not None
        assert get_report_settings(db_conn, evidence_id=2) is None


class TestTableCreation:
    """Test automatic table creation."""

    def test_creates_table_on_get(self, db_conn):
        """Table is created when get_report_settings is called."""
        get_report_settings(db_conn, evidence_id=1)

        # Check table exists
        cursor = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='report_settings'"
        )
        assert cursor.fetchone() is not None

    def test_creates_table_on_save(self, db_conn):
        """Table is created when save_report_settings is called."""
        save_report_settings(db_conn, evidence_id=1)

        # Check table exists
        cursor = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='report_settings'"
        )
        assert cursor.fetchone() is not None

    def test_idempotent_table_creation(self, db_conn):
        """Multiple calls don't fail due to table already existing."""
        get_report_settings(db_conn, evidence_id=1)
        get_report_settings(db_conn, evidence_id=2)
        save_report_settings(db_conn, evidence_id=1)
        save_report_settings(db_conn, evidence_id=1)
        # Should not raise

    def test_migration_adds_collapsed_columns(self, db_conn):
        """Migration adds collapsed columns to existing table without them."""
        # Create old table schema without collapsed columns
        db_conn.execute("""
            CREATE TABLE report_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                evidence_id INTEGER NOT NULL UNIQUE,
                author_function TEXT,
                author_name TEXT,
                author_date TEXT,
                branding_org_name TEXT,
                branding_footer_text TEXT,
                branding_logo_path TEXT,
                locale TEXT NOT NULL DEFAULT 'en',
                date_format TEXT NOT NULL DEFAULT 'eu',
                updated_at_utc TEXT NOT NULL
            )
        """)
        db_conn.commit()

        # Insert a row with old schema
        db_conn.execute("""
            INSERT INTO report_settings (evidence_id, author_name, updated_at_utc)
            VALUES (1, 'Test', '2026-01-22T12:00:00Z')
        """)
        db_conn.commit()

        # Now call get_report_settings - should trigger migration
        result = get_report_settings(db_conn, evidence_id=1)

        # Should have default collapsed values
        assert result is not None
        assert result["collapsed_title"] is False  # default 0
        assert result["collapsed_author"] is True  # default 1
        assert result["collapsed_branding"] is True  # default 1
        assert result["collapsed_appendix"] is True  # default 1
        assert result["author_name"] == "Test"  # existing data preserved


class TestReportTabWidgetDefaults:
    """Tests for global default settings in ReportTabWidget."""

    @pytest.mark.gui_offscreen
    def test_set_default_settings_stores_values(self, qtbot):
        """Test set_default_settings stores values correctly."""
        from reports.ui.report_tab_widget import ReportTabWidget

        widget = ReportTabWidget()
        qtbot.addWidget(widget)

        defaults = {
            "default_author_function": "Forensiker",
            "default_author_name": "Test User",
            "default_org_name": "Test Org",
            "default_footer_text": "Footer",
            "default_logo_path": "branding/logo.png",
            "default_locale": "de",
            "default_date_format": "us",
        }

        widget.set_default_settings(defaults, None)

        assert widget._default_settings == defaults

    @pytest.mark.gui_offscreen
    def test_apply_global_defaults_fills_fields(self, qtbot):
        """Test _apply_global_defaults populates UI fields."""
        from reports.ui.report_tab_widget import ReportTabWidget

        widget = ReportTabWidget()
        qtbot.addWidget(widget)

        defaults = {
            "default_author_function": "Senior Analyst",
            "default_author_name": "Jane Doe",
            "default_org_name": "ACME Corp",
            "default_footer_text": "Confidential",
            "default_locale": "de",
            "default_date_format": "us",
        }

        widget.set_default_settings(defaults, None)
        widget._loading_settings = True  # Prevent auto-save
        widget._apply_global_defaults()
        widget._loading_settings = False

        assert widget._author_function_input.text() == "Senior Analyst"
        assert widget._author_name_input.text() == "Jane Doe"
        assert widget._branding_org_input.text() == "ACME Corp"
        assert widget._branding_footer_input.text() == "Confidential"
        assert widget._locale_combo.currentData() == "de"
        assert widget._date_format_combo.currentData() == "us"

    @pytest.mark.gui_offscreen
    def test_apply_global_defaults_with_logo_path(self, qtbot, tmp_path):
        """Test _apply_global_defaults resolves logo path from config dir."""
        from reports.ui.report_tab_widget import ReportTabWidget

        widget = ReportTabWidget()
        qtbot.addWidget(widget)

        # Create a logo file
        branding_dir = tmp_path / "branding"
        branding_dir.mkdir()
        logo_file = branding_dir / "logo.png"
        logo_file.write_bytes(b"fake png")

        defaults = {
            "default_logo_path": "branding/logo.png",
        }

        widget.set_default_settings(defaults, tmp_path)
        widget._loading_settings = True
        widget._apply_global_defaults()
        widget._loading_settings = False

        assert widget._branding_logo_input.text() == str(logo_file)

    @pytest.mark.gui_offscreen
    def test_apply_global_defaults_skips_missing_logo(self, qtbot, tmp_path):
        """Test _apply_global_defaults skips logo if file doesn't exist."""
        from reports.ui.report_tab_widget import ReportTabWidget

        widget = ReportTabWidget()
        qtbot.addWidget(widget)

        defaults = {
            "default_logo_path": "branding/missing.png",
        }

        widget.set_default_settings(defaults, tmp_path)
        widget._loading_settings = True
        widget._apply_global_defaults()
        widget._loading_settings = False

        # Logo field should remain empty
        assert widget._branding_logo_input.text() == ""

    @pytest.mark.gui_offscreen
    def test_apply_global_defaults_without_settings(self, qtbot):
        """Test _apply_global_defaults does nothing without settings."""
        from reports.ui.report_tab_widget import ReportTabWidget

        widget = ReportTabWidget()
        qtbot.addWidget(widget)

        # Get initial values
        initial_function = widget._author_function_input.text()

        # Apply with no defaults set
        widget._loading_settings = True
        widget._apply_global_defaults()
        widget._loading_settings = False

        # Should not change
        assert widget._author_function_input.text() == initial_function
