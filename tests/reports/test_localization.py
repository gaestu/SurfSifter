"""Tests for report localization and branding support."""

import pytest

from reports.locales import (
    TRANSLATIONS,
    SUPPORTED_LOCALES,
    DEFAULT_LOCALE,
    LOCALE_NAMES,
    get_translations,
    get_field_label,
    get_locale_name,
)
from reports.generator import ReportBuilder, ReportData


class TestTranslations:
    """Test the TRANSLATIONS dictionary and helper functions."""

    def test_supported_locales_contains_expected(self):
        """English and German should be supported."""
        assert "en" in SUPPORTED_LOCALES
        assert "de" in SUPPORTED_LOCALES

    def test_default_locale_is_english(self):
        """Default locale should be English."""
        assert DEFAULT_LOCALE == "en"

    def test_locale_names_defined(self):
        """All supported locales should have display names."""
        for locale in SUPPORTED_LOCALES:
            assert locale in LOCALE_NAMES

    def test_english_translations_exist(self):
        """English translations dictionary should exist and have keys."""
        assert "en" in TRANSLATIONS
        en = TRANSLATIONS["en"]
        assert len(en) > 0
        # Test some core keys
        assert "toc_title" in en
        assert "case_number" in en
        assert "appendix" in en

    def test_german_translations_exist(self):
        """German translations dictionary should exist and have keys."""
        assert "de" in TRANSLATIONS
        de = TRANSLATIONS["de"]
        assert len(de) > 0
        # Test some core keys
        assert "toc_title" in de
        assert "case_number" in de
        assert "appendix" in de

    def test_german_has_all_english_keys(self):
        """German should have all keys that English has."""
        en_keys = set(TRANSLATIONS["en"].keys())
        de_keys = set(TRANSLATIONS["de"].keys())
        missing = en_keys - de_keys
        assert missing == set(), f"German missing keys: {missing}"

    def test_translations_are_different(self):
        """German and English translations should be different for key items."""
        en = TRANSLATIONS["en"]
        de = TRANSLATIONS["de"]
        # These should definitely be different
        assert en["toc_title"] != de["toc_title"]
        assert en["appendix"] != de["appendix"]
        assert en["case_number"] != de["case_number"]


class TestGetTranslations:
    """Test the get_translations() helper function."""

    def test_get_english_translations(self):
        """get_translations('en') returns English dict."""
        t = get_translations("en")
        assert isinstance(t, dict)
        assert t.get("toc_title") == "Table of Contents"

    def test_get_german_translations(self):
        """get_translations('de') returns German dict."""
        t = get_translations("de")
        assert isinstance(t, dict)
        assert t.get("toc_title") == "Inhaltsverzeichnis"

    def test_unknown_locale_returns_english(self):
        """Unknown locale falls back to English."""
        t = get_translations("fr")
        assert t == TRANSLATIONS["en"]

    def test_none_locale_returns_english(self):
        """None locale falls back to English."""
        t = get_translations(None)
        assert t == TRANSLATIONS["en"]


class TestGetFieldLabel:
    """Test the get_field_label() helper function."""

    def test_english_field_label(self):
        """get_field_label returns English label for known indicator."""
        label = get_field_label("system:os_version", "en")
        assert label == "OS Version"

    def test_german_field_label(self):
        """get_field_label returns German label for known indicator."""
        label = get_field_label("system:os_version", "de")
        assert label == "Betriebssystemversion"

    def test_unknown_indicator_returns_title(self):
        """Unknown indicator returns title-cased version of type."""
        label = get_field_label("unknown:mystery", "en")
        assert label == "Mystery"


class TestGetLocaleName:
    """Test the get_locale_name() helper function."""

    def test_english_display_name(self):
        """English locale should display as 'English'."""
        assert get_locale_name("en") == "English"

    def test_german_display_name(self):
        """German locale should display as 'Deutsch'."""
        assert get_locale_name("de") == "Deutsch"

    def test_unknown_returns_code(self):
        """Unknown locale returns the code itself."""
        assert get_locale_name("xy") == "xy"


class TestReportBuilderLocale:
    """Test ReportBuilder locale support."""

    @pytest.fixture
    def mock_db(self, tmp_path):
        """Create a minimal in-memory database for testing."""
        import sqlite3
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        # Create minimal tables
        db.execute("""
            CREATE TABLE custom_sections (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                title TEXT,
                content TEXT,
                sort_order INTEGER
            )
        """)
        db.execute("""
            CREATE TABLE section_modules (
                id INTEGER PRIMARY KEY,
                section_id INTEGER,
                module_id TEXT,
                config TEXT,
                sort_order INTEGER
            )
        """)
        db.execute("""
            CREATE TABLE appendix_modules (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                module_id TEXT,
                config TEXT,
                sort_order INTEGER
            )
        """)
        return db

    def test_builder_default_locale(self, mock_db):
        """ReportBuilder defaults to English locale."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        # Check internal state
        assert builder._locale == "en"

    def test_builder_set_locale_constructor(self, mock_db):
        """ReportBuilder accepts locale in constructor."""
        builder = ReportBuilder(mock_db, evidence_id=1, locale="de")
        assert builder._locale == "de"

    def test_builder_set_locale_method(self, mock_db):
        """ReportBuilder.set_locale() updates locale."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        builder.set_locale("de")
        assert builder._locale == "de"

    def test_builder_set_locale_returns_self(self, mock_db):
        """set_locale() returns self for method chaining."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        result = builder.set_locale("de")
        assert result is builder


class TestReportBuilderDateFormat:
    """Test ReportBuilder date format support."""

    @pytest.fixture
    def mock_db(self):
        """Create a minimal in-memory database for testing."""
        import sqlite3
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        db.execute("""
            CREATE TABLE custom_sections (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                title TEXT,
                content TEXT,
                sort_order INTEGER
            )
        """)
        db.execute("""
            CREATE TABLE section_modules (
                id INTEGER PRIMARY KEY,
                section_id INTEGER,
                module_id TEXT,
                config TEXT,
                sort_order INTEGER
            )
        """)
        db.execute("""
            CREATE TABLE appendix_modules (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                module_id TEXT,
                config TEXT,
                sort_order INTEGER
            )
        """)
        return db

    def test_set_date_format_eu(self, mock_db):
        """set_date_format() sets EU format."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        builder.set_date_format("eu")
        assert builder._data.date_format == "eu"

    def test_set_date_format_us(self, mock_db):
        """set_date_format() sets US format."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        builder.set_date_format("us")
        assert builder._data.date_format == "us"

    def test_set_date_format_returns_self(self, mock_db):
        """set_date_format() returns self for method chaining."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        result = builder.set_date_format("eu")
        assert result is builder

    def test_format_author_date_eu_from_qt(self, mock_db):
        """_format_author_date formats Qt date (dd.MM.yyyy) to EU format."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        builder.set_date_format("eu")
        builder._data.author_date = "22.01.2026"  # Qt format output
        assert builder._format_author_date() == "22.01.2026"

    def test_format_author_date_us_from_qt(self, mock_db):
        """_format_author_date converts Qt date to US format."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        builder.set_date_format("us")
        builder._data.author_date = "22.01.2026"  # Qt format output
        assert builder._format_author_date() == "01/22/2026"

    def test_format_author_date_eu_from_iso(self, mock_db):
        """_format_author_date converts ISO date to EU format."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        builder.set_date_format("eu")
        builder._data.author_date = "2026-01-22"
        assert builder._format_author_date() == "22.01.2026"

    def test_format_author_date_us_from_iso(self, mock_db):
        """_format_author_date converts ISO date to US format."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        builder.set_date_format("us")
        builder._data.author_date = "2026-01-22"
        assert builder._format_author_date() == "01/22/2026"

    def test_format_author_date_empty(self, mock_db):
        """_format_author_date returns empty string for empty date."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        builder._data.author_date = ""
        assert builder._format_author_date() == ""

    def test_format_author_date_none(self, mock_db):
        """_format_author_date returns empty string for None date."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        builder._data.author_date = None
        assert builder._format_author_date() == ""


class TestReportBuilderBranding:
    """Test ReportBuilder branding support."""

    @pytest.fixture
    def mock_db(self):
        """Create a minimal in-memory database for testing."""
        import sqlite3
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        db.execute("""
            CREATE TABLE custom_sections (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                title TEXT,
                content TEXT,
                sort_order INTEGER
            )
        """)
        db.execute("""
            CREATE TABLE section_modules (
                id INTEGER PRIMARY KEY,
                section_id INTEGER,
                module_id TEXT,
                config TEXT,
                sort_order INTEGER
            )
        """)
        db.execute("""
            CREATE TABLE appendix_modules (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                module_id TEXT,
                config TEXT,
                sort_order INTEGER
            )
        """)
        return db

    def test_branding_defaults_none(self, mock_db):
        """ReportBuilder branding fields default to None."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        assert builder._data.branding_org_name is None
        assert builder._data.branding_footer_text is None
        assert builder._data.branding_logo_path is None

    def test_set_branding_org_name(self, mock_db):
        """set_branding() sets org_name."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        builder.set_branding(org_name="Test Org")
        assert builder._data.branding_org_name == "Test Org"

    def test_set_branding_footer_text(self, mock_db):
        """set_branding() sets footer_text."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        builder.set_branding(footer_text="Confidential")
        assert builder._data.branding_footer_text == "Confidential"

    def test_set_branding_logo_path(self, mock_db):
        """set_branding() sets logo_path."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        builder.set_branding(logo_path="/path/to/logo.png")
        assert builder._data.branding_logo_path == "/path/to/logo.png"

    def test_set_branding_all_fields(self, mock_db):
        """set_branding() can set all fields at once."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        builder.set_branding(
            org_name="Forensic Lab",
            footer_text="Internal Use Only",
            logo_path="/logo.png",
        )
        assert builder._data.branding_org_name == "Forensic Lab"
        assert builder._data.branding_footer_text == "Internal Use Only"
        assert builder._data.branding_logo_path == "/logo.png"

    def test_set_branding_returns_self(self, mock_db):
        """set_branding() returns self for method chaining."""
        builder = ReportBuilder(mock_db, evidence_id=1)
        result = builder.set_branding(org_name="Test")
        assert result is builder


class TestReportDataLocaleAndBranding:
    """Test ReportData dataclass locale and branding fields."""

    def test_locale_default(self):
        """ReportData.locale defaults to 'en'."""
        data = ReportData()
        assert data.locale == "en"

    def test_date_format_default(self):
        """ReportData.date_format defaults to 'eu'."""
        data = ReportData()
        assert data.date_format == "eu"

    def test_locale_custom(self):
        """ReportData.locale can be set."""
        data = ReportData(locale="de")
        assert data.locale == "de"

    def test_branding_defaults(self):
        """ReportData branding fields default to None."""
        data = ReportData()
        assert data.branding_org_name is None
        assert data.branding_footer_text is None
        assert data.branding_logo_path is None

    def test_branding_custom(self):
        """ReportData branding fields can be set."""
        data = ReportData(
            branding_org_name="Test Org",
            branding_footer_text="Footer",
            branding_logo_path="/logo.png",
        )
        assert data.branding_org_name == "Test Org"
        assert data.branding_footer_text == "Footer"
        assert data.branding_logo_path == "/logo.png"


class TestTranslationCoverage:
    """Test that all module templates have required translation keys."""

    def test_page_counter_keys(self):
        """page and of keys exist for footer counter."""
        assert "page" in TRANSLATIONS["en"]
        assert "of" in TRANSLATIONS["en"]
        assert "page" in TRANSLATIONS["de"]
        assert "of" in TRANSLATIONS["de"]
        # Verify they're different
        assert TRANSLATIONS["en"]["page"] != TRANSLATIONS["de"]["page"]

    def test_system_summary_keys(self):
        """system_summary module keys exist in both languages."""
        required = [
            "system_info",
            "user_accounts",
            "installed_software",
            "autostart_entries",
            "network_config",
        ]
        for key in required:
            assert key in TRANSLATIONS["en"], f"EN missing: {key}"
            assert key in TRANSLATIONS["de"], f"DE missing: {key}"

    def test_activity_summary_keys(self):
        """activity_summary module keys exist in both languages."""
        required = [
            "activity_overview",
            "activity_period",
            "duration",
            "total_events",
            "daily_activity",
            "inactivity_gaps",
        ]
        for key in required:
            assert key in TRANSLATIONS["en"], f"EN missing: {key}"
            assert key in TRANSLATIONS["de"], f"DE missing: {key}"

    def test_url_summary_keys(self):
        """url_summary module keys exist in both languages."""
        required = [
            "url",
            "urls",
            "domain",
            "occurrences",
            "first_seen",
            "last_seen",
            "no_urls_found",
        ]
        for key in required:
            assert key in TRANSLATIONS["en"], f"EN missing: {key}"
            assert key in TRANSLATIONS["de"], f"DE missing: {key}"

    def test_images_module_keys(self):
        """images module keys exist in both languages."""
        required = [
            "no_preview",
            "md5",
            "no_images_found",
            "images",
        ]
        for key in required:
            assert key in TRANSLATIONS["en"], f"EN missing: {key}"
            assert key in TRANSLATIONS["de"], f"DE missing: {key}"

    def test_tagged_file_list_keys(self):
        """tagged_file_list module keys exist in both languages."""
        required = [
            "path",
            "file_name",
            "modified",
            "deleted",
            "no_files_found",
            "files",
        ]
        for key in required:
            assert key in TRANSLATIONS["en"], f"EN missing: {key}"
            assert key in TRANSLATIONS["de"], f"DE missing: {key}"

    def test_base_template_keys(self):
        """base_report.html template keys exist in both languages."""
        required = [
            "toc_title",
            "appendix",
            "case_number",
            "evidence",
            "investigator",
            "page_of",
        ]
        for key in required:
            assert key in TRANSLATIONS["en"], f"EN missing: {key}"
            assert key in TRANSLATIONS["de"], f"DE missing: {key}"
