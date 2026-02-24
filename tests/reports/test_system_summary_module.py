"""Tests for the System Summary report module."""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List

import pytest

from reports.modules.system_summary.module import SystemSummaryModule


@pytest.fixture
def module() -> SystemSummaryModule:
    """Create a SystemSummaryModule instance."""
    return SystemSummaryModule()


@pytest.fixture
def mock_db(tmp_path) -> sqlite3.Connection:
    """Create an in-memory SQLite database with test data."""
    db_path = tmp_path / "test_evidence.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Create os_indicators table
    conn.execute("""
        CREATE TABLE os_indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            name TEXT NOT NULL,
            value TEXT,
            path TEXT,
            hive TEXT,
            confidence TEXT,
            detected_at_utc TEXT,
            provenance TEXT,
            extra_json TEXT
        )
    """)

    # Insert test data
    test_indicators = [
        # System info
        (1, "system:computer_name", "ComputerName", "TEST-PC", "ControlSet001\\Control\\ComputerName", "SYSTEM"),
        (1, "system:os_version", "ProductName", "Windows 10 Pro", "Microsoft\\Windows NT\\CurrentVersion", "SOFTWARE"),
        (1, "system:os_build", "CurrentBuild", "19045", "Microsoft\\Windows NT\\CurrentVersion", "SOFTWARE"),
        (1, "system:os_display_version", "DisplayVersion", "22H2", "Microsoft\\Windows NT\\CurrentVersion", "SOFTWARE"),
        (1, "system:install_date", "InstallDate", "2023-04-25 16:46:38", "Microsoft\\Windows NT\\CurrentVersion", "SOFTWARE"),
        (1, "system:registered_owner", "RegisteredOwner", "John Doe", "Microsoft\\Windows NT\\CurrentVersion", "SOFTWARE"),
        (1, "system:timezone_standard", "StandardName", "Pacific Standard Time", "ControlSet001\\Control\\TimeZone", "SYSTEM"),
        (1, "system:last_shutdown", "ShutdownTime", "2024-07-29 22:09:26", "ControlSet001\\Control\\Windows", "SYSTEM"),
        (1, "system:rdp_status", "fDenyTSConnections", "0", "ControlSet001\\Control\\Terminal Server", "SYSTEM"),

        # User accounts - new format (system:user_profile with username as value)
        (1, "system:user_profile", "ProfileImagePath", "John", "ProfileList\\S-1-5-21-xxx", "SOFTWARE"),
        (1, "system:user_profile", "ProfileImagePath", "Admin", "ProfileList\\S-1-5-21-yyy", "SOFTWARE"),
        (1, "system:user_profile", "ProfileImagePath", "Guest", "ProfileList\\S-1-5-21-zzz", "SOFTWARE"),

        # SAM user accounts (actual local users)
        (1, "system:local_user", "Names", "John", "SAM\\Domains\\Account\\Users\\Names\\John", "SAM"),
        (1, "system:local_user", "Names", "Admin", "SAM\\Domains\\Account\\Users\\Names\\Admin", "SAM"),
        (1, "system:local_user", "Names", "Administrator", "SAM\\Domains\\Account\\Users\\Names\\Administrator", "SAM"),

        # Installed software
        (1, "system:installed_software", "DisplayName", "Google Chrome", "Uninstall\\Google Chrome", "SOFTWARE"),
        (1, "system:installed_software", "DisplayName", "Microsoft Office", "Uninstall\\Office", "SOFTWARE"),
        (1, "system:installed_software", "DisplayName", "7-Zip", "Uninstall\\7-Zip", "SOFTWARE"),

        # Autostart
        (1, "startup:run_key", "SecurityHealth", "%ProgramFiles%\\Windows Defender\\MSASCuiL.exe", "Run", "SOFTWARE"),
        (1, "startup:run_key", "iTunesHelper", "\"C:\\Program Files\\iTunes\\iTunesHelper.exe\"", "Run", "SOFTWARE"),

        # Network
        (1, "network:dhcp_ip", "DhcpIPAddress", "192.168.1.100", "Tcpip\\Parameters\\Interfaces\\{guid}", "SYSTEM"),
        (1, "network:dns_server", "DhcpNameServer", "8.8.8.8", "Tcpip\\Parameters\\Interfaces\\{guid}", "SYSTEM"),
        (1, "network:connected_profile", "ProfileName", "Home WiFi", "NetworkList\\Profiles\\{guid}", "SOFTWARE"),
        (1, "network:mapped_drive", "RemotePath", "\\\\server\\share", "Network\\Z", "NTUSER"),
    ]

    for ind in test_indicators:
        conn.execute(
            """INSERT INTO os_indicators
               (evidence_id, type, name, value, path, hive)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ind
        )

    conn.commit()
    return conn


class TestSystemSummaryModuleMetadata:
    """Tests for module metadata."""

    def test_module_id(self, module: SystemSummaryModule):
        """Module ID should be system_summary."""
        assert module.metadata.module_id == "system_summary"

    def test_module_name(self, module: SystemSummaryModule):
        """Module name should include Windows."""
        assert "Windows" in module.metadata.name

    def test_module_category(self, module: SystemSummaryModule):
        """Module category should be System."""
        assert module.metadata.category == "System"


class TestSystemSummaryFilterFields:
    """Tests for filter field definitions."""

    def test_has_sections_filter(self, module: SystemSummaryModule):
        """Should have sections multi-select filter."""
        fields = module.get_filter_fields()
        sections_field = next((f for f in fields if f.key == "sections"), None)
        assert sections_field is not None
        assert sections_field.filter_type.value == "multi_select"

    def test_sections_options(self, module: SystemSummaryModule):
        """Should have all expected section options."""
        fields = module.get_filter_fields()
        sections_field = next((f for f in fields if f.key == "sections"), None)
        option_values = [opt[0] for opt in sections_field.options]

        assert "system_info" in option_values
        assert "users" in option_values
        assert "installed_software" in option_values
        assert "autostart" in option_values
        assert "network" in option_values

    def test_has_software_limit_filter(self, module: SystemSummaryModule):
        """Should have software_limit dropdown."""
        fields = module.get_filter_fields()
        limit_field = next((f for f in fields if f.key == "software_limit"), None)
        assert limit_field is not None
        assert limit_field.filter_type.value == "dropdown"

    def test_has_show_paths_filter(self, module: SystemSummaryModule):
        """Should have show_paths checkbox."""
        fields = module.get_filter_fields()
        paths_field = next((f for f in fields if f.key == "show_paths"), None)
        assert paths_field is not None
        assert paths_field.filter_type.value == "checkbox"


class TestSystemSummaryDataBuilding:
    """Tests for internal data building methods."""

    def test_build_system_info(self, module: SystemSummaryModule, mock_db: sqlite3.Connection):
        """Should build system info with correct values."""
        indicators = module._query_indicators(mock_db, 1)
        sys_info = module._build_system_info(indicators)

        assert sys_info["count"] > 0
        labels = [item["label"] for item in sys_info["info_items"]]
        assert "Computer Name" in labels
        assert "OS Version" in labels

    def test_build_users(self, module: SystemSummaryModule, mock_db: sqlite3.Connection):
        """Should extract usernames from profile and SAM data."""
        indicators = module._query_indicators(mock_db, 1)
        users_data = module._build_users(indicators)

        # 3 profile users + 1 SAM-only user (Administrator)
        assert users_data["count"] == 4
        assert users_data["sam_user_count"] == 3  # John, Admin, Administrator

        usernames = [u["username"] for u in users_data["users"]]
        assert "John" in usernames
        assert "Admin" in usernames
        assert "Guest" in usernames
        assert "Administrator" in usernames  # SAM-only user

        # Check SAM user flags
        john_user = next(u for u in users_data["users"] if u["username"] == "John")
        assert john_user["is_sam_user"] is True

        guest_user = next(u for u in users_data["users"] if u["username"] == "Guest")
        assert guest_user["is_sam_user"] is False  # Profile only, not in SAM

    def test_build_software(self, module: SystemSummaryModule, mock_db: sqlite3.Connection):
        """Should list installed software sorted alphabetically."""
        indicators = module._query_indicators(mock_db, 1)
        software_data = module._build_software(indicators, "all", "eu")

        assert software_data["total_count"] == 3
        names = [sw["name"] for sw in software_data["software"]]
        # Should be sorted alphabetically
        assert names == sorted(names, key=str.lower)

    def test_build_software_with_limit(self, module: SystemSummaryModule, mock_db: sqlite3.Connection):
        """Should respect software limit."""
        indicators = module._query_indicators(mock_db, 1)
        software_data = module._build_software(indicators, "2", "eu")

        assert software_data["shown_count"] == 2
        assert software_data["total_count"] == 3

    def test_build_autostart(self, module: SystemSummaryModule, mock_db: sqlite3.Connection):
        """Should extract autostart entries grouped by type."""
        indicators = module._query_indicators(mock_db, 1)
        autostart_data = module._build_autostart(indicators)

        assert autostart_data["count"] == 2
        # Check run_keys (run key entries)
        assert len(autostart_data["run_keys"]) == 2
        names = [e["name"] for e in autostart_data["run_keys"]]
        assert "SecurityHealth" in names
        assert "iTunesHelper" in names
        # Check that BHOs and services are empty lists
        assert autostart_data["bhos"] == []
        assert autostart_data["services"] == []

    def test_build_network(self, module: SystemSummaryModule, mock_db: sqlite3.Connection):
        """Should extract network configuration."""
        indicators = module._query_indicators(mock_db, 1)
        network_data = module._build_network(indicators)

        assert network_data["count"] > 0
        assert len(network_data["profiles"]) == 1
        assert len(network_data["mapped_drives"]) == 1


class TestSystemSummaryValueFormatting:
    """Tests for value formatting methods."""

    def test_format_install_date_preformatted(self, module: SystemSummaryModule):
        """Install date is formatted according to date_format setting."""
        # Pre-formatted value from registry extractor gets reformatted
        result = module._format_value("system:install_date", "2023-04-25 16:46:38", "en", "eu")
        assert result == "25.04.2023 16:46:38"

    def test_format_last_shutdown_preformatted(self, module: SystemSummaryModule):
        """Last shutdown is formatted according to date_format setting."""
        # Pre-formatted value from registry extractor gets reformatted
        result = module._format_value("system:last_shutdown", "2024-07-29 22:09:26", "en", "eu")
        assert result == "29.07.2024 22:09:26"

    def test_format_rdp_status_enabled(self, module: SystemSummaryModule):
        """RDP status 0 should be Enabled."""
        result = module._format_value("system:rdp_status", "0")
        assert result == "Enabled"

    def test_format_rdp_status_disabled(self, module: SystemSummaryModule):
        """RDP status 1 should be Disabled."""
        result = module._format_value("system:rdp_status", "1")
        assert result == "Disabled"

    def test_format_empty_value(self, module: SystemSummaryModule):
        """Empty value should return dash."""
        result = module._format_value("system:os_version", None)
        assert result == "—"

    def test_extract_username_windows_path(self, module: SystemSummaryModule):
        """Should extract username from Windows path."""
        result = module._extract_username("C:\\Users\\JohnDoe")
        assert result == "JohnDoe"

    def test_extract_username_empty(self, module: SystemSummaryModule):
        """Should return Unknown for empty path."""
        result = module._extract_username("")
        assert result == "Unknown"


class TestSystemSummaryRender:
    """Tests for the render method."""

    def test_render_all_sections(self, module: SystemSummaryModule, mock_db: sqlite3.Connection):
        """Should render HTML with all sections."""
        config = {
            "sections": [
                "system_info",
                "users",
                "installed_software",
                "autostart",
                "network",
            ],
            "software_limit": "50",
            "show_paths": False,
        }

        html = module.render(mock_db, 1, config)

        assert "System Information" in html
        assert "User Accounts" in html
        assert "Installed Software" in html
        assert "Autostart Entries" in html
        assert "Network Configuration" in html

    def test_render_single_section(self, module: SystemSummaryModule, mock_db: sqlite3.Connection):
        """Should render only selected sections."""
        config = {
            "sections": ["system_info"],
            "software_limit": "50",
            "show_paths": False,
        }

        html = module.render(mock_db, 1, config)

        assert "System Information" in html
        assert "User Accounts" not in html
        assert "Installed Software" not in html

    def test_render_with_paths(self, module: SystemSummaryModule, mock_db: sqlite3.Connection):
        """Should include registry paths when show_paths is True."""
        config = {
            "sections": ["system_info"],
            "software_limit": "50",
            "show_paths": True,
        }

        html = module.render(mock_db, 1, config)

        # Registry paths should be in the output
        assert "Microsoft\\Windows NT\\CurrentVersion" in html or "registry-path" in html


class TestSystemSummaryConfigSummary:
    """Tests for config summary formatting."""

    def test_format_config_summary(self, module: SystemSummaryModule):
        """Should format selected sections as summary."""
        config = {"sections": ["system_info", "users"]}
        summary = module.format_config_summary(config)

        assert "System" in summary
        assert "Users" in summary

    def test_format_config_summary_empty(self, module: SystemSummaryModule):
        """Should handle empty sections."""
        config = {"sections": []}
        summary = module.format_config_summary(config)

        assert "No sections" in summary


class TestSystemSummaryShowSystemProfiles:
    """Tests for show_system_profiles filter."""

    @pytest.fixture
    def db_with_system_profiles(self, tmp_path) -> sqlite3.Connection:
        """Create a database with system and user profiles."""
        db_path = tmp_path / "test_profiles.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE os_indicators (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                type TEXT,
                name TEXT,
                value TEXT,
                path TEXT,
                hive TEXT,
                confidence TEXT,
                detected_at_utc TEXT,
                provenance TEXT,
                extra_json TEXT
            )
        """)

        # Mix of user and system profiles
        test_indicators = [
            (1, "system:user_profile", "ProfileImagePath", "John", "ProfileList\\S-1-5-21-1", "SOFTWARE"),
            (1, "system:user_profile", "ProfileImagePath", "systemprofile", "ProfileList\\S-1-5-18", "SOFTWARE"),
            (1, "system:user_profile", "ProfileImagePath", "LocalService", "ProfileList\\S-1-5-19", "SOFTWARE"),
            (1, "system:user_profile", "ProfileImagePath", "NetworkService", "ProfileList\\S-1-5-20", "SOFTWARE"),
            (1, "system:user_profile", "ProfileImagePath", "Public", "ProfileList\\S-1-5-21-2", "SOFTWARE"),
            (1, "system:user_profile", "ProfileImagePath", "Default", "ProfileList\\S-1-5-21-3", "SOFTWARE"),
            (1, "system:user_profile", "ProfileImagePath", "Alice", "ProfileList\\S-1-5-21-4", "SOFTWARE"),
        ]

        for ind in test_indicators:
            conn.execute(
                """INSERT INTO os_indicators
                   (evidence_id, type, name, value, path, hive)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                ind
            )
        conn.commit()
        return conn

    def test_hide_system_profiles_by_default(
        self, module: SystemSummaryModule, db_with_system_profiles: sqlite3.Connection
    ):
        """Should filter out system profiles by default."""
        indicators = module._query_indicators(db_with_system_profiles, 1)
        users_data = module._build_users(indicators, show_system_profiles=False)

        usernames = [u["username"] for u in users_data["users"]]
        assert "John" in usernames
        assert "Alice" in usernames
        assert "systemprofile" not in usernames
        assert "LocalService" not in usernames
        assert "NetworkService" not in usernames
        assert "Public" not in usernames
        assert "Default" not in usernames
        assert len(usernames) == 2

    def test_show_system_profiles_when_enabled(
        self, module: SystemSummaryModule, db_with_system_profiles: sqlite3.Connection
    ):
        """Should include system profiles when show_system_profiles=True."""
        indicators = module._query_indicators(db_with_system_profiles, 1)
        users_data = module._build_users(indicators, show_system_profiles=True)

        usernames = [u["username"] for u in users_data["users"]]
        assert "John" in usernames
        assert "Alice" in usernames
        assert "systemprofile" in usernames
        assert "LocalService" in usernames
        assert "NetworkService" in usernames
        assert "Public" in usernames
        assert "Default" in usernames
        assert len(usernames) == 7


class TestSystemSummarySoftwareMetadata:
    """Tests for software version/date/publisher extraction."""

    @pytest.fixture
    def db_with_software_metadata(self, tmp_path) -> sqlite3.Connection:
        """Create a database with software entries including extra_json."""
        import json
        db_path = tmp_path / "test_software.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE os_indicators (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                type TEXT,
                name TEXT,
                value TEXT,
                path TEXT,
                hive TEXT,
                confidence TEXT,
                detected_at_utc TEXT,
                provenance TEXT,
                extra_json TEXT
            )
        """)

        # Software with full metadata
        sw1_extra = json.dumps({
            "version": "120.0.6099.130",
            "install_date": "20240115",
            "install_date_formatted": "2024-01-15",
            "publisher": "Google LLC",
        })

        # Software with partial metadata
        sw2_extra = json.dumps({
            "version": "16.0.17328.20000",
        })

        # Software with no metadata
        sw3_extra = None

        test_indicators = [
            (1, "system:installed_software", "DisplayName", "Google Chrome",
             "Uninstall\\Chrome", "SOFTWARE", sw1_extra),
            (1, "system:installed_software", "DisplayName", "Microsoft Office",
             "Uninstall\\Office", "SOFTWARE", sw2_extra),
            (1, "system:installed_software", "DisplayName", "Notepad++",
             "Uninstall\\Notepad++", "SOFTWARE", sw3_extra),
        ]

        for ind in test_indicators:
            conn.execute(
                """INSERT INTO os_indicators
                   (evidence_id, type, name, value, path, hive, extra_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                ind
            )
        conn.commit()
        return conn

    def test_extracts_software_metadata(
        self, module: SystemSummaryModule, db_with_software_metadata: sqlite3.Connection
    ):
        """Should extract version, install_date, publisher from extra_json."""
        indicators = module._query_indicators(db_with_software_metadata, 1)
        software_data = module._build_software(indicators, "all", "eu")

        # Find Chrome entry
        chrome = next(sw for sw in software_data["software"] if sw["name"] == "Google Chrome")
        assert chrome["version"] == "120.0.6099.130"
        assert chrome["install_date"] == "15.01.2024"  # Formatted with EU date format
        assert chrome["publisher"] == "Google LLC"

    def test_handles_partial_metadata(
        self, module: SystemSummaryModule, db_with_software_metadata: sqlite3.Connection
    ):
        """Should handle software with partial metadata."""
        indicators = module._query_indicators(db_with_software_metadata, 1)
        software_data = module._build_software(indicators, "all", "eu")

        # Find Office entry (has version only)
        office = next(sw for sw in software_data["software"] if sw["name"] == "Microsoft Office")
        assert office["version"] == "16.0.17328.20000"
        assert office["install_date"] is None
        assert office["publisher"] is None

    def test_handles_no_metadata(
        self, module: SystemSummaryModule, db_with_software_metadata: sqlite3.Connection
    ):
        """Should handle software with no extra_json."""
        indicators = module._query_indicators(db_with_software_metadata, 1)
        software_data = module._build_software(indicators, "all", "eu")

        # Find Notepad++ entry (no metadata)
        notepad = next(sw for sw in software_data["software"] if sw["name"] == "Notepad++")
        assert notepad["version"] is None
        assert notepad["install_date"] is None
        assert notepad["publisher"] is None


class TestSystemSummaryEmptyDatabase:
    """Tests with empty database."""

    @pytest.fixture
    def empty_db(self, tmp_path) -> sqlite3.Connection:
        """Create an empty database."""
        db_path = tmp_path / "empty_evidence.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE os_indicators (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                type TEXT,
                name TEXT,
                value TEXT,
                path TEXT,
                hive TEXT,
                confidence TEXT,
                detected_at_utc TEXT,
                provenance TEXT,
                extra_json TEXT
            )
        """)
        return conn

    def test_render_empty_database(self, module: SystemSummaryModule, empty_db: sqlite3.Connection):
        """Should render gracefully with no data."""
        config = {
            "sections": ["system_info", "users"],
            "software_limit": "50",
            "show_paths": False,
        }

        html = module.render(empty_db, 1, config)

        assert "No system information found" in html
        assert "No user accounts found" in html


class TestSystemSummaryShellFolders:
    """Tests for shell folder paths in system info."""

    @pytest.fixture
    def db_with_shell_folders(self, tmp_path) -> sqlite3.Connection:
        """Create a database with shell folder indicators."""
        db_path = tmp_path / "test_shell_folders.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE os_indicators (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                type TEXT,
                name TEXT,
                value TEXT,
                path TEXT,
                hive TEXT,
                confidence TEXT,
                detected_at_utc TEXT,
                provenance TEXT,
                extra_json TEXT
            )
        """)

        test_indicators = [
            (1, "system:downloads_path", "{374DE290-123F-4565-9164-39C4925E467B}",
             "C:\\Users\\John\\Downloads", "User Shell Folders", "NTUSER"),
            (1, "system:pictures_path", "My Pictures",
             "C:\\Users\\John\\Pictures", "User Shell Folders", "NTUSER"),
            (1, "system:videos_path", "My Video",
             "C:\\Users\\John\\Videos", "User Shell Folders", "NTUSER"),
            (1, "system:documents_path", "Personal",
             "C:\\Users\\John\\Documents", "User Shell Folders", "NTUSER"),
            (1, "system:desktop_path", "Desktop",
             "C:\\Users\\John\\Desktop", "User Shell Folders", "NTUSER"),
        ]

        for ind in test_indicators:
            conn.execute(
                """INSERT INTO os_indicators
                   (evidence_id, type, name, value, path, hive)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                ind
            )
        conn.commit()
        return conn

    def test_build_includes_shell_folders(
        self, module: SystemSummaryModule, db_with_shell_folders: sqlite3.Connection
    ):
        """Should include shell folder paths in system info."""
        indicators = module._query_indicators(db_with_shell_folders, 1)
        sys_info = module._build_system_info(indicators)

        labels = [item["label"] for item in sys_info["info_items"]]
        assert "Downloads Path" in labels
        assert "Pictures Path" in labels
        assert "Videos Path" in labels
        assert "Documents Path" in labels
        assert "Desktop Path" in labels
        assert sys_info["count"] == 5

    def test_render_with_shell_folders(
        self, module: SystemSummaryModule, db_with_shell_folders: sqlite3.Connection
    ):
        """Should render shell folder paths in system info section."""
        config = {
            "sections": ["system_info"],
            "software_limit": "50",
            "show_paths": False,
        }
        html = module.render(db_with_shell_folders, 1, config)
        assert "C:\\Users\\John\\Pictures" in html
        assert "C:\\Users\\John\\Documents" in html


class TestSystemSummaryRunOnceKey:
    """Tests for startup:run_once_key handling in autostart."""

    @pytest.fixture
    def db_with_run_once(self, tmp_path) -> sqlite3.Connection:
        """Create a database with run_once_key entries."""
        db_path = tmp_path / "test_run_once.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE os_indicators (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                type TEXT,
                name TEXT,
                value TEXT,
                path TEXT,
                hive TEXT,
                confidence TEXT,
                detected_at_utc TEXT,
                provenance TEXT,
                extra_json TEXT
            )
        """)

        test_indicators = [
            (1, "startup:run_key", "SecurityHealth",
             "%ProgramFiles%\\Windows Defender\\MSASCuiL.exe",
             "Software\\Microsoft\\Windows\\CurrentVersion\\Run", "SOFTWARE"),
            (1, "startup:run_once_key", "WExtract",
             "C:\\Windows\\Temp\\setup.exe /cleanup",
             "Software\\Microsoft\\Windows\\CurrentVersion\\RunOnce", "NTUSER"),
        ]

        for ind in test_indicators:
            conn.execute(
                """INSERT INTO os_indicators
                   (evidence_id, type, name, value, path, hive)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                ind
            )
        conn.commit()
        return conn

    def test_run_once_key_included(
        self, module: SystemSummaryModule, db_with_run_once: sqlite3.Connection
    ):
        """startup:run_once_key should be included in autostart run_keys."""
        indicators = module._query_indicators(db_with_run_once, 1)
        autostart_data = module._build_autostart(indicators)

        assert autostart_data["count"] == 2
        assert len(autostart_data["run_keys"]) == 2

        names = [e["name"] for e in autostart_data["run_keys"]]
        assert "SecurityHealth" in names
        assert "WExtract" in names

    def test_run_once_key_flagged(
        self, module: SystemSummaryModule, db_with_run_once: sqlite3.Connection
    ):
        """RunOnce entries should have is_run_once=True."""
        indicators = module._query_indicators(db_with_run_once, 1)
        autostart_data = module._build_autostart(indicators)

        wextract = next(e for e in autostart_data["run_keys"] if e["name"] == "WExtract")
        assert wextract["is_run_once"] is True

        security = next(e for e in autostart_data["run_keys"] if e["name"] == "SecurityHealth")
        assert security["is_run_once"] is False


class TestSystemSummaryBrowserDetection:
    """Tests for browser detection section."""

    @pytest.fixture
    def db_with_browsers(self, tmp_path) -> sqlite3.Connection:
        """Create a database with browser detection indicators."""
        db_path = tmp_path / "test_browsers.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE os_indicators (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                type TEXT,
                name TEXT,
                value TEXT,
                path TEXT,
                hive TEXT,
                confidence TEXT,
                detected_at_utc TEXT,
                provenance TEXT,
                extra_json TEXT
            )
        """)

        test_indicators = [
            # Registered browsers
            (1, "browser:registered_browser", "IEXPLORE.EXE", "IEXPLORE.EXE",
             "Clients\\StartMenuInternet\\IEXPLORE.EXE", "SOFTWARE"),
            (1, "browser:registered_browser", "Microsoft Edge", "Microsoft Edge",
             "Clients\\StartMenuInternet\\Microsoft Edge", "SOFTWARE"),

            # App paths
            (1, "browser:app_path", "(Default)", "C:\\Program Files\\Microsoft\\Edge\\msedge.exe",
             "App Paths\\msedge.exe", "SOFTWARE"),
            (1, "browser:app_path", "Path", "C:\\Program Files\\Microsoft\\Edge",
             "App Paths\\msedge.exe", "SOFTWARE"),

            # IE settings
            (1, "browser:home_page", "Start Page", "https://www.msn.com",
             "Software\\Microsoft\\Internet Explorer\\Main", "NTUSER"),
            (1, "browser:search_page", "Search Page", "https://www.bing.com",
             "Software\\Microsoft\\Internet Explorer\\Main", "NTUSER"),
            (1, "browser:search_scope", "Bing", "https://www.bing.com/search?q={searchTerms}",
             "Software\\Microsoft\\Internet Explorer\\SearchScopes\\{guid}", "NTUSER"),
        ]

        for ind in test_indicators:
            conn.execute(
                """INSERT INTO os_indicators
                   (evidence_id, type, name, value, path, hive)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                ind
            )
        conn.commit()
        return conn

    def test_build_browser_detection(
        self, module: SystemSummaryModule, db_with_browsers: sqlite3.Connection
    ):
        """Should extract registered browsers, app paths, and IE settings."""
        indicators = module._query_indicators(db_with_browsers, 1)
        browser_data = module._build_browser_detection(indicators)

        assert len(browser_data["registered_browsers"]) == 2
        assert len(browser_data["app_paths"]) == 2
        assert len(browser_data["ie_settings"]) == 3
        assert browser_data["count"] == 7

    def test_registered_browser_names(
        self, module: SystemSummaryModule, db_with_browsers: sqlite3.Connection
    ):
        """Should extract browser names from registered_browser indicators."""
        indicators = module._query_indicators(db_with_browsers, 1)
        browser_data = module._build_browser_detection(indicators)

        names = [b["name"] for b in browser_data["registered_browsers"]]
        assert "IEXPLORE.EXE" in names
        assert "Microsoft Edge" in names

    def test_ie_settings_labels(
        self, module: SystemSummaryModule, db_with_browsers: sqlite3.Connection
    ):
        """IE settings should have proper labels."""
        indicators = module._query_indicators(db_with_browsers, 1)
        browser_data = module._build_browser_detection(indicators)

        labels = [s["label"] for s in browser_data["ie_settings"]]
        assert "Home Page" in labels
        assert "Search Page" in labels
        assert any("Search Scope" in l for l in labels)

    def test_render_browser_detection_section(
        self, module: SystemSummaryModule, db_with_browsers: sqlite3.Connection
    ):
        """Should render browser detection section."""
        config = {
            "sections": ["browser_detection"],
            "software_limit": "50",
            "show_paths": False,
        }
        html = module.render(db_with_browsers, 1, config)
        assert "Browser Detection" in html
        assert "IEXPLORE.EXE" in html
        assert "msedge.exe" in html


class TestSystemSummaryUserActivity:
    """Tests for user activity section."""

    @pytest.fixture
    def db_with_activity(self, tmp_path) -> sqlite3.Connection:
        """Create a database with user activity indicators."""
        db_path = tmp_path / "test_activity.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE os_indicators (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                type TEXT,
                name TEXT,
                value TEXT,
                path TEXT,
                hive TEXT,
                confidence TEXT,
                detected_at_utc TEXT,
                provenance TEXT,
                extra_json TEXT
            )
        """)

        test_indicators = [
            (1, "recent_documents", ".docx", "report.docx",
             "RecentDocs\\.docx", "NTUSER"),
            (1, "recent_documents:image", ".jpg", "photo.jpg",
             "RecentDocs\\.jpg", "NTUSER"),
            (1, "recent_documents:image", ".png", "screenshot.png",
             "RecentDocs\\.png", "NTUSER"),
            (1, "browser:typed_url", "url1", "https://www.google.com",
             "TypedURLs", "NTUSER"),
            (1, "browser:typed_url", "url2", "https://www.example.com",
             "TypedURLs", "NTUSER"),
            (1, "typed_paths", "url1", "C:\\Users\\John\\Documents",
             "TypedPaths", "NTUSER"),
            (1, "open_save_dialog_mru", "0", "document.pdf",
             "ComDlg32\\OpenSavePidlMRU\\pdf", "NTUSER"),
            (1, "open_save_dialog_last_visited", "0", "notepad.exe",
             "ComDlg32\\LastVisitedPidlMRU", "NTUSER"),
            (1, "user_activity:explorer_search", "0", "secret files",
             "WordWheelQuery", "NTUSER"),
        ]

        for ind in test_indicators:
            conn.execute(
                """INSERT INTO os_indicators
                   (evidence_id, type, name, value, path, hive)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                ind
            )
        conn.commit()
        return conn

    def test_build_user_activity(
        self, module: SystemSummaryModule, db_with_activity: sqlite3.Connection
    ):
        """Should extract all user activity types."""
        indicators = module._query_indicators(db_with_activity, 1)
        activity_data = module._build_user_activity(indicators)

        assert len(activity_data["recent_docs"]) == 1
        assert len(activity_data["recent_images"]) == 2
        assert len(activity_data["typed_urls"]) == 2
        assert len(activity_data["typed_paths"]) == 1
        assert len(activity_data["open_save_mru"]) == 1
        assert len(activity_data["open_save_last_visited"]) == 1
        assert len(activity_data["explorer_searches"]) == 1
        assert activity_data["count"] == 9

    def test_recent_images_highlighted(
        self, module: SystemSummaryModule, db_with_activity: sqlite3.Connection
    ):
        """Recent images should be in their own list (for forensic highlighting)."""
        indicators = module._query_indicators(db_with_activity, 1)
        activity_data = module._build_user_activity(indicators)

        image_values = [i["value"] for i in activity_data["recent_images"]]
        assert "photo.jpg" in image_values
        assert "screenshot.png" in image_values

    def test_render_user_activity_section(
        self, module: SystemSummaryModule, db_with_activity: sqlite3.Connection
    ):
        """Should render user activity section."""
        config = {
            "sections": ["user_activity"],
            "software_limit": "50",
            "show_paths": False,
        }
        html = module.render(db_with_activity, 1, config)
        assert "User Activity" in html
        assert "photo.jpg" in html
        assert "https://www.google.com" in html
        assert "secret files" in html


class TestSystemSummaryExecutionHistory:
    """Tests for execution history section."""

    @pytest.fixture
    def db_with_execution(self, tmp_path) -> sqlite3.Connection:
        """Create a database with execution history indicators."""
        import json
        db_path = tmp_path / "test_execution.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE os_indicators (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                type TEXT,
                name TEXT,
                value TEXT,
                path TEXT,
                hive TEXT,
                confidence TEXT,
                detected_at_utc TEXT,
                provenance TEXT,
                extra_json TEXT
            )
        """)

        extra1 = json.dumps({
            "decoded_path": "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
            "run_count": 150,
            "focus_count": 120,
            "focus_time": "2:35:00",
            "last_run": "2024-07-28 14:30:00",
        })

        extra2 = json.dumps({
            "decoded_path": "C:\\Windows\\explorer.exe",
            "run_count": 45,
            "focus_count": 30,
            "focus_time": "0:45:00",
            "last_run": "2024-07-29 10:15:00",
        })

        extra3 = json.dumps({
            "decoded_path": "C:\\Program Files\\CCleaner\\CCleaner.exe",
            "run_count": 5,
            "focus_count": 3,
            "focus_time": "0:02:00",
            "last_run": "2024-07-25 08:00:00",
        })

        test_indicators = [
            (1, "execution:user_assist", "chrome.exe",
             "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
             "UserAssist\\{GUID}\\Count", "NTUSER", extra1),
            (1, "execution:user_assist", "explorer.exe",
             "C:\\Windows\\explorer.exe",
             "UserAssist\\{GUID}\\Count", "NTUSER", extra2),
            (1, "execution:user_assist", "CCleaner.exe",
             "C:\\Program Files\\CCleaner\\CCleaner.exe",
             "UserAssist\\{GUID}\\Count", "NTUSER", extra3),
        ]

        for ind in test_indicators:
            conn.execute(
                """INSERT INTO os_indicators
                   (evidence_id, type, name, value, path, hive, extra_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                ind
            )
        conn.commit()
        return conn

    def test_build_execution_history(
        self, module: SystemSummaryModule, db_with_execution: sqlite3.Connection
    ):
        """Should extract execution history entries."""
        indicators = module._query_indicators(db_with_execution, 1)
        exec_data = module._build_execution_history(indicators)

        assert exec_data["count"] == 3
        assert len(exec_data["entries"]) == 3

    def test_execution_sorted_by_run_count(
        self, module: SystemSummaryModule, db_with_execution: sqlite3.Connection
    ):
        """Entries should be sorted by run_count descending."""
        indicators = module._query_indicators(db_with_execution, 1)
        exec_data = module._build_execution_history(indicators)

        counts = [e["run_count"] for e in exec_data["entries"]]
        assert counts == [150, 45, 5]

    def test_execution_extra_json_parsed(
        self, module: SystemSummaryModule, db_with_execution: sqlite3.Connection
    ):
        """Should parse extra_json for run_count, focus_count, etc."""
        indicators = module._query_indicators(db_with_execution, 1)
        exec_data = module._build_execution_history(indicators)

        chrome = exec_data["entries"][0]
        assert chrome["decoded_path"] == "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
        assert chrome["run_count"] == 150
        assert chrome["focus_count"] == 120
        assert chrome["focus_time"] == "2:35:00"
        assert chrome["last_run"]  # Should be formatted

    def test_render_execution_history_section(
        self, module: SystemSummaryModule, db_with_execution: sqlite3.Connection
    ):
        """Should render execution history section."""
        config = {
            "sections": ["execution_history"],
            "software_limit": "50",
            "show_paths": False,
        }
        html = module.render(db_with_execution, 1, config)
        assert "Execution History" in html
        assert "chrome.exe" in html
        assert "150" in html

    def test_render_empty_execution_history(
        self, module: SystemSummaryModule
    ):
        """Should show empty message when no execution data."""
        # Use indicators list directly
        exec_data = module._build_execution_history([])
        assert exec_data["count"] == 0
        assert exec_data["entries"] == []


class TestSystemSummaryProxyAndPolicy:
    """Tests for proxy and internet policy in network section."""

    @pytest.fixture
    def db_with_proxy(self, tmp_path) -> sqlite3.Connection:
        """Create a database with proxy/policy indicators."""
        db_path = tmp_path / "test_proxy.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE os_indicators (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                type TEXT,
                name TEXT,
                value TEXT,
                path TEXT,
                hive TEXT,
                confidence TEXT,
                detected_at_utc TEXT,
                provenance TEXT,
                extra_json TEXT
            )
        """)

        test_indicators = [
            (1, "network:proxy_settings", "ProxyEnable", "1",
             "Internet Settings\\ProxyEnable", "NTUSER"),
            (1, "network:proxy_settings", "ProxyServer", "proxy.corp.com:8080",
             "Internet Settings\\ProxyServer", "NTUSER"),
            (1, "network:internet_policy", "EnableHTTP1_1", "1",
             "Internet Settings\\EnableHTTP1_1", "SOFTWARE"),
        ]

        for ind in test_indicators:
            conn.execute(
                """INSERT INTO os_indicators
                   (evidence_id, type, name, value, path, hive)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                ind
            )
        conn.commit()
        return conn

    def test_proxy_in_network_info(
        self, module: SystemSummaryModule, db_with_proxy: sqlite3.Connection
    ):
        """Proxy and policy indicators should appear in network info_items."""
        indicators = module._query_indicators(db_with_proxy, 1)
        network_data = module._build_network(indicators)

        labels = [item["label"] for item in network_data["info_items"]]
        assert "Proxy Settings" in labels
        assert "Internet Policy" in labels
        assert network_data["count"] == 3


class TestSystemSummaryNewSectionSelection:
    """Tests for new section options in filter fields."""

    def test_sections_include_new_options(self, module: SystemSummaryModule):
        """Filter options should include the 3 new sections."""
        fields = module.get_filter_fields()
        sections_field = next((f for f in fields if f.key == "sections"), None)
        option_values = [opt[0] for opt in sections_field.options]

        assert "browser_detection" in option_values
        assert "user_activity" in option_values
        assert "execution_history" in option_values

    def test_config_summary_new_sections(self, module: SystemSummaryModule):
        """Config summary should include new section labels."""
        config = {"sections": ["browser_detection", "user_activity", "execution_history"]}
        summary = module.format_config_summary(config)

        assert "Browsers" in summary
        assert "Activity" in summary
        assert "Execution" in summary


# ==================== Real-world data tests ====================
# The following tests use data formats matching actual extraction output,
# where ind['name'] == ind['type'] (indicator type string) and the actual
# entry name is only in extra_json.value_name or path suffix.


class TestRealDataAutostart:
    """Tests for autostart with real extraction data format."""

    @pytest.fixture
    def db_real_autostart(self, tmp_path) -> sqlite3.Connection:
        """Create DB matching real extraction output for autostart."""
        import json
        db_path = tmp_path / "test_real_autostart.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE os_indicators (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                type TEXT,
                name TEXT,
                value TEXT,
                path TEXT,
                hive TEXT,
                confidence TEXT,
                detected_at_utc TEXT,
                provenance TEXT,
                extra_json TEXT
            )
        """)

        # Real extraction: name == type, actual name in extra_json/path
        test_indicators = [
            # Run key entries (extract_all_values path)
            (1, "startup:run_key", "startup:run_key",
             "C:\\Windows\\SysWOW64\\OneDriveSetup.exe /thfirstsetup",
             "Microsoft\\Windows\\CurrentVersion\\Run\\OneDriveSetup",
             "SOFTWARE", "1.0", "registry",
             json.dumps({"type": "startup:run_key", "value_name": "OneDriveSetup",
                         "raw_value": "C:\\Windows\\SysWOW64\\OneDriveSetup.exe /thfirstsetup",
                         "key_last_modified": "2024-01-15"})),
            (1, "startup:run_key", "startup:run_key",
             "%ProgramFiles%\\Windows Defender\\MSASCuiL.exe",
             "Microsoft\\Windows\\CurrentVersion\\Run\\SecurityHealth",
             "SOFTWARE", "1.0", "registry",
             json.dumps({"type": "startup:run_key", "value_name": "SecurityHealth",
                         "raw_value": "%ProgramFiles%\\Windows Defender\\MSASCuiL.exe",
                         "key_last_modified": "2024-01-15"})),
            # RunOnce entry
            (1, "startup:run_key", "startup:run_key",
             "C:\\Windows\\Temp\\setup.exe /cleanup",
             "Microsoft\\Windows\\CurrentVersion\\RunOnce\\WExtract",
             "NTUSER.DAT", "1.0", "registry",
             json.dumps({"type": "startup:run_key", "value_name": "WExtract",
                         "raw_value": "C:\\Windows\\Temp\\setup.exe /cleanup",
                         "key_last_modified": "2024-01-15"})),
            # BHO entry (extract path: value = key name = CLSID)
            (1, "startup:bho", "startup:bho",
             "{018E93F6-5B8A-4C18-8F4D-B0A4D8B0E3FF}",
             "Microsoft\\Windows\\CurrentVersion\\Explorer\\Browser Helper Objects\\{018E93F6-5B8A-4C18-8F4D-B0A4D8B0E3FF}",
             "SOFTWARE", "1.0", "registry", None),
        ]

        for ind in test_indicators:
            conn.execute(
                """INSERT INTO os_indicators
                   (evidence_id, type, name, value, path, hive, confidence, provenance, extra_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ind
            )
        conn.commit()
        return conn

    def test_run_key_names_from_extra_json(
        self, module: SystemSummaryModule, db_real_autostart: sqlite3.Connection
    ):
        """Run key names should be extracted from extra_json.value_name, not from name column."""
        indicators = module._query_indicators(db_real_autostart, 1)
        autostart_data = module._build_autostart(indicators)

        names = [e["name"] for e in autostart_data["run_keys"]]
        assert "OneDriveSetup" in names
        assert "SecurityHealth" in names
        assert "WExtract" in names
        # Must NOT show the indicator type string
        assert "startup:run_key" not in names

    def test_bho_clsid_from_value(
        self, module: SystemSummaryModule, db_real_autostart: sqlite3.Connection
    ):
        """BHO CLSID should come from ind['value'], not ind['name']."""
        indicators = module._query_indicators(db_real_autostart, 1)
        autostart_data = module._build_autostart(indicators)

        assert len(autostart_data["bhos"]) == 1
        assert autostart_data["bhos"][0]["clsid"] == "{018E93F6-5B8A-4C18-8F4D-B0A4D8B0E3FF}"
        # Must NOT be the indicator type
        assert autostart_data["bhos"][0]["clsid"] != "startup:bho"

    def test_run_key_name_fallback_to_path(self, module: SystemSummaryModule):
        """When extra_json is missing, fall back to last path segment."""
        indicators = [{
            "type": "startup:run_key",
            "name": "startup:run_key",
            "value": "C:\\some\\app.exe",
            "path": "Software\\Microsoft\\Windows\\CurrentVersion\\Run\\MyApp",
            "hive": "SOFTWARE",
            "extra_json": None,
        }]
        autostart_data = module._build_autostart(indicators)
        assert autostart_data["run_keys"][0]["name"] == "MyApp"


class TestRealDataTimezone:
    """Tests for timezone DLL resource reference handling."""

    @pytest.fixture
    def db_dll_timezone(self, tmp_path) -> sqlite3.Connection:
        """Create DB with DLL resource reference timezone value."""
        db_path = tmp_path / "test_tz.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE os_indicators (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                type TEXT,
                name TEXT,
                value TEXT,
                path TEXT,
                hive TEXT,
                confidence TEXT,
                detected_at_utc TEXT,
                provenance TEXT,
                extra_json TEXT
            )
        """)

        test_indicators = [
            (1, "system:computer_name", "system:computer_name", "TEST-PC",
             "ControlSet001\\Control\\ComputerName", "SYSTEM"),
            # Timezone with DLL resource reference
            (1, "system:timezone_standard", "system:timezone_standard", "@tzres.dll,-322",
             "ControlSet001\\Control\\TimeZoneInformation\\StandardName", "SYSTEM"),
            # Timezone key with readable name
            (1, "system:timezone_key", "system:timezone_key", "W. Europe Standard Time",
             "ControlSet001\\Control\\TimeZoneInformation\\TimeZoneKeyName", "SYSTEM"),
        ]

        for ind in test_indicators:
            conn.execute(
                """INSERT INTO os_indicators
                   (evidence_id, type, name, value, path, hive)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                ind
            )
        conn.commit()
        return conn

    def test_dll_timezone_replaced_with_key_value(
        self, module: SystemSummaryModule, db_dll_timezone: sqlite3.Connection
    ):
        """DLL resource reference should be replaced with timezone_key value."""
        indicators = module._query_indicators(db_dll_timezone, 1)
        sys_info = module._build_system_info(indicators)

        values = {item["label"]: item["value"] for item in sys_info["info_items"]}
        assert "W. Europe Standard Time" in values.values()
        # DLL reference should NOT appear
        assert "@tzres.dll,-322" not in values.values()

    def test_timezone_key_not_duplicated(
        self, module: SystemSummaryModule, db_dll_timezone: sqlite3.Connection
    ):
        """Timezone_key should not appear as a separate row when standard is present."""
        indicators = module._query_indicators(db_dll_timezone, 1)
        sys_info = module._build_system_info(indicators)

        labels = [item["label"] for item in sys_info["info_items"]]
        # Only "Timezone" should appear, not "Timezone Key"
        timezone_labels = [l for l in labels if "Timezone" in l or "timezone" in l.lower()]
        assert len(timezone_labels) == 1

    def test_readable_timezone_shown_directly(self, module: SystemSummaryModule):
        """Readable timezone_standard value should be shown as-is."""
        indicators = [
            {"type": "system:timezone_standard", "name": "system:timezone_standard",
             "value": "Pacific Standard Time", "path": "", "hive": "SYSTEM",
             "extra_json": None, "id": 1, "confidence": "1.0",
             "detected_at_utc": None, "provenance": "registry"},
            {"type": "system:timezone_key", "name": "system:timezone_key",
             "value": "Pacific Standard Time", "path": "", "hive": "SYSTEM",
             "extra_json": None, "id": 2, "confidence": "1.0",
             "detected_at_utc": None, "provenance": "registry"},
        ]
        sys_info = module._build_system_info(indicators)
        values = [item["value"] for item in sys_info["info_items"]]
        assert "Pacific Standard Time" in values


class TestRealDataNetwork:
    """Tests for network deduplication, cleaning, and filtering."""

    @pytest.fixture
    def db_real_network(self, tmp_path) -> sqlite3.Connection:
        """Create DB matching real extraction output for network."""
        db_path = tmp_path / "test_real_net.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE os_indicators (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                type TEXT,
                name TEXT,
                value TEXT,
                path TEXT,
                hive TEXT,
                confidence TEXT,
                detected_at_utc TEXT,
                provenance TEXT,
                extra_json TEXT
            )
        """)

        test_indicators = [
            # Interface 1 — active adapter with real values
            (1, "network:dhcp_ip", "network:dhcp_ip", "192.168.178.50",
             "Tcpip\\Parameters\\Interfaces\\{AAA}\\DhcpIPAddress", "SYSTEM"),
            (1, "network:dns_server", "network:dns_server", "192.168.178.1",
             "Tcpip\\Parameters\\Interfaces\\{AAA}\\DhcpNameServer", "SYSTEM"),
            (1, "network:default_gateway", "network:default_gateway", "['192.168.178.1']",
             "Tcpip\\Parameters\\Interfaces\\{AAA}\\DefaultGateway", "SYSTEM"),
            (1, "network:domain", "network:domain", "fritz.box",
             "Tcpip\\Parameters\\Interfaces\\{AAA}\\Domain", "SYSTEM"),
            (1, "network:dhcp_server", "network:dhcp_server", "192.168.178.1",
             "Tcpip\\Parameters\\Interfaces\\{AAA}\\DhcpServer", "SYSTEM"),

            # Interface 2 — inactive adapter with zero values
            (1, "network:dhcp_ip", "network:dhcp_ip", "0",
             "Tcpip\\Parameters\\Interfaces\\{BBB}\\DhcpIPAddress", "SYSTEM"),
            (1, "network:dns_server", "network:dns_server", "0",
             "Tcpip\\Parameters\\Interfaces\\{BBB}\\DhcpNameServer", "SYSTEM"),
            (1, "network:default_gateway", "network:default_gateway", "['']",
             "Tcpip\\Parameters\\Interfaces\\{BBB}\\DefaultGateway", "SYSTEM"),
            (1, "network:domain", "network:domain", "0",
             "Tcpip\\Parameters\\Interfaces\\{BBB}\\Domain", "SYSTEM"),

            # Interface 3 — duplicate of interface 1 values
            (1, "network:dhcp_ip", "network:dhcp_ip", "192.168.178.50",
             "Tcpip\\Parameters\\Interfaces\\{CCC}\\DhcpIPAddress", "SYSTEM"),
            (1, "network:dns_server", "network:dns_server", "192.168.178.1",
             "Tcpip\\Parameters\\Interfaces\\{CCC}\\DhcpNameServer", "SYSTEM"),

            # Proxy (value "0" means disabled)
            (1, "network:proxy_settings", "network:proxy_settings", "0",
             "Internet Settings\\ProxyEnable", "NTUSER"),

            # Internet policy
            (1, "network:internet_policy", "network:internet_policy", "1",
             "Internet Settings\\EnableHTTP1_1", "SOFTWARE"),
        ]

        for ind in test_indicators:
            conn.execute(
                """INSERT INTO os_indicators
                   (evidence_id, type, name, value, path, hive)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                ind
            )
        conn.commit()
        return conn

    def test_zero_values_filtered_for_addresses(
        self, module: SystemSummaryModule, db_real_network: sqlite3.Connection
    ):
        """Zero values for DHCP/DNS/gateway should be filtered out."""
        indicators = module._query_indicators(db_real_network, 1)
        network_data = module._build_network(indicators)

        values = [item["value"] for item in network_data["info_items"]]
        assert "0" not in values

    def test_list_format_cleaned(
        self, module: SystemSummaryModule, db_real_network: sqlite3.Connection
    ):
        """['192.168.178.1'] should become 192.168.178.1."""
        indicators = module._query_indicators(db_real_network, 1)
        network_data = module._build_network(indicators)

        values = [item["value"] for item in network_data["info_items"]]
        assert "192.168.178.1" in values
        assert "['192.168.178.1']" not in values

    def test_empty_list_filtered(
        self, module: SystemSummaryModule, db_real_network: sqlite3.Connection
    ):
        """[''] gateway should be filtered out entirely."""
        indicators = module._query_indicators(db_real_network, 1)
        network_data = module._build_network(indicators)

        values = [item["value"] for item in network_data["info_items"]]
        assert "" not in values
        assert "['']" not in values

    def test_duplicates_removed(
        self, module: SystemSummaryModule, db_real_network: sqlite3.Connection
    ):
        """Same type+value from different interfaces should appear only once."""
        indicators = module._query_indicators(db_real_network, 1)
        network_data = module._build_network(indicators)

        # DHCP IP 192.168.178.50 appears in {AAA} and {CCC} — should show once
        dhcp_items = [i for i in network_data["info_items"]
                      if i["value"] == "192.168.178.50"]
        assert len(dhcp_items) == 1

    def test_domain_zero_filtered(
        self, module: SystemSummaryModule, db_real_network: sqlite3.Connection
    ):
        """Domain '0' should be filtered, 'fritz.box' should remain."""
        indicators = module._query_indicators(db_real_network, 1)
        network_data = module._build_network(indicators)

        domain_items = [i for i in network_data["info_items"]
                        if "Domain" in i["label"] or "Domäne" in i["label"]]
        assert len(domain_items) == 1
        assert domain_items[0]["value"] == "fritz.box"

    def test_proxy_disabled_translated(
        self, module: SystemSummaryModule, db_real_network: sqlite3.Connection
    ):
        """Proxy '0' should be translated to 'Disabled'."""
        indicators = module._query_indicators(db_real_network, 1)
        network_data = module._build_network(indicators)

        proxy_items = [i for i in network_data["info_items"]
                       if "Proxy" in i["label"]]
        assert len(proxy_items) == 1
        assert proxy_items[0]["value"] == "Disabled"


class TestRealDataBrowserDetection:
    """Tests for browser detection with real extraction data format."""

    @pytest.fixture
    def db_real_browsers(self, tmp_path) -> sqlite3.Connection:
        """Create DB matching real extraction output."""
        import json
        db_path = tmp_path / "test_real_browsers.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        conn.execute("""
            CREATE TABLE os_indicators (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                type TEXT,
                name TEXT,
                value TEXT,
                path TEXT,
                hive TEXT,
                confidence TEXT,
                detected_at_utc TEXT,
                provenance TEXT,
                extra_json TEXT
            )
        """)

        test_indicators = [
            # Registered browser (extract path): name==type, value==key name
            (1, "browser:registered_browser", "browser:registered_browser",
             "Google Chrome",
             "Clients\\StartMenuInternet\\Google Chrome", "SOFTWARE",
             "0.9", "registry", None),
            # App path (extract_all_values): name==type, value==exe path
            (1, "browser:app_path", "browser:app_path",
             "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
             "Microsoft\\Windows\\CurrentVersion\\App Paths\\chrome.exe\\(Default)",
             "SOFTWARE", "0.85", "registry",
             json.dumps({"type": "browser:app_path", "value_name": "(Default)",
                         "raw_value": "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
                         "key_last_modified": "2024-01-15"})),
        ]

        for ind in test_indicators:
            conn.execute(
                """INSERT INTO os_indicators
                   (evidence_id, type, name, value, path, hive, confidence, provenance, extra_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ind
            )
        conn.commit()
        return conn

    def test_registered_browser_name_from_value(
        self, module: SystemSummaryModule, db_real_browsers: sqlite3.Connection
    ):
        """Registered browser name should come from value, not name column."""
        indicators = module._query_indicators(db_real_browsers, 1)
        browser_data = module._build_browser_detection(indicators)

        names = [b["name"] for b in browser_data["registered_browsers"]]
        assert "Google Chrome" in names
        assert "browser:registered_browser" not in names

    def test_app_path_exe_name_extracted(
        self, module: SystemSummaryModule, db_real_browsers: sqlite3.Connection
    ):
        """App path name should show the exe name, not the indicator type."""
        indicators = module._query_indicators(db_real_browsers, 1)
        browser_data = module._build_browser_detection(indicators)

        names = [ap["name"] for ap in browser_data["app_paths"]]
        assert "chrome.exe" in names
        assert "browser:app_path" not in names


class TestExtractEntryName:
    """Unit tests for the _extract_entry_name helper."""

    def test_legacy_data_name_differs_from_type(self, module: SystemSummaryModule):
        """When name != type, return name directly (legacy data)."""
        ind = {"type": "startup:run_key", "name": "SecurityHealth",
               "value": "cmd.exe", "path": "", "extra_json": None}
        assert module._extract_entry_name(ind) == "SecurityHealth"

    def test_real_data_with_extra_json(self, module: SystemSummaryModule):
        """When name == type, extract from extra_json.value_name."""
        import json
        ind = {"type": "startup:run_key", "name": "startup:run_key",
               "value": "cmd.exe", "path": "Run\\MyApp",
               "extra_json": json.dumps({"value_name": "MyApp"})}
        assert module._extract_entry_name(ind) == "MyApp"

    def test_real_data_fallback_to_path(self, module: SystemSummaryModule):
        """When name == type and no extra_json, fall back to path suffix."""
        ind = {"type": "startup:run_key", "name": "startup:run_key",
               "value": "cmd.exe", "path": "Run\\MyApp", "extra_json": None}
        assert module._extract_entry_name(ind) == "MyApp"

    def test_empty_everything(self, module: SystemSummaryModule):
        """When everything is empty, return name."""
        ind = {"type": "startup:run_key", "name": "startup:run_key",
               "value": "", "path": "", "extra_json": None}
        assert module._extract_entry_name(ind) == "startup:run_key"


class TestCleanNetworkValue:
    """Unit tests for the _clean_network_value helper."""

    def test_clean_list_format(self, module: SystemSummaryModule):
        """Should strip Python list formatting."""
        assert module._clean_network_value("['192.168.178.1']") == "192.168.178.1"

    def test_clean_multi_item_list(self, module: SystemSummaryModule):
        """Should handle multi-item list."""
        assert module._clean_network_value("['8.8.8.8', '8.8.4.4']") == "8.8.8.8, 8.8.4.4"

    def test_filter_empty_list(self, module: SystemSummaryModule):
        """Empty list items should result in empty string."""
        assert module._clean_network_value("['']") == ""

    def test_filter_dll_reference(self, module: SystemSummaryModule):
        """DLL resource references should be filtered."""
        assert module._clean_network_value("@tzres.dll,-322") == ""

    def test_domain_zero_filtered(self, module: SystemSummaryModule):
        """Domain '0' should be filtered."""
        assert module._clean_network_value("0", "network:domain") == ""

    def test_dhcp_zero_filtered(self, module: SystemSummaryModule):
        """DHCP IP '0' should be filtered."""
        assert module._clean_network_value("0", "network:dhcp_ip") == ""

    def test_proxy_zero_disabled(self, module: SystemSummaryModule):
        """Proxy '0' should become 'Disabled'."""
        assert module._clean_network_value("0", "network:proxy_settings") == "Disabled"

    def test_proxy_one_enabled(self, module: SystemSummaryModule):
        """Proxy '1' should become 'Enabled'."""
        assert module._clean_network_value("1", "network:proxy_settings") == "Enabled"

    def test_normal_value_unchanged(self, module: SystemSummaryModule):
        """Normal values should pass through unchanged."""
        assert module._clean_network_value("192.168.1.1") == "192.168.1.1"


class TestExtractAppPathExe:
    """Unit tests for the _extract_app_path_exe helper."""

    def test_standard_app_path(self, module: SystemSummaryModule):
        """Should extract exe name from App Paths key."""
        path = "Microsoft\\Windows\\CurrentVersion\\App Paths\\chrome.exe\\(Default)"
        assert module._extract_app_path_exe(path) == "chrome.exe"

    def test_no_app_paths_segment(self, module: SystemSummaryModule):
        """Should return empty when path doesn't contain App Paths."""
        path = "Software\\Microsoft\\Run\\SomeApp"
        assert module._extract_app_path_exe(path) == ""

    def test_empty_path(self, module: SystemSummaryModule):
        """Should return empty for empty path."""
        assert module._extract_app_path_exe("") == ""
