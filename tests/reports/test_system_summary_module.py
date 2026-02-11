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
