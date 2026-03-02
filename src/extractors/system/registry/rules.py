"""
Registry Analysis Rules

Python-based registry analysis rules for the SystemRegistryExtractor.
Migrated from rules/extractors/registry_offline.yml for modular architecture.

These rules define:
- Path patterns for locating registry hives
- Key/value patterns for extracting forensic indicators
- Deep Freeze detection
- System information extraction
- User activity analysis
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional


@dataclass
class RegistryValue:
    """Definition for a registry value to extract."""
    name: str
    indicator: str
    extract: bool = True
    regex: Optional[str] = None
    type: Optional[str] = None  # e.g., "unix_timestamp"
    confidence: float = 1.0
    note: Optional[str] = None


@dataclass
class RegistryKey:
    """Definition for a registry key to analyze."""
    path: str
    values: List[RegistryValue] = field(default_factory=list)
    extract: bool = False  # Extract entire key as indicator
    extract_all_values: bool = False  # Extract all values under this key
    extract_software_entry: bool = False  # Extract as software entry with full metadata
    custom_handler: Optional[str] = None  # Route to dedicated parser for complex binary formats
    indicator: Optional[str] = None
    confidence: float = 1.0
    note: Optional[str] = None


@dataclass
class RegistryAction:
    """Action configuration for registry analysis."""
    type: str  # "registry_reader"
    hive: str  # "SYSTEM", "SOFTWARE", "NTUSER"
    keys: List[RegistryKey]
    provenance: str
    index_as: str = "os_indicators"


@dataclass
class RegistryTarget:
    """A registry analysis target (collection of keys to analyze)."""
    name: str
    description: str
    os: str
    paths: List[str]  # Glob patterns for hive locations
    actions: List[RegistryAction]
    extractor: str = "registry_offline"


# =============================================================================
# System Information - SOFTWARE Hive
# =============================================================================

SYSTEM_INFO_SOFTWARE = RegistryTarget(
    name="system_info_software",
    os="windows",
    description="System configuration (Software hive)",
    paths=[
        "**/Windows/System32/config/SOFTWARE",
        "**/WINDOWS/system32/config/SOFTWARE",
    ],
    actions=[
        RegistryAction(
            type="registry_reader",
            hive="SOFTWARE",
            provenance="registry_system_info",
            index_as="os_indicators",
            keys=[
                # OS Version
                RegistryKey(
                    path="Microsoft\\Windows NT\\CurrentVersion",
                    values=[
                        RegistryValue(name="ProductName", indicator="system:os_version"),
                        RegistryValue(name="CurrentBuild", indicator="system:os_build"),
                        RegistryValue(name="DisplayVersion", indicator="system:os_display_version"),
                        RegistryValue(name="RegisteredOwner", indicator="system:registered_owner"),
                        RegistryValue(
                            name="InstallDate",
                            indicator="system:install_date",
                            type="unix_timestamp",
                        ),
                    ],
                ),
                # Installed Software (64-bit)
                RegistryKey(
                    path="Microsoft\\Windows\\CurrentVersion\\Uninstall\\*",
                    extract_software_entry=True,
                    indicator="system:installed_software",
                    note="Installed software with full metadata (Publisher, Version, etc.)",
                ),
                # Installed Software (32-bit on 64-bit Windows)
                RegistryKey(
                    path="WOW6432Node\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\*",
                    extract_software_entry=True,
                    indicator="system:installed_software",
                    note="32-bit software on 64-bit Windows",
                ),
                # User Accounts (ProfileList) - extract username from path
                RegistryKey(
                    path="Microsoft\\Windows NT\\CurrentVersion\\ProfileList\\*",
                    values=[
                        RegistryValue(
                            name="ProfileImagePath",
                            indicator="system:user_profile",
                            type="profile_path",
                            note="User profile path - username extracted from final path component",
                        ),
                    ],
                ),
                # Network Profiles (connection history)
                RegistryKey(
                    path="Microsoft\\Windows NT\\CurrentVersion\\NetworkList\\Profiles\\*",
                    values=[
                        RegistryValue(
                            name="ProfileName",
                            indicator="network:profile_name",
                            note="Name of the network profile (SSID or network name)",
                        ),
                        RegistryValue(
                            name="DateLastConnected",
                            indicator="network:profile_last_connected",
                            type="systemtime_bytes",
                            note="Last connection time (SYSTEMTIME structure)",
                        ),
                        RegistryValue(
                            name="DateCreated",
                            indicator="network:profile_created",
                            type="systemtime_bytes",
                            note="Profile creation time (SYSTEMTIME structure)",
                        ),
                        RegistryValue(
                            name="NameType",
                            indicator="network:profile_name_type",
                            note="Network type: 6=wired, 23=VPN, 71=wireless, 243=mobile broadband",
                        ),
                        RegistryValue(
                            name="Category",
                            indicator="network:profile_category",
                            note="Network category: 0=public, 1=private, 2=domain",
                        ),
                    ],
                ),
                # Startup (Run/RunOnce)
                RegistryKey(
                    path="Microsoft\\Windows\\CurrentVersion\\Run",
                    extract_all_values=True,
                    indicator="startup:run_key",
                ),
                RegistryKey(
                    path="Microsoft\\Windows\\CurrentVersion\\RunOnce",
                    extract_all_values=True,
                    indicator="startup:run_key",
                ),
                # Browser Helper Objects
                RegistryKey(
                    path="Microsoft\\Windows\\CurrentVersion\\Explorer\\Browser Helper Objects\\*",
                    extract=True,
                    indicator="startup:bho",
                ),
                # Internet Settings policies (machine-level)
                RegistryKey(
                    path="Policies\\Microsoft\\Windows\\CurrentVersion\\Internet Settings",
                    extract_all_values=True,
                    indicator="network:internet_policy",
                    confidence=0.75,
                    note="Machine-level Internet policy settings",
                ),
                # Installed/registered browsers (StartMenuInternet)
                RegistryKey(
                    path="Clients\\StartMenuInternet\\*",
                    extract=True,
                    indicator="browser:registered_browser",
                    confidence=0.90,
                    note="Registered browser applications",
                ),
                # Browser executable paths
                RegistryKey(
                    path="Microsoft\\Windows\\CurrentVersion\\App Paths\\chrome.exe",
                    extract_all_values=True,
                    indicator="browser:app_path",
                    confidence=0.85,
                    note="Chrome executable path",
                ),
                RegistryKey(
                    path="Microsoft\\Windows\\CurrentVersion\\App Paths\\msedge.exe",
                    extract_all_values=True,
                    indicator="browser:app_path",
                    confidence=0.85,
                    note="Edge executable path",
                ),
                RegistryKey(
                    path="Microsoft\\Windows\\CurrentVersion\\App Paths\\firefox.exe",
                    extract_all_values=True,
                    indicator="browser:app_path",
                    confidence=0.85,
                    note="Firefox executable path",
                ),
                RegistryKey(
                    path="Microsoft\\Windows\\CurrentVersion\\App Paths\\IEXPLORE.EXE",
                    extract_all_values=True,
                    indicator="browser:app_path",
                    confidence=0.85,
                    note="Internet Explorer executable path",
                ),
                RegistryKey(
                    path="Microsoft\\Windows\\CurrentVersion\\App Paths\\brave.exe",
                    extract_all_values=True,
                    indicator="browser:app_path",
                    confidence=0.85,
                    note="Brave executable path",
                ),
                RegistryKey(
                    path="Microsoft\\Windows\\CurrentVersion\\App Paths\\opera.exe",
                    extract_all_values=True,
                    indicator="browser:app_path",
                    confidence=0.85,
                    note="Opera executable path",
                ),
                RegistryKey(
                    path="Microsoft\\Windows\\CurrentVersion\\App Paths\\vivaldi.exe",
                    extract_all_values=True,
                    indicator="browser:app_path",
                    confidence=0.85,
                    note="Vivaldi executable path",
                ),
            ],
        ),
    ],
)


# =============================================================================
# System Information - SYSTEM Hive
# =============================================================================

SYSTEM_INFO_SYSTEM = RegistryTarget(
    name="system_info_system",
    os="windows",
    description="System configuration (System hive)",
    paths=[
        "**/Windows/System32/config/SYSTEM",
        "**/WINDOWS/system32/config/SYSTEM",
    ],
    actions=[
        RegistryAction(
            type="registry_reader",
            hive="SYSTEM",
            provenance="registry_system_info",
            index_as="os_indicators",
            keys=[
                # Computer Name
                RegistryKey(
                    path="ControlSet001\\Control\\ComputerName\\ComputerName",
                    values=[
                        RegistryValue(name="ComputerName", indicator="system:computer_name"),
                    ],
                ),
                # Timezone
                RegistryKey(
                    path="ControlSet001\\Control\\TimeZoneInformation",
                    values=[
                        RegistryValue(name="StandardName", indicator="system:timezone_standard"),
                        RegistryValue(name="TimeZoneKeyName", indicator="system:timezone_key"),
                    ],
                ),
                # Network Interfaces
                RegistryKey(
                    path="ControlSet001\\Services\\Tcpip\\Parameters\\Interfaces\\*",
                    values=[
                        RegistryValue(name="DhcpIPAddress", indicator="network:dhcp_ip"),
                        RegistryValue(name="DhcpNameServer", indicator="network:dns_server"),
                        RegistryValue(name="DhcpDefaultGateway", indicator="network:default_gateway"),
                    ],
                ),
                # Shutdown Time
                RegistryKey(
                    path="ControlSet001\\Control\\Windows",
                    values=[
                        RegistryValue(
                            name="ShutdownTime",
                            indicator="system:last_shutdown",
                            type="filetime_bytes",
                            note="Last system shutdown time (FILETIME)",
                        ),
                    ],
                ),
                # RDP Status
                RegistryKey(
                    path="ControlSet001\\Control\\Terminal Server",
                    values=[
                        RegistryValue(name="fDenyTSConnections", indicator="system:rdp_status"),
                    ],
                ),
            ],
        ),
    ],
)


# =============================================================================
# System Information - NTUSER.DAT (User Configuration)
# =============================================================================

SYSTEM_INFO_NTUSER = RegistryTarget(
    name="system_info_ntuser",
    os="windows",
    description="User configuration (NTUSER.DAT)",
    paths=[
        "**/Users/*/NTUSER.DAT",
        "**/Documents and Settings/*/NTUSER.DAT",
    ],
    actions=[
        RegistryAction(
            type="registry_reader",
            hive="NTUSER",
            provenance="registry_system_info",
            index_as="os_indicators",
            keys=[
                # Mapped Drives
                RegistryKey(
                    path="Network\\*",
                    values=[
                        RegistryValue(name="RemotePath", indicator="network:mapped_drive"),
                    ],
                ),
                # User Run
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Run",
                    extract_all_values=True,
                    indicator="startup:run_key",
                ),
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\RunOnce",
                    extract_all_values=True,
                    indicator="startup:run_key",
                ),
                # Default Browser
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\Shell\\Associations\\UrlAssociations\\http\\UserChoice",
                    values=[
                        RegistryValue(name="ProgId", indicator="system:default_browser"),
                    ],
                ),
                # User Shell Folders (Downloads, Pictures, Videos, Documents, Desktop)
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\User Shell Folders",
                    values=[
                        RegistryValue(
                            name="{374DE290-123F-4565-9164-39C4925E467B}",
                            indicator="system:downloads_path",
                        ),
                        RegistryValue(name="My Pictures", indicator="system:pictures_path"),
                        RegistryValue(
                            name="{F42EE2D3-909F-4907-8871-4C22FC0BF756}",
                            indicator="system:videos_path",
                        ),
                        RegistryValue(name="Personal", indicator="system:documents_path"),
                        RegistryValue(name="Desktop", indicator="system:desktop_path"),
                    ],
                ),
            ],
        ),
    ],
)


# =============================================================================
# User Activity Analysis
# =============================================================================

USER_ACTIVITY = RegistryTarget(
    name="user_activity",
    os="windows",
    description="User account activity and recent files",
    paths=[
        "**/Users/*/NTUSER.DAT",
        "**/Documents and Settings/*/NTUSER.DAT",
    ],
    actions=[
        RegistryAction(
            type="registry_reader",
            hive="NTUSER",
            provenance="registry_user_activity",
            index_as="os_indicators",
            keys=[
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\RecentDocs",
                    extract=True,
                    indicator="recent_documents",
                    confidence=0.75,
                    note="Recently accessed files (MRU list)",
                ),
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\ComDlg32\\OpenSavePidlMRU",
                    extract=True,
                    indicator="open_save_dialog_mru",
                    confidence=0.75,
                    note="File Open/Save dialog history",
                ),
                # LastVisitedPidlMRU (folders visited via Open/Save dialogs)
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\ComDlg32\\LastVisitedPidlMRU",
                    extract=True,
                    indicator="open_save_dialog_last_visited",
                    confidence=0.75,
                    note="Last visited folders in Open/Save dialogs",
                ),
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\TypedPaths",
                    extract=True,
                    indicator="typed_paths",
                    confidence=0.80,
                    note="Manually typed paths in Explorer",
                ),
                # TypedURLs (IE/Legacy Edge)
                RegistryKey(
                    path="Software\\Microsoft\\Internet Explorer\\TypedURLs",
                    custom_handler="typed_urls",
                    indicator="browser:typed_url",
                    confidence=0.85,
                    note="IE/legacy Edge typed URLs with timestamps",
                ),
                # RecentDocs per-extension (image types)
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\RecentDocs\\.jpg",
                    custom_handler="recent_docs_extension",
                    indicator="recent_documents:image",
                    confidence=0.75,
                    note="Recently opened .jpg files",
                ),
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\RecentDocs\\.jpeg",
                    custom_handler="recent_docs_extension",
                    indicator="recent_documents:image",
                    confidence=0.75,
                    note="Recently opened .jpeg files",
                ),
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\RecentDocs\\.png",
                    custom_handler="recent_docs_extension",
                    indicator="recent_documents:image",
                    confidence=0.75,
                    note="Recently opened .png files",
                ),
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\RecentDocs\\.gif",
                    custom_handler="recent_docs_extension",
                    indicator="recent_documents:image",
                    confidence=0.75,
                    note="Recently opened .gif files",
                ),
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\RecentDocs\\.bmp",
                    custom_handler="recent_docs_extension",
                    indicator="recent_documents:image",
                    confidence=0.75,
                    note="Recently opened .bmp files",
                ),
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\RecentDocs\\.webp",
                    custom_handler="recent_docs_extension",
                    indicator="recent_documents:image",
                    confidence=0.75,
                    note="Recently opened .webp files",
                ),
                # WordWheelQuery (Explorer search box history)
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\WordWheelQuery",
                    custom_handler="word_wheel_query",
                    indicator="user_activity:explorer_search",
                    confidence=0.80,
                    note="Explorer search box history",
                ),
                # UserAssist (application execution history)
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\UserAssist\\*\\Count",
                    custom_handler="user_assist",
                    indicator="execution:user_assist",
                    confidence=0.90,
                    note="Application execution history with timestamps",
                ),
                # IE Main (Start Page, Search Page)
                RegistryKey(
                    path="Software\\Microsoft\\Internet Explorer\\Main",
                    values=[
                        RegistryValue(
                            name="Start Page",
                            indicator="browser:home_page",
                            confidence=0.80,
                        ),
                        RegistryValue(
                            name="Search Page",
                            indicator="browser:search_page",
                            confidence=0.80,
                        ),
                    ],
                ),
                # IE SearchScopes (wildcard)
                RegistryKey(
                    path="Software\\Microsoft\\Internet Explorer\\SearchScopes\\*",
                    extract_all_values=True,
                    indicator="browser:search_scope",
                    confidence=0.75,
                    note="IE configured search scopes",
                ),
                # Internet Settings (user-level proxy/config)
                RegistryKey(
                    path="Software\\Microsoft\\Windows\\CurrentVersion\\Internet Settings",
                    values=[
                        RegistryValue(
                            name="ProxyServer",
                            indicator="network:proxy_settings",
                            confidence=0.80,
                        ),
                        RegistryValue(
                            name="ProxyEnable",
                            indicator="network:proxy_settings",
                            confidence=0.80,
                        ),
                        RegistryValue(
                            name="AutoConfigURL",
                            indicator="network:proxy_settings",
                            confidence=0.80,
                        ),
                    ],
                ),
            ],
        ),
    ],
)


# =============================================================================
# Network Configuration
# =============================================================================

NETWORK_CONFIG = RegistryTarget(
    name="network_config",
    os="windows",
    description="Network configuration and proxy settings",
    paths=[
        "**/Windows/System32/config/SYSTEM",
    ],
    actions=[
        RegistryAction(
            type="registry_reader",
            hive="SYSTEM",
            provenance="registry_network",
            index_as="os_indicators",
            keys=[
                RegistryKey(
                    path="ControlSet001\\Services\\Tcpip\\Parameters\\Interfaces\\*",
                    values=[
                        RegistryValue(name="DhcpIPAddress", indicator="network:dhcp_ip"),
                        RegistryValue(name="DhcpServer", indicator="network:dhcp_server"),
                        RegistryValue(name="NameServer", indicator="network:dns_server"),
                        RegistryValue(name="Domain", indicator="network:domain"),
                    ],
                ),
            ],
        ),
    ],
)


# =============================================================================
# Startup Items
# =============================================================================

STARTUP_ITEMS = RegistryTarget(
    name="startup_items",
    os="windows",
    description="Autostart items",
    paths=[
        "**/Windows/System32/config/SOFTWARE",
        "**/WINDOWS/system32/config/SOFTWARE",
    ],
    actions=[
        RegistryAction(
            type="registry_reader",
            hive="SOFTWARE",
            provenance="registry_startup",
            index_as="os_indicators",
            keys=[
                RegistryKey(
                    path="Microsoft\\Windows\\CurrentVersion\\Run",
                    extract=True,
                    indicator="startup:run_key",
                ),
                RegistryKey(
                    path="Microsoft\\Windows\\CurrentVersion\\RunOnce",
                    extract=True,
                    indicator="startup:run_once_key",
                ),
            ],
        ),
    ],
)


# =============================================================================
# SAM User Accounts
# =============================================================================

SAM_USER_ACCOUNTS = RegistryTarget(
    name="sam_user_accounts",
    os="windows",
    description="Local user accounts from SAM hive",
    paths=[
        "**/Windows/System32/config/SAM",
        "**/WINDOWS/system32/config/SAM",
    ],
    actions=[
        RegistryAction(
            type="registry_reader",
            hive="SAM",
            provenance="registry_sam",
            index_as="os_indicators",
            keys=[
                # User account names - iterate Names subkey
                RegistryKey(
                    path="SAM\\Domains\\Account\\Users\\Names\\*",
                    extract=True,
                    indicator="system:local_user",
                    note="Local user account from SAM hive Names subkey",
                ),
            ],
        ),
    ],
)


# =============================================================================
# All Registry Targets (for iteration)
# =============================================================================

REGISTRY_TARGETS: List[RegistryTarget] = [
    SYSTEM_INFO_SOFTWARE,
    SYSTEM_INFO_SYSTEM,
    SYSTEM_INFO_NTUSER,
    USER_ACTIVITY,
    NETWORK_CONFIG,
    STARTUP_ITEMS,
    SAM_USER_ACCOUNTS,
]


def get_registry_targets() -> List[RegistryTarget]:
    """
    Get all registry analysis targets.

    Returns:
        List of RegistryTarget objects
    """
    return REGISTRY_TARGETS


def get_targets_for_hive(hive_type: str) -> List[RegistryTarget]:
    """
    Get targets that apply to a specific hive type.

    Args:
        hive_type: Hive type ("SYSTEM", "SOFTWARE", "NTUSER")

    Returns:
        List of matching RegistryTarget objects
    """
    return [
        target for target in REGISTRY_TARGETS
        if any(
            action.hive.upper() == hive_type.upper()
            for action in target.actions
        )
    ]


def target_to_dict(target: RegistryTarget) -> Dict[str, Any]:
    """
    Convert a RegistryTarget to dictionary format for backward compatibility.

    Args:
        target: RegistryTarget object

    Returns:
        Dictionary matching the old YAML rule format
    """
    return {
        "name": target.name,
        "os": target.os,
        "extractor": target.extractor,
        "description": target.description,
        "paths": target.paths,
        "actions": [
            {
                "type": action.type,
                "hive": action.hive,
                "provenance": action.provenance,
                "index_as": action.index_as,
                "keys": [
                    _key_to_dict(key) for key in action.keys
                ],
            }
            for action in target.actions
        ],
    }


def _key_to_dict(key: RegistryKey) -> Dict[str, Any]:
    """Convert a RegistryKey to dictionary format."""
    result: Dict[str, Any] = {"path": key.path}

    if key.values:
        result["values"] = [
            _value_to_dict(v) for v in key.values
        ]

    if key.extract:
        result["extract"] = True

    if key.extract_all_values:
        result["extract_all_values"] = True

    if key.extract_software_entry:
        result["extract_software_entry"] = True

    if key.custom_handler:
        result["custom_handler"] = key.custom_handler

    if key.indicator:
        result["indicator"] = key.indicator

    if key.confidence != 1.0:
        result["confidence"] = key.confidence

    if key.note:
        result["note"] = key.note

    return result


def _value_to_dict(value: RegistryValue) -> Dict[str, Any]:
    """Convert a RegistryValue to dictionary format."""
    result: Dict[str, Any] = {
        "name": value.name,
        "indicator": value.indicator,
    }

    if value.extract:
        result["extract"] = True

    if value.regex:
        result["regex"] = value.regex

    if value.type:
        result["type"] = value.type

    if value.confidence != 1.0:
        result["confidence"] = value.confidence

    if value.note:
        result["note"] = value.note

    return result


def get_all_targets_as_dicts() -> List[Dict[str, Any]]:
    """
    Get all registry targets as dictionaries (backward compatibility).

    Returns:
        List of target dictionaries matching old YAML format
    """
    return [target_to_dict(t) for t in REGISTRY_TARGETS]
