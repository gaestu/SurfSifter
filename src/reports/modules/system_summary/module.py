"""System Summary Report Module.

Displays a comprehensive Windows system summary including:
- System information (OS, computer name, install date, timezone)
- User accounts (from ProfileList and SAM hive)
- Installed software
- Autostart entries
- Network configuration
- Deep Freeze detection (if present)

Data is sourced from the os_indicators table populated by the registry extractor.

Note: Timestamps (install_date, last_shutdown) are pre-formatted by the registry
extractor using FILETIME/Unix timestamp conversion, so no conversion is needed here.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from ...dates import format_date, format_datetime
from ...paths import get_module_template_dir
from ..base import (
    BaseReportModule,
    FilterField,
    FilterType,
    ModuleMetadata,
)


class SystemSummaryModule(BaseReportModule):
    """Module for displaying Windows system summary in reports."""

    # Section identifiers
    SECTION_SYSTEM_INFO = "system_info"
    SECTION_USERS = "users"
    SECTION_INSTALLED_SOFTWARE = "installed_software"
    SECTION_AUTOSTART = "autostart"
    SECTION_NETWORK = "network"
    SECTION_BROWSER_DETECTION = "browser_detection"
    SECTION_USER_ACTIVITY = "user_activity"
    SECTION_EXECUTION_HISTORY = "execution_history"

    # Mapping of indicator type prefixes to sections
    INDICATOR_TYPE_TO_SECTION = {
        "system:": SECTION_SYSTEM_INFO,
        "network:": SECTION_NETWORK,
        "startup:": SECTION_AUTOSTART,
    }

    # System info indicators that are single-value (not lists)
    SYSTEM_INFO_SINGLE_VALUES = {
        "system:os_version": "OS Version",
        "system:os_build": "OS Build",
        "system:os_display_version": "OS Display Version",
        "system:registered_owner": "Registered Owner",
        "system:install_date": "Install Date",
        "system:computer_name": "Computer Name",
        "system:timezone_standard": "Timezone",
        "system:timezone_key": "Timezone Key",
        "system:last_shutdown": "Last Shutdown",
        "system:rdp_status": "RDP Status",
        "system:default_browser": "Default Browser",
        "system:downloads_path": "Downloads Path",
        "system:pictures_path": "Pictures Path",
        "system:videos_path": "Videos Path",
        "system:documents_path": "Documents Path",
        "system:desktop_path": "Desktop Path",
    }

    # User-related indicator types
    USER_INDICATOR_TYPES = {
        "system:user_profile": "Profile User",  # From ProfileList (username extracted)
        "system:local_user": "Local User",      # From SAM hive
        "system:user_account": "User Account",  # Legacy: full profile path
    }

    # Network info single values
    NETWORK_INFO_LABELS = {
        "network:dhcp_ip": "DHCP IP Address",
        "network:dns_server": "DNS Server",
        "network:default_gateway": "Default Gateway",
        "network:dhcp_server": "DHCP Server",
        "network:domain": "Domain",
        "network:connected_profile": "Connected Network",
        "network:profile_last_connected": "Last Connected",
        "network:mapped_drive": "Mapped Drive",
        "network:proxy_settings": "Proxy Settings",
        "network:internet_policy": "Internet Policy",
    }

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="system_summary",
            name="System Summary (Windows)",
            description="Displays Windows system information, users, software, autostart, and network config from registry",
            category="System",
            icon="🖥️",
        )

    def get_filter_fields(self) -> List[FilterField]:
        """Return filter fields for section selection."""
        return [
            FilterField(
                key="sections",
                label="Sections to Include",
                filter_type=FilterType.MULTI_SELECT,
                default=[
                    self.SECTION_SYSTEM_INFO,
                    self.SECTION_USERS,
                    self.SECTION_NETWORK,
                ],
                options=[
                    (self.SECTION_SYSTEM_INFO, "System Information"),
                    (self.SECTION_USERS, "User Accounts"),
                    (self.SECTION_INSTALLED_SOFTWARE, "Installed Software"),
                    (self.SECTION_AUTOSTART, "Autostart Entries"),
                    (self.SECTION_NETWORK, "Network Configuration"),
                    (self.SECTION_BROWSER_DETECTION, "Browser Detection"),
                    (self.SECTION_USER_ACTIVITY, "User Activity"),
                    (self.SECTION_EXECUTION_HISTORY, "Execution History"),
                ],
                help_text="Select which sections to include in the report",
                required=True,
            ),
            FilterField(
                key="show_system_profiles",
                label="Show System Profiles",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Include system accounts (LocalService, NetworkService, systemprofile)",
                required=False,
            ),
            FilterField(
                key="software_limit",
                label="Software List Limit",
                filter_type=FilterType.DROPDOWN,
                default="50",
                options=[
                    ("20", "20 items"),
                    ("50", "50 items"),
                    ("100", "100 items"),
                    ("all", "All items"),
                ],
                help_text="Maximum number of installed software items to display",
                required=False,
            ),
            FilterField(
                key="show_paths",
                label="Show Registry Paths",
                filter_type=FilterType.CHECKBOX,
                default=False,
                help_text="Include registry path details for each indicator",
                required=False,
            ),
        ]

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        """Render the system summary as HTML.

        Args:
            db_conn: SQLite connection to evidence database
            evidence_id: Current evidence ID
            config: Filter configuration from user

        Returns:
            Rendered HTML string
        """
        from jinja2 import Environment, FileSystemLoader
        from ...locales import get_translations, get_field_label, DEFAULT_LOCALE

        # Extract config values
        sections = config.get("sections", [self.SECTION_SYSTEM_INFO])
        if isinstance(sections, str):
            sections = [sections]
        software_limit = config.get("software_limit", "50")
        show_paths = config.get("show_paths", False)
        show_system_profiles = config.get("show_system_profiles", False)

        # Get locale and translations
        locale = config.get("_locale", DEFAULT_LOCALE)
        t = config.get("_translations") or get_translations(locale)
        date_format = config.get("_date_format", "eu")

        # Query all indicators for this evidence
        indicators = self._query_indicators(db_conn, evidence_id)

        # Organize data by section (pass locale for label translation)
        section_data = {
            self.SECTION_SYSTEM_INFO: self._build_system_info(indicators, locale, date_format),
            self.SECTION_USERS: self._build_users(indicators, show_system_profiles, t),
            self.SECTION_INSTALLED_SOFTWARE: self._build_software(indicators, software_limit, date_format),
            self.SECTION_AUTOSTART: self._build_autostart(indicators),
            self.SECTION_NETWORK: self._build_network(indicators, locale, date_format, t),
            self.SECTION_BROWSER_DETECTION: self._build_browser_detection(indicators, locale),
            self.SECTION_USER_ACTIVITY: self._build_user_activity(indicators, locale, date_format),
            self.SECTION_EXECUTION_HISTORY: self._build_execution_history(indicators, locale, date_format),
        }

        # Render template
        template_dir = get_module_template_dir(__file__)
        env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
        template = env.get_template("template.html")

        return template.render(
            t=t,
            locale=locale,
            sections=sections,
            section_data=section_data,
            show_paths=show_paths,
            SECTION_SYSTEM_INFO=self.SECTION_SYSTEM_INFO,
            SECTION_USERS=self.SECTION_USERS,
            SECTION_INSTALLED_SOFTWARE=self.SECTION_INSTALLED_SOFTWARE,
            SECTION_AUTOSTART=self.SECTION_AUTOSTART,
            SECTION_NETWORK=self.SECTION_NETWORK,
            SECTION_BROWSER_DETECTION=self.SECTION_BROWSER_DETECTION,
            SECTION_USER_ACTIVITY=self.SECTION_USER_ACTIVITY,
            SECTION_EXECUTION_HISTORY=self.SECTION_EXECUTION_HISTORY,
        )

    def _query_indicators(
        self, db_conn: sqlite3.Connection, evidence_id: int
    ) -> List[Dict[str, Any]]:
        """Query all OS indicators for the evidence.

        Args:
            db_conn: SQLite connection
            evidence_id: Evidence ID

        Returns:
            List of indicator dicts
        """
        try:
            db_conn.row_factory = sqlite3.Row
            cursor = db_conn.execute(
                """
                SELECT id, type, name, value, path, hive, confidence,
                       detected_at_utc, provenance, extra_json
                FROM os_indicators
                WHERE evidence_id = ?
                ORDER BY type, name
                """,
                (evidence_id,),
            )
            return [dict(row) for row in cursor.fetchall()]
        except Exception:
            return []

    def _build_system_info(
        self, indicators: List[Dict[str, Any]], locale: str = "en", date_format: str = "eu"
    ) -> Dict[str, Any]:
        """Build system information section data.

        Args:
            indicators: List of indicator dicts
            locale: Locale code for label translation

        Returns dict with 'items' list of {label, value, path} dicts.
        """
        from ...locales import get_field_label

        items = []
        seen_types = set()

        # Define preferred order
        ordered_types = [
            "system:computer_name",
            "system:os_version",
            "system:os_display_version",
            "system:os_build",
            "system:install_date",
            "system:registered_owner",
            "system:timezone_standard",
            "system:timezone_key",
            "system:last_shutdown",
            "system:rdp_status",
            "system:default_browser",
            "system:downloads_path",
            "system:pictures_path",
            "system:videos_path",
            "system:documents_path",
            "system:desktop_path",
        ]

        # Pre-scan for timezone_key value as fallback for DLL resource refs
        timezone_key_value = None
        for ind in indicators:
            if ind["type"] == "system:timezone_key":
                timezone_key_value = ind["value"]
                break

        # Add items in preferred order
        for ind_type in ordered_types:
            for ind in indicators:
                if ind["type"] == ind_type and ind_type not in seen_types:
                    label = get_field_label(ind_type, locale)
                    value = self._format_value(
                        ind["type"], ind["value"], locale, date_format
                    )

                    # Handle DLL resource reference for timezone
                    # e.g. "@tzres.dll,-322" → use timezone_key value instead
                    if ind_type == "system:timezone_standard" and value.startswith("@"):
                        if timezone_key_value:
                            value = timezone_key_value
                        else:
                            # Skip — no readable fallback available
                            seen_types.add(ind_type)
                            break

                    # Skip timezone_key as separate row when we already
                    # displayed a timezone value via timezone_standard
                    if ind_type == "system:timezone_key" and "system:timezone_standard" in seen_types:
                        seen_types.add(ind_type)
                        break

                    items.append({
                        "label": label,
                        "value": value,
                        "path": ind.get("path", ""),
                    })
                    seen_types.add(ind_type)
                    break

        return {"info_items": items, "count": len(items)}

    def _build_users(
        self,
        indicators: List[Dict[str, Any]],
        show_system_profiles: bool = False,
        t: Dict[str, str] | None = None,
    ) -> Dict[str, Any]:
        """Build user accounts section data.

        Args:
            indicators: List of indicator dicts
            show_system_profiles: If False, filter out system accounts

        Returns dict with 'users' list combining SAM users and profile users.
        Handles both new (system:user_profile, system:local_user) and legacy
        (system:user_account) indicator types.
        """
        import json

        # System profile names to filter out (case-insensitive)
        SYSTEM_PROFILES = {
            "systemprofile",
            "localservice",
            "networkservice",
            "public",
            "default",
            "default user",
        }

        def is_system_profile(username: str) -> bool:
            """Check if username is a system profile."""
            return username.lower() in SYSTEM_PROFILES

        t = t or {}
        unknown_label = t.get("unknown", "Unknown")

        # Collect SAM users (actual local accounts)
        sam_users = set()
        for ind in indicators:
            if ind["type"] == "system:local_user":
                username = ind["value"] or ""
                if username:
                    sam_users.add(username)

        # Collect profile users with their paths
        profile_users = []
        seen_usernames = set()

        for ind in indicators:
            if ind["type"] == "system:user_profile":
                # New format: value is username, profile_path in extra_json
                username = ind["value"] or ""
                profile_path = ""
                if ind.get("extra_json"):
                    try:
                        extra = json.loads(ind["extra_json"])
                        profile_path = extra.get("profile_path", "")
                    except (json.JSONDecodeError, TypeError):
                        pass

                # Skip system profiles unless requested
                if not show_system_profiles and is_system_profile(username):
                    continue

                if username and username not in seen_usernames:
                    profile_users.append({
                        "username": username,
                        "profile_path": profile_path,
                        "path": ind.get("path", ""),
                        "is_sam_user": username in sam_users,
                    })
                    seen_usernames.add(username)

            elif ind["type"] == "system:user_account":
                # Legacy format: value is full profile path
                profile_path = ind["value"] or ""
                username = self._extract_username(profile_path, unknown_label)

                # Skip system profiles unless requested
                if not show_system_profiles and is_system_profile(username):
                    continue

                if username and username not in seen_usernames:
                    profile_users.append({
                        "username": username,
                        "profile_path": profile_path,
                        "path": ind.get("path", ""),
                        "is_sam_user": username in sam_users,
                    })
                    seen_usernames.add(username)

        # Add SAM-only users (accounts without profiles, e.g., disabled accounts)
        sam_only_users = []
        for username in sam_users:
            # Skip system profiles unless requested
            if not show_system_profiles and is_system_profile(username):
                continue
            if username not in seen_usernames:
                sam_only_users.append({
                    "username": username,
                    "profile_path": "",
                    "path": "",
                    "is_sam_user": True,
                })

        # Combine: profile users first, then SAM-only users
        all_users = profile_users + sam_only_users

        return {
            "users": all_users,
            "count": len(all_users),
            "sam_user_count": len(sam_users),
        }

    def _build_software(
        self, indicators: List[Dict[str, Any]], limit: str, date_format: str
    ) -> Dict[str, Any]:
        """Build installed software section data.

        Extracts version, install date, and publisher from extra_json.
        Returns dict with 'software' list and total count.
        """
        import json

        software_map: Dict[str, Dict[str, Any]] = {}

        for ind in indicators:
            if ind["type"] == "system:installed_software":
                name = ind["value"] or ind["name"]
                if name and name not in software_map:
                    # Extract additional metadata from extra_json
                    version = None
                    install_date = None
                    publisher = None
                    forensic_interest = False

                    if ind.get("extra_json"):
                        try:
                            extra = json.loads(ind["extra_json"])
                            version = extra.get("version")
                            # Prefer formatted date if available
                            install_date = extra.get("install_date_formatted") or extra.get("install_date")
                            if install_date:
                                install_date = format_date(install_date, date_format)
                            publisher = extra.get("publisher")
                            forensic_interest = extra.get("forensic_interest", False)
                        except (json.JSONDecodeError, TypeError):
                            pass

                    software_map[name] = {
                        "name": name,
                        "version": version,
                        "install_date": install_date,
                        "publisher": publisher,
                        "forensic_interest": forensic_interest,
                        "path": ind.get("path", ""),
                    }

        software_list = sorted(software_map.values(), key=lambda x: x["name"].lower())
        total_count = len(software_list)

        # Apply limit
        if limit != "all":
            try:
                limit_int = int(limit)
                software_list = software_list[:limit_int]
            except ValueError:
                pass

        return {
            "software": software_list,
            "shown_count": len(software_list),
            "total_count": total_count,
        }

    def _extract_entry_name(self, ind: Dict[str, Any]) -> str:
        """Extract the actual meaningful name from an indicator.

        In real extracted data, ind['name'] == ind['type'] (both store the
        indicator type string like 'startup:run_key').  The actual meaningful
        name lives in extra_json.value_name or as the last path segment.

        For indicators created with ``extract=True`` (BHO, registered browsers),
        the key name is in ind['value'] instead.
        """
        import json

        name = ind.get("name", "") or ""
        ind_type = ind.get("type", "") or ""

        # If name is different from type, it's already meaningful
        if name and name != ind_type:
            return name

        # Try extra_json.value_name
        if ind.get("extra_json"):
            try:
                extra = json.loads(ind["extra_json"])
                vname = extra.get("value_name")
                if vname:
                    return str(vname)
            except (json.JSONDecodeError, TypeError):
                pass

        # Try last segment of path
        path = ind.get("path", "") or ""
        if path:
            last_seg = path.replace("/", "\\").rsplit("\\", 1)[-1]
            if last_seg:
                return last_seg

        return name

    def _build_autostart(self, indicators: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build autostart entries section data.

        Groups entries by type (run keys, BHOs, services) and deduplicates
        entries that appear in both HKLM and HKCU.

        Handles both legacy data (where ``name`` contains the actual entry name)
        and real extraction data (where ``name`` == indicator type and the actual
        entry name must be recovered from ``extra_json.value_name`` or ``path``).

        Returns dict with grouped 'run_keys', 'bhos', 'services' lists.
        """
        run_keys = []
        bhos = []
        services = []

        # Track seen (name, command) pairs to deduplicate
        seen_run_entries: Dict[tuple, Dict[str, Any]] = {}
        seen_bho_clsids: set = set()

        for ind in indicators:
            ind_type = ind["type"]
            entry_value = ind["value"] or ""
            path = ind.get("path", "")

            # Determine scope (HKLM vs HKCU) from path
            scope = "HKLM"
            if "NTUSER" in path.upper() or "HKU" in path.upper() or "HKCU" in path.upper():
                scope = "HKCU"

            if ind_type in ("startup:run_key", "startup:run_once_key"):
                # Extract actual entry name (handles both legacy and real data)
                entry_name = self._extract_entry_name(ind)

                # Skip entries where name is a key name (not a value name)
                if entry_name in ("Run", "RunOnce"):
                    continue
                # Skip empty/invalid entries
                if not entry_name or not entry_value:
                    continue

                # Determine if this is a RunOnce key
                is_run_once = ind_type == "startup:run_once_key" or "RunOnce" in path

                # Deduplicate by (name, command) - combine scopes if same entry in HKLM + HKCU
                key = (entry_name.lower(), entry_value.lower())
                if key in seen_run_entries:
                    # Already seen - update scope to show both if different
                    existing = seen_run_entries[key]
                    if existing["scope"] != scope:
                        existing["scope"] = "HKLM+HKCU"
                else:
                    seen_run_entries[key] = {
                        "name": entry_name,
                        "command": entry_value,
                        "scope": scope,
                        "is_run_once": is_run_once,
                        "path": path,
                    }

            elif ind_type == "startup:bho":
                # BHOs use extract=True: value has the key name (CLSID)
                clsid = entry_value or ""
                if clsid and clsid not in seen_bho_clsids:
                    seen_bho_clsids.add(clsid)
                    bhos.append({
                        "clsid": clsid,
                        "path": path,
                    })

            elif ind_type == "startup:service":
                entry_name = self._extract_entry_name(ind)
                if entry_name:
                    services.append({
                        "name": entry_name,
                        "command": entry_value,
                        "path": path,
                    })

        # Convert seen_run_entries dict to sorted list
        run_keys = sorted(seen_run_entries.values(), key=lambda x: x["name"].lower())

        # Calculate total count
        total_count = len(run_keys) + len(bhos) + len(services)

        return {
            "run_keys": run_keys,
            "bhos": bhos,
            "services": services,
            "count": total_count,
        }

    def _build_network(
        self,
        indicators: List[Dict[str, Any]],
        locale: str = "en",
        date_format: str = "eu",
        t: Dict[str, str] | None = None,
    ) -> Dict[str, Any]:
        """Build network configuration section data.

        Args:
            indicators: List of indicator dicts
            locale: Locale code for label translation

        Returns dict with grouped network info including profiles with metadata.
        """
        import json
        from ...locales import get_field_label

        t = t or {}
        unknown_label = t.get("unknown", "Unknown")

        # Group by interface/profile
        items_dedup: Dict[tuple, Dict[str, Any]] = {}  # Deduplicated network items
        profiles_map: Dict[str, Dict[str, Any]] = {}  # Key by registry path prefix
        mapped_drives = []

        # Network type mapping (NameType values)
        NETWORK_TYPE_MAP = {
            6: "wired",      # Ethernet
            23: "vpn",       # VPN
            71: "wireless",  # WiFi
            243: "mobile",   # Mobile broadband
        }

        # Category mapping
        CATEGORY_MAP = {
            0: "public",
            1: "private",
            2: "domain",
        }

        for ind in indicators:
            ind_type = ind["type"]
            path = ind.get("path", "")

            if ind_type == "network:profile_name":
                # Extract profile key from path (everything up to the value name)
                profile_key = self._get_profile_key(path)
                if profile_key not in profiles_map:
                    profiles_map[profile_key] = {"path": path}
                profiles_map[profile_key]["name"] = ind["value"] or unknown_label

            elif ind_type == "network:profile_last_connected":
                profile_key = self._get_profile_key(path)
                if profile_key not in profiles_map:
                    profiles_map[profile_key] = {"path": path}
                profiles_map[profile_key]["last_connected_raw"] = ind["value"] or ""

            elif ind_type == "network:profile_created":
                profile_key = self._get_profile_key(path)
                if profile_key not in profiles_map:
                    profiles_map[profile_key] = {"path": path}
                profiles_map[profile_key]["created_raw"] = ind["value"] or ""

            elif ind_type == "network:profile_name_type":
                profile_key = self._get_profile_key(path)
                if profile_key not in profiles_map:
                    profiles_map[profile_key] = {"path": path}
                try:
                    name_type = int(ind["value"]) if ind["value"] else None
                    profiles_map[profile_key]["network_type"] = NETWORK_TYPE_MAP.get(
                        name_type, "unknown"
                    )
                except (ValueError, TypeError):
                    profiles_map[profile_key]["network_type"] = "unknown"

            elif ind_type == "network:profile_category":
                profile_key = self._get_profile_key(path)
                if profile_key not in profiles_map:
                    profiles_map[profile_key] = {"path": path}
                try:
                    category = int(ind["value"]) if ind["value"] else None
                    profiles_map[profile_key]["category"] = CATEGORY_MAP.get(
                        category, str(category) if category is not None else ""
                    )
                except (ValueError, TypeError):
                    profiles_map[profile_key]["category"] = ""

            elif ind_type == "network:connected_profile":
                # Legacy indicator - treat as profile name
                profile_key = self._get_profile_key(path)
                if profile_key not in profiles_map:
                    profiles_map[profile_key] = {"path": path}
                profiles_map[profile_key]["name"] = ind["value"] or unknown_label

            elif ind_type == "network:mapped_drive":
                mapped_drives.append({
                    "path": ind["value"] or "",
                    "registry_path": ind.get("path", ""),
                })
            elif ind_type in self.NETWORK_INFO_LABELS:
                raw_value = ind["value"] or ""
                cleaned = self._clean_network_value(raw_value, ind_type, locale)

                # Skip meaningless values
                if not cleaned:
                    continue

                label = get_field_label(ind_type, locale)
                dedup_key = (ind_type, cleaned)
                if dedup_key not in items_dedup:
                    items_dedup[dedup_key] = {
                        "label": label,
                        "value": cleaned,
                        "path": ind.get("path", ""),
                    }

        items = list(items_dedup.values())

        # Convert profiles_map to list, filtering out incomplete entries
        profiles = []
        for profile_key, profile_data in profiles_map.items():
            if profile_data.get("name"):  # Only include if we have a name
                profiles.append({
                    "name": profile_data.get("name", unknown_label),
                    "network_type": profile_data.get("network_type", ""),
                    "category": profile_data.get("category", ""),
                    "last_connected": format_datetime(
                        profile_data.get("last_connected_raw", ""),
                        date_format,
                        include_time=True,
                        include_seconds=True,
                    ),
                    "created": format_datetime(
                        profile_data.get("created_raw", ""),
                        date_format,
                        include_time=True,
                        include_seconds=True,
                    ),
                    "last_connected_raw": profile_data.get("last_connected_raw", ""),
                    "created_raw": profile_data.get("created_raw", ""),
                    "path": profile_data.get("path", ""),
                })

        # Sort profiles by last_connected (most recent first)
        profiles.sort(key=lambda x: x.get("last_connected_raw", ""), reverse=True)

        return {
            "info_items": items,
            "profiles": profiles,
            "mapped_drives": mapped_drives,
            "count": len(items) + len(profiles) + len(mapped_drives),
        }

    def _get_profile_key(self, path: str) -> str:
        """Extract profile key from registry path.

        Removes the value name to get the key path that groups related values.
        E.g., 'Microsoft\\...\\Profiles\\{GUID}\\ProfileName' -> 'Microsoft\\...\\Profiles\\{GUID}'
        """
        if not path:
            return ""
        # Split by backslash and remove the last component (value name)
        parts = path.replace("/", "\\").rsplit("\\", 1)
        return parts[0] if parts else path

    def _format_value(
        self,
        ind_type: str,
        value: Optional[str],
        locale: str = "en",
        date_format: str = "eu",
    ) -> str:
        """Format indicator value for display.

        Note: Timestamps (install_date, last_shutdown) are now pre-formatted
        by the registry extractor, so no conversion is needed here.
        """
        if not value:
            return "—"

        # RDP status: convert numeric to human-readable
        if ind_type == "system:rdp_status":
            if value == "0":
                return "Aktiviert" if locale == "de" else "Enabled"
            elif value == "1":
                return "Deaktiviert" if locale == "de" else "Disabled"
            return value

        # Format known date fields according to report preference
        if ind_type in {"system:install_date", "system:last_shutdown"}:
            return format_datetime(value, date_format, include_time=True, include_seconds=True)

        return value

    def _extract_username(self, profile_path: str, unknown_label: str = "Unknown") -> str:
        """Extract username from profile path.

        E.g., 'C:\\Users\\John' -> 'John'
        """
        if not profile_path:
            return unknown_label
        # Normalize separators
        path = profile_path.replace("\\", "/")
        parts = path.rstrip("/").split("/")
        if parts:
            return parts[-1]
        return unknown_label

    def _clean_network_value(
        self, value: str, ind_type: str = "", locale: str = "en"
    ) -> str:
        """Clean a network indicator value for display.

        - Strips Python-list formatting: ``['x']`` → ``x``
        - Filters meaningless values: ``0``, empty strings, DLL refs
        - Translates proxy enable/disable
        """
        if not value:
            return ""

        v = value.strip()

        # Clean Python list formatting: ['192.168.178.1'] → 192.168.178.1
        if v.startswith("[") and v.endswith("]"):
            inner = v[1:-1].strip()
            parts = [p.strip().strip("'\"") for p in inner.split(",")]
            parts = [p for p in parts if p]
            v = ", ".join(parts)

        # Skip DLL resource references
        if v.startswith("@"):
            return ""

        # Domain "0" is meaningless (no domain configured)
        if ind_type == "network:domain" and v == "0":
            return ""

        # Proxy enable: translate numeric to readable
        if ind_type == "network:proxy_settings":
            if v == "0":
                return "Deaktiviert" if locale == "de" else "Disabled"
            if v == "1":
                return "Aktiviert" if locale == "de" else "Enabled"

        # Generic: filter pure-zero values for address fields
        if v == "0" and ind_type in (
            "network:dhcp_ip", "network:dns_server",
            "network:default_gateway", "network:dhcp_server",
        ):
            return ""

        return v

    def _extract_app_path_exe(self, path: str) -> str:
        """Extract the executable name from an App Paths registry path.

        E.g., ``...\\App Paths\\chrome.exe\\(Default)`` → ``chrome.exe``

        Returns:
            The executable filename, or empty string if not found.
        """
        if not path:
            return ""
        parts = path.replace("/", "\\").split("\\")
        for i, part in enumerate(parts):
            if part == "App Paths" and i + 1 < len(parts):
                return parts[i + 1]
        return ""

    def _build_browser_detection(
        self, indicators: List[Dict[str, Any]], locale: str = "en"
    ) -> Dict[str, Any]:
        """Build browser detection section data.

        Collects registered browsers, app paths, home/search pages, and
        IE/Edge search scopes from os_indicators.

        Returns dict with 'registered_browsers', 'app_paths', 'ie_settings' lists.
        """
        from ...locales import get_field_label

        registered_browsers = []
        app_paths = []
        ie_settings = []

        for ind in indicators:
            ind_type = ind["type"]

            if ind_type == "browser:registered_browser":
                # value contains the key name (browser ID) from extract=True path
                browser_name = ind["value"] or ""
                if browser_name:
                    registered_browsers.append({
                        "name": browser_name,
                        "path": ind.get("path", ""),
                    })

            elif ind_type == "browser:app_path":
                # Extract exe name from registry path
                # Path example: "...\App Paths\chrome.exe\(Default)"
                exe_name = self._extract_app_path_exe(ind.get("path", ""))
                entry_name = self._extract_entry_name(ind)
                app_paths.append({
                    "name": exe_name or entry_name or "",
                    "value": ind["value"] or "",
                    "path": ind.get("path", ""),
                })

            elif ind_type in ("browser:home_page", "browser:search_page"):
                label = "Home Page" if ind_type == "browser:home_page" else "Search Page"
                ie_settings.append({
                    "label": label,
                    "value": ind["value"] or "",
                    "path": ind.get("path", ""),
                })

            elif ind_type == "browser:search_scope":
                scope_name = self._extract_entry_name(ind)
                ie_settings.append({
                    "label": f"Search Scope: {scope_name or ''}",
                    "value": ind["value"] or "",
                    "path": ind.get("path", ""),
                })

        return {
            "registered_browsers": registered_browsers,
            "app_paths": app_paths,
            "ie_settings": ie_settings,
            "count": len(registered_browsers) + len(app_paths) + len(ie_settings),
        }

    def _build_user_activity(
        self,
        indicators: List[Dict[str, Any]],
        locale: str = "en",
        date_format: str = "eu",
    ) -> Dict[str, Any]:
        """Build user activity section data.

        Collects recent documents, typed URLs, typed paths, open/save dialog MRU,
        and explorer search terms from os_indicators.

        Returns dict with grouped activity lists.
        """
        import json

        recent_docs = []
        recent_images = []
        typed_urls = []
        typed_paths = []
        open_save_mru = []
        open_save_last_visited = []
        explorer_searches = []

        for ind in indicators:
            ind_type = ind["type"]
            entry_name = self._extract_entry_name(ind)

            if ind_type == "recent_documents":
                recent_docs.append({
                    "name": entry_name or "",
                    "value": ind["value"] or "",
                    "path": ind.get("path", ""),
                })

            elif ind_type == "recent_documents:image":
                recent_images.append({
                    "name": entry_name or "",
                    "value": ind["value"] or "",
                    "path": ind.get("path", ""),
                })

            elif ind_type == "browser:typed_url":
                typed_urls.append({
                    "name": entry_name or "",
                    "value": ind["value"] or "",
                    "path": ind.get("path", ""),
                })

            elif ind_type == "typed_paths":
                typed_paths.append({
                    "name": entry_name or "",
                    "value": ind["value"] or "",
                    "path": ind.get("path", ""),
                })

            elif ind_type == "open_save_dialog_mru":
                open_save_mru.append({
                    "name": entry_name or "",
                    "value": ind["value"] or "",
                    "path": ind.get("path", ""),
                })

            elif ind_type == "open_save_dialog_last_visited":
                open_save_last_visited.append({
                    "name": entry_name or "",
                    "value": ind["value"] or "",
                    "path": ind.get("path", ""),
                })

            elif ind_type == "user_activity:explorer_search":
                explorer_searches.append({
                    "name": entry_name or "",
                    "value": ind["value"] or "",
                    "path": ind.get("path", ""),
                })

        total = (
            len(recent_docs) + len(recent_images) + len(typed_urls) +
            len(typed_paths) + len(open_save_mru) + len(open_save_last_visited) +
            len(explorer_searches)
        )

        return {
            "recent_docs": recent_docs,
            "recent_images": recent_images,
            "typed_urls": typed_urls,
            "typed_paths": typed_paths,
            "open_save_mru": open_save_mru,
            "open_save_last_visited": open_save_last_visited,
            "explorer_searches": explorer_searches,
            "count": total,
        }

    def _build_execution_history(
        self,
        indicators: List[Dict[str, Any]],
        locale: str = "en",
        date_format: str = "eu",
    ) -> Dict[str, Any]:
        """Build execution history section data.

        Collects UserAssist entries showing program execution evidence.
        Parses extra_json for run_count, focus_count, focus_time, last_run,
        and decoded_path.

        Returns dict with 'entries' list sorted by run_count descending.
        """
        import json

        entries = []

        for ind in indicators:
            if ind["type"] != "execution:user_assist":
                continue

            decoded_path = ind["value"] or ""
            run_count = None
            focus_count = None
            focus_time = None
            last_run = None

            if ind.get("extra_json"):
                try:
                    extra = json.loads(ind["extra_json"])
                    decoded_path = extra.get("decoded_path", decoded_path)
                    run_count = extra.get("run_count")
                    focus_count = extra.get("focus_count")
                    focus_time = extra.get("focus_time")
                    last_run = extra.get("last_run")
                    if last_run:
                        last_run = format_datetime(
                            last_run, date_format,
                            include_time=True, include_seconds=True,
                        )
                except (json.JSONDecodeError, TypeError):
                    pass

            entries.append({
                "decoded_path": decoded_path,
                "run_count": run_count,
                "focus_count": focus_count,
                "focus_time": focus_time,
                "last_run": last_run or "",
                "path": ind.get("path", ""),
            })

        # Sort by run_count descending (None values last)
        entries.sort(key=lambda x: (x["run_count"] or 0), reverse=True)

        return {
            "entries": entries,
            "count": len(entries),
        }

    def format_config_summary(self, config: Dict[str, Any]) -> str:
        """Format a human-readable summary of the configuration."""
        sections = config.get("sections", [])
        if isinstance(sections, str):
            sections = [sections]

        section_labels = {
            self.SECTION_SYSTEM_INFO: "System",
            self.SECTION_USERS: "Users",
            self.SECTION_INSTALLED_SOFTWARE: "Software",
            self.SECTION_AUTOSTART: "Autostart",
            self.SECTION_NETWORK: "Network",
            self.SECTION_BROWSER_DETECTION: "Browsers",
            self.SECTION_USER_ACTIVITY: "Activity",
            self.SECTION_EXECUTION_HISTORY: "Execution",
        }

        selected = [section_labels.get(s, s) for s in sections]
        return f"Sections: {', '.join(selected)}" if selected else "No sections selected"
