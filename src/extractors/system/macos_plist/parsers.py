"""
macOS Plist Extractor — parser functions.

Pure parsing functions using plistlib (stdlib). Each returns a list of indicator
dicts ready for the ``os_indicators`` table.

All functions follow the same contract:
    parse_*(plist_data: dict, source_path: str, run_id: str) -> list[dict]

If a plist is empty or missing expected keys the parser returns ``[]``
instead of raising.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _indicator(
    indicator_type: str,
    name: str,
    value: str,
    source_path: str,
    run_id: str,
    *,
    confidence: str = "high",
    extra: Optional[Dict[str, Any]] = None,
    detected_at_utc: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a single indicator dict for ``insert_os_indicators``."""
    return {
        "type": indicator_type,
        "name": name,
        "value": value,
        "path": source_path,
        "hive": source_path,            # repurposed: source plist path
        "confidence": confidence,
        "detected_at_utc": detected_at_utc or datetime.now(timezone.utc).isoformat(),
        "provenance": "macos_plist",
        "extra_json": json.dumps(extra) if extra else None,
        "run_id": run_id,
    }


def _safe_str(val: Any) -> str:
    """Convert arbitrary plist value to a safe string representation."""
    if val is None:
        return ""
    if isinstance(val, bytes):
        return val.hex()
    if isinstance(val, datetime):
        return val.isoformat()
    return str(val)


# ---------------------------------------------------------------------------
# System parsers
# ---------------------------------------------------------------------------

def parse_system_version(plist_data: dict, source_path: str, run_id: str) -> List[Dict[str, Any]]:
    """Parse ``SystemVersion.plist`` → ``system:os_version_macos`` indicators."""
    product_name = plist_data.get("ProductName")
    product_version = plist_data.get("ProductVersion")
    build_version = plist_data.get("ProductBuildVersion")

    if not product_version:
        return []

    display = product_name or "macOS"
    value = f"{display} {product_version}"
    if build_version:
        value += f" ({build_version})"

    extra = {
        "product_name": _safe_str(product_name),
        "product_version": _safe_str(product_version),
        "build_version": _safe_str(build_version),
    }

    return [_indicator("system:os_version_macos", "macOS Version", value, source_path, run_id, extra=extra)]


def parse_system_profiler(plist_data: dict, source_path: str, run_id: str) -> List[Dict[str, Any]]:
    """Parse ``com.apple.SystemProfiler.plist`` → ``system:hardware_macos``."""
    results: List[Dict[str, Any]] = []

    # SystemProfiler stores CPU type in different key layouts depending on version
    # "CPU Names" is handled separately below (it's often a dict)
    for key in ("CPU Speed", "Model Name", "Machine Name"):
        val = plist_data.get(key)
        if val:
            results.append(
                _indicator(
                    "system:hardware_macos",
                    f"Hardware: {key}",
                    _safe_str(val),
                    source_path,
                    run_id,
                    extra={"profiler_key": key, "raw_value": _safe_str(val)},
                )
            )

    # The "CPU Names" key is often a dict mapping arch → description
    cpu_names = plist_data.get("CPU Names")
    if isinstance(cpu_names, dict):
        for arch, desc in cpu_names.items():
            results.append(
                _indicator(
                    "system:hardware_macos",
                    f"CPU ({arch})",
                    _safe_str(desc),
                    source_path,
                    run_id,
                    extra={"architecture": _safe_str(arch), "cpu_name": _safe_str(desc)},
                )
            )

    return results


def parse_global_preferences(plist_data: dict, source_path: str, run_id: str) -> List[Dict[str, Any]]:
    """Parse ``.GlobalPreferences.plist`` → timezone / locale indicators."""
    results: List[Dict[str, Any]] = []

    tz = plist_data.get("AppleTimezone") or plist_data.get("com.apple.timezone")
    if tz:
        results.append(
            _indicator("system:timezone_macos", "Timezone", _safe_str(tz), source_path, run_id)
        )

    locale = plist_data.get("AppleLocale")
    if locale:
        results.append(
            _indicator("system:timezone_macos", "Locale", _safe_str(locale), source_path, run_id)
        )

    languages = plist_data.get("AppleLanguages")
    if isinstance(languages, (list, tuple)) and languages:
        results.append(
            _indicator(
                "system:timezone_macos",
                "Preferred Languages",
                ", ".join(str(l) for l in languages),
                source_path,
                run_id,
                extra={"languages": [str(l) for l in languages]},
            )
        )

    return results


def parse_network_config(plist_data: dict, source_path: str, run_id: str) -> List[Dict[str, Any]]:
    """Parse ``SystemConfiguration/preferences.plist`` → network config indicators."""
    results: List[Dict[str, Any]] = []

    # Computer name
    sys_prefs = plist_data.get("System") or {}
    sys_name = sys_prefs.get("System", {}).get("ComputerName") if isinstance(sys_prefs, dict) else None
    if sys_name:
        results.append(
            _indicator("system:network_config_macos", "Computer Name", _safe_str(sys_name), source_path, run_id)
        )

    # Hostname from its direct key
    hostname = sys_prefs.get("System", {}).get("HostName") if isinstance(sys_prefs, dict) else None
    if hostname:
        results.append(
            _indicator("system:network_config_macos", "Hostname", _safe_str(hostname), source_path, run_id)
        )

    # Network interfaces
    net_interfaces = plist_data.get("NetworkServices")
    if isinstance(net_interfaces, dict):
        for svc_id, svc_data in net_interfaces.items():
            if not isinstance(svc_data, dict):
                continue
            iface_name = svc_data.get("UserDefinedName", svc_id)
            ipv4 = svc_data.get("IPv4", {})
            method = ipv4.get("ConfigMethod", "") if isinstance(ipv4, dict) else ""
            results.append(
                _indicator(
                    "system:network_config_macos",
                    f"Network Interface: {iface_name}",
                    f"Method={method}",
                    source_path,
                    run_id,
                    confidence="medium",
                    extra={
                        "service_id": svc_id,
                        "interface_name": _safe_str(iface_name),
                        "ipv4_config_method": _safe_str(method),
                    },
                )
            )

    return results


# ---------------------------------------------------------------------------
# Installed applications
# ---------------------------------------------------------------------------

def parse_app_info_plist(plist_data: dict, source_path: str, run_id: str) -> List[Dict[str, Any]]:
    """Parse ``Info.plist`` from an application → ``system:installed_app_macos``."""
    bundle_name = plist_data.get("CFBundleName") or plist_data.get("CFBundleDisplayName")
    bundle_id = plist_data.get("CFBundleIdentifier")
    bundle_version = plist_data.get("CFBundleShortVersionString") or plist_data.get("CFBundleVersion")

    # Must have at least a name or identifier
    if not bundle_name and not bundle_id:
        return []

    display_name = bundle_name or bundle_id or "Unknown"
    value = display_name
    if bundle_version:
        value += f" {bundle_version}"

    extra: Dict[str, Any] = {
        "name": _safe_str(display_name),
        "bundle_identifier": _safe_str(bundle_id),
        "version": _safe_str(bundle_version),
    }

    # Optional metadata
    for key in ("CFBundleExecutable", "LSMinimumSystemVersion", "DTSDKName",
                "CFBundlePackageType", "NSHumanReadableCopyright"):
        val = plist_data.get(key)
        if val:
            extra[key] = _safe_str(val)

    return [_indicator("system:installed_app_macos", display_name, value, source_path, run_id, extra=extra)]


def parse_install_receipt(plist_data: dict, source_path: str, run_id: str) -> List[Dict[str, Any]]:
    """Parse installer receipt plist → ``system:install_receipt_macos``."""
    pkg_id = plist_data.get("PackageIdentifier") or plist_data.get("packageIdentifier")
    pkg_version = plist_data.get("PackageVersion") or plist_data.get("packageVersion")
    install_date = plist_data.get("InstallDate") or plist_data.get("installDate")

    if not pkg_id:
        return []

    value = pkg_id
    if pkg_version:
        value += f" ({pkg_version})"

    detected_at_utc = None
    if isinstance(install_date, datetime):
        detected_at_utc = install_date.isoformat()

    extra = {
        "package_identifier": _safe_str(pkg_id),
        "package_version": _safe_str(pkg_version),
        "install_date": _safe_str(install_date),
        "install_process_name": _safe_str(plist_data.get("InstallProcessName")),
        "install_prefix_path": _safe_str(plist_data.get("InstallPrefixPath")),
    }

    return [
        _indicator(
            "system:install_receipt_macos",
            _safe_str(pkg_id),
            value,
            source_path,
            run_id,
            extra=extra,
            detected_at_utc=detected_at_utc,
        )
    ]


# ---------------------------------------------------------------------------
# Application execution
# ---------------------------------------------------------------------------

def parse_launch_services(plist_data: dict, source_path: str, run_id: str) -> List[Dict[str, Any]]:
    """Parse LaunchServices plist → ``execution:launch_services``."""
    results: List[Dict[str, Any]] = []

    # LSHandlers contains URL scheme and document type handler mappings
    handlers = plist_data.get("LSHandlers", [])
    if not isinstance(handlers, list):
        return results

    for handler in handlers:
        if not isinstance(handler, dict):
            continue

        # URL scheme handler
        url_scheme = handler.get("LSHandlerURLScheme")
        role_all = handler.get("LSHandlerRoleAll") or handler.get("LSHandlerRoleViewer")

        if url_scheme and role_all:
            results.append(
                _indicator(
                    "execution:launch_services",
                    f"URL Scheme Handler: {url_scheme}",
                    _safe_str(role_all),
                    source_path,
                    run_id,
                    confidence="medium",
                    extra={
                        "url_scheme": _safe_str(url_scheme),
                        "handler_bundle_id": _safe_str(role_all),
                    },
                )
            )

        # Content type handler
        content_type = handler.get("LSHandlerContentType")
        if content_type and role_all:
            results.append(
                _indicator(
                    "execution:launch_services",
                    f"Content Type Handler: {content_type}",
                    _safe_str(role_all),
                    source_path,
                    run_id,
                    confidence="medium",
                    extra={
                        "content_type": _safe_str(content_type),
                        "handler_bundle_id": _safe_str(role_all),
                    },
                )
            )

    return results


# ---------------------------------------------------------------------------
# User activity
# ---------------------------------------------------------------------------

def parse_recent_items(plist_data: dict, source_path: str, run_id: str) -> List[Dict[str, Any]]:
    """Parse ``com.apple.recentitems.plist`` → ``user_activity:recent_items``."""
    results: List[Dict[str, Any]] = []

    # Recent items can have Hosts, RecentDocuments, RecentApplications, RecentServers
    for category in ("RecentDocuments", "RecentApplications", "RecentServers", "Hosts"):
        section = plist_data.get(category, {})
        if not isinstance(section, dict):
            continue
        items = section.get("CustomListItems", [])
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            name = item.get("Name", "")
            if not name:
                continue
            # Bookmark data may contain file path info but is binary
            extra = {"category": category, "name": _safe_str(name)}
            bookmark = item.get("Bookmark")
            if isinstance(bookmark, bytes):
                extra["has_bookmark"] = "true"

            results.append(
                _indicator(
                    "user_activity:recent_items",
                    f"{category}: {name}",
                    _safe_str(name),
                    source_path,
                    run_id,
                    extra=extra,
                )
            )

    return results


def parse_finder_preferences(plist_data: dict, source_path: str, run_id: str) -> List[Dict[str, Any]]:
    """Parse ``com.apple.finder.plist`` → ``user_activity:finder_prefs``."""
    results: List[Dict[str, Any]] = []

    # Recent folders (FXRecentFolders)
    recent_folders = plist_data.get("FXRecentFolders", [])
    if isinstance(recent_folders, list):
        for folder in recent_folders:
            if not isinstance(folder, dict):
                continue
            name = folder.get("name", "")
            if name:
                results.append(
                    _indicator(
                        "user_activity:finder_prefs",
                        f"Recent Folder: {name}",
                        _safe_str(name),
                        source_path,
                        run_id,
                        extra={"folder_name": name},
                    )
                )

    # Desktop view settings (can reveal user behaviour)
    desktop_view = plist_data.get("DesktopViewSettings")
    if isinstance(desktop_view, dict):
        icon_view = desktop_view.get("IconViewSettings", {})
        if isinstance(icon_view, dict):
            arrange_by = icon_view.get("arrangeBy")
            if arrange_by:
                results.append(
                    _indicator(
                        "user_activity:finder_prefs",
                        "Desktop ArrangeBy",
                        _safe_str(arrange_by),
                        source_path,
                        run_id,
                        confidence="low",
                    )
                )

    # Show hidden files preference
    show_all = plist_data.get("AppleShowAllFiles")
    if show_all is not None:
        results.append(
            _indicator(
                "user_activity:finder_prefs",
                "Show All Files",
                str(show_all),
                source_path,
                run_id,
                confidence="medium",
                extra={"forensic_note": "User can see hidden files — may indicate advanced user"},
            )
        )

    return results


def parse_spotlight_preferences(plist_data: dict, source_path: str, run_id: str) -> List[Dict[str, Any]]:
    """Parse ``com.apple.Spotlight.plist`` → ``user_activity:spotlight_searches``."""
    results: List[Dict[str, Any]] = []

    # UserShortcuts holds Spotlight search shortcuts
    shortcuts = plist_data.get("UserShortcuts")
    if isinstance(shortcuts, dict):
        for query, data in shortcuts.items():
            display_name = data.get("DISPLAY_NAME", query) if isinstance(data, dict) else query
            results.append(
                _indicator(
                    "user_activity:spotlight_searches",
                    f"Spotlight Shortcut: {query}",
                    _safe_str(display_name),
                    source_path,
                    run_id,
                    extra={"shortcut_key": query, "display_name": _safe_str(display_name)},
                )
            )

    # orderedItems — disabled search categories (privacy indicator)
    ordered = plist_data.get("orderedItems")
    if isinstance(ordered, list):
        disabled = [
            item.get("name", "?")
            for item in ordered
            if isinstance(item, dict) and not item.get("enabled", True)
        ]
        if disabled:
            results.append(
                _indicator(
                    "user_activity:spotlight_searches",
                    "Disabled Spotlight Categories",
                    ", ".join(disabled),
                    source_path,
                    run_id,
                    confidence="medium",
                    extra={"disabled_categories": disabled},
                )
            )

    return results


def parse_quarantine_events(db_path: str, source_path: str, run_id: str) -> List[Dict[str, Any]]:
    """
    Parse ``QuarantineEventsV2`` SQLite database → ``user_activity:quarantine_events``.

    NOTE: This file is actually a SQLite database, *not* a plist.
    The extractor copies it alongside plist files and this parser reads it
    with ``sqlite3``.
    """
    results: List[Dict[str, Any]] = []

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(
                "SELECT LSQuarantineEventIdentifier, LSQuarantineTimeStamp, "
                "LSQuarantineAgentBundleIdentifier, LSQuarantineAgentName, "
                "LSQuarantineDataURLString, LSQuarantineOriginURLString, "
                "LSQuarantineSenderName, LSQuarantineSenderAddress, "
                "LSQuarantineTypeNumber "
                "FROM LSQuarantineEvent "
                "ORDER BY LSQuarantineTimeStamp DESC "
                "LIMIT 5000"
            )
            for row in cursor:
                agent_name = row["LSQuarantineAgentName"] or ""
                origin_url = row["LSQuarantineOriginURLString"] or ""
                data_url = row["LSQuarantineDataURLString"] or ""
                timestamp_raw = row["LSQuarantineTimeStamp"]

                # macOS Quarantine timestamps are seconds since 2001-01-01 00:00:00 UTC
                detected_at_utc = None
                if timestamp_raw is not None:
                    try:
                        # Cocoa epoch: 2001-01-01 00:00:00 UTC
                        cocoa_epoch = datetime(2001, 1, 1, tzinfo=timezone.utc)
                        ts = cocoa_epoch.timestamp() + float(timestamp_raw)
                        detected_at_utc = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                    except (ValueError, TypeError, OverflowError):
                        pass

                value_parts = []
                if agent_name:
                    value_parts.append(f"Agent: {agent_name}")
                if origin_url:
                    value_parts.append(f"From: {origin_url}")
                elif data_url:
                    value_parts.append(f"URL: {data_url}")

                results.append(
                    _indicator(
                        "user_activity:quarantine_events",
                        f"Quarantine: {agent_name or 'unknown'}",
                        " | ".join(value_parts) if value_parts else "quarantine event",
                        source_path,
                        run_id,
                        extra={
                            "event_id": _safe_str(row["LSQuarantineEventIdentifier"]),
                            "agent_bundle_id": _safe_str(row["LSQuarantineAgentBundleIdentifier"]),
                            "agent_name": _safe_str(agent_name),
                            "data_url": _safe_str(data_url),
                            "origin_url": _safe_str(origin_url),
                            "sender_name": _safe_str(row["LSQuarantineSenderName"]),
                            "sender_address": _safe_str(row["LSQuarantineSenderAddress"]),
                            "type_number": _safe_str(row["LSQuarantineTypeNumber"]),
                        },
                        detected_at_utc=detected_at_utc,
                    )
                )
        finally:
            conn.close()
    except Exception as exc:
        # File may not be a valid SQLite DB or table may not exist — graceful fallback
        import logging
        logging.getLogger("extractors.system.macos_plist.parsers").debug(
            "Could not parse quarantine DB %s: %s", db_path, exc
        )

    return results


# ---------------------------------------------------------------------------
# Browser trace
# ---------------------------------------------------------------------------

def parse_default_browser(plist_data: dict, source_path: str, run_id: str) -> List[Dict[str, Any]]:
    """Parse ``launchservices.secure.plist`` → default browser detection."""
    results: List[Dict[str, Any]] = []

    handlers = plist_data.get("LSHandlers", [])
    if not isinstance(handlers, list):
        return results

    for handler in handlers:
        if not isinstance(handler, dict):
            continue

        url_scheme = handler.get("LSHandlerURLScheme", "")
        role_all = handler.get("LSHandlerRoleAll", "")

        if url_scheme in ("http", "https") and role_all:
            results.append(
                _indicator(
                    "browser_trace:default_browser_macos",
                    f"Default Browser ({url_scheme})",
                    _safe_str(role_all),
                    source_path,
                    run_id,
                    extra={
                        "url_scheme": url_scheme,
                        "handler_bundle_id": _safe_str(role_all),
                    },
                )
            )

    return results
