"""Tests for Safari extension parsers."""

from __future__ import annotations

import json
import plistlib
from datetime import datetime, timezone
from pathlib import Path

from extractors.browser.safari._parsers import (
    SafariExtension,
    get_extension_stats,
    parse_appex_info_plist,
    parse_extensions_plist,
    parse_safariextz_info,
    parse_webextension_manifest,
)


def _plist_bytes(payload: object) -> bytes:
    return plistlib.dumps(payload, fmt=plistlib.FMT_BINARY)


# =========================================================================
# parse_extensions_plist
# =========================================================================


def test_parse_extensions_plist_valid(tmp_path: Path) -> None:
    """Multiple extensions with all fields."""
    added = datetime(2023, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
    payload = {
        "Installed Extensions": [
            {
                "Bundle Identifier": "com.example.myaddon",
                "Archive File Name": "MyAddon.safariextz",
                "Bundle Directory Name": "MyAddon.safariextension",
                "Enabled": True,
                "Added Date": added,
                "Apple-signed": False,
                "Developer Identifier": "DEV123",
                "Bundle Version": "1.2.3",
            },
            {
                "Bundle Identifier": "com.apple.safari.contentblocker",
                "Bundle Directory Name": "ContentBlocker.safariextension",
                "Enabled": False,
                "Apple-signed": True,
                "Bundle Version": "2.0",
            },
        ]
    }
    path = tmp_path / "Extensions.plist"
    # Use XML format since FMT_BINARY doesn't support datetime objects
    path.write_bytes(plistlib.dumps(payload, fmt=plistlib.FMT_XML))
    exts = parse_extensions_plist(path)

    assert len(exts) == 2

    ext0 = exts[0]
    assert ext0.bundle_identifier == "com.example.myaddon"
    assert ext0.name == "MyAddon"  # .safariextz stripped
    assert ext0.version == "1.2.3"
    assert ext0.enabled is True
    assert ext0.apple_signed is False
    assert ext0.developer_identifier == "DEV123"
    assert ext0.added_date_utc is not None
    assert ext0.extension_era == "legacy"
    assert ext0.unknown_keys == []

    ext1 = exts[1]
    assert ext1.bundle_identifier == "com.apple.safari.contentblocker"
    assert ext1.name == "ContentBlocker.safariextension"  # from BundleDirectoryName
    assert ext1.enabled is False
    assert ext1.apple_signed is True


def test_parse_extensions_plist_empty(tmp_path: Path) -> None:
    """No extensions → empty list."""
    payload = {"Installed Extensions": []}
    path = tmp_path / "Extensions.plist"
    path.write_bytes(_plist_bytes(payload))
    assert parse_extensions_plist(path) == []


def test_parse_extensions_plist_corrupt(tmp_path: Path) -> None:
    """Corrupt plist → empty list, no crash."""
    path = tmp_path / "Extensions.plist"
    path.write_bytes(b"\x00\x01corrupt garbage")
    assert parse_extensions_plist(path) == []


def test_parse_extensions_plist_unknown_keys(tmp_path: Path) -> None:
    """Unknown keys collected in unknown_keys."""
    payload = {
        "Installed Extensions": [
            {
                "Bundle Identifier": "com.test.ext",
                "Bundle Directory Name": "Test",
                "Enabled": True,
                "SomeNewKey": "value",
                "AnotherUnknown": 42,
            },
        ]
    }
    path = tmp_path / "Extensions.plist"
    path.write_bytes(_plist_bytes(payload))
    exts = parse_extensions_plist(path)
    assert len(exts) == 1
    assert sorted(exts[0].unknown_keys) == ["AnotherUnknown", "SomeNewKey"]


# =========================================================================
# parse_safariextz_info
# =========================================================================


def test_parse_safariextz_info_valid(tmp_path: Path) -> None:
    payload = {
        "CFBundleIdentifier": "com.example.addon",
        "CFBundleDisplayName": "My Addon",
        "CFBundleShortVersionString": "1.0.0",
        "Description": "Does things",
        "Author": "Jane Doe",
        "Website": "https://example.com",
        "Permissions": {"WebsiteAccess": "Some Sites", "AllowedDomains": ["example.com"]},
    }
    path = tmp_path / "Info.plist"
    path.write_bytes(_plist_bytes(payload))
    ext = parse_safariextz_info(path)
    assert ext is not None
    assert ext.bundle_identifier == "com.example.addon"
    assert ext.name == "My Addon"
    assert ext.version == "1.0.0"
    assert ext.description == "Does things"
    assert ext.author == "Jane Doe"
    assert ext.website == "https://example.com"
    assert ext.permissions is not None
    assert ext.extension_era == "legacy"


def test_parse_safariextz_info_corrupt(tmp_path: Path) -> None:
    path = tmp_path / "Info.plist"
    path.write_bytes(b"not a plist")
    assert parse_safariextz_info(path) is None


# =========================================================================
# parse_appex_info_plist
# =========================================================================


def test_parse_appex_info_plist_safari(tmp_path: Path) -> None:
    """Safari extension point → returns SafariExtension."""
    payload = {
        "CFBundleIdentifier": "com.example.app.Extension",
        "CFBundleName": "Example Extension",
        "CFBundleShortVersionString": "2.1",
        "NSExtension": {
            "NSExtensionPointIdentifier": "com.apple.Safari.extension",
            "NSExtensionPrincipalClass": "SafariExtensionHandler",
            "SFSafariWebsiteAccess": {
                "Allowed Domains": ["*.example.com"],
                "Level": "Some",
            },
        },
        "NSHumanReadableCopyright": "Copyright 2024",
    }
    path = tmp_path / "Info.plist"
    path.write_bytes(_plist_bytes(payload))
    ext = parse_appex_info_plist(path)
    assert ext is not None
    assert ext.bundle_identifier == "com.example.app.Extension"
    assert ext.name == "Example Extension"
    assert ext.version == "2.1"
    assert ext.extension_era == "app_extension"
    assert ext.extension_point == "com.apple.Safari.extension"
    assert ext.permissions is not None  # SFSafariWebsiteAccess serialized


def test_parse_appex_info_plist_non_safari(tmp_path: Path) -> None:
    """Non-Safari extension point → returns None."""
    payload = {
        "CFBundleIdentifier": "com.example.app.ShareExtension",
        "CFBundleName": "Share Extension",
        "NSExtension": {
            "NSExtensionPointIdentifier": "com.apple.share-services",
        },
    }
    path = tmp_path / "Info.plist"
    path.write_bytes(_plist_bytes(payload))
    assert parse_appex_info_plist(path) is None


def test_parse_appex_info_plist_web_extension_point(tmp_path: Path) -> None:
    """Web extension point identifier also accepted."""
    payload = {
        "CFBundleIdentifier": "com.example.webext",
        "CFBundleDisplayName": "Web Ext",
        "NSExtension": {
            "NSExtensionPointIdentifier": "com.apple.Safari.web-extension",
        },
    }
    path = tmp_path / "Info.plist"
    path.write_bytes(_plist_bytes(payload))
    ext = parse_appex_info_plist(path)
    assert ext is not None
    assert ext.extension_point == "com.apple.Safari.web-extension"


# =========================================================================
# parse_webextension_manifest
# =========================================================================


def test_parse_webextension_manifest(tmp_path: Path) -> None:
    """Valid manifest.json → SafariExtension with permissions."""
    manifest = {
        "manifest_version": 3,
        "name": "My Web Extension",
        "version": "1.0",
        "description": "A web extension",
        "permissions": ["tabs", "storage"],
        "host_permissions": ["*://*.example.com/*"],
        "content_scripts": [{"matches": ["*://*.example.com/*"], "js": ["content.js"]}],
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")

    bundle_info = SafariExtension(
        bundle_identifier="com.example.app.WebExtension",
        name="Parent App",
        version="2.0",
        extension_era="app_extension",
        extension_point="com.apple.Safari.web-extension",
    )

    ext = parse_webextension_manifest(path, bundle_info=bundle_info)
    assert ext is not None
    assert ext.bundle_identifier == "com.example.app.WebExtension"  # from bundle_info
    assert ext.name == "My Web Extension"  # from manifest
    assert ext.version == "1.0"
    assert ext.manifest_version == 3
    assert ext.extension_era == "web_extension"
    assert ext.permissions is not None
    assert "tabs" in ext.permissions
    assert ext.host_permissions is not None
    assert ext.content_scripts is not None


def test_parse_webextension_manifest_no_bundle_info(tmp_path: Path) -> None:
    """Manifest parsed without bundle_info still works."""
    manifest = {
        "manifest_version": 2,
        "name": "Standalone",
        "version": "0.1",
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")

    ext = parse_webextension_manifest(path)
    assert ext is not None
    assert ext.name == "Standalone"
    assert ext.bundle_identifier == "Standalone"  # falls back to name
    assert ext.extension_era == "web_extension"


def test_parse_webextension_manifest_corrupt(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text("not json {{", encoding="utf-8")
    assert parse_webextension_manifest(path) is None


# =========================================================================
# get_extension_stats
# =========================================================================


def test_get_extension_stats() -> None:
    """Correct counts by era, enabled, signed."""
    extensions = [
        SafariExtension(
            bundle_identifier="a", name="A", enabled=True,
            apple_signed=True, extension_era="legacy",
        ),
        SafariExtension(
            bundle_identifier="b", name="B", enabled=False,
            apple_signed=False, extension_era="legacy",
        ),
        SafariExtension(
            bundle_identifier="c", name="C",
            extension_era="app_extension",
        ),
        SafariExtension(
            bundle_identifier="d", name="D",
            extension_era="web_extension",
        ),
    ]
    stats = get_extension_stats(extensions)
    assert stats["total_count"] == 4
    assert stats["enabled_count"] == 1
    assert stats["apple_signed_count"] == 1
    assert stats["by_era"]["legacy"] == 2
    assert stats["by_era"]["app_extension"] == 1
    assert stats["by_era"]["web_extension"] == 1


def test_get_extension_stats_empty() -> None:
    stats = get_extension_stats([])
    assert stats["total_count"] == 0
    assert stats["enabled_count"] == 0
