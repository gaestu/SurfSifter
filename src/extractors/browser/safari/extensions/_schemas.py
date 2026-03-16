"""
Safari Extensions schema definitions and known keys.

Covers all 3 eras of Safari extensions:
- Legacy (.safariextz / Extensions.plist): Safari 5–12
- App Extension (.appex bundles): macOS 10.12+
- Web Extension (manifest.json inside .appex): Safari 14+

Used by parsers and the extractor for schema warning discovery.
"""
from __future__ import annotations

from typing import Set

# Re-use Chromium's manifest.json known keys for Web Extensions
from extractors.browser.chromium.extensions._schemas import (
    KNOWN_MANIFEST_KEYS,
)

# =============================================================================
# Legacy Era: Extensions.plist (Safari 5–12)
# =============================================================================

# Top-level keys in ~/Library/Safari/Extensions/Extensions.plist
KNOWN_EXTENSIONS_PLIST_KEYS: Set[str] = {
    "Installed Extensions",
}

# Per-extension entry keys within the "Installed Extensions" array
KNOWN_EXTENSION_ENTRY_KEYS: Set[str] = {
    "Archive File Name",
    "Bundle Directory Name",
    "Enabled",
    "Hidden Bars",
    "Added Date",
    "Apple-signed",
    "Author Certificate",
    "Bundle Identifier",
    "Bundle Version",
    "Developer Identifier",
}

# =============================================================================
# Legacy Era: .safariextz Info.plist keys
# =============================================================================

# Keys found inside extracted .safariextz bundles' Info.plist
KNOWN_SAFARIEXTZ_INFO_KEYS: Set[str] = {
    "CFBundleDisplayName",
    "CFBundleIdentifier",
    "CFBundleShortVersionString",
    "CFBundleVersion",
    "Author",
    "Description",
    "Website",
    "Content",
    "Permissions",
    "Update Manifest URL",
    # Standard CFBundle keys commonly present
    "CFBundleName",
    "CFBundleInfoDictionaryVersion",
    "CFBundleDevelopmentRegion",
    "CFBundleExecutable",
    "CFBundlePackageType",
    "CFBundleSignature",
    "CFBundleSupportedPlatforms",
    "DTPlatformBuild",
    "DTPlatformVersion",
    "DTSDKBuild",
    "DTSDKName",
    "DTXcode",
    "DTXcodeBuild",
    "BuildMachineOSBuild",
}

# =============================================================================
# App Extension Era: .appex/Contents/Info.plist (macOS 10.12+)
# =============================================================================

# NSExtension dictionary keys within .appex Info.plist
KNOWN_NSEXTENSION_KEYS: Set[str] = {
    "NSExtensionPointIdentifier",
    "NSExtensionPrincipalClass",
    "SFSafariToolbarItem",
    "SFSafariContentScript",
    "SFSafariWebsiteAccess",
    "SFSafariStyleSheet",
    "SFSafariContextMenu",
}

# Safari-specific NSExtensionPointIdentifier values
SAFARI_EXTENSION_POINT_IDENTIFIERS: Set[str] = {
    "com.apple.Safari.extension",
    "com.apple.Safari.content-blocker",
    "com.apple.Safari.web-extension",
}

# Standard Info.plist keys found in .appex bundles
KNOWN_APPEX_INFO_KEYS: Set[str] = {
    "CFBundleDisplayName",
    "CFBundleExecutable",
    "CFBundleIdentifier",
    "CFBundleInfoDictionaryVersion",
    "CFBundleName",
    "CFBundlePackageType",
    "CFBundleShortVersionString",
    "CFBundleVersion",
    "CFBundleDevelopmentRegion",
    "CFBundleSignature",
    "CFBundleSupportedPlatforms",
    "NSExtension",
    "NSHumanReadableCopyright",
    "LSMinimumSystemVersion",
    "BuildMachineOSBuild",
    "DTPlatformBuild",
    "DTPlatformName",
    "DTPlatformVersion",
    "DTSDKBuild",
    "DTSDKName",
    "DTXcode",
    "DTXcodeBuild",
    "DTCompiler",
}

# =============================================================================
# Exported for convenience
# =============================================================================

__all__ = [
    "KNOWN_EXTENSIONS_PLIST_KEYS",
    "KNOWN_EXTENSION_ENTRY_KEYS",
    "KNOWN_SAFARIEXTZ_INFO_KEYS",
    "KNOWN_NSEXTENSION_KEYS",
    "SAFARI_EXTENSION_POINT_IDENTIFIERS",
    "KNOWN_APPEX_INFO_KEYS",
    "KNOWN_MANIFEST_KEYS",
]
