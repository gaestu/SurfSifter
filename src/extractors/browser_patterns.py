"""
Shared Browser Patterns

Centralized browser path patterns for all browser artifact extractors.
This prevents drift between history/cache/cookies/bookmarks extractors.

Supported browsers:
- Chrome, Edge, Firefox, Safari (existing)
- Opera, Brave (new)

Cache format support:
- Simple cache: {16-hex}_0/_1/_s files, index-dir/the-real-index
- Blockfile cache: data_0/1/2/3, index, f_* (legacy pre-2015)

Phase 1 Browser Artifacts:
- Autofill: Web Data SQLite (Chromium), formhistory.sqlite (Firefox)
- Sessions: Current Session/Current Tabs/Session Storage (Chromium), sessionstore.jsonlz4 (Firefox)
- Permissions: Preferences JSON (Chromium), permissions.sqlite (Firefox)
- Media History: Media History SQLite (Chromium 86+, no Firefox equivalent)

Phase 2 Browser Forensics:
- Transport Security: TransportSecurity (Chromium), SiteSecurityServiceState.txt (Firefox)
- Jump Lists: Windows taskbar shortcuts (*.automaticDestinations-ms, *.customDestinations-ms)

Phase 3 Deep Storage & Apps:
- Extensions: Extensions/{id}/{version}/manifest.json (Chromium), extensions.json (Firefox)
- Local Storage: Local Storage/leveldb/ (Chromium), webappsstore.sqlite (Firefox)
- Session Storage: Session Storage/ (Chromium), Firefox ephemeral
- IndexedDB: IndexedDB/ (Chromium LevelDB), storage/default/*/idb/*.sqlite (Firefox SQLite)
- Sync Data: Sync Data/ (Chromium LevelDB), signedInUser.json (Firefox)
"""

from __future__ import annotations

from typing import Dict, List, Any


# Comprehensive browser patterns supporting Windows, macOS, and Linux
BROWSER_PATTERNS: Dict[str, Dict[str, Any]] = {
    "chrome": {
        "display_name": "Google Chrome",
        "engine": "chromium",
        "paths": {
            "history": [
                # Windows - explicit Default + Profile patterns
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/History",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */History",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/History",
                "Users/*/Library/Application Support/Google/Chrome/Profile */History",
                # Linux
                "home/*/.config/google-chrome/Default/History",
                "home/*/.config/google-chrome/Profile */History",
            ],
            "cookies": [
                # Windows
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Cookies",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Cookies",
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Network/Cookies",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Network/Cookies",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/Cookies",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Cookies",
                "Users/*/Library/Application Support/Google/Chrome/Default/Network/Cookies",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Network/Cookies",
                # Linux
                "home/*/.config/google-chrome/Default/Cookies",
                "home/*/.config/google-chrome/Profile */Cookies",
                "home/*/.config/google-chrome/Default/Network/Cookies",
                "home/*/.config/google-chrome/Profile */Network/Cookies",
            ],
            "bookmarks": [
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Bookmarks",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Bookmarks",
                "Users/*/Library/Application Support/Google/Chrome/Default/Bookmarks",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Bookmarks",
                "home/*/.config/google-chrome/Default/Bookmarks",
                "home/*/.config/google-chrome/Profile */Bookmarks",
            ],
            "cache": [
                # Windows - Entry files (modern simple cache format)
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Cache/Cache_Data/[0-9a-f]*_s",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Cache/Cache_Data/[0-9a-f]*_s",
                # Windows - Simple cache index files
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Cache/Cache_Data/index",
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Cache/Cache_Data/index-dir/the-real-index",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Cache/Cache_Data/index",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Cache/Cache_Data/index-dir/the-real-index",
                # Windows - Block files (simple cache external files)
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Cache/Cache_Data/f_*",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Cache/Cache_Data/f_*",
                # Windows - Blockfile cache (legacy pre-2015, also in Cache/ not Cache_Data/)
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Cache/index",
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Cache/data_*",
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Cache/f_*",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Cache/index",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Cache/data_*",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Cache/f_*",
                # macOS - Entry files
                "Users/*/Library/Caches/Google/Chrome/Default/Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/Library/Caches/Google/Chrome/Default/Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/Library/Caches/Google/Chrome/Default/Cache/Cache_Data/[0-9a-f]*_s",
                "Users/*/Library/Caches/Google/Chrome/Profile */Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/Library/Caches/Google/Chrome/Profile */Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/Library/Caches/Google/Chrome/Profile */Cache/Cache_Data/[0-9a-f]*_s",
                # macOS - Simple cache index files
                "Users/*/Library/Caches/Google/Chrome/Default/Cache/Cache_Data/index",
                "Users/*/Library/Caches/Google/Chrome/Default/Cache/Cache_Data/index-dir/the-real-index",
                "Users/*/Library/Caches/Google/Chrome/Profile */Cache/Cache_Data/index",
                "Users/*/Library/Caches/Google/Chrome/Profile */Cache/Cache_Data/index-dir/the-real-index",
                # macOS - Block files (simple cache)
                "Users/*/Library/Caches/Google/Chrome/Default/Cache/Cache_Data/f_*",
                "Users/*/Library/Caches/Google/Chrome/Profile */Cache/Cache_Data/f_*",
                # macOS - Blockfile cache (legacy)
                "Users/*/Library/Caches/Google/Chrome/Default/Cache/index",
                "Users/*/Library/Caches/Google/Chrome/Default/Cache/data_*",
                "Users/*/Library/Caches/Google/Chrome/Default/Cache/f_*",
                "Users/*/Library/Caches/Google/Chrome/Profile */Cache/index",
                "Users/*/Library/Caches/Google/Chrome/Profile */Cache/data_*",
                "Users/*/Library/Caches/Google/Chrome/Profile */Cache/f_*",
                # Linux - Entry files
                "home/*/.config/google-chrome/Default/Cache/Cache_Data/[0-9a-f]*_0",
                "home/*/.config/google-chrome/Default/Cache/Cache_Data/[0-9a-f]*_1",
                "home/*/.config/google-chrome/Default/Cache/Cache_Data/[0-9a-f]*_s",
                "home/*/.config/google-chrome/Profile */Cache/Cache_Data/[0-9a-f]*_0",
                "home/*/.config/google-chrome/Profile */Cache/Cache_Data/[0-9a-f]*_1",
                "home/*/.config/google-chrome/Profile */Cache/Cache_Data/[0-9a-f]*_s",
                # Linux - Simple cache index files
                "home/*/.config/google-chrome/Default/Cache/Cache_Data/index",
                "home/*/.config/google-chrome/Default/Cache/Cache_Data/index-dir/the-real-index",
                "home/*/.config/google-chrome/Profile */Cache/Cache_Data/index",
                "home/*/.config/google-chrome/Profile */Cache/Cache_Data/index-dir/the-real-index",
                # Linux - Block files (simple cache)
                "home/*/.config/google-chrome/Default/Cache/Cache_Data/f_*",
                "home/*/.config/google-chrome/Profile */Cache/Cache_Data/f_*",
                # Linux - Blockfile cache (legacy)
                "home/*/.config/google-chrome/Default/Cache/index",
                "home/*/.config/google-chrome/Default/Cache/data_*",
                "home/*/.config/google-chrome/Default/Cache/f_*",
                "home/*/.config/google-chrome/Profile */Cache/index",
                "home/*/.config/google-chrome/Profile */Cache/data_*",
                "home/*/.config/google-chrome/Profile */Cache/f_*",
            ],
            "downloads": [
                # Downloads history is stored in the History database
                # (same paths as history)
            ],
            # Phase 1 Browser Artifacts
            "autofill": [
                # Windows
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Web Data",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Web Data",
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Login Data",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Login Data",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/Web Data",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Web Data",
                "Users/*/Library/Application Support/Google/Chrome/Default/Login Data",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Login Data",
                # Linux
                "home/*/.config/google-chrome/Default/Web Data",
                "home/*/.config/google-chrome/Profile */Web Data",
                "home/*/.config/google-chrome/Default/Login Data",
                "home/*/.config/google-chrome/Profile */Login Data",
            ],
            "sessions": [
                # Windows
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Current Session",
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Current Tabs",
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Last Session",
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Last Tabs",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Current Session",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Current Tabs",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Last Session",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Last Tabs",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/Current Session",
                "Users/*/Library/Application Support/Google/Chrome/Default/Current Tabs",
                "Users/*/Library/Application Support/Google/Chrome/Default/Last Session",
                "Users/*/Library/Application Support/Google/Chrome/Default/Last Tabs",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Current Session",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Current Tabs",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Last Session",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Last Tabs",
                # Linux
                "home/*/.config/google-chrome/Default/Current Session",
                "home/*/.config/google-chrome/Default/Current Tabs",
                "home/*/.config/google-chrome/Default/Last Session",
                "home/*/.config/google-chrome/Default/Last Tabs",
                "home/*/.config/google-chrome/Profile */Current Session",
                "home/*/.config/google-chrome/Profile */Current Tabs",
                "home/*/.config/google-chrome/Profile */Last Session",
                "home/*/.config/google-chrome/Profile */Last Tabs",
            ],
            "permissions": [
                # Windows
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Preferences",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Preferences",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/Preferences",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Preferences",
                # Linux
                "home/*/.config/google-chrome/Default/Preferences",
                "home/*/.config/google-chrome/Profile */Preferences",
            ],
            "media_history": [
                # Windows
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Media History",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Media History",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/Media History",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Media History",
                # Linux
                "home/*/.config/google-chrome/Default/Media History",
                "home/*/.config/google-chrome/Profile */Media History",
            ],
            # Phase 2 Browser Forensics
            "transport_security": [
                # Windows
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/TransportSecurity",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */TransportSecurity",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/TransportSecurity",
                "Users/*/Library/Application Support/Google/Chrome/Profile */TransportSecurity",
                # Linux
                "home/*/.config/google-chrome/Default/TransportSecurity",
                "home/*/.config/google-chrome/Profile */TransportSecurity",
            ],
            # Phase 3 Deep Storage & Apps
            "extensions": [
                # Windows - Extension manifest files
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Extensions/*/*/manifest.json",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Extensions/*/*/manifest.json",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/Extensions/*/*/manifest.json",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Extensions/*/*/manifest.json",
                # Linux
                "home/*/.config/google-chrome/Default/Extensions/*/*/manifest.json",
                "home/*/.config/google-chrome/Profile */Extensions/*/*/manifest.json",
            ],
            "extension_preferences": [
                # Preferences JSON contains extension state (enabled/disabled, permissions)
                # Windows
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Preferences",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Preferences",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/Preferences",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Preferences",
                # Linux
                "home/*/.config/google-chrome/Default/Preferences",
                "home/*/.config/google-chrome/Profile */Preferences",
            ],
            "local_storage": [
                # Windows - LevelDB Local Storage
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Local Storage/leveldb/*",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Local Storage/leveldb/*",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/Local Storage/leveldb/*",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Local Storage/leveldb/*",
                # Linux
                "home/*/.config/google-chrome/Default/Local Storage/leveldb/*",
                "home/*/.config/google-chrome/Profile */Local Storage/leveldb/*",
            ],
            "session_storage": [
                # Windows - Session Storage directory
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Session Storage/*",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Session Storage/*",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/Session Storage/*",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Session Storage/*",
                # Linux
                "home/*/.config/google-chrome/Default/Session Storage/*",
                "home/*/.config/google-chrome/Profile */Session Storage/*",
            ],
            "indexeddb": [
                # Windows - IndexedDB LevelDB + blob storage
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/IndexedDB/*",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */IndexedDB/*",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/IndexedDB/*",
                "Users/*/Library/Application Support/Google/Chrome/Profile */IndexedDB/*",
                # Linux
                "home/*/.config/google-chrome/Default/IndexedDB/*",
                "home/*/.config/google-chrome/Profile */IndexedDB/*",
            ],
            "sync_data": [
                # Windows - Sync Data LevelDB
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Sync Data/*",
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Sync Data Backup/*",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Sync Data/*",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Sync Data Backup/*",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/Sync Data/*",
                "Users/*/Library/Application Support/Google/Chrome/Default/Sync Data Backup/*",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Sync Data/*",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Sync Data Backup/*",
                # Linux
                "home/*/.config/google-chrome/Default/Sync Data/*",
                "home/*/.config/google-chrome/Default/Sync Data Backup/*",
                "home/*/.config/google-chrome/Profile */Sync Data/*",
                "home/*/.config/google-chrome/Profile */Sync Data Backup/*",
            ],
            # Service Worker CacheStorage
            "cache_storage": [
                # Windows - Service Worker CacheStorage LevelDB
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Service Worker/CacheStorage/*",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Service Worker/CacheStorage/*",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/Service Worker/CacheStorage/*",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Service Worker/CacheStorage/*",
                # Linux
                "home/*/.config/google-chrome/Default/Service Worker/CacheStorage/*",
                "home/*/.config/google-chrome/Profile */Service Worker/CacheStorage/*",
            ],
            # Phase 4 Visual Enrichment
            "favicons": [
                # Windows - Favicons SQLite database
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Favicons",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Favicons",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/Favicons",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Favicons",
                # Linux
                "home/*/.config/google-chrome/Default/Favicons",
                "home/*/.config/google-chrome/Profile */Favicons",
            ],
            "top_sites": [
                # Windows - Top Sites SQLite database
                "Users/*/AppData/Local/Google/Chrome/User Data/Default/Top Sites",
                "Users/*/AppData/Local/Google/Chrome/User Data/Profile */Top Sites",
                # macOS
                "Users/*/Library/Application Support/Google/Chrome/Default/Top Sites",
                "Users/*/Library/Application Support/Google/Chrome/Profile */Top Sites",
                # Linux
                "home/*/.config/google-chrome/Default/Top Sites",
                "home/*/.config/google-chrome/Profile */Top Sites",
            ],
        },
    },
    "edge": {
        "display_name": "Microsoft Edge",
        "engine": "chromium",
        "paths": {
            "history": [
                # Windows - explicit Default + Profile patterns for resilience
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/History",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */History",
                # macOS
                "Users/*/Library/Application Support/Microsoft Edge/Default/History",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */History",
                # Linux
                "home/*/.config/microsoft-edge/Default/History",
                "home/*/.config/microsoft-edge/Profile */History",
            ],
            "cookies": [
                # Windows
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Cookies",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Cookies",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Network/Cookies",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Network/Cookies",
                # macOS
                "Users/*/Library/Application Support/Microsoft Edge/Default/Cookies",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Cookies",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Network/Cookies",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Network/Cookies",
                # Linux
                "home/*/.config/microsoft-edge/Default/Cookies",
                "home/*/.config/microsoft-edge/Profile */Cookies",
                "home/*/.config/microsoft-edge/Default/Network/Cookies",
                "home/*/.config/microsoft-edge/Profile */Network/Cookies",
            ],
            "bookmarks": [
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Bookmarks",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Bookmarks",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Bookmarks",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Bookmarks",
                "home/*/.config/microsoft-edge/Default/Bookmarks",
                "home/*/.config/microsoft-edge/Profile */Bookmarks",
            ],
            "cache": [
                # Windows - Entry files
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Cache/Cache_Data/[0-9a-f]*_s",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Cache/Cache_Data/[0-9a-f]*_s",
                # Windows - Index files
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Cache/Cache_Data/index",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Cache/Cache_Data/index-dir/the-real-index",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Cache/Cache_Data/index",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Cache/Cache_Data/index-dir/the-real-index",
                # Windows - Block files (legacy)
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Cache/Cache_Data/f_*",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Cache/Cache_Data/f_*",
                # macOS - Entry files
                "Users/*/Library/Caches/Microsoft Edge/Default/Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/Library/Caches/Microsoft Edge/Default/Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/Library/Caches/Microsoft Edge/Default/Cache/Cache_Data/[0-9a-f]*_s",
                "Users/*/Library/Caches/Microsoft Edge/Profile */Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/Library/Caches/Microsoft Edge/Profile */Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/Library/Caches/Microsoft Edge/Profile */Cache/Cache_Data/[0-9a-f]*_s",
                # macOS - Index files
                "Users/*/Library/Caches/Microsoft Edge/Default/Cache/Cache_Data/index",
                "Users/*/Library/Caches/Microsoft Edge/Default/Cache/Cache_Data/index-dir/the-real-index",
                "Users/*/Library/Caches/Microsoft Edge/Profile */Cache/Cache_Data/index",
                "Users/*/Library/Caches/Microsoft Edge/Profile */Cache/Cache_Data/index-dir/the-real-index",
                # macOS - Block files (legacy)
                "Users/*/Library/Caches/Microsoft Edge/Default/Cache/Cache_Data/f_*",
                "Users/*/Library/Caches/Microsoft Edge/Profile */Cache/Cache_Data/f_*",
            ],
            "downloads": [],
            # Phase 1 Browser Artifacts
            "autofill": [
                # Windows
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Web Data",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Web Data",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Login Data",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Login Data",
                # macOS
                "Users/*/Library/Application Support/Microsoft Edge/Default/Web Data",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Web Data",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Login Data",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Login Data",
                # Linux
                "home/*/.config/microsoft-edge/Default/Web Data",
                "home/*/.config/microsoft-edge/Profile */Web Data",
                "home/*/.config/microsoft-edge/Default/Login Data",
                "home/*/.config/microsoft-edge/Profile */Login Data",
            ],
            "sessions": [
                # Windows
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Current Session",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Current Tabs",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Last Session",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Last Tabs",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Current Session",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Current Tabs",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Last Session",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Last Tabs",
                # macOS
                "Users/*/Library/Application Support/Microsoft Edge/Default/Current Session",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Current Tabs",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Last Session",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Last Tabs",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Current Session",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Current Tabs",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Last Session",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Last Tabs",
                # Linux
                "home/*/.config/microsoft-edge/Default/Current Session",
                "home/*/.config/microsoft-edge/Default/Current Tabs",
                "home/*/.config/microsoft-edge/Default/Last Session",
                "home/*/.config/microsoft-edge/Default/Last Tabs",
                "home/*/.config/microsoft-edge/Profile */Current Session",
                "home/*/.config/microsoft-edge/Profile */Current Tabs",
                "home/*/.config/microsoft-edge/Profile */Last Session",
                "home/*/.config/microsoft-edge/Profile */Last Tabs",
            ],
            "permissions": [
                # Windows
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Preferences",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Preferences",
                # macOS
                "Users/*/Library/Application Support/Microsoft Edge/Default/Preferences",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Preferences",
                # Linux
                "home/*/.config/microsoft-edge/Default/Preferences",
                "home/*/.config/microsoft-edge/Profile */Preferences",
            ],
            "media_history": [
                # Windows
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Media History",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Media History",
                # macOS
                "Users/*/Library/Application Support/Microsoft Edge/Default/Media History",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Media History",
                # Linux
                "home/*/.config/microsoft-edge/Default/Media History",
                "home/*/.config/microsoft-edge/Profile */Media History",
            ],
            # Phase 2 Browser Forensics
            "transport_security": [
                # Windows
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/TransportSecurity",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */TransportSecurity",
                # macOS
                "Users/*/Library/Application Support/Microsoft Edge/Default/TransportSecurity",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */TransportSecurity",
                # Linux
                "home/*/.config/microsoft-edge/Default/TransportSecurity",
                "home/*/.config/microsoft-edge/Profile */TransportSecurity",
            ],
            # Phase 3 Deep Storage & Apps
            "extensions": [
                # Windows
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Extensions/*/*/manifest.json",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Extensions/*/*/manifest.json",
                # macOS
                "Users/*/Library/Application Support/Microsoft Edge/Default/Extensions/*/*/manifest.json",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Extensions/*/*/manifest.json",
                # Linux
                "home/*/.config/microsoft-edge/Default/Extensions/*/*/manifest.json",
                "home/*/.config/microsoft-edge/Profile */Extensions/*/*/manifest.json",
            ],
            "extension_preferences": [
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Preferences",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Preferences",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Preferences",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Preferences",
                "home/*/.config/microsoft-edge/Default/Preferences",
                "home/*/.config/microsoft-edge/Profile */Preferences",
            ],
            "local_storage": [
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Local Storage/leveldb/*",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Local Storage/leveldb/*",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Local Storage/leveldb/*",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Local Storage/leveldb/*",
                "home/*/.config/microsoft-edge/Default/Local Storage/leveldb/*",
                "home/*/.config/microsoft-edge/Profile */Local Storage/leveldb/*",
            ],
            "session_storage": [
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Session Storage/*",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Session Storage/*",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Session Storage/*",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Session Storage/*",
                "home/*/.config/microsoft-edge/Default/Session Storage/*",
                "home/*/.config/microsoft-edge/Profile */Session Storage/*",
            ],
            "indexeddb": [
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/IndexedDB/*",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */IndexedDB/*",
                "Users/*/Library/Application Support/Microsoft Edge/Default/IndexedDB/*",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */IndexedDB/*",
                "home/*/.config/microsoft-edge/Default/IndexedDB/*",
                "home/*/.config/microsoft-edge/Profile */IndexedDB/*",
            ],
            "sync_data": [
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Sync Data/*",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Sync Data Backup/*",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Sync Data/*",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Sync Data Backup/*",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Sync Data/*",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Sync Data Backup/*",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Sync Data/*",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Sync Data Backup/*",
                "home/*/.config/microsoft-edge/Default/Sync Data/*",
                "home/*/.config/microsoft-edge/Default/Sync Data Backup/*",
                "home/*/.config/microsoft-edge/Profile */Sync Data/*",
                "home/*/.config/microsoft-edge/Profile */Sync Data Backup/*",
            ],
            # Service Worker CacheStorage
            "cache_storage": [
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Service Worker/CacheStorage/*",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Service Worker/CacheStorage/*",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Service Worker/CacheStorage/*",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Service Worker/CacheStorage/*",
                "home/*/.config/microsoft-edge/Default/Service Worker/CacheStorage/*",
                "home/*/.config/microsoft-edge/Profile */Service Worker/CacheStorage/*",
            ],
            # Phase 4 Visual Enrichment
            "favicons": [
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Favicons",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Favicons",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Favicons",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Favicons",
                "home/*/.config/microsoft-edge/Default/Favicons",
                "home/*/.config/microsoft-edge/Profile */Favicons",
            ],
            "top_sites": [
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Default/Top Sites",
                "Users/*/AppData/Local/Microsoft/Edge/User Data/Profile */Top Sites",
                "Users/*/Library/Application Support/Microsoft Edge/Default/Top Sites",
                "Users/*/Library/Application Support/Microsoft Edge/Profile */Top Sites",
                "home/*/.config/microsoft-edge/Default/Top Sites",
                "home/*/.config/microsoft-edge/Profile */Top Sites",
            ],
        },
    },
    "opera": {
        "display_name": "Opera",
        "engine": "chromium",
        "paths": {
            "history": [
                # Windows - Opera Stable + Opera GX
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/History",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/History",
                # macOS
                "Users/*/Library/Application Support/com.operasoftware.Opera/History",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/History",
                # Linux
                "home/*/.config/opera/History",
            ],
            "cookies": [
                # Windows
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Cookies",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Cookies",
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Network/Cookies",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Network/Cookies",
                # macOS
                "Users/*/Library/Application Support/com.operasoftware.Opera/Cookies",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Cookies",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Network/Cookies",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Network/Cookies",
                # Linux
                "home/*/.config/opera/Cookies",
                "home/*/.config/opera/Network/Cookies",
            ],
            "bookmarks": [
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Bookmarks",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Bookmarks",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Bookmarks",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Bookmarks",
                "home/*/.config/opera/Bookmarks",
            ],
            "cache": [
                # Windows - Entry files
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Cache/Cache_Data/[0-9a-f]*_s",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Cache/Cache_Data/[0-9a-f]*_s",
                # Windows - Index files
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Cache/Cache_Data/index",
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Cache/Cache_Data/index-dir/the-real-index",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Cache/Cache_Data/index",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Cache/Cache_Data/index-dir/the-real-index",
                # Windows - Block files (legacy)
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Cache/Cache_Data/f_*",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Cache/Cache_Data/f_*",
                # macOS - Entry files
                "Users/*/Library/Caches/com.operasoftware.Opera/Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/Library/Caches/com.operasoftware.Opera/Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/Library/Caches/com.operasoftware.Opera/Cache/Cache_Data/[0-9a-f]*_s",
                "Users/*/Library/Caches/com.operasoftware.OperaGX/Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/Library/Caches/com.operasoftware.OperaGX/Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/Library/Caches/com.operasoftware.OperaGX/Cache/Cache_Data/[0-9a-f]*_s",
                # macOS - Index files
                "Users/*/Library/Caches/com.operasoftware.Opera/Cache/Cache_Data/index",
                "Users/*/Library/Caches/com.operasoftware.Opera/Cache/Cache_Data/index-dir/the-real-index",
                "Users/*/Library/Caches/com.operasoftware.OperaGX/Cache/Cache_Data/index",
                "Users/*/Library/Caches/com.operasoftware.OperaGX/Cache/Cache_Data/index-dir/the-real-index",
                # macOS - Block files (legacy)
                "Users/*/Library/Caches/com.operasoftware.Opera/Cache/Cache_Data/f_*",
                "Users/*/Library/Caches/com.operasoftware.OperaGX/Cache/Cache_Data/f_*",
                # Linux - Entry files
                "home/*/.config/opera/Cache/Cache_Data/[0-9a-f]*_0",
                "home/*/.config/opera/Cache/Cache_Data/[0-9a-f]*_1",
                "home/*/.config/opera/Cache/Cache_Data/[0-9a-f]*_s",
                # Linux - Index files
                "home/*/.config/opera/Cache/Cache_Data/index",
                "home/*/.config/opera/Cache/Cache_Data/index-dir/the-real-index",
                # Linux - Block files (legacy)
                "home/*/.config/opera/Cache/Cache_Data/f_*",
            ],
            "downloads": [],
            # Phase 1 Browser Artifacts
            "autofill": [
                # Windows
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Web Data",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Web Data",
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Login Data",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Login Data",
                # macOS
                "Users/*/Library/Application Support/com.operasoftware.Opera/Web Data",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Web Data",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Login Data",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Login Data",
                # Linux
                "home/*/.config/opera/Web Data",
                "home/*/.config/opera/Login Data",
            ],
            "sessions": [
                # Windows
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Current Session",
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Current Tabs",
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Last Session",
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Last Tabs",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Current Session",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Current Tabs",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Last Session",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Last Tabs",
                # macOS
                "Users/*/Library/Application Support/com.operasoftware.Opera/Current Session",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Current Tabs",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Last Session",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Last Tabs",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Current Session",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Current Tabs",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Last Session",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Last Tabs",
                # Linux
                "home/*/.config/opera/Current Session",
                "home/*/.config/opera/Current Tabs",
                "home/*/.config/opera/Last Session",
                "home/*/.config/opera/Last Tabs",
            ],
            "permissions": [
                # Windows
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Preferences",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Preferences",
                # macOS
                "Users/*/Library/Application Support/com.operasoftware.Opera/Preferences",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Preferences",
                # Linux
                "home/*/.config/opera/Preferences",
            ],
            "media_history": [
                # Windows
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Media History",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Media History",
                # macOS
                "Users/*/Library/Application Support/com.operasoftware.Opera/Media History",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Media History",
                # Linux
                "home/*/.config/opera/Media History",
            ],
            # Phase 2 Browser Forensics
            "transport_security": [
                # Windows
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/TransportSecurity",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/TransportSecurity",
                # macOS
                "Users/*/Library/Application Support/com.operasoftware.Opera/TransportSecurity",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/TransportSecurity",
                # Linux
                "home/*/.config/opera/TransportSecurity",
            ],
            # Phase 3 Deep Storage & Apps
            "extensions": [
                # Windows
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Extensions/*/*/manifest.json",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Extensions/*/*/manifest.json",
                # macOS
                "Users/*/Library/Application Support/com.operasoftware.Opera/Extensions/*/*/manifest.json",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Extensions/*/*/manifest.json",
                # Linux
                "home/*/.config/opera/Extensions/*/*/manifest.json",
            ],
            "extension_preferences": [
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Preferences",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Preferences",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Preferences",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Preferences",
                "home/*/.config/opera/Preferences",
            ],
            "local_storage": [
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Local Storage/leveldb/*",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Local Storage/leveldb/*",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Local Storage/leveldb/*",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Local Storage/leveldb/*",
                "home/*/.config/opera/Local Storage/leveldb/*",
            ],
            "session_storage": [
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Session Storage/*",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Session Storage/*",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Session Storage/*",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Session Storage/*",
                "home/*/.config/opera/Session Storage/*",
            ],
            "indexeddb": [
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/IndexedDB/*",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/IndexedDB/*",
                "Users/*/Library/Application Support/com.operasoftware.Opera/IndexedDB/*",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/IndexedDB/*",
                "home/*/.config/opera/IndexedDB/*",
            ],
            "sync_data": [
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Sync Data/*",
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Sync Data Backup/*",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Sync Data/*",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Sync Data Backup/*",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Sync Data/*",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Sync Data Backup/*",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Sync Data/*",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Sync Data Backup/*",
                "home/*/.config/opera/Sync Data/*",
                "home/*/.config/opera/Sync Data Backup/*",
            ],
            # Service Worker CacheStorage
            "cache_storage": [
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Service Worker/CacheStorage/*",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Service Worker/CacheStorage/*",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Service Worker/CacheStorage/*",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Service Worker/CacheStorage/*",
                "home/*/.config/opera/Service Worker/CacheStorage/*",
            ],
            "favicons": [
                # Windows
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Favicons",
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Favicons-journal",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Favicons",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Favicons-journal",
                # macOS
                "Users/*/Library/Application Support/com.operasoftware.Opera/Favicons",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Favicons-journal",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Favicons",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Favicons-journal",
                # Linux
                "home/*/.config/opera/Favicons",
                "home/*/.config/opera/Favicons-journal",
            ],
            "top_sites": [
                # Windows
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Top Sites",
                "Users/*/AppData/Roaming/Opera Software/Opera Stable/Top Sites-journal",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Top Sites",
                "Users/*/AppData/Roaming/Opera Software/Opera GX Stable/Top Sites-journal",
                # macOS
                "Users/*/Library/Application Support/com.operasoftware.Opera/Top Sites",
                "Users/*/Library/Application Support/com.operasoftware.Opera/Top Sites-journal",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Top Sites",
                "Users/*/Library/Application Support/com.operasoftware.OperaGX/Top Sites-journal",
                # Linux
                "home/*/.config/opera/Top Sites",
                "home/*/.config/opera/Top Sites-journal",
            ],
        },
    },
    "brave": {
        "display_name": "Brave",
        "engine": "chromium",
        "paths": {
            "history": [
                # Windows
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/History",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */History",
                # macOS
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/History",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */History",
                # Linux
                "home/*/.config/BraveSoftware/Brave-Browser/Default/History",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */History",
            ],
            "cookies": [
                # Windows
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Cookies",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Cookies",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Network/Cookies",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Network/Cookies",
                # macOS
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Cookies",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Cookies",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Network/Cookies",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Network/Cookies",
                # Linux
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Cookies",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Cookies",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Network/Cookies",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Network/Cookies",
            ],
            "bookmarks": [
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Bookmarks",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Bookmarks",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Bookmarks",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Bookmarks",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Bookmarks",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Bookmarks",
            ],
            "cache": [
                # Windows - Entry files
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Cache/Cache_Data/[0-9a-f]*_s",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Cache/Cache_Data/[0-9a-f]*_s",
                # Windows - Index files
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Cache/Cache_Data/index",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Cache/Cache_Data/index-dir/the-real-index",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Cache/Cache_Data/index",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Cache/Cache_Data/index-dir/the-real-index",
                # Windows - Block files (legacy)
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Cache/Cache_Data/f_*",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Cache/Cache_Data/f_*",
                # macOS - Entry files
                "Users/*/Library/Caches/BraveSoftware/Brave-Browser/Default/Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/Library/Caches/BraveSoftware/Brave-Browser/Default/Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/Library/Caches/BraveSoftware/Brave-Browser/Default/Cache/Cache_Data/[0-9a-f]*_s",
                "Users/*/Library/Caches/BraveSoftware/Brave-Browser/Profile */Cache/Cache_Data/[0-9a-f]*_0",
                "Users/*/Library/Caches/BraveSoftware/Brave-Browser/Profile */Cache/Cache_Data/[0-9a-f]*_1",
                "Users/*/Library/Caches/BraveSoftware/Brave-Browser/Profile */Cache/Cache_Data/[0-9a-f]*_s",
                # macOS - Index files
                "Users/*/Library/Caches/BraveSoftware/Brave-Browser/Default/Cache/Cache_Data/index",
                "Users/*/Library/Caches/BraveSoftware/Brave-Browser/Default/Cache/Cache_Data/index-dir/the-real-index",
                "Users/*/Library/Caches/BraveSoftware/Brave-Browser/Profile */Cache/Cache_Data/index",
                "Users/*/Library/Caches/BraveSoftware/Brave-Browser/Profile */Cache/Cache_Data/index-dir/the-real-index",
                # macOS - Block files (legacy)
                "Users/*/Library/Caches/BraveSoftware/Brave-Browser/Default/Cache/Cache_Data/f_*",
                "Users/*/Library/Caches/BraveSoftware/Brave-Browser/Profile */Cache/Cache_Data/f_*",
                # Linux - Entry files
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Cache/Cache_Data/[0-9a-f]*_0",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Cache/Cache_Data/[0-9a-f]*_1",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Cache/Cache_Data/[0-9a-f]*_s",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Cache/Cache_Data/[0-9a-f]*_0",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Cache/Cache_Data/[0-9a-f]*_1",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Cache/Cache_Data/[0-9a-f]*_s",
                # Linux - Index files
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Cache/Cache_Data/index",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Cache/Cache_Data/index-dir/the-real-index",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Cache/Cache_Data/index",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Cache/Cache_Data/index-dir/the-real-index",
                # Linux - Block files (legacy)
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Cache/Cache_Data/f_*",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Cache/Cache_Data/f_*",
            ],
            "downloads": [],
            # Phase 1 Browser Artifacts
            "autofill": [
                # Windows
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Web Data",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Web Data",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Login Data",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Login Data",
                # macOS
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Web Data",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Web Data",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Login Data",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Login Data",
                # Linux
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Web Data",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Web Data",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Login Data",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Login Data",
            ],
            "sessions": [
                # Windows
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Current Session",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Current Tabs",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Last Session",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Last Tabs",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Current Session",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Current Tabs",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Last Session",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Last Tabs",
                # macOS
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Current Session",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Current Tabs",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Last Session",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Last Tabs",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Current Session",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Current Tabs",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Last Session",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Last Tabs",
                # Linux
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Current Session",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Current Tabs",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Last Session",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Last Tabs",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Current Session",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Current Tabs",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Last Session",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Last Tabs",
            ],
            "permissions": [
                # Windows
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Preferences",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Preferences",
                # macOS
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Preferences",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Preferences",
                # Linux
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Preferences",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Preferences",
            ],
            "media_history": [
                # Windows
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Media History",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Media History",
                # macOS
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Media History",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Media History",
                # Linux
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Media History",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Media History",
            ],
            # Phase 2 Browser Forensics
            "transport_security": [
                # Windows
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/TransportSecurity",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */TransportSecurity",
                # macOS
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/TransportSecurity",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */TransportSecurity",
                # Linux
                "home/*/.config/BraveSoftware/Brave-Browser/Default/TransportSecurity",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */TransportSecurity",
            ],
            # Phase 3 Deep Storage & Apps
            "extensions": [
                # Windows
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Extensions/*/*/manifest.json",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Extensions/*/*/manifest.json",
                # macOS
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Extensions/*/*/manifest.json",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Extensions/*/*/manifest.json",
                # Linux
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Extensions/*/*/manifest.json",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Extensions/*/*/manifest.json",
            ],
            "extension_preferences": [
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Preferences",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Preferences",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Preferences",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Preferences",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Preferences",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Preferences",
            ],
            "local_storage": [
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Local Storage/leveldb/*",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Local Storage/leveldb/*",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Local Storage/leveldb/*",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Local Storage/leveldb/*",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Local Storage/leveldb/*",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Local Storage/leveldb/*",
            ],
            "session_storage": [
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Session Storage/*",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Session Storage/*",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Session Storage/*",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Session Storage/*",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Session Storage/*",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Session Storage/*",
            ],
            "indexeddb": [
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/IndexedDB/*",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */IndexedDB/*",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/IndexedDB/*",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */IndexedDB/*",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/IndexedDB/*",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */IndexedDB/*",
            ],
            "sync_data": [
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Sync Data/*",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Sync Data Backup/*",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Sync Data/*",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Sync Data Backup/*",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Sync Data/*",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Sync Data Backup/*",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Sync Data/*",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Sync Data Backup/*",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Sync Data/*",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Sync Data Backup/*",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Sync Data/*",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Sync Data Backup/*",
            ],
            # Service Worker CacheStorage
            "cache_storage": [
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Service Worker/CacheStorage/*",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Service Worker/CacheStorage/*",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Service Worker/CacheStorage/*",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Service Worker/CacheStorage/*",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Service Worker/CacheStorage/*",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Service Worker/CacheStorage/*",
            ],
            "favicons": [
                # Windows
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Favicons",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Favicons-journal",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Favicons",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Favicons-journal",
                # macOS
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Favicons",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Favicons-journal",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Favicons",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Favicons-journal",
                # Linux
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Favicons",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Favicons-journal",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Favicons",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Favicons-journal",
            ],
            "top_sites": [
                # Windows
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Top Sites",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Default/Top Sites-journal",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Top Sites",
                "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Profile */Top Sites-journal",
                # macOS
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Top Sites",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Default/Top Sites-journal",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Top Sites",
                "Users/*/Library/Application Support/BraveSoftware/Brave-Browser/Profile */Top Sites-journal",
                # Linux
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Top Sites",
                "home/*/.config/BraveSoftware/Brave-Browser/Default/Top Sites-journal",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Top Sites",
                "home/*/.config/BraveSoftware/Brave-Browser/Profile */Top Sites-journal",
            ],
        },
    },
    "firefox": {
        "display_name": "Mozilla Firefox",
        "engine": "gecko",
        "paths": {
            "history": [
                # Windows
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/places.sqlite",
                # macOS
                "Users/*/Library/Application Support/Firefox/Profiles/*/places.sqlite",
                # Linux
                "home/*/.mozilla/firefox/*/places.sqlite",
            ],
            "cookies": [
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/cookies.sqlite",
                "Users/*/Library/Application Support/Firefox/Profiles/*/cookies.sqlite",
                "home/*/.mozilla/firefox/*/cookies.sqlite",
            ],
            "bookmarks": [
                # Bookmarks are stored in places.sqlite (same as history)
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/places.sqlite",
                "Users/*/Library/Application Support/Firefox/Profiles/*/places.sqlite",
                "home/*/.mozilla/firefox/*/places.sqlite",
            ],
            # Firefox uses cache2 format (separate extractor)
            "cache": [],
            "downloads": [],
            # Phase 1 Browser Artifacts
            "autofill": [
                # Firefox uses formhistory.sqlite for form data
                # Windows
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/formhistory.sqlite",
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/logins.json",
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/key4.db",
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/signons.sqlite",  # Legacy
                # macOS
                "Users/*/Library/Application Support/Firefox/Profiles/*/formhistory.sqlite",
                "Users/*/Library/Application Support/Firefox/Profiles/*/logins.json",
                "Users/*/Library/Application Support/Firefox/Profiles/*/key4.db",
                "Users/*/Library/Application Support/Firefox/Profiles/*/signons.sqlite",
                # Linux
                "home/*/.mozilla/firefox/*/formhistory.sqlite",
                "home/*/.mozilla/firefox/*/logins.json",
                "home/*/.mozilla/firefox/*/key4.db",
                "home/*/.mozilla/firefox/*/signons.sqlite",
            ],
            "sessions": [
                # Firefox uses sessionstore.jsonlz4 and recovery files
                # Windows
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/sessionstore.jsonlz4",
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/sessionstore-backups/recovery.jsonlz4",
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/sessionstore-backups/previous.jsonlz4",
                # macOS
                "Users/*/Library/Application Support/Firefox/Profiles/*/sessionstore.jsonlz4",
                "Users/*/Library/Application Support/Firefox/Profiles/*/sessionstore-backups/recovery.jsonlz4",
                "Users/*/Library/Application Support/Firefox/Profiles/*/sessionstore-backups/previous.jsonlz4",
                # Linux
                "home/*/.mozilla/firefox/*/sessionstore.jsonlz4",
                "home/*/.mozilla/firefox/*/sessionstore-backups/recovery.jsonlz4",
                "home/*/.mozilla/firefox/*/sessionstore-backups/previous.jsonlz4",
            ],
            "permissions": [
                # Firefox uses permissions.sqlite
                # Windows
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/permissions.sqlite",
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/content-prefs.sqlite",
                # macOS
                "Users/*/Library/Application Support/Firefox/Profiles/*/permissions.sqlite",
                "Users/*/Library/Application Support/Firefox/Profiles/*/content-prefs.sqlite",
                # Linux
                "home/*/.mozilla/firefox/*/permissions.sqlite",
                "home/*/.mozilla/firefox/*/content-prefs.sqlite",
            ],
            # Firefox does not have a unified media history database
            "media_history": [],
            # Phase 2 Browser Forensics
            # Firefox stores HSTS in SiteSecurityServiceState.txt
            "transport_security": [
                # Windows
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/SiteSecurityServiceState.txt",
                # macOS
                "Users/*/Library/Application Support/Firefox/Profiles/*/SiteSecurityServiceState.txt",
                # Linux
                "home/*/.mozilla/firefox/*/SiteSecurityServiceState.txt",
            ],
            # Phase 3 Deep Storage & Apps
            # Firefox uses extensions.json for extension metadata
            "extensions": [
                # Windows
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/extensions.json",
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/addons.json",
                # macOS
                "Users/*/Library/Application Support/Firefox/Profiles/*/extensions.json",
                "Users/*/Library/Application Support/Firefox/Profiles/*/addons.json",
                # Linux
                "home/*/.mozilla/firefox/*/extensions.json",
                "home/*/.mozilla/firefox/*/addons.json",
            ],
            # Firefox uses storage/default/{origin}/ls/ for Local Storage (SQLite)
            "local_storage": [
                # Windows
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/webappsstore.sqlite",
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/storage/default/*/ls/*.sqlite",
                # macOS
                "Users/*/Library/Application Support/Firefox/Profiles/*/webappsstore.sqlite",
                "Users/*/Library/Application Support/Firefox/Profiles/*/storage/default/*/ls/*.sqlite",
                # Linux
                "home/*/.mozilla/firefox/*/webappsstore.sqlite",
                "home/*/.mozilla/firefox/*/storage/default/*/ls/*.sqlite",
            ],
            # Firefox Session Storage is ephemeral and doesn't persist to disk consistently
            "session_storage": [],
            # Firefox uses SQLite files for IndexedDB
            "indexeddb": [
                # Windows
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/storage/default/*/idb/*.sqlite",
                # macOS
                "Users/*/Library/Application Support/Firefox/Profiles/*/storage/default/*/idb/*.sqlite",
                # Linux
                "home/*/.mozilla/firefox/*/storage/default/*/idb/*.sqlite",
            ],
            # Firefox sync uses signedInUser.json for account info
            "sync_data": [
                # Windows
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/signedInUser.json",
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/weave/*",
                # macOS
                "Users/*/Library/Application Support/Firefox/Profiles/*/signedInUser.json",
                "Users/*/Library/Application Support/Firefox/Profiles/*/weave/*",
                # Linux
                "home/*/.mozilla/firefox/*/signedInUser.json",
                "home/*/.mozilla/firefox/*/weave/*",
            ],
            # Firefox ServiceWorker CacheStorage
            # Firefox uses storage/default/{origin}/cache/ directory
            "cache_storage": [
                # Windows
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/storage/default/*/cache/*",
                # macOS
                "Users/*/Library/Application Support/Firefox/Profiles/*/storage/default/*/cache/*",
                # Linux
                "home/*/.mozilla/firefox/*/storage/default/*/cache/*",
            ],
            # Phase 4 Visual Enrichment
            # Firefox stores favicons in favicons.sqlite (linked to places.sqlite via moz_icons)
            "favicons": [
                # Windows
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/favicons.sqlite",
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/favicons.sqlite-journal",
                "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/favicons.sqlite-wal",
                # macOS
                "Users/*/Library/Application Support/Firefox/Profiles/*/favicons.sqlite",
                "Users/*/Library/Application Support/Firefox/Profiles/*/favicons.sqlite-journal",
                "Users/*/Library/Application Support/Firefox/Profiles/*/favicons.sqlite-wal",
                # Linux
                "home/*/.mozilla/firefox/*/favicons.sqlite",
                "home/*/.mozilla/firefox/*/favicons.sqlite-journal",
                "home/*/.mozilla/firefox/*/favicons.sqlite-wal",
            ],
            # Firefox does not have a Top Sites SQLite database (uses frecency in places.sqlite)
            "top_sites": [],
        },
    },
    "tor": {
        "display_name": "Tor Browser",
        "engine": "gecko",
        "paths": {},
    },
    "safari": {
        "display_name": "Apple Safari",
        "engine": "webkit",
        "paths": {
            # Safari is macOS-only (also iOS but typically extracted differently)
            "history": [
                "Users/*/Library/Safari/History.db",
                "Users/*/Library/Safari/History.db-journal",
                "Users/*/Library/Safari/History.db-wal",
            ],
            "cookies": [
                # Safari uses binary cookies format (.binarycookies)
                "Users/*/Library/Cookies/Cookies.binarycookies",
                # Legacy location
                "Users/*/Library/Safari/Cookies/Cookies.binarycookies",
            ],
            "bookmarks": [
                # Safari uses binary plist format for bookmarks
                "Users/*/Library/Safari/Bookmarks.plist",
            ],
            "cache": [
                # Safari cache is more complex with multiple databases
                "Users/*/Library/Caches/com.apple.Safari/Cache.db",
                "Users/*/Library/Caches/com.apple.Safari/fsCachedData/*",
            ],
            "downloads": [
                # Safari downloads are tracked in Downloads.plist
                "Users/*/Library/Safari/Downloads.plist",
            ],
            "autofill": [
                # Safari autofill (form values) - stored via Keychain
                # Web form data is in ~/Library/Safari/Form Values
                "Users/*/Library/Safari/Form Values",
            ],
            "sessions": [
                # Safari session data
                "Users/*/Library/Safari/LastSession.plist",
            ],
            "permissions": [],  # Safari permissions are in system preferences
            "media_history": [],  # Safari doesn't have unified media history
            "transport_security": [],  # Safari uses system HSTS
            "extensions": [
                # Safari extensions (App Extensions)
                "Users/*/Library/Safari/Extensions/*.safariextz",
                "Users/*/Library/Safari/Extensions/Extensions.plist",
            ],
            "local_storage": [
                # Safari Local Storage
                "Users/*/Library/Safari/LocalStorage/*",
                "Users/*/Library/WebKit/WebsiteData/LocalStorage/*",
            ],
            "session_storage": [],  # Ephemeral
            "indexeddb": [
                # Safari IndexedDB
                "Users/*/Library/Safari/Databases/*",
                "Users/*/Library/WebKit/WebsiteData/IndexedDB/*",
            ],
            "sync_data": [],  # Safari sync is via iCloud (not locally accessible)
            # Service Worker CacheStorage
            # Safari stores Service Worker caches in WebsiteData
            "cache_storage": [
                "Users/*/Library/WebKit/WebsiteData/ServiceWorkers/*",
                "Users/*/Library/Caches/com.apple.Safari/ServiceWorkers/*",
            ],
            # Phase 4 Visual Enrichment
            "favicons": [
                # Safari Touch Icons and favicons
                "Users/*/Library/Safari/Touch Icons Cache/*",
                "Users/*/Library/Safari/Favicon Cache/*",
            ],
            "top_sites": [
                # Safari Top Sites / Frequently Visited
                "Users/*/Library/Safari/TopSites.plist",
            ],
        },
    },
}


def get_browser_paths(browser: str, artifact_type: str) -> List[str]:
    """
    Get paths for a browser and artifact type.

    Args:
        browser: Browser key (chrome, edge, firefox, etc.)
        artifact_type: Artifact type (history, cookies, bookmarks, cache)

    Returns:
        List of glob patterns for the artifact type
    """
    if browser not in BROWSER_PATTERNS:
        return []
    return BROWSER_PATTERNS[browser]["paths"].get(artifact_type, [])


def get_browsers_for_artifact(artifact_type: str) -> List[str]:
    """
    Get list of browsers that support an artifact type.

    Args:
        artifact_type: Artifact type (history, cookies, bookmarks, cache)

    Returns:
        List of browser keys that have patterns for the artifact type
    """
    return [
        browser for browser, config in BROWSER_PATTERNS.items()
        if config["paths"].get(artifact_type)
    ]


def get_all_browsers() -> List[str]:
    """
    Get list of all supported browser keys.

    Returns:
        List of all browser keys
    """
    return list(BROWSER_PATTERNS.keys())


def get_browser_display_name(browser: str) -> str:
    """
    Get display name for a browser.

    Args:
        browser: Browser key

    Returns:
        Human-readable display name
    """
    if browser not in BROWSER_PATTERNS:
        return browser.title()
    return BROWSER_PATTERNS[browser]["display_name"]


def get_browser_engine(browser: str) -> str:
    """
    Get engine type for a browser (chromium, gecko, webkit).

    Args:
        browser: Browser key

    Returns:
        Engine type string
    """
    if browser not in BROWSER_PATTERNS:
        return "unknown"
    return BROWSER_PATTERNS[browser]["engine"]


def get_legacy_browser_patterns() -> Dict[str, Dict[str, Any]]:
    """
    Return patterns in the format used by BrowserHistoryExtractor.BROWSER_PATTERNS.

    This provides backward compatibility for extractors that use the legacy format:
    {
        "chrome": {
            "display_name": "Google Chrome",
            "history": ["path1", "path2"],
        },
        ...
    }

    Returns:
        Dict mapping browser keys to legacy pattern format
    """
    legacy = {}
    for browser, config in BROWSER_PATTERNS.items():
        legacy[browser] = {
            "display_name": config["display_name"],
            "history": config["paths"].get("history", []),
        }
    return legacy


def get_cache_patterns() -> Dict[str, List[str]]:
    """
    Return cache patterns in format used by CacheSimpleExtractor.

    Returns:
        Dict mapping browser keys to list of cache path patterns
    """
    cache_patterns = {}
    for browser, config in BROWSER_PATTERNS.items():
        if config["paths"].get("cache"):
            cache_patterns[browser] = config["paths"]["cache"]
    return cache_patterns
