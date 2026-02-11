"""
AppID Registry Loader

Centralized Windows Jump List AppID registry loaded from appids.json.
Provides lookup functions for identifying applications from their AppIDs.

AppIDs are CRC64 hashes derived from application executable paths,
used as the 16-character hex prefix of .automaticDestinations-ms filenames.

Example:
    5d696d521de238c3.automaticDestinations-ms -> Google Chrome

Usage:
    from extractors._shared.appid_loader import (
        get_app_name,
        get_browser_name,
        is_browser_appid,
        get_all_browser_appids,
        get_category_for_appid,
    )

    # Get application name for any AppID
    name = get_app_name("5d696d521de238c3")  # "Google Chrome"

    # Check if AppID is a browser
    is_browser, browser_name = is_browser_appid("5d696d521de238c3")  # (True, "Chrome")

    # Get all browser AppIDs for filtering
    browser_appids = get_all_browser_appids()  # Set of all browser AppIDs
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

LOGGER = logging.getLogger(__name__)

# Path to the JSON registry file
_APPIDS_JSON_PATH = Path(__file__).parent / "appids.json"

# Category display names for forensic reports
CATEGORY_DISPLAY_NAMES: Dict[str, str] = {
    "browsers": "Web Browser",
    "email_clients": "Email Client",
    "instant_messaging": "Instant Messaging",
    "p2p_file_sharing": "P2P/File Sharing",
    "ftp_clients": "FTP Client",
    "usenet_newsreaders": "Usenet/Newsreader",
    "remote_desktop": "Remote Desktop",
    "vpn": "VPN Client",
    "office_productivity": "Office/Productivity",
    "media_players": "Media Player",
    "image_viewers": "Image Viewer",
    "graphics_editors": "Graphics Editor",
    "development_tools": "Development Tool",
    "text_editors": "Text Editor",
    "file_managers": "File Manager",
    "archive_tools": "Archive/Compression",
    "system_utilities": "System Utility",
    "security_tools": "Security/Encryption",
    "database_tools": "Database Tool",
    "disc_burning": "Disc Burning",
    "gaming": "Gaming",
    "cloud_storage": "Cloud Storage",
    "social_media": "Social Media",
    "other": "Other",
}

# Browser name mapping (internal key -> display name)
BROWSER_DISPLAY_NAMES: Dict[str, str] = {
    "chrome": "Chrome",
    "edge": "Edge",
    "firefox": "Firefox",
    "internet_explorer": "Internet Explorer",
    "opera": "Opera",
    "safari": "Safari",
    "brave": "Brave",
    "tor": "Tor Browser",
    "pale_moon": "Pale Moon",
    "waterfox": "Waterfox",
    "seamonkey": "SeaMonkey",
    "torch": "Torch",
    "epic": "Epic Privacy Browser",
    "mozilla_suite": "Mozilla Suite",
}

# Application name mapping (internal key -> display name)
APP_DISPLAY_NAMES: Dict[str, str] = {
    # Browsers are handled separately
    # Email
    "outlook": "Microsoft Outlook",
    "thunderbird": "Mozilla Thunderbird",
    "windows_live_mail": "Windows Live Mail",
    "em_client": "eM Client",
    # IM
    "skype": "Skype",
    "teams": "Microsoft Teams",
    "slack": "Slack",
    "discord": "Discord",
    "telegram": "Telegram",
    "whatsapp": "WhatsApp",
    "icq": "ICQ",
    "aim": "AIM",
    "yahoo_messenger": "Yahoo Messenger",
    "msn_messenger": "MSN Messenger",
    "google_talk": "Google Talk",
    "pidgin": "Pidgin",
    "miranda": "Miranda IM",
    "trillian": "Trillian",
    "line": "LINE",
    "zoom": "Zoom",
    "oovoo": "ooVoo",
    "viber": "Viber",
    "signal": "Signal",
    "wickr": "Wickr",
    "xfire": "Xfire",
    "paltalk": "Paltalk",
    "gadu_gadu": "Gadu-Gadu",
    "qq": "QQ",
    "nimbuzz": "Nimbuzz",
    "digsby": "Digsby",
    "palringo": "Palringo",
    "camfrog": "Camfrog",
    "hexchat": "HexChat",
    "mirc": "mIRC",
    "xchat": "X-Chat",
    # P2P
    "utorrent": "µTorrent",
    "bittorrent": "BitTorrent",
    "qbittorrent": "qBittorrent",
    "vuze_azureus": "Vuze/Azureus",
    "deluge": "Deluge",
    "bitcomet": "BitComet",
    "bitlord": "BitLord",
    "bitspirit": "BitSpirit",
    "bittornado": "BitTornado",
    "emule": "eMule",
    "amule": "aMule",
    "limewire": "LimeWire",
    "frostwire": "FrostWire",
    "bearshare": "BearShare",
    "shareaza": "Shareaza",
    "kazaa": "Kazaa",
    "imesh": "iMesh",
    "ares": "Ares",
    "winmx": "WinMX",
    "dc_plus_plus": "DC++",
    "soulseek": "Soulseek",
    "gnutella": "Gnutella",
    "gnunet": "GNUnet",
    "i2p": "I2P",
    "morpheus": "Morpheus",
    "lphant": "Lphant",
    "piolet": "Piolet",
    "cabos": "Cabos",
    "exodus": "Exodus",
    "stealthnet": "StealthNet",
    "mldonkey": "MLDonkey",
    "retroshare": "RetroShare",
    "trustyfiles": "TrustyFiles",
    "blubster": "Blubster",
    "neebly": "Neebly",
    "scour": "Scour",
    "manolito": "Manolito",
    # FTP
    "filezilla": "FileZilla",
    "winscp": "WinSCP",
    "cyberduck": "Cyberduck",
    "cuteftp": "CuteFTP",
    "flashfxp": "FlashFXP",
    "ftprush": "FTPRush",
    "smartftp": "SmartFTP",
    "wiseftp": "WiseFTP",
    "bulletproof_ftp": "BulletProof FTP",
    "coreftp": "Core FTP",
    "ftp_voyager": "FTP Voyager",
    "ftp_explorer": "FTP Explorer",
    "crossftp": "CrossFTP",
    "classic_ftp": "Classic FTP",
    "roboftp": "Robo-FTP",
    "leechftp": "LeechFTP",
    "coffeecup_ftp": "CoffeeCup FTP",
    "alftp": "ALFTP",
    "3d_ftp": "3D-FTP",
    "titan_ftp": "Titan FTP",
    "sysax_ftp": "Sysax FTP",
    "fling_ftp": "Fling FTP",
    "bitkinex": "BitKinex",
    "pbftpclient": "pbFTPClient",
    "uploadftp": "UploadFTP",
    "expandrive": "ExpanDrive",
    # Remote
    "rdp": "Remote Desktop",
    "teamviewer": "TeamViewer",
    "anydesk": "AnyDesk",
    "realvnc": "RealVNC",
    "putty": "PuTTY",
    "logmein": "LogMeIn",
    "vmware_player": "VMware Player",
    "vmware_workstation": "VMware Workstation",
    "virtualbox": "VirtualBox",
    "virtual_pc": "Virtual PC",
    "cisco_anyconnect": "Cisco AnyConnect",
    "absolutetelnet": "AbsoluteTelnet",
    # VPN
    "nordvpn": "NordVPN",
    # Office
    "word": "Microsoft Word",
    "excel": "Microsoft Excel",
    "powerpoint": "Microsoft PowerPoint",
    "access": "Microsoft Access",
    "onenote": "Microsoft OneNote",
    "infopath": "Microsoft InfoPath",
    "publisher": "Microsoft Publisher",
    "visio": "Microsoft Visio",
    "project": "Microsoft Project",
    "libreoffice": "LibreOffice",
    "adobe_reader": "Adobe Reader",
    "adobe_acrobat": "Adobe Acrobat",
    "foxit_reader": "Foxit Reader",
    "pdf_xchange": "PDF-XChange",
    "pdf_architect": "PDF Architect",
    "pdf_creator": "PDFCreator",
    "evernote": "Evernote",
    "kindle": "Kindle",
    "calibre": "Calibre",
    "xmind": "XMind",
    "minitab": "Minitab",
    # Media
    "vlc": "VLC",
    "windows_media_player": "Windows Media Player",
    "itunes": "iTunes",
    "winamp": "Winamp",
    "realplayer": "RealPlayer",
    "gom_player": "GOM Player",
    "mpc_hc": "Media Player Classic HC",
    "mpc_be": "Media Player Classic BE",
    "kmplayer": "KMPlayer",
    "potplayer": "PotPlayer",
    "sm_player": "SMPlayer",
    "foobar2000": "foobar2000",
    "musicbee": "MusicBee",
    "mediamonkey": "MediaMonkey",
    "quintessential": "Quintessential Player",
    "jetaudio": "JetAudio",
    "jetvideo": "JetVideo",
    "allplayer": "ALLPlayer",
    "crystal_player": "Crystal Player",
    "songbird": "Songbird",
    "dsplayer": "DSPlayer",
    "cdisplay": "CDisplay",
    "groove_music": "Groove Music",
    "movies_tv": "Movies & TV",
    "media_center": "Windows Media Center",
    "j_river": "J. River Media Center",
    "zune": "Zune",
    "mediaportal": "MediaPortal",
    "dvbviewer": "DVBViewer",
    "powerdvd": "PowerDVD",
    "quicktime": "QuickTime",
    # Image
    "irfanview": "IrfanView",
    "xnview": "XnView",
    "acdsee": "ACDSee",
    "faststone": "FastStone Image Viewer",
    "fastpictureviewer": "FastPictureViewer",
    "honeyview": "HoneyView",
    "imagine": "Imagine",
    "imgseek": "imgSeek",
    "imatch": "IMatch",
    "image_axs_pro": "Image AXS Pro",
    "allpicturez": "AllPicturez",
    "autopix": "AutoPix",
    "picasa": "Picasa",
    "digikam": "digiKam",
    "zoner_photostudio": "Zoner Photo Studio",
    "photo_viewer": "Windows Photo Viewer",
    "photos_win10": "Windows Photos",
    "google_earth": "Google Earth",
    # Graphics
    "photoshop": "Adobe Photoshop",
    "gimp": "GIMP",
    "paint_net": "Paint.NET",
    "paint": "Microsoft Paint",
    "paint_shop_pro": "Paint Shop Pro",
    "corel_draw": "CorelDRAW",
    "corel_photo_paint": "Corel PHOTO-PAINT",
    "illustrator": "Adobe Illustrator",
    "dreamweaver": "Adobe Dreamweaver",
    "premiere": "Adobe Premiere",
    "flash": "Adobe Flash",
    "soundbooth": "Adobe Soundbooth",
    "snagit": "Snagit",
    "infrarecorder": "InfraRecorder",
    "imgburn": "ImgBurn",
    # Dev
    "visual_studio": "Visual Studio",
    "vscode": "Visual Studio Code",
    "vscodium": "VSCodium",
    "android_studio": "Android Studio",
    "eclipse": "Eclipse",
    "ida_pro": "IDA Pro",
    "python": "Python",
    "java": "Java",
    "powershell": "PowerShell",
    "powershell_studio": "PowerShell Studio",
    "cmd": "Command Prompt",
    "wscript": "Windows Script Host",
    "sapien_packager": "SAPIEN Packager",
    "sapien_snippet_editor": "SAPIEN SnippetEditor",
    "dotpeek": "dotPeek",
    "wireshark": "Wireshark",
    # Text
    "notepad": "Notepad",
    "notepad_plus_plus": "Notepad++",
    "sublime_text": "Sublime Text",
    "editpad_pro": "EditPad Pro",
    "hex_editor_neo": "Hex Editor Neo",
    "hxd": "HxD",
    "winhex": "WinHex",
    "programmer_notepad": "Programmer's Notepad",
    "far_manager": "Far Manager",
    "010_editor": "010 Editor",
    "wordpad": "WordPad",
    # File managers
    "explorer": "Windows Explorer",
    "total_commander": "Total Commander",
    "free_commander": "FreeCommander",
    "directory_opus": "Directory Opus",
    "quick_access": "Quick Access",
    # Archive
    "7zip": "7-Zip",
    "winrar": "WinRAR",
    "winzip": "WinZip",
    "peazip": "PeaZip",
    "pkzip": "PKZIP",
    "isobuster": "IsoBuster",
    # System
    "ccleaner": "CCleaner",
    "defraggler": "Defraggler",
    "disk_defrag": "Disk Defrag",
    "disk_cleanup": "Disk Cleanup",
    "smart_defrag": "Smart Defrag",
    "ultradefrag": "UltraDefrag",
    "recuva": "Recuva",
    "eraser": "Eraser",
    "bcwipe": "BCWipe",
    "window_washer": "WindowWasher",
    "revo_uninstaller": "Revo Uninstaller",
    "teracopy": "TeraCopy",
    "fastcopy": "FastCopy",
    "task_scheduler": "Task Scheduler",
    "event_viewer": "Event Viewer",
    "services": "Services",
    "computer_management": "Computer Management",
    "system_restore": "System Restore",
    "system_info": "System Information",
    "run_dialog": "Run",
    "snipping_tool": "Snipping Tool",
    "calculator": "Calculator",
    "sticky_notes": "Sticky Notes",
    "stickies": "Stickies",
    "print_management": "Print Management",
    "connected_devices": "Connected Devices",
    "help_support": "Help and Support",
    "defender": "Windows Defender",
    "xps_viewer": "XPS Viewer",
    "dvd_maker": "Windows DVD Maker",
    "movie_maker": "Windows Movie Maker",
    "photo_gallery": "Windows Photo Gallery",
    "cortana": "Cortana",
    "game_bar": "Xbox Game Bar",
    "xbox": "Xbox",
    "your_phone": "Your Phone",
    "windows_terminal": "Windows Terminal",
    "store": "Microsoft Store",
    "feedback_hub": "Feedback Hub",
    "maps": "Windows Maps",
    "people": "People",
    "get_help": "Get Help",
    # Security
    "truecrypt": "TrueCrypt",
    "veracrypt": "VeraCrypt",
    "pgp_desktop": "PGP Desktop",
    "keepass": "KeePass",
    "1password": "1Password",
    "roboform": "RoboForm",
    "malwarebytes": "Malwarebytes",
    "emsisoft": "Emsisoft Anti-Malware",
    "stunnel": "stunnel",
    "bcarchive": "BCArchive",
    "zenmap": "Zenmap",
    # Database
    "db_browser_sqlite": "DB Browser for SQLite",
    "sqlite_expert": "SQLite Expert",
    "sqlite_spy": "SQLiteSpy",
    "registry_explorer": "Registry Explorer",
    "json_viewer": "JSON Viewer",
    "json_buddy": "JSON Buddy",
    "pst_walker": "PST Walker",
    "bplister": "BPLister",
    "plist_editor": "plist Editor",
    "event_log_explorer": "Event Log Explorer",
    "log_viewer": "Log Viewer",
    "xml_notepad": "XML Notepad",
    "modern_csv": "Modern CSV",
    "winmerge": "WinMerge",
    # Disc
    "cdburnerxp": "CDBurnerXP",
    "nero": "Nero",
    # Gaming
    "steam": "Steam",
    "origin": "EA Origin",
    "gog_galaxy": "GOG Galaxy",
    "battlenet": "Battle.net",
    "gaming_app": "Gaming App",
    # Cloud
    "dropbox": "Dropbox",
    "google_drive": "Google Drive",
    "onedrive": "OneDrive",
    # Social
    "twitter": "Twitter",
    "imvu": "IMVU",
}


@lru_cache(maxsize=1)
def _load_registry() -> Dict:
    """Load the AppID registry from JSON file (cached)."""
    try:
        with open(_APPIDS_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        LOGGER.error("AppID registry file not found: %s", _APPIDS_JSON_PATH)
        return {}
    except json.JSONDecodeError as e:
        LOGGER.error("Invalid JSON in AppID registry: %s", e)
        return {}


@lru_cache(maxsize=1)
def _build_appid_index() -> Dict[str, Tuple[str, str]]:
    """
    Build reverse index: appid -> (category, app_key).

    Returns:
        Dict mapping lowercase AppID to (category, app_key) tuple.
    """
    registry = _load_registry()
    index: Dict[str, Tuple[str, str]] = {}

    for category, apps in registry.items():
        if category.startswith("$"):
            continue  # Skip metadata keys
        if not isinstance(apps, dict):
            continue

        for app_key, appids in apps.items():
            if app_key.startswith("$"):
                continue  # Skip comment keys
            if not isinstance(appids, list):
                continue

            for appid in appids:
                if appid:
                    index[appid.lower()] = (category, app_key)

    return index


@lru_cache(maxsize=1)
def get_all_browser_appids() -> frozenset:
    """
    Get all known browser AppIDs.

    Returns:
        Frozen set of all browser AppIDs (lowercase).
    """
    registry = _load_registry()
    browsers = registry.get("browsers", {})

    appids = set()
    for browser_key, browser_appids in browsers.items():
        if browser_key.startswith("$"):
            continue
        if isinstance(browser_appids, list):
            for appid in browser_appids:
                if appid:
                    appids.add(appid.lower())

    return frozenset(appids)


def is_browser_appid(appid: str) -> Tuple[bool, str]:
    """
    Check if an AppID belongs to a web browser.

    Args:
        appid: 16-character hex AppID (case-insensitive).

    Returns:
        Tuple of (is_browser, browser_name).
        browser_name is empty string if not a browser.
    """
    if not appid:
        return False, ""

    appid_lower = appid.lower()
    index = _build_appid_index()

    if appid_lower in index:
        category, app_key = index[appid_lower]
        if category == "browsers":
            display_name = BROWSER_DISPLAY_NAMES.get(app_key, app_key.replace("_", " ").title())
            return True, display_name

    return False, ""


def get_browser_name(appid: str) -> Optional[str]:
    """
    Get browser name for an AppID.

    Args:
        appid: 16-character hex AppID.

    Returns:
        Browser display name or None if not a browser.
    """
    is_browser, name = is_browser_appid(appid)
    return name if is_browser else None


def get_app_name(appid: str) -> str:
    """
    Get application display name for any AppID.

    Args:
        appid: 16-character hex AppID.

    Returns:
        Application display name, or "Unknown (AppID:XXXXXXXX)" if not found.
    """
    if not appid:
        return "Unknown"

    appid_lower = appid.lower()
    index = _build_appid_index()

    if appid_lower in index:
        category, app_key = index[appid_lower]

        # Check browser names first
        if category == "browsers":
            return BROWSER_DISPLAY_NAMES.get(app_key, app_key.replace("_", " ").title())

        # Check general app names
        return APP_DISPLAY_NAMES.get(app_key, app_key.replace("_", " ").title())

    # Return truncated AppID for unknown apps
    return f"Unknown (AppID:{appid[:8]})"


def get_category_for_appid(appid: str) -> Optional[str]:
    """
    Get the category for an AppID.

    Args:
        appid: 16-character hex AppID.

    Returns:
        Category key (e.g., "browsers", "p2p_file_sharing") or None.
    """
    if not appid:
        return None

    index = _build_appid_index()
    result = index.get(appid.lower())

    return result[0] if result else None


def get_category_display_name(category: str) -> str:
    """
    Get human-readable display name for a category.

    Args:
        category: Category key (e.g., "browsers").

    Returns:
        Display name (e.g., "Web Browser").
    """
    return CATEGORY_DISPLAY_NAMES.get(category, category.replace("_", " ").title())


def get_browser_appids_for_browser(browser_name: str) -> Set[str]:
    """
    Get all AppIDs for a specific browser.

    Args:
        browser_name: Browser name (case-insensitive, matches display or key name).

    Returns:
        Set of AppIDs (lowercase) for that browser.
    """
    registry = _load_registry()
    browsers = registry.get("browsers", {})

    browser_lower = browser_name.lower()

    # Find matching browser key
    for browser_key, appids in browsers.items():
        if browser_key.startswith("$"):
            continue

        # Match by key or display name
        display = BROWSER_DISPLAY_NAMES.get(browser_key, "").lower()
        if browser_key.lower() == browser_lower or display == browser_lower:
            if isinstance(appids, list):
                return {aid.lower() for aid in appids if aid}

    return set()


def get_forensic_categories() -> Set[str]:
    """
    Get categories of high forensic interest.

    Returns:
        Set of category keys that are forensically significant.
    """
    return {
        "browsers",
        "email_clients",
        "instant_messaging",
        "p2p_file_sharing",
        "ftp_clients",
        "usenet_newsreaders",
        "remote_desktop",
        "vpn",
        "cloud_storage",
        "security_tools",
    }


def is_forensically_interesting(appid: str) -> Tuple[bool, str]:
    """
    Check if an AppID is from a forensically interesting application.

    Args:
        appid: 16-character hex AppID.

    Returns:
        Tuple of (is_interesting, reason).
    """
    if not appid:
        return False, ""

    category = get_category_for_appid(appid)
    if not category:
        return False, ""

    forensic_categories = get_forensic_categories()
    if category in forensic_categories:
        reason = CATEGORY_DISPLAY_NAMES.get(category, category)
        return True, reason

    return False, ""


# Convenience: build dict for backward compatibility with jump_lists extractor
def load_browser_appids() -> Dict[str, str]:
    """
    Load browser AppID mappings (backward compatible).

    Returns:
        Dict mapping AppID (lowercase) to browser display name.
    """
    registry = _load_registry()
    browsers = registry.get("browsers", {})

    result: Dict[str, str] = {}
    for browser_key, appids in browsers.items():
        if browser_key.startswith("$"):
            continue
        if not isinstance(appids, list):
            continue

        display_name = BROWSER_DISPLAY_NAMES.get(browser_key, browser_key.replace("_", " ").title())
        for appid in appids:
            if appid:
                result[appid.lower()] = display_name

    return result
