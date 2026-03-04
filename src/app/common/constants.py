"""Shared constants used across multiple UI modules."""
from __future__ import annotations

# Human-readable display names for artifact types stored in tag_associations.
# Used by tagging dialogs and the tag/match summary panel.
ARTIFACT_DISPLAY_NAMES: dict[str, str] = {
    "url": "URL",
    "image": "Image",
    "file_list": "File",
    "download": "Download",
    "cookie": "Cookie",
    "bookmark": "Bookmark",
    "browser_download": "Browser Download",
    "autofill": "Autofill Entry",
    "credential": "Credential",
    "session_tab": "Session Tab",
    "site_permission": "Site Permission",
    "media_playback": "Media Playback",
    "local_storage": "Local Storage Key",
    "session_storage": "Session Storage Key",
    "installed_software": "Installed Application",
    "jump_list": "Jump List Entry",
    "jump_list_entry": "Jump List Entry",
    "timeline": "Timeline Event",
}
