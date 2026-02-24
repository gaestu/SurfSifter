"""
Report localization module.

Provides translation dictionaries for report templates and modules.
Supports English (en) and German (de) locales.

Usage:
    from reports.locales import get_translations, SUPPORTED_LOCALES

    t = get_translations("de")
    print(t["toc_title"])  # "Inhaltsverzeichnis"
"""

from __future__ import annotations

from typing import Dict

# Type alias for translation dictionary
TranslationDict = Dict[str, str]

TRANSLATIONS: Dict[str, TranslationDict] = {
    "en": {
        # ===================
        # Base template
        # ===================
        "toc_title": "Table of Contents",
        "page_of": "Page {page} of {pages}",
        "page": "Page",
        "of": "of",
        "generated": "Generated",
        "appendix": "Appendix",
        "no_content": "No Content",
        "no_sections_message": "No sections have been added to this report. Use the Reports tab to add custom sections and modules.",

        # Title page labels
        "case_number": "Case Number",
        "evidence": "Evidence",
        "investigator": "Investigator",
        "department": "Department",
        "notes": "Notes",

        # Author section
        "report_created_by": "Report Created By",
        "function": "Function",
        "name": "Name",
        "date": "Date",

        # ===================
        # System summary module
        # ===================
        "system_info": "System Information",
        "user_accounts": "User Accounts",
        "installed_software": "Installed Software",
        "autostart_entries": "Autostart Entries",
        "network_config": "Network Configuration",
        "property": "Property",
        "value": "Value",
        "no_system_info": "No system information found in registry data.",
        "no_users_found": "No user accounts found in registry data.",
        "no_software_found": "No installed software found in registry data.",
        "no_autostart_found": "No autostart entries found in registry data.",
        "no_network_found": "No network configuration found in registry data.",
        "connected_profiles": "Connected Network Profiles",
        "mapped_drives": "Mapped Network Drives",
        "local_accounts_found": "{count} local account(s) found in SAM hive",

        # System info field labels
        "os_version": "OS Version",
        "os_build": "OS Build",
        "os_display_version": "OS Display Version",
        "registered_owner": "Registered Owner",
        "install_date": "Install Date",
        "computer_name": "Computer Name",
        "timezone": "Timezone",
        "timezone_key": "Timezone Key",
        "last_shutdown": "Last Shutdown",
        "rdp_status": "RDP Status",
        "default_browser": "Default Browser",
        "downloads_path": "Downloads Path",
        "rdp_enabled": "Enabled",
        "rdp_disabled": "Disabled",

        # User table headers
        "username": "Username",
        "profile_path": "Profile Path",
        "account_type": "Account Type",
        "registry_path": "Registry Path",
        "local_account": "Local Account",
        "profile_only": "Profile Only",

        # Software table
        "software_name": "Software Name",
        "version": "Version",
        "publisher": "Publisher",
        "install_location": "Install Location",

        # Autostart table
        "entry_name": "Name",
        "command_path": "Command / Path",
        "source": "Source",
        "run_keys": "Run Keys (Startup Programs)",
        "run_once": "RunOnce",
        "browser_helper_objects": "Browser Helper Objects (BHO)",
        "clsid": "CLSID",
        "services": "Services",
        "service_name": "Service Name",
        "type": "Type",

        # Network labels
        "dhcp_ip": "DHCP IP Address",
        "dns_server": "DNS Server",
        "default_gateway": "Default Gateway",
        "dhcp_server": "DHCP Server",
        "domain": "Domain",
        "connected_network": "Connected Network",
        "last_connected": "Last Connected",
        "mapped_drive": "Mapped Drive",
        "interface_config": "Interface Configuration",
        "network_profiles": "Network Profiles",
        "profile_name": "Profile Name",
        "network_type": "Type",
        "category": "Category",
        "created": "Created",
        "wifi": "WiFi",
        "wired": "Wired",
        "mobile": "Mobile",
        "private": "Private",
        "public": "Public",
        "drive_path": "Network Path",
        "proxy_settings": "Proxy Settings",
        "internet_policy": "Internet Policy",

        # Shell folder paths
        "pictures_path": "Pictures Path",
        "videos_path": "Videos Path",
        "documents_path": "Documents Path",
        "desktop_path": "Desktop Path",

        # Browser detection section
        "browser_detection": "Browser Detection",
        "registered_browsers": "Registered Browsers",
        "browser_name": "Browser",
        "browser_app_paths": "Browser Application Paths",
        "ie_edge_settings": "IE / Edge Settings",
        "no_browser_detection": "No browser detection data found in registry data.",

        # User activity section
        "user_activity_title": "User Activity",
        "recent_images": "Recently Accessed Images",
        "typed_urls": "Typed URLs (IE/Edge)",
        "typed_paths_title": "Typed Paths (Explorer)",
        "recent_documents": "Recent Documents",
        "open_save_mru": "Open/Save Dialog History",
        "open_save_last_visited": "Last Visited Folders (Open/Save)",
        "explorer_searches": "Explorer Search Terms",
        "search_term": "Search Term",
        "no_user_activity": "No user activity data found in registry data.",

        # Execution history section
        "execution_history": "Execution History",
        "program_path": "Program Path",
        "run_count": "Run Count",
        "focus_count": "Focus Count",
        "focus_time": "Focus Time",
        "last_run": "Last Run",
        "no_execution_history": "No execution history found in registry data.",

        # ===================
        # Activity summary module
        # ===================
        "activity_overview": "Activity Overview",
        "activity_period": "Activity Period",
        "duration": "Duration",
        "days": "days",
        "hours": "hours",
        "weeks": "weeks",
        "total_events": "Total Events",
        "avg_events_per_day": "Average Events/Day",
        "no_timeline_events": "No timeline events found for the selected filters.",
        "events_by_type": "Events by Type",
        "events_by_type_desc": "This table shows the distribution of recorded events across different activity categories. Higher counts indicate more frequent activity in that category during the analysis period.",
        "event_type": "Event Type",
        "count": "Count",
        "percentage": "Percentage",
        "daily_activity": "Daily Activity",
        "daily_activity_desc": "Visual overview of system activity intensity per day. Longer bars indicate higher activity.",
        "date": "Date",
        "activity": "Activity",
        "events": "Events",
        "showing_first_days": "Showing first",
        "inactivity_gaps": "Significant Inactivity Gaps",
        "from": "From",
        "to": "To",
        "no_gaps_found": "No significant inactivity gaps found",
        "event_group_all": "All Events",
        "event_group_browser": "Browser Activity",
        "event_group_downloads": "Downloads",
        "event_group_authentication": "Authentication",
        "event_group_media": "Media Playback",

        # ===================
        # URL summary module
        # ===================
        "url": "URL",
        "urls": "URLs",
        "domain": "Domain",
        "occurrences": "Occurrences",
        "tags": "Tags",
        "first_seen": "First Seen",
        "last_seen": "Last Seen",
        "no_urls_found": "No URLs found matching the filter criteria.",
        "domains": "domains",
        "distinct_urls": "distinct URLs",
        "filter": "Filter",
        "and_more": "and",
        "more_urls": "more URLs",

        # ===================
        # URL activity timeline module
        # ===================
        "url_activity_summary": "URL Activity Summary",
        "total_url_events": "Total URL Events",
        "unique_urls": "Unique URLs",
        "domain_activity": "Domain Activity",
        "Domains": "Domains",
        "showing": "Showing",
        "of": "of",
        "no_url_events_found": "No URL events found matching the filter criteria.",

        # ===================
        # Images module
        # ===================
        "no_preview": "No Preview",
        "md5": "MD5",
        "no_images_found": "No images found matching the filter criteria.",
        "images": "images",
        "exif_taken": "Taken",
        "exif_camera": "Camera",
        "exif_model": "Model",
        "exif_gps_lat": "GPS Lat",
        "exif_gps_lon": "GPS Lon",
        "showing_x_of_y_images": "Showing {shown} of {total} images",

        # ===================
        # Screenshots module
        # ===================
        "screenshot_captured": "Captured",
        "screenshot_uploaded": "Uploaded",
        "screenshot_additional": "Additional Screenshots",
        "screenshots": "screenshot(s)",
        "no_screenshots_found": "No screenshots found.",
        "untitled": "Untitled",
        "total": "Total",

        # ===================
        # Downloaded images module
        # ===================
        "downloaded": "Downloaded",
        "size": "Size",
        "no_downloaded_images": "No downloaded images found matching the filter criteria.",

        # ===================
        # Browser Downloads module
        # ===================
        "filename": "Filename",
        "state": "State",
        "start_time": "Start Time",
        "end_time": "End Time",
        "no_downloads_found": "No browser downloads found matching the filter criteria.",

        # ===================
        # Tagged file list module
        # ===================
        "tagged_file_list_title": "File List",
        "path": "Path",
        "file_name": "File Name",
        "extension": "Ext",
        "modified": "Modified",
        "accessed": "Accessed",
        "deleted": "deleted",
        "no_files_found": "No files found matching the filter criteria.",
        "files": "files",
        "unlimited": "Unlimited",

        # ===================
        # Credentials module
        # ===================
        "origin_url": "Origin URL",
        "username_field": "Username Field",
        "username": "Username",
        "browser": "Browser",
        "profile": "Profile",
        "password": "Password",
        "created": "Created",
        "last_used": "Last Used",
        "stored": "Stored",
        "none": "None",
        "credentials": "credentials",
        "no_credentials_found": "No credentials found matching the filter criteria.",

        # ===================
        # Autofill module
        # ===================
        "field_name": "Field Name",
        "value": "Value",
        "use_count": "Use Count",
        "first_used": "First Used",
        "entries": "entries",
        "no_autofill_found": "No autofill entries found matching the filter criteria.",

        # ===================
        # Autofill Form Data module
        # ===================
        "autofill_form_data_title": "Autofill Form Data",
        "autofill_form_data_description": "Autofill data contains values that the browser automatically saved from web forms. This includes usernames, addresses, phone numbers, and other information the user entered on websites. The data shows which fields were filled, when they were first and last used.",
        "autofill_no_domain": "Unknown Domain",

        # ===================
        # Web Storage Details module
        # ===================
        "web_storage_title": "Web Storage Details",
        "web_storage_description": "Web storage allows websites to store data locally in the browser. This section shows key-value pairs that websites have stored, which may contain user preferences, session data, or tracking information.",
        "web_storage_stored_site": "Stored Site",
        "web_storage_key": "Key",
        "web_storage_value": "Value",
        "web_storage_type": "Type",
        "web_storage_local": "Local",
        "web_storage_session": "Session",
        "web_storage_sites": "sites",
        "web_storage_entries": "entries",
        "no_web_storage_entries": "No storage entries found.",
        "no_web_storage_sites": "No web storage sites found matching the filter criteria.",

        # ===================
        # Jump Lists module
        # ===================
        "jl_description": "Jump Lists are a Windows feature that tracks recently and frequently used items per application. They are stored as .lnk shortcut files and can contain file paths, URLs, and access timestamps. This data provides insight into which applications were used and what documents or websites were accessed.",
        "jl_application": "Application",
        "jl_appid": "App ID",
        "jl_title": "Title",
        "jl_url": "URL",
        "jl_target_path": "Target Path",
        "jl_access_time": "Access Time",
        "jl_creation_time": "Creation Time",
        "jl_access_count": "Access Count",
        "jl_pin_status": "Pin Status",
        "jl_jumplist_path": "Jump List Path",
        "jl_entries": "entries",
        "no_jump_lists_found": "No jump list entries found matching the filter criteria.",

        # ===================
        # Installed Applications module
        # ===================
        "installed_applications_title": "Installed Applications",
        "no_installed_applications_found": "No installed applications found matching the filter criteria.",
        "applications": "applications",

        # ===================
        # Bookmarks module
        # ===================
        "bookmarks_title": "Bookmarks",
        "bookmark_title": "Title",
        "bookmark_url": "URL",
        "bookmark_folder": "Folder",
        "bookmark_date_added": "Date Added",
        "bookmark_browser": "Browser",
        "no_bookmarks_found": "No bookmarks found matching the filter criteria.",
        "bookmarks": "bookmarks",

        # ===================
        # Browser History module
        # ===================
        "browser_history_title": "Browser History",
        "browser_history_description": "Browser history records websites visited by the user, including page titles, visit times, and how the pages were accessed. This data provides insight into browsing patterns and user activity.",
        "browser_history_visit_time": "Visit Time",
        "browser_history_visit_count": "Visits",
        "browser_history_transition_type": "Access Type",
        "browser_history_entries": "entries",
        "no_browser_history_found": "No browser history entries found matching the filter criteria.",

        # ===================
        # Site Engagement module
        # ===================
        "site_engagement_title": "Site Engagement",
        "site_engagement_description": "Site engagement data shows how often and how intensively a user has interacted with websites. This includes visit frequency, session duration, and media playback activity.",
        "site_engagement_origin": "Origin",
        "site_engagement_type": "Type",
        "site_engagement_type_site": "Site",
        "site_engagement_type_media": "Media",
        "site_engagement_score": "Score",
        "site_engagement_visits": "Visits",
        "site_engagement_last_engagement": "Last Engagement",
        "site_engagement_entries": "entries",
        "site_engagement_min_score": "score ≥ {score}",
        "no_site_engagement_found": "No site engagement data found matching the filter criteria.",
        "sort_score_highest": "highest score first",
        "sort_score_lowest": "lowest score first",
        "sort_visits_most": "most visits first",
        "sort_visits_least": "least visits first",

        # ===================
        # Appendix URL list
        # ===================
        "no_urls_for_filters": "No URLs found for the selected filters.",

        # ===================
        # Common / shared
        # ===================
        "showing_x_of_y": "showing {shown} of {total}",
        "and_x_more": "... and {count} more items not shown",
        "and_x_more_urls": "... and {count} more URLs",
        "no_data": "No data found.",
        "error_loading": "Error loading data",
        "unknown": "Unknown",
        "filter_any_tag": "with any tag",
        "filter_tagged": "tagged \"{tag}\"",
        "filter_any_match": "with any match",
        "filter_matching": "matching \"{match}\"",
        "filter_any_source": "from any source",
        "filter_from_source": "from \"{source}\"",
        "filter_grouped_by_domain": "grouped by domain",
        "filter_sorted_by": "sorted by {sort}",
        "filter_all_files": "all files",
        "filter_all_urls": "all URLs",
        "filter_all_domains": "All domains",
        "filter_any_tagged": "Any tagged",
        "filter_tag_label": "Tag: {tag}",
        "filter_all_tags": "All tags",
        "filter_all_matches": "All matches",
        "filter_any_hash_match": "Any hash match",
        "filter_match_label": "Match: {match}",
        "filter_domain_label": "Domain: {domain}",
        "filter_tagged_urls": "tagged URLs",
        "filter_matched_urls": "matched URLs",
        "filter_tag_value": "tag: {tag}",
        "filter_match_value": "match: {match}",
        "filter_domain_contains": "domain contains: {domain}",
        "filter_min_occurrences": "≥{count} occurrences",
        "filter_including_deleted": "including deleted",
        "sort_newest_first": "newest first",
        "sort_oldest_first": "oldest first",
        "sort_name_az": "name A-Z",
        "sort_name_za": "name Z-A",
        "sort_path_az": "path A-Z",
        "sort_path_za": "path Z-A",
        "sort_most_frequent_first": "most frequent first",
        "sort_least_frequent_first": "least frequent first",
        "sort_first_seen_newest": "first seen newest",
        "sort_first_seen_oldest": "first seen oldest",
        "sort_last_seen_newest": "last seen newest",
        "sort_last_seen_oldest": "last seen oldest",
        "sort_url_az": "URL A-Z",
        "sort_url_za": "URL Z-A",
        "sort_largest_first": "largest first",
        "sort_smallest_first": "smallest first",
        "sort_domain_shortest_first": "domain shortest first",
        "sort_domain_longest_first": "domain longest first",
    },

    "de": {
        # ===================
        # Base template
        # ===================
        "toc_title": "Inhaltsverzeichnis",
        "page_of": "Seite {page} von {pages}",
        "page": "Seite",
        "of": "von",
        "generated": "Erstellt",
        "appendix": "Anhang",
        "no_content": "Kein Inhalt",
        "no_sections_message": "Diesem Bericht wurden keine Abschnitte hinzugefügt. Verwenden Sie die Registerkarte Berichte, um benutzerdefinierte Abschnitte und Module hinzuzufügen.",

        # Title page labels
        "case_number": "Fallnummer",
        "evidence": "Asservat",
        "investigator": "Ermittler",
        "department": "Abteilung",
        "notes": "Notizen",

        # Author section
        "report_created_by": "Bericht erstellt von",
        "function": "Funktion",
        "name": "Name",
        "date": "Datum",

        # ===================
        # System summary module
        # ===================
        "system_info": "Systeminformationen",
        "user_accounts": "Benutzerkonten",
        "installed_software": "Installierte Software",
        "autostart_entries": "Autostart-Einträge",
        "network_config": "Netzwerkkonfiguration",
        "property": "Eigenschaft",
        "value": "Wert",
        "no_system_info": "Keine Systeminformationen in den Registry-Daten gefunden.",
        "no_users_found": "Keine Benutzerkonten in den Registry-Daten gefunden.",
        "no_software_found": "Keine installierte Software in den Registry-Daten gefunden.",
        "no_autostart_found": "Keine Autostart-Einträge in den Registry-Daten gefunden.",
        "no_network_found": "Keine Netzwerkkonfiguration in den Registry-Daten gefunden.",
        "connected_profiles": "Verbundene Netzwerkprofile",
        "mapped_drives": "Zugeordnete Netzlaufwerke",
        "local_accounts_found": "{count} lokale(s) Konto/Konten in SAM-Hive gefunden",

        # System info field labels
        "os_version": "Betriebssystemversion",
        "os_build": "Build-Nummer",
        "os_display_version": "Anzeigeversion",
        "registered_owner": "Registrierter Besitzer",
        "install_date": "Installationsdatum",
        "computer_name": "Computername",
        "timezone": "Zeitzone",
        "timezone_key": "Zeitzonenschlüssel",
        "last_shutdown": "Letzte Abschaltung",
        "rdp_status": "RDP-Status",
        "default_browser": "Standardbrowser",
        "downloads_path": "Downloads-Pfad",
        "rdp_enabled": "Aktiviert",
        "rdp_disabled": "Deaktiviert",

        # User table headers
        "username": "Benutzername",
        "profile_path": "Profilpfad",
        "account_type": "Kontotyp",
        "registry_path": "Registry-Pfad",
        "local_account": "Lokales Konto",
        "profile_only": "Nur Profil",

        # Software table
        "software_name": "Softwarename",
        "version": "Version",
        "publisher": "Herausgeber",
        "install_location": "Installationspfad",

        # Autostart table
        "entry_name": "Name",
        "command_path": "Befehl / Pfad",
        "source": "Quelle",
        "run_keys": "Run-Schlüssel (Autostart-Programme)",
        "run_once": "RunOnce",
        "browser_helper_objects": "Browser-Hilfsobjekte (BHO)",
        "clsid": "CLSID",
        "services": "Dienste",
        "service_name": "Dienstname",
        "type": "Typ",

        # Network labels
        "dhcp_ip": "DHCP-IP-Adresse",
        "dns_server": "DNS-Server",
        "default_gateway": "Standardgateway",
        "dhcp_server": "DHCP-Server",
        "domain": "Domäne",
        "connected_network": "Verbundenes Netzwerk",
        "last_connected": "Zuletzt verbunden",
        "mapped_drive": "Zugeordnetes Laufwerk",
        "interface_config": "Schnittstellenkonfiguration",
        "network_profiles": "Netzwerkprofile",
        "profile_name": "Profilname",
        "network_type": "Typ",
        "category": "Kategorie",
        "created": "Erstellt",
        "wifi": "WLAN",
        "wired": "Kabel",
        "mobile": "Mobil",
        "private": "Privat",
        "public": "Öffentlich",
        "drive_path": "Netzwerkpfad",
        "proxy_settings": "Proxy-Einstellungen",
        "internet_policy": "Internetrichtlinie",

        # Shell folder paths
        "pictures_path": "Bilderpfad",
        "videos_path": "Videopfad",
        "documents_path": "Dokumentenpfad",
        "desktop_path": "Desktoppfad",

        # Browser detection section
        "browser_detection": "Browsererkennung",
        "registered_browsers": "Registrierte Browser",
        "browser_name": "Browser",
        "browser_app_paths": "Browser-Anwendungspfade",
        "ie_edge_settings": "IE / Edge Einstellungen",
        "no_browser_detection": "Keine Browsererkennungsdaten in den Registry-Daten gefunden.",

        # User activity section
        "user_activity_title": "Benutzeraktivität",
        "recent_images": "Kürzlich aufgerufene Bilder",
        "typed_urls": "Eingegebene URLs (IE/Edge)",
        "typed_paths_title": "Eingegebene Pfade (Explorer)",
        "recent_documents": "Zuletzt verwendete Dokumente",
        "open_save_mru": "Öffnen/Speichern-Dialogverlauf",
        "open_save_last_visited": "Zuletzt besuchte Ordner (Öffnen/Speichern)",
        "explorer_searches": "Explorer-Suchbegriffe",
        "search_term": "Suchbegriff",
        "no_user_activity": "Keine Benutzeraktivitätsdaten in den Registry-Daten gefunden.",

        # Execution history section
        "execution_history": "Ausführungsverlauf",
        "program_path": "Programmpfad",
        "run_count": "Ausführungen",
        "focus_count": "Fokus-Anzahl",
        "focus_time": "Fokus-Zeit",
        "last_run": "Letzte Ausführung",
        "no_execution_history": "Kein Ausführungsverlauf in den Registry-Daten gefunden.",

        # ===================
        # Activity summary module
        # ===================
        "activity_overview": "Aktivitätsübersicht",
        "activity_period": "Aktivitätszeitraum",
        "duration": "Dauer",
        "days": "Tage",
        "hours": "Stunden",
        "weeks": "Wochen",
        "total_events": "Ereignisse gesamt",
        "avg_events_per_day": "Durchschnittliche Ereignisse/Tag",
        "no_timeline_events": "Keine Zeitleisten-Ereignisse für die ausgewählten Filter gefunden.",
        "events_by_type": "Ereignisse nach Typ",
        "events_by_type_desc": "Diese Tabelle zeigt die Verteilung der aufgezeichneten Ereignisse auf verschiedene Aktivitätskategorien. Höhere Werte deuten auf häufigere Aktivität in dieser Kategorie während des Analysezeitraums hin.",
        "event_type": "Ereignistyp",
        "count": "Anzahl",
        "percentage": "Prozent",
        "daily_activity": "Tägliche Aktivität",
        "daily_activity_desc": "Visuelle Übersicht der Systemaktivitätsintensität pro Tag. Längere Balken zeigen höhere Aktivität an.",
        "date": "Datum",
        "activity": "Aktivität",
        "events": "Ereignisse",
        "showing_first_days": "Zeige erste",
        "inactivity_gaps": "Signifikante Inaktivitätslücken",
        "from": "Von",
        "to": "Bis",
        "no_gaps_found": "Keine signifikanten Inaktivitätslücken gefunden",
        "event_group_all": "Alle Ereignisse",
        "event_group_browser": "Browser-Aktivität",
        "event_group_downloads": "Downloads",
        "event_group_authentication": "Authentifizierung",
        "event_group_media": "Medienwiedergabe",

        # ===================
        # URL summary module
        # ===================
        "url": "URL",
        "urls": "URLs",
        "domain": "Domäne",
        "occurrences": "Vorkommen",
        "tags": "Tags",
        "first_seen": "Zuerst gesehen",
        "last_seen": "Zuletzt gesehen",
        "no_urls_found": "Keine URLs gefunden, die den Filterkriterien entsprechen.",
        "domains": "Domänen",
        "distinct_urls": "verschiedene URLs",
        "filter": "Filter",
        "and_more": "und",
        "more_urls": "weitere URLs",

        # ===================
        # URL activity timeline module
        # ===================
        "url_activity_summary": "URL-Aktivitätsübersicht",
        "total_url_events": "URL-Ereignisse gesamt",
        "unique_urls": "Eindeutige URLs",
        "domain_activity": "Domänenaktivität",
        "Domains": "Domänen",
        "showing": "Zeige",
        "of": "von",
        "no_url_events_found": "Keine URL-Ereignisse gefunden, die den Filterkriterien entsprechen.",

        # ===================
        # Images module
        # ===================
        "no_preview": "Keine Vorschau",
        "md5": "MD5",
        "no_images_found": "Keine Bilder gefunden, die den Filterkriterien entsprechen.",
        "images": "Bilder",
        "exif_taken": "Aufgenommen",
        "exif_camera": "Kamera",
        "exif_model": "Modell",
        "exif_gps_lat": "GPS-Breitengrad",
        "exif_gps_lon": "GPS-Längengrad",
        "showing_x_of_y_images": "{shown} von {total} Bildern",

        # ===================
        # Screenshots module
        # ===================
        "screenshot_captured": "Aufgenommen",
        "screenshot_uploaded": "Hochgeladen",
        "screenshot_additional": "Weitere Screenshots",
        "screenshots": "Screenshot(s)",
        "no_screenshots_found": "Keine Screenshots gefunden.",
        "untitled": "Ohne Titel",
        "total": "Gesamt",

        # ===================
        # Downloaded images module
        # ===================
        "downloaded": "Heruntergeladen",
        "size": "Grösse",
        "no_downloaded_images": "Keine heruntergeladenen Bilder gefunden, die den Filterkriterien entsprechen.",

        # ===================
        # Browser Downloads module
        # ===================
        "filename": "Dateiname",
        "state": "Status",
        "start_time": "Startzeit",
        "end_time": "Endzeit",
        "no_downloads_found": "Keine Browser-Downloads gefunden, die den Filterkriterien entsprechen.",

        # ===================
        # Tagged file list module
        # ===================
        "tagged_file_list_title": "Dateiliste",
        "path": "Pfad",
        "file_name": "Dateiname",
        "extension": "Erw",
        "modified": "Geändert",
        "accessed": "Zugegriffen",
        "deleted": "gelöscht",
        "no_files_found": "Keine Dateien gefunden, die den Filterkriterien entsprechen.",
        "files": "Dateien",
        "unlimited": "Unbegrenzt",

        # ===================
        # Credentials module
        # ===================
        "origin_url": "Ursprungs-URL",
        "username_field": "Benutzerfeld",
        "username": "Benutzername",
        "browser": "Browser",
        "profile": "Profil",
        "password": "Passwort",
        "created": "Erstellt",
        "last_used": "Zuletzt benutzt",
        "stored": "Gespeichert",
        "none": "Keines",
        "credentials": "Anmeldedaten",
        "no_credentials_found": "Keine Anmeldedaten gefunden, die den Filterkriterien entsprechen.",

        # ===================
        # Autofill module
        # ===================
        "field_name": "Feldname",
        "value": "Wert",
        "use_count": "Anzahl",
        "first_used": "Erstmals benutzt",
        "entries": "Einträge",
        "no_autofill_found": "Keine Autofill-Einträge gefunden, die den Filterkriterien entsprechen.",

        # ===================
        # Autofill Form Data module
        # ===================
        "autofill_form_data_title": "Autofill-Formulardaten",
        "autofill_form_data_description": "Autofill-Daten enthalten Werte, die der Browser automatisch aus Webformularen gespeichert hat. Dazu gehören Benutzernamen, Adressen, Telefonnummern und andere Informationen, die der Benutzer auf Websites eingegeben hat. Die Daten zeigen, welche Felder ausgefüllt wurden und wann sie erstmals und zuletzt verwendet wurden.",
        "autofill_no_domain": "Unbekannte Domain",

        # ===================
        # Web Storage Details module
        # ===================
        "web_storage_title": "Web-Speicher Details",
        "web_storage_description": "Web-Speicher ermöglicht Websites, Daten lokal im Browser zu speichern. Dieser Abschnitt zeigt Schlüssel-Wert-Paare, die von Websites gespeichert wurden und Benutzereinstellungen, Sitzungsdaten oder Tracking-Informationen enthalten können.",
        "web_storage_stored_site": "Gespeicherte Seite",
        "web_storage_key": "Schlüssel",
        "web_storage_value": "Wert",
        "web_storage_type": "Typ",
        "web_storage_local": "Lokal",
        "web_storage_session": "Sitzung",
        "web_storage_sites": "Seiten",
        "web_storage_entries": "Einträge",
        "no_web_storage_entries": "Keine Speichereinträge gefunden.",
        "no_web_storage_sites": "Keine Web-Storage-Seiten gefunden, die den Filterkriterien entsprechen.",

        # ===================
        # Jump Lists module
        # ===================
        "jl_description": "Sprunglisten (Jump Lists) sind eine Windows-Funktion, die kürzlich und häufig verwendete Elemente pro Anwendung erfasst. Sie werden als .lnk-Verknüpfungsdateien gespeichert und können Dateipfade, URLs und Zugriffszeitstempel enthalten. Diese Daten geben Aufschluss darüber, welche Anwendungen verwendet und welche Dokumente oder Websites aufgerufen wurden.",
        "jl_application": "Anwendung",
        "jl_appid": "App-ID",
        "jl_title": "Titel",
        "jl_url": "URL",
        "jl_target_path": "Zielpfad",
        "jl_access_time": "Zugriffszeit",
        "jl_creation_time": "Erstellungszeit",
        "jl_access_count": "Zugriffe",
        "jl_pin_status": "Anheftstatus",
        "jl_jumplist_path": "Sprunglisten-Pfad",
        "jl_entries": "Einträge",
        "no_jump_lists_found": "Keine Sprunglisten-Einträge gefunden, die den Filterkriterien entsprechen.",

        # ===================
        # Installed Applications module
        # ===================
        "installed_applications_title": "Installierte Anwendungen",
        "no_installed_applications_found": "Keine installierten Anwendungen gefunden, die den Filterkriterien entsprechen.",
        "applications": "Anwendungen",

        # ===================
        # Bookmarks module
        # ===================
        "bookmarks_title": "Lesezeichen",
        "bookmark_title": "Titel",
        "bookmark_url": "URL",
        "bookmark_folder": "Ordner",
        "bookmark_date_added": "Hinzugefügt",
        "bookmark_browser": "Browser",
        "no_bookmarks_found": "Keine Lesezeichen gefunden, die den Filterkriterien entsprechen.",
        "bookmarks": "Lesezeichen",

        # ===================
        # Browser History module
        # ===================
        "browser_history_title": "Browserverlauf",
        "browser_history_description": "Der Browserverlauf dokumentiert die vom Benutzer besuchten Websites, einschliesslich Seitentitel, Besuchszeiten und wie die Seiten aufgerufen wurden. Diese Daten geben Einblick in Surfmuster und Benutzeraktivitäten.",
        "browser_history_visit_time": "Besuchszeit",
        "browser_history_visit_count": "Besuche",
        "browser_history_transition_type": "Zugriffsart",
        "browser_history_entries": "Einträge",
        "no_browser_history_found": "Keine Browserverlaufseinträge gefunden, die den Filterkriterien entsprechen.",

        # ===================
        # Site Engagement module
        # ===================
        "site_engagement_title": "Seitenaktivität",
        "site_engagement_description": "Seitenaktivitätsdaten zeigen, wie oft und wie intensiv ein Benutzer mit Websites interagiert hat. Dies umfasst Besuchshäufigkeit, Sitzungsdauer und Medienwiedergabe-Aktivität.",
        "site_engagement_origin": "Ursprung",
        "site_engagement_type": "Typ",
        "site_engagement_type_site": "Seite",
        "site_engagement_type_media": "Medien",
        "site_engagement_score": "Punktzahl",
        "site_engagement_visits": "Besuche",
        "site_engagement_last_engagement": "Letzte Aktivität",
        "site_engagement_entries": "Einträge",
        "site_engagement_min_score": "Punktzahl ≥ {score}",
        "no_site_engagement_found": "Keine Seitenaktivitätsdaten gefunden, die den Filterkriterien entsprechen.",
        "sort_score_highest": "höchste Punktzahl zuerst",
        "sort_score_lowest": "niedrigste Punktzahl zuerst",
        "sort_visits_most": "meiste Besuche zuerst",
        "sort_visits_least": "wenigste Besuche zuerst",

        # ===================
        # Appendix URL list
        # ===================
        "no_urls_for_filters": "Keine URLs für die ausgewählten Filter gefunden.",

        # ===================
        # Common / shared
        # ===================
        "showing_x_of_y": "zeige {shown} von {total}",
        "and_x_more": "... und {count} weitere Einträge nicht angezeigt",
        "and_x_more_urls": "... und {count} weitere URLs",
        "no_data": "Keine Daten gefunden.",
        "error_loading": "Fehler beim Laden der Daten",
        "unknown": "Unbekannt",
        "filter_any_tag": "mit beliebigem Tag",
        "filter_tagged": "markiert mit \"{tag}\"",
        "filter_any_match": "mit beliebigem Treffer",
        "filter_matching": "entspricht \"{match}\"",
        "filter_any_source": "aus beliebiger Quelle",
        "filter_from_source": "aus \"{source}\"",
        "filter_grouped_by_domain": "nach Domain gruppiert",
        "filter_sorted_by": "sortiert nach {sort}",
        "filter_all_files": "alle Dateien",
        "filter_all_urls": "alle URLs",
        "filter_all_domains": "Alle Domains",
        "filter_any_tagged": "Beliebig markiert",
        "filter_tag_label": "Tag: {tag}",
        "filter_all_tags": "Alle Tags",
        "filter_all_matches": "Alle Treffer",
        "filter_any_hash_match": "Beliebiger Hash-Treffer",
        "filter_match_label": "Treffer: {match}",
        "filter_domain_label": "Domain: {domain}",
        "filter_tagged_urls": "markierte URLs",
        "filter_matched_urls": "übereinstimmende URLs",
        "filter_tag_value": "Tag: {tag}",
        "filter_match_value": "Treffer: {match}",
        "filter_domain_contains": "Domain enthält: {domain}",
        "filter_min_occurrences": "≥{count} Vorkommen",
        "filter_including_deleted": "einschliesslich gelöschter Dateien",
        "sort_newest_first": "neueste zuerst",
        "sort_oldest_first": "älteste zuerst",
        "sort_name_az": "Name A-Z",
        "sort_name_za": "Name Z-A",
        "sort_path_az": "Pfad A-Z",
        "sort_path_za": "Pfad Z-A",
        "sort_most_frequent_first": "häufigste zuerst",
        "sort_least_frequent_first": "seltenste zuerst",
        "sort_first_seen_newest": "zuerst gesehen (neueste)",
        "sort_first_seen_oldest": "zuerst gesehen (älteste)",
        "sort_last_seen_newest": "zuletzt gesehen (neueste)",
        "sort_last_seen_oldest": "zuletzt gesehen (älteste)",
        "sort_url_az": "URL A-Z",
        "sort_url_za": "URL Z-A",
        "sort_largest_first": "grösste zuerst",
        "sort_smallest_first": "kleinste zuerst",
        "sort_domain_shortest_first": "kürzeste Domain zuerst",
        "sort_domain_longest_first": "längste Domain zuerst",
    },
}

SUPPORTED_LOCALES = list(TRANSLATIONS.keys())
DEFAULT_LOCALE = "en"

# Locale display names for UI
LOCALE_NAMES = {
    "en": "English",
    "de": "Deutsch",
}


def get_translations(locale: str = "en") -> TranslationDict:
    """Return translation dict for locale, fallback to English.

    Args:
        locale: Locale code (e.g., "en", "de")

    Returns:
        Dictionary mapping translation keys to localized strings
    """
    return TRANSLATIONS.get(locale, TRANSLATIONS[DEFAULT_LOCALE])


def get_locale_name(locale: str) -> str:
    """Return display name for a locale.

    Args:
        locale: Locale code (e.g., "en", "de")

    Returns:
        Human-readable locale name
    """
    return LOCALE_NAMES.get(locale, locale)


def format_translation(t: TranslationDict, key: str, **kwargs) -> str:
    """Get a translation and format it with provided values.

    Args:
        t: Translation dictionary
        key: Translation key
        **kwargs: Format arguments

    Returns:
        Formatted translation string, or key if not found
    """
    template = t.get(key, key)
    try:
        return template.format(**kwargs)
    except (KeyError, ValueError):
        return template


# Field label mappings for modules (indicator type -> translation key)
SYSTEM_INFO_LABEL_KEYS = {
    "system:os_version": "os_version",
    "system:os_build": "os_build",
    "system:os_display_version": "os_display_version",
    "system:registered_owner": "registered_owner",
    "system:install_date": "install_date",
    "system:computer_name": "computer_name",
    "system:timezone_standard": "timezone",
    "system:timezone_key": "timezone_key",
    "system:last_shutdown": "last_shutdown",
    "system:rdp_status": "rdp_status",
    "system:default_browser": "default_browser",
    "system:downloads_path": "downloads_path",
    "system:pictures_path": "pictures_path",
    "system:videos_path": "videos_path",
    "system:documents_path": "documents_path",
    "system:desktop_path": "desktop_path",
}

NETWORK_LABEL_KEYS = {
    "network:dhcp_ip": "dhcp_ip",
    "network:dns_server": "dns_server",
    "network:default_gateway": "default_gateway",
    "network:dhcp_server": "dhcp_server",
    "network:domain": "domain",
    "network:connected_profile": "connected_network",
    "network:profile_last_connected": "last_connected",
    "network:mapped_drive": "mapped_drive",
    "network:proxy_settings": "proxy_settings",
    "network:internet_policy": "internet_policy",
}


def get_field_label(indicator_type: str, locale: str = "en") -> str:
    """Get localized label for a system indicator type.

    Args:
        indicator_type: Indicator type (e.g., "system:os_version")
        locale: Locale code

    Returns:
        Localized label string
    """
    t = get_translations(locale)

    # Check system info labels
    if indicator_type in SYSTEM_INFO_LABEL_KEYS:
        key = SYSTEM_INFO_LABEL_KEYS[indicator_type]
        return t.get(key, key)

    # Check network labels
    if indicator_type in NETWORK_LABEL_KEYS:
        key = NETWORK_LABEL_KEYS[indicator_type]
        return t.get(key, key)

    # Fallback: extract readable name from indicator type
    # e.g., "system:os_version" -> "Os Version"
    if ":" in indicator_type:
        name = indicator_type.split(":")[-1]
    else:
        name = indicator_type
    return name.replace("_", " ").title()
