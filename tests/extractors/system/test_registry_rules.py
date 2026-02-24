"""Tests for registry analysis rules.

Validates that new rules are properly defined in REGISTRY_TARGETS,
indicator types match expected values, and custom_handler fields are set correctly.
"""
from extractors.system.registry.rules import (
    REGISTRY_TARGETS,
    SYSTEM_INFO_SOFTWARE,
    SYSTEM_INFO_NTUSER,
    USER_ACTIVITY,
    get_registry_targets,
    get_targets_for_hive,
    target_to_dict,
)


class TestRegistryTargetsList:
    """Tests for the REGISTRY_TARGETS list integrity."""

    def test_all_targets_present(self):
        """Verify all expected targets are in REGISTRY_TARGETS."""
        target_names = [t.name for t in REGISTRY_TARGETS]
        assert "system_info_software" in target_names
        assert "system_info_system" in target_names
        assert "system_info_ntuser" in target_names
        assert "user_activity" in target_names
        assert "network_config" in target_names
        assert "startup_items" in target_names
        assert "sam_user_accounts" in target_names

    def test_get_registry_targets_returns_all(self):
        """Verify get_registry_targets returns the full list."""
        targets = get_registry_targets()
        assert len(targets) == len(REGISTRY_TARGETS)

    def test_ntuser_targets_found(self):
        """Verify NTUSER hive targets are found."""
        ntuser_targets = get_targets_for_hive("NTUSER")
        ntuser_names = [t.name for t in ntuser_targets]
        assert "system_info_ntuser" in ntuser_names
        assert "user_activity" in ntuser_names


class TestTypedUrlsRule:
    """Tests for TypedURLs rule definition."""

    def test_typed_urls_rule_exists(self):
        """Verify TypedURLs rule is in USER_ACTIVITY target."""
        action = USER_ACTIVITY.actions[0]
        typed_urls_keys = [
            k for k in action.keys
            if k.custom_handler == "typed_urls"
        ]
        assert len(typed_urls_keys) == 1

    def test_typed_urls_rule_properties(self):
        """Verify TypedURLs rule has correct properties."""
        action = USER_ACTIVITY.actions[0]
        key = next(k for k in action.keys if k.custom_handler == "typed_urls")
        assert key.indicator == "browser:typed_url"
        assert key.confidence == 0.85
        assert "TypedURLs" in key.path


class TestRecentDocsExtensionRules:
    """Tests for RecentDocs per-extension rules."""

    def test_recent_docs_extension_rules_exist(self):
        """Verify RecentDocs per-extension rules for image types."""
        action = USER_ACTIVITY.actions[0]
        recent_docs_keys = [
            k for k in action.keys
            if k.custom_handler == "recent_docs_extension"
        ]
        # Should have rules for .jpg, .jpeg, .png, .gif, .bmp, .webp
        assert len(recent_docs_keys) == 6

    def test_recent_docs_extensions_covered(self):
        """Verify all expected image extensions are covered."""
        action = USER_ACTIVITY.actions[0]
        extensions = [
            k.path.split("\\")[-1]
            for k in action.keys
            if k.custom_handler == "recent_docs_extension"
        ]
        for ext in [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"]:
            assert ext in extensions, f"Missing extension: {ext}"

    def test_recent_docs_indicator_type(self):
        """Verify all RecentDocs extension rules use correct indicator."""
        action = USER_ACTIVITY.actions[0]
        for key in action.keys:
            if key.custom_handler == "recent_docs_extension":
                assert key.indicator == "recent_documents:image"
                assert key.confidence == 0.75


class TestWordWheelQueryRule:
    """Tests for WordWheelQuery rule definition."""

    def test_word_wheel_query_rule_exists(self):
        """Verify WordWheelQuery rule is in USER_ACTIVITY."""
        action = USER_ACTIVITY.actions[0]
        wwq_keys = [
            k for k in action.keys
            if k.custom_handler == "word_wheel_query"
        ]
        assert len(wwq_keys) == 1

    def test_word_wheel_query_properties(self):
        """Verify WordWheelQuery rule has correct properties."""
        action = USER_ACTIVITY.actions[0]
        key = next(k for k in action.keys if k.custom_handler == "word_wheel_query")
        assert key.indicator == "user_activity:explorer_search"
        assert key.confidence == 0.80
        assert "WordWheelQuery" in key.path


class TestUserAssistRule:
    """Tests for UserAssist rule definition."""

    def test_user_assist_rule_exists(self):
        """Verify UserAssist rule is in USER_ACTIVITY."""
        action = USER_ACTIVITY.actions[0]
        ua_keys = [
            k for k in action.keys
            if k.custom_handler == "user_assist"
        ]
        assert len(ua_keys) == 1

    def test_user_assist_properties(self):
        """Verify UserAssist rule has correct properties."""
        action = USER_ACTIVITY.actions[0]
        key = next(k for k in action.keys if k.custom_handler == "user_assist")
        assert key.indicator == "execution:user_assist"
        assert key.confidence == 0.90
        assert "UserAssist" in key.path
        assert key.path.endswith("\\*\\Count")


class TestUserShellFolderExtensions:
    """Tests for new User Shell Folder values."""

    def test_shell_folders_have_pictures_path(self):
        """Verify Pictures path is in User Shell Folders."""
        action = SYSTEM_INFO_NTUSER.actions[0]
        shell_folders_key = next(
            k for k in action.keys
            if "User Shell Folders" in k.path
        )
        value_indicators = [v.indicator for v in shell_folders_key.values]
        assert "system:pictures_path" in value_indicators

    def test_shell_folders_have_all_new_paths(self):
        """Verify all new shell folder paths are present."""
        action = SYSTEM_INFO_NTUSER.actions[0]
        shell_folders_key = next(
            k for k in action.keys
            if "User Shell Folders" in k.path
        )
        value_indicators = [v.indicator for v in shell_folders_key.values]
        assert "system:downloads_path" in value_indicators
        assert "system:pictures_path" in value_indicators
        assert "system:videos_path" in value_indicators
        assert "system:documents_path" in value_indicators
        assert "system:desktop_path" in value_indicators


class TestBrowserConfigRules:
    """Tests for browser configuration rules (IE Main, SearchScopes, Internet Settings)."""

    def test_ie_main_rule_exists(self):
        """Verify IE Main (Start Page, Search Page) rule exists."""
        action = USER_ACTIVITY.actions[0]
        ie_main_keys = [
            k for k in action.keys
            if "Internet Explorer\\Main" in k.path
        ]
        assert len(ie_main_keys) == 1

    def test_ie_main_values(self):
        """Verify IE Main rule has Start Page and Search Page values."""
        action = USER_ACTIVITY.actions[0]
        key = next(k for k in action.keys if "Internet Explorer\\Main" in k.path)
        value_indicators = [v.indicator for v in key.values]
        assert "browser:home_page" in value_indicators
        assert "browser:search_page" in value_indicators

    def test_ie_search_scopes_rule_exists(self):
        """Verify IE SearchScopes wildcard rule exists."""
        action = USER_ACTIVITY.actions[0]
        scope_keys = [
            k for k in action.keys
            if "SearchScopes" in k.path and k.extract_all_values
        ]
        assert len(scope_keys) == 1
        assert scope_keys[0].indicator == "browser:search_scope"

    def test_internet_settings_rule_exists(self):
        """Verify Internet Settings proxy rule exists."""
        action = USER_ACTIVITY.actions[0]
        inet_keys = [
            k for k in action.keys
            if "Internet Settings" in k.path and k.values
        ]
        assert len(inet_keys) == 1
        value_indicators = [v.indicator for v in inet_keys[0].values]
        assert "network:proxy_settings" in value_indicators


class TestInternetPolicyRule:
    """Tests for machine-level Internet Settings policy rule."""

    def test_internet_policy_in_software_hive(self):
        """Verify Internet policy rule is in SOFTWARE hive target."""
        action = SYSTEM_INFO_SOFTWARE.actions[0]
        policy_keys = [
            k for k in action.keys
            if "Policies" in k.path and "Internet Settings" in k.path
        ]
        assert len(policy_keys) == 1
        assert policy_keys[0].indicator == "network:internet_policy"
        assert policy_keys[0].extract_all_values is True


class TestTargetToDict:
    """Tests for target_to_dict serialization with custom_handler."""

    def test_custom_handler_serialized(self):
        """Verify custom_handler is included in dict output."""
        target_dict = target_to_dict(USER_ACTIVITY)
        keys = target_dict["actions"][0]["keys"]

        typed_urls = [k for k in keys if k.get("custom_handler") == "typed_urls"]
        assert len(typed_urls) == 1

        user_assist = [k for k in keys if k.get("custom_handler") == "user_assist"]
        assert len(user_assist) == 1

    def test_no_custom_handler_omitted(self):
        """Verify custom_handler is NOT present in dicts for keys without it."""
        target_dict = target_to_dict(USER_ACTIVITY)
        keys = target_dict["actions"][0]["keys"]

        # The first few keys (RecentDocs, OpenSavePidlMRU, TypedPaths) have no custom_handler
        recent_docs = next(k for k in keys if "RecentDocs" in k["path"] and "custom_handler" not in k)
        assert "custom_handler" not in recent_docs


class TestLastVisitedPidlMRURule:
    """Tests for LastVisitedPidlMRU rule definition."""

    def test_last_visited_rule_exists(self):
        """Verify LastVisitedPidlMRU rule is in USER_ACTIVITY."""
        action = USER_ACTIVITY.actions[0]
        lv_keys = [
            k for k in action.keys
            if "LastVisitedPidlMRU" in k.path
        ]
        assert len(lv_keys) == 1

    def test_last_visited_rule_properties(self):
        """Verify LastVisitedPidlMRU rule has correct properties."""
        action = USER_ACTIVITY.actions[0]
        key = next(k for k in action.keys if "LastVisitedPidlMRU" in k.path)
        assert key.indicator == "open_save_dialog_last_visited"
        assert key.extract is True
        assert key.confidence == 0.75


class TestRegisteredBrowsersRule:
    """Tests for StartMenuInternet (registered browsers) rule."""

    def test_start_menu_internet_rule_exists(self):
        """Verify StartMenuInternet wildcard rule is in SOFTWARE hive."""
        action = SYSTEM_INFO_SOFTWARE.actions[0]
        smi_keys = [
            k for k in action.keys
            if "StartMenuInternet" in k.path
        ]
        assert len(smi_keys) == 1

    def test_start_menu_internet_properties(self):
        """Verify StartMenuInternet rule has correct properties."""
        action = SYSTEM_INFO_SOFTWARE.actions[0]
        key = next(k for k in action.keys if "StartMenuInternet" in k.path)
        assert key.indicator == "browser:registered_browser"
        assert key.extract is True
        assert key.confidence == 0.90
        assert key.path.endswith("\\*")


class TestBrowserAppPathsRules:
    """Tests for browser App Paths rules."""

    def test_app_paths_rules_exist(self):
        """Verify App Paths rules exist for known browsers."""
        action = SYSTEM_INFO_SOFTWARE.actions[0]
        app_path_keys = [
            k for k in action.keys
            if "App Paths" in k.path and k.indicator == "browser:app_path"
        ]
        assert len(app_path_keys) == 7

    def test_app_paths_browsers_covered(self):
        """Verify all expected browser executables have App Paths rules."""
        action = SYSTEM_INFO_SOFTWARE.actions[0]
        app_path_exes = [
            k.path.split("\\")[-1].lower()
            for k in action.keys
            if "App Paths" in k.path and k.indicator == "browser:app_path"
        ]
        for exe in ["chrome.exe", "msedge.exe", "firefox.exe", "iexplore.exe",
                     "brave.exe", "opera.exe", "vivaldi.exe"]:
            assert exe in app_path_exes, f"Missing App Path rule for: {exe}"

    def test_app_paths_use_extract_all_values(self):
        """Verify all App Paths rules use extract_all_values."""
        action = SYSTEM_INFO_SOFTWARE.actions[0]
        for key in action.keys:
            if "App Paths" in key.path and key.indicator == "browser:app_path":
                assert key.extract_all_values is True
                assert key.confidence == 0.85
