"""Curated Chromium Preferences and Local State configuration parser."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from core.logging import get_logger

LOGGER = get_logger("extractors.browser.chromium.config.parser")

ScalarTypes = (str, bool, int, float)


@dataclass(frozen=True)
class ConfigSpec:
    """A single allowlisted Chromium configuration path."""

    path: str
    config_key: str
    config_type: str
    expected_types: Tuple[type, ...] = ScalarTypes
    notes: Optional[str] = None


PREFERENCES_CONFIG_SPECS: Tuple[ConfigSpec, ...] = (
    ConfigSpec("profile.name", "profile_name", "profile"),
    ConfigSpec("profile.exit_type", "exit_type", "profile"),
    ConfigSpec("profile.created_by_version", "created_by_version", "profile"),
    ConfigSpec("profile.creation_time", "creation_time", "profile"),
    ConfigSpec("homepage", "homepage_url", "startup"),
    ConfigSpec("homepage_is_newtabpage", "homepage_is_newtab", "startup"),
    ConfigSpec("session.restore_on_startup", "restore_on_startup", "startup"),
    ConfigSpec("session.startup_urls", "startup_urls", "startup", (list,)),
    ConfigSpec("browser.show_home_button", "show_home_button", "startup"),
    ConfigSpec("default_search_provider_data.short_name", "default_search_engine", "search"),
    ConfigSpec("default_search_provider_data.keyword", "default_search_keyword", "search"),
    ConfigSpec("default_search_provider_data.search_url", "default_search_url", "search"),
    ConfigSpec("search.suggest_enabled", "search_suggestions_enabled", "search"),
    ConfigSpec("download.default_directory", "download_directory", "download"),
    ConfigSpec("download.prompt_for_download", "download_prompt_for_download", "download"),
    ConfigSpec("download.directory_upgrade", "download_directory_upgrade", "download"),
    ConfigSpec("download.extensions_to_open", "download_extensions_to_open", "download"),
    ConfigSpec("savefile.default_directory", "savefile_directory", "download"),
    ConfigSpec("safebrowsing.enabled", "safe_browsing_enabled", "security"),
    ConfigSpec("safebrowsing.enhanced", "safe_browsing_enhanced", "security"),
    ConfigSpec("safebrowsing.scout_reporting_enabled", "safe_browsing_reporting_enabled", "security"),
    ConfigSpec("https_only_mode_enabled", "https_only_mode_enabled", "security"),
    ConfigSpec("https_first_mode_enabled", "https_first_mode_enabled", "security"),
    ConfigSpec("enable_do_not_track", "do_not_track_enabled", "privacy"),
    ConfigSpec("enable_referrers", "referrers_enabled", "privacy"),
    ConfigSpec("enable_hyperlink_auditing", "hyperlink_auditing_enabled", "privacy"),
    ConfigSpec("dns_over_https.mode", "secure_dns_mode", "security"),
    ConfigSpec("dns_over_https.templates", "secure_dns_templates", "security"),
    ConfigSpec("webrtc.ip_handling_policy", "webrtc_ip_handling_policy", "privacy"),
    ConfigSpec("signin.allowed", "signin_allowed", "account"),
    ConfigSpec("signin.interception_enabled", "signin_interception_enabled", "account"),
    ConfigSpec("sync.requested", "sync_requested", "account"),
    ConfigSpec("sync.keep_everything_synced", "sync_keep_everything_synced", "account"),
    ConfigSpec("extensions.ui.developer_mode", "extensions_developer_mode", "extensions"),
    ConfigSpec("extensions.developer_mode", "extensions_developer_mode", "extensions"),
    ConfigSpec("extensions.disabled", "extensions_disabled", "extensions", (bool, list)),
)

LOCAL_STATE_CONFIG_SPECS: Tuple[ConfigSpec, ...] = (
    ConfigSpec("profile.last_used", "last_used_profile", "local_state", notes="Local State profile inventory"),
    ConfigSpec("profile.last_active_profiles", "last_active_profiles", "local_state", (list,), "Local State profile inventory"),
    ConfigSpec("profile.info_cache", "profile_info_cache", "local_state", (dict,), "Local State profile inventory"),
    ConfigSpec("profile.profile_counts_reported", "profile_counts_reported", "local_state"),
    ConfigSpec("browser.enabled_labs_experiments", "enabled_labs_experiments", "local_state", (list,)),
    ConfigSpec("dns_over_https.mode", "secure_dns_mode", "local_state"),
    ConfigSpec("dns_over_https.templates", "secure_dns_templates", "local_state"),
    ConfigSpec("policy.last_statistics_update", "policy_last_statistics_update", "local_state"),
    ConfigSpec("user_experience_metrics.stability.exited_cleanly", "exited_cleanly", "local_state"),
)


def parse_preferences_config(
    preferences_data: dict,
    browser: str,
    profile: str,
    source_path: str,
    run_id: str,
    *,
    partition_index: Optional[int] = None,
    fs_type: Optional[str] = None,
    logical_path: Optional[str] = None,
    forensic_path: Optional[str] = None,
    warning_collector: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Extract allowlisted profile-level Chromium Preferences records."""
    return _parse_config_specs(
        preferences_data,
        PREFERENCES_CONFIG_SPECS,
        browser=browser,
        profile=profile,
        source_path=source_path,
        run_id=run_id,
        partition_index=partition_index,
        fs_type=fs_type,
        logical_path=logical_path,
        forensic_path=forensic_path,
        warning_collector=warning_collector,
        artifact_type="preferences",
    )


def parse_local_state_config(
    local_state_data: dict,
    browser: str,
    source_path: str,
    run_id: str,
    *,
    partition_index: Optional[int] = None,
    fs_type: Optional[str] = None,
    logical_path: Optional[str] = None,
    forensic_path: Optional[str] = None,
    warning_collector: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Extract allowlisted user-data-root Chromium Local State records."""
    return _parse_config_specs(
        local_state_data,
        LOCAL_STATE_CONFIG_SPECS,
        browser=browser,
        profile=None,
        source_path=source_path,
        run_id=run_id,
        partition_index=partition_index,
        fs_type=fs_type,
        logical_path=logical_path,
        forensic_path=forensic_path,
        warning_collector=warning_collector,
        artifact_type="local_state",
    )


def _parse_config_specs(
    data: dict,
    specs: Iterable[ConfigSpec],
    *,
    browser: str,
    profile: Optional[str],
    source_path: str,
    run_id: str,
    partition_index: Optional[int],
    fs_type: Optional[str],
    logical_path: Optional[str],
    forensic_path: Optional[str],
    warning_collector: Optional[Any],
    artifact_type: str,
) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        _add_type_warning(
            warning_collector,
            source_path=source_path,
            artifact_type=artifact_type,
            item_name="root",
            item_value=type(data).__name__,
            expected="dict",
        )
        return []

    records: List[Dict[str, Any]] = []
    seen_keys: Dict[Tuple[str, str], Tuple[str, Optional[str]]] = {}
    for spec in specs:
        found, value = _get_nested(data, spec.path)
        if not found:
            continue

        serialized_value = None if value is None else _serialize_config_value(value)

        if value is not None and not isinstance(value, spec.expected_types):
            _add_type_warning(
                warning_collector,
                source_path=source_path,
                artifact_type=artifact_type,
                item_name=spec.path,
                item_value=type(value).__name__,
                expected="|".join(t.__name__ for t in spec.expected_types),
            )
            continue

        key = (spec.config_type, spec.config_key)
        if key in seen_keys:
            previous_path, previous_value = seen_keys[key]
            if previous_value != serialized_value:
                _add_duplicate_warning(
                    warning_collector,
                    source_path=source_path,
                    artifact_type=artifact_type,
                    config_key=spec.config_key,
                    previous_path=previous_path,
                    previous_value=previous_value,
                    current_path=spec.path,
                    current_value=serialized_value,
                )
            continue
        seen_keys[key] = (spec.path, serialized_value)

        records.append({
            "run_id": run_id,
            "browser": browser,
            "profile": profile,
            "config_type": spec.config_type,
            "config_key": spec.config_key,
            "config_value": serialized_value,
            "value_count": _value_count(value),
            "source_path": source_path,
            "partition_index": partition_index,
            "fs_type": fs_type,
            "logical_path": logical_path or source_path,
            "forensic_path": forensic_path,
            "notes": spec.notes,
        })

    return records


def _get_nested(data: dict, path: str) -> Tuple[bool, Any]:
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False, None
        current = current[part]
    return True, current


def _serialize_config_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _value_count(value: Any) -> int:
    if isinstance(value, (list, dict)):
        return len(value)
    return 1


def _add_type_warning(
    warning_collector: Optional[Any],
    *,
    source_path: str,
    artifact_type: str,
    item_name: str,
    item_value: str,
    expected: str,
) -> None:
    if not warning_collector:
        return
    warning_collector.add_warning(
        warning_type="json_type_mismatch",
        item_name=item_name,
        severity="warning",
        category="json",
        artifact_type=artifact_type,
        source_file=source_path,
        item_value=item_value,
        context_json={"expected": expected},
    )


def _add_duplicate_warning(
    warning_collector: Optional[Any],
    *,
    source_path: str,
    artifact_type: str,
    config_key: str,
    previous_path: str,
    previous_value: Optional[str],
    current_path: str,
    current_value: Optional[str],
) -> None:
    if not warning_collector:
        return
    warning_collector.add_warning(
        warning_type="schema_mismatch",
        item_name=config_key,
        severity="info",
        category="json",
        artifact_type=artifact_type,
        source_file=source_path,
        item_value=current_value,
        context_json={
            "previous_path": previous_path,
            "previous_value": previous_value,
            "current_path": current_path,
        },
    )