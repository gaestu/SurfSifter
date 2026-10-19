"""Curated Firefox prefs.js and user.js parser."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from core.logging import get_logger

LOGGER = get_logger("extractors.browser.firefox.config.parser")

COMMENT_PATTERN = re.compile(r"/\*.*?\*/", re.DOTALL)
USER_PREF_PATTERN = re.compile(
    r"^\s*user_pref\s*\(\s*(?P<key>\"(?:\\.|[^\"\\])*\")\s*,\s*(?P<value>.+?)\s*\)\s*;\s*$",
    re.DOTALL,
)
STATEMENT_PREFIX_PATTERN = re.compile(r"^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\(")
INTEGER_PATTERN = re.compile(r"^-?[0-9]+$")


@dataclass(frozen=True)
class PrefSpec:
    """Allowlisted Firefox preference specification."""

    key_pattern: str
    config_key: str
    category: str


ALLOWLIST: Tuple[PrefSpec, ...] = (
    PrefSpec("browser.startup.homepage", "startup_homepage", "startup"),
    PrefSpec("browser.startup.page", "startup_page", "startup"),
    PrefSpec("browser.sessionstore.resume_from_crash", "session_resume_from_crash", "startup"),
    PrefSpec("browser.startup.homepage_override.mstone", "startup_override_mstone", "startup"),
    PrefSpec("browser.download.dir", "download_directory", "download"),
    PrefSpec("browser.download.folderList", "download_folder_list", "download"),
    PrefSpec("browser.download.useDownloadDir", "download_use_default_directory", "download"),
    PrefSpec("browser.helperApps.alwaysAsk.force", "helper_apps_always_ask_force", "download"),
    PrefSpec("browser.helperApps.neverAsk.saveToDisk", "helper_apps_never_ask_save_to_disk", "download"),
    PrefSpec("browser.helperApps.neverAsk.openFile", "helper_apps_never_ask_open_file", "download"),
    PrefSpec("browser.search.defaultenginename", "search_default_engine", "search"),
    PrefSpec("browser.search.region", "search_region", "search"),
    PrefSpec("browser.search.suggest.enabled", "search_suggestions_enabled", "search"),
    PrefSpec("browser.urlbar.suggest.searches", "urlbar_search_suggestions", "search"),
    PrefSpec("network.proxy.type", "proxy_type", "network"),
    PrefSpec("network.proxy.http", "proxy_http_host", "network"),
    PrefSpec("network.proxy.http_port", "proxy_http_port", "network"),
    PrefSpec("network.proxy.ssl", "proxy_ssl_host", "network"),
    PrefSpec("network.proxy.ssl_port", "proxy_ssl_port", "network"),
    PrefSpec("network.proxy.socks", "proxy_socks_host", "network"),
    PrefSpec("network.proxy.socks_port", "proxy_socks_port", "network"),
    PrefSpec("network.proxy.socks_version", "proxy_socks_version", "network"),
    PrefSpec("network.proxy.socks_remote_dns", "proxy_socks_remote_dns", "network"),
    PrefSpec("network.proxy.autoconfig_url", "proxy_autoconfig_url", "network"),
    PrefSpec("network.trr.mode", "doh_mode", "network"),
    PrefSpec("network.trr.uri", "doh_uri", "network"),
    PrefSpec("network.trr.bootstrapAddress", "doh_bootstrap_address", "network"),
    PrefSpec("network.trr.excluded-domains", "doh_excluded_domains", "network"),
    PrefSpec("browser.privatebrowsing.autostart", "private_browsing_autostart", "privacy"),
    PrefSpec("places.history.enabled", "history_enabled", "privacy"),
    PrefSpec("network.cookie.cookieBehavior", "cookie_behavior", "privacy"),
    PrefSpec("privacy.clearOnShutdown.*", "privacy_clear_on_shutdown", "privacy"),
    PrefSpec("privacy.sanitize.*", "privacy_sanitize", "privacy"),
    PrefSpec("privacy.donottrackheader.enabled", "do_not_track_enabled", "privacy"),
    PrefSpec("privacy.globalprivacycontrol.enabled", "global_privacy_control_enabled", "privacy"),
    PrefSpec("dom.security.https_only_mode*", "https_only_mode", "security"),
    PrefSpec("security.tls.version.min", "tls_version_min", "security"),
    PrefSpec("security.tls.version.max", "tls_version_max", "security"),
    PrefSpec("security.enterprise_roots.enabled", "enterprise_roots_enabled", "security"),
    PrefSpec("security.mixed_content.block_active_content", "mixed_content_block_active", "security"),
    PrefSpec("security.mixed_content.block_display_content", "mixed_content_block_display", "security"),
    PrefSpec("network.http.referer.defaultPolicy", "referrer_default_policy", "security"),
    PrefSpec("media.peerconnection.enabled", "webrtc_enabled", "webrtc"),
    PrefSpec("media.peerconnection.ice.proxy_only", "webrtc_ice_proxy_only", "webrtc"),
    PrefSpec("media.peerconnection.ice.default_address_only", "webrtc_default_address_only", "webrtc"),
    PrefSpec("media.peerconnection.ice.no_host", "webrtc_no_host", "webrtc"),
    PrefSpec("xpinstall.signatures.required", "xpinstall_signatures_required", "extensions"),
    PrefSpec("extensions.autoDisableScopes", "extensions_auto_disable_scopes", "extensions"),
    PrefSpec("extensions.enabledScopes", "extensions_enabled_scopes", "extensions"),
    PrefSpec("extensions.update.enabled", "extensions_update_enabled", "extensions"),
    PrefSpec("toolkit.telemetry.enabled", "telemetry_enabled", "telemetry"),
    PrefSpec("toolkit.telemetry.unified", "telemetry_unified", "telemetry"),
    PrefSpec("toolkit.telemetry.archive.enabled", "telemetry_archive_enabled", "telemetry"),
    PrefSpec("datareporting.healthreport.uploadEnabled", "healthreport_upload_enabled", "telemetry"),
    PrefSpec("datareporting.policy.dataSubmissionEnabled", "data_submission_enabled", "telemetry"),
    PrefSpec("services.sync.prefs.sync.*", "sync_pref_indicator", "sync"),
)


def parse_firefox_pref_file(
    raw_text: str,
    *,
    browser: str,
    profile: str,
    source_path: str,
    run_id: str,
    config_type: str,
    partition_index: Optional[int] = None,
    fs_type: Optional[str] = None,
    logical_path: Optional[str] = None,
    forensic_path: Optional[str] = None,
    warning_collector: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Parse allowlisted Firefox user preference entries from prefs.js/user.js."""
    content = COMMENT_PATTERN.sub("", raw_text)
    records: List[Dict[str, Any]] = []
    parsed_entries: List[Tuple[str, Optional[str], str, str, int]] = []

    statements = _split_statements(content)
    for line_no, statement in statements:
        stripped = statement.strip()
        if not stripped:
            continue

        parsed = _parse_user_pref_statement(stripped)
        if parsed is None:
            _record_statement_warning(
                warning_collector,
                source_path=source_path,
                config_type=config_type,
                statement=stripped,
                line_no=line_no,
            )
            continue

        key, value = parsed
        matched_spec = _match_allowlist(key)
        if matched_spec is None:
            continue

        parsed_entries.append((
            key,
            value,
            matched_spec.config_key,
            matched_spec.category,
            line_no,
        ))

    key_counts: Dict[str, int] = {}
    for key, _, _, _, _ in parsed_entries:
        key_counts[key] = key_counts.get(key, 0) + 1

    for key, value, config_key, category, line_no in parsed_entries:
        records.append({
            "run_id": run_id,
            "browser": browser,
            "profile": profile,
            "config_type": config_type,
            "config_key": config_key if "*" not in config_key else key,
            "config_value": value,
            "value_count": key_counts.get(key, 1),
            "source_path": source_path,
            "partition_index": partition_index,
            "fs_type": fs_type,
            "logical_path": logical_path or source_path,
            "forensic_path": forensic_path,
            "notes": f"pref_key={key}; category={category}; line={line_no}",
        })

    return records


def _split_statements(content: str) -> List[Tuple[int, str]]:
    """Split libpref text into semicolon-terminated statements."""
    statements: List[Tuple[int, str]] = []
    buffer: List[str] = []
    in_string = False
    escape = False
    start_line = 1
    line_no = 1
    statement_started = False

    i = 0
    while i < len(content):
        ch = content[i]

        if ch == "\n":
            line_no += 1

        if not in_string:
            if ch == "#":
                i = _skip_to_line_end(content, i)
                continue
            if ch == "/" and i + 1 < len(content) and content[i + 1] == "/":
                i = _skip_to_line_end(content, i)
                continue

        if not statement_started and not ch.isspace():
            start_line = line_no
            statement_started = True

        buffer.append(ch)

        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
        else:
            if ch == '"':
                in_string = True
            elif ch == ";":
                statement = "".join(buffer).strip()
                if statement:
                    statements.append((start_line, statement))
                buffer.clear()
                statement_started = False

        i += 1

    trailing = "".join(buffer).strip()
    if trailing:
        statements.append((start_line, trailing))

    return statements


def _skip_to_line_end(content: str, index: int) -> int:
    while index < len(content) and content[index] != "\n":
        index += 1
    return index


def _parse_user_pref_statement(statement: str) -> Optional[Tuple[str, Optional[str]]]:
    match = USER_PREF_PATTERN.match(statement)
    if not match:
        return None

    try:
        key = _parse_string_literal(match.group("key"))
    except ValueError:
        return None

    raw_value = match.group("value").strip()
    value = _parse_pref_value(raw_value)
    if value is None and raw_value.lower() != "null":
        return None

    return key, value


def _parse_pref_value(raw_value: str) -> Optional[str]:
    lower = raw_value.lower()
    if lower == "true":
        return "true"
    if lower == "false":
        return "false"
    if lower == "null":
        return None
    if INTEGER_PATTERN.match(raw_value):
        return str(int(raw_value))
    if raw_value.startswith('"') and raw_value.endswith('"'):
        return _parse_string_literal(raw_value)

    return None


def _parse_string_literal(token: str) -> str:
    if len(token) < 2 or token[0] != '"' or token[-1] != '"':
        raise ValueError("Invalid quoted string")

    body = token[1:-1]
    out: List[str] = []
    i = 0
    while i < len(body):
        ch = body[i]
        if ch != "\\":
            out.append(ch)
            i += 1
            continue

        if i + 1 >= len(body):
            raise ValueError("Dangling escape")

        nxt = body[i + 1]
        if nxt in {'\\', '"', '/'}:
            out.append(nxt)
            i += 2
            continue
        if nxt == "b":
            out.append("\b")
            i += 2
            continue
        if nxt == "f":
            out.append("\f")
            i += 2
            continue
        if nxt == "n":
            out.append("\n")
            i += 2
            continue
        if nxt == "r":
            out.append("\r")
            i += 2
            continue
        if nxt == "t":
            out.append("\t")
            i += 2
            continue
        if nxt == "u":
            code = body[i + 2:i + 6]
            if len(code) != 4 or not all(c in "0123456789abcdefABCDEF" for c in code):
                raise ValueError("Invalid unicode escape")
            out.append(chr(int(code, 16)))
            i += 6
            continue

        out.append(nxt)
        i += 2

    return "".join(out)


def _match_allowlist(pref_key: str) -> Optional[PrefSpec]:
    for spec in ALLOWLIST:
        pattern = spec.key_pattern
        if pattern.endswith("*"):
            if pref_key.startswith(pattern[:-1]):
                return spec
        elif pref_key == pattern:
            return spec
    return None


def _record_statement_warning(
    warning_collector: Optional[Any],
    *,
    source_path: str,
    config_type: str,
    statement: str,
    line_no: int,
) -> None:
    if warning_collector is None:
        return

    statement_type = "unknown"
    type_match = STATEMENT_PREFIX_PATTERN.match(statement)
    if type_match:
        statement_type = type_match.group(1)

    warning_collector.add_warning(
        warning_type="schema_mismatch",
        item_name=f"line_{line_no}",
        severity="warning",
        category="json",
        artifact_type=config_type,
        source_file=source_path,
        item_value=statement_type,
        context_json={"statement": statement[:400]},
    )
