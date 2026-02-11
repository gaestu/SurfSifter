"""
Tor Browser file parsers.

Parsers for Tor configuration and state files:
- torrc: Main configuration file
- state: Tor state with timestamps, guards, circuit build times

Initial implementation with schema warning support
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING

from ._schemas import KNOWN_TORRC_DIRECTIVES, KNOWN_STATE_KEYS, get_directive_category
from core.logging import get_logger

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

__all__ = [
    "parse_torrc",
    "parse_state_file",
    "parse_cached_file",
]

LOGGER = get_logger("extractors.browser.firefox.tor_state._parsers")


def parse_torrc(
    file_path: Path,
    source_path: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Dict[str, Any]:
    """
    Parse a torrc configuration file.

    Args:
        file_path: Path to extracted torrc file
        source_path: Original source path for provenance
        warning_collector: Optional collector for schema warnings

    Returns:
        Dict with:
        - settings: Dict[key, List[values]] for multi-value support
        - records: List of dicts ready for database insertion
        - summary: Summary stats about the config
        - found_directives: Set of all directive names found
    """
    try:
        content = file_path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        LOGGER.warning("Failed to read torrc at %s: %s", file_path, e)
        if warning_collector:
            warning_collector.add_warning(
                warning_type="file_read_error",
                category="binary",
                severity="error",
                artifact_type="tor_config",
                source_file=source_path,
                item_name=str(file_path),
                item_value=str(e),
            )
        return {
            "settings": {},
            "records": [],
            "summary": {"setting_count": 0, "error": str(e)},
            "found_directives": set(),
        }

    settings: Dict[str, List[str]] = {}
    found_directives: Set[str] = set()

    for line_num, line in enumerate(content.splitlines(), 1):
        line = line.strip()

        # Skip empty lines and comments
        if not line or line.startswith("#"):
            continue

        # Strip inline comments
        if "#" in line:
            # Be careful: some values contain # (e.g., bridge fingerprints)
            # Only strip comment if there's whitespace before #
            comment_match = re.search(r'\s+#', line)
            if comment_match:
                line = line[:comment_match.start()].strip()

        if not line:
            continue

        # Parse directive and value
        parts = line.split(None, 1)
        directive = parts[0]
        value = parts[1] if len(parts) > 1 else ""

        found_directives.add(directive)
        settings.setdefault(directive, []).append(value)

    # Check for unknown directives and report warnings
    if warning_collector:
        unknown_directives = found_directives - KNOWN_TORRC_DIRECTIVES
        for directive in sorted(unknown_directives):
            warning_collector.add_warning(
                warning_type="unknown_config_directive",
                category="config",
                severity="info",
                artifact_type="tor_config",
                source_file=source_path,
                item_name=directive,
                item_value=str(settings.get(directive, [])),
            )

    # Build records for database insertion
    records = []
    for directive, values in settings.items():
        for value in values:
            records.append({
                "config_key": directive,
                "config_value": value,
                "value_count": len(values),
            })

    # Build summary
    use_bridges = settings.get("UseBridges", [])
    bridge_count = len(settings.get("Bridge", []))

    summary = {
        "setting_count": len(settings),
        "directive_count": sum(len(v) for v in settings.values()),
        "use_bridges": use_bridges[-1] if use_bridges else None,
        "bridge_count": bridge_count,
        "transport_plugins": settings.get("ClientTransportPlugin", []),
        "socks_ports": settings.get("SocksPort", []),
        "control_ports": settings.get("ControlPort", []),
        "exit_nodes": settings.get("ExitNodes", []),
        "entry_nodes": settings.get("EntryNodes", []),
        "exclude_nodes": settings.get("ExcludeNodes", []),
        "cookie_auth": settings.get("CookieAuthentication", []),
        "hidden_service_dirs": settings.get("HiddenServiceDir", []),
        "has_hidden_services": len(settings.get("HiddenServiceDir", [])) > 0,
    }

    return {
        "settings": settings,
        "records": records,
        "summary": summary,
        "found_directives": found_directives,
    }


def parse_state_file(
    file_path: Path,
    source_path: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Dict[str, Any]:
    """
    Parse a Tor state file.

    The state file contains runtime state including:
    - TorVersion: Version info
    - LastWritten: Last write timestamp
    - Guard: Entry guard information
    - CircuitBuildTimeBin: Circuit build time histogram
    - BWHistory*: Bandwidth history

    Args:
        file_path: Path to extracted state file
        source_path: Original source path for provenance
        warning_collector: Optional collector for schema warnings

    Returns:
        Dict with:
        - entries: Dict[key, List[values]]
        - records: List of dicts ready for database insertion
        - summary: Summary stats
        - found_keys: Set of all state keys found
    """
    try:
        content = file_path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        LOGGER.warning("Failed to read state file at %s: %s", file_path, e)
        if warning_collector:
            warning_collector.add_warning(
                warning_type="file_read_error",
                category="binary",
                severity="error",
                artifact_type="tor_state",
                source_file=source_path,
                item_name=str(file_path),
                item_value=str(e),
            )
        return {
            "entries": {},
            "records": [],
            "summary": {"entry_count": 0, "error": str(e)},
            "found_keys": set(),
        }

    entries: Dict[str, List[str]] = {}
    found_keys: Set[str] = set()

    for line_num, line in enumerate(content.splitlines(), 1):
        line = line.strip()

        if not line:
            continue

        # State file format: KEY VALUE
        parts = line.split(None, 1)
        key = parts[0]
        value = parts[1] if len(parts) > 1 else ""

        found_keys.add(key)
        entries.setdefault(key, []).append(value)

    # Check for unknown keys and report warnings
    if warning_collector:
        unknown_keys = found_keys - KNOWN_STATE_KEYS
        for key in sorted(unknown_keys):
            warning_collector.add_warning(
                warning_type="unknown_state_key",
                category="config",
                severity="info",
                artifact_type="tor_state",
                source_file=source_path,
                item_name=key,
                item_value=str(entries.get(key, [])[:3]),  # First 3 values only
            )

    # Build records for database insertion
    records = []
    for key, values in entries.items():
        for value in values:
            # Try to parse timestamps from certain keys
            timestamp_utc = None
            if key == "LastWritten":
                timestamp_utc = _parse_tor_timestamp(value)
            elif key in ("AccountingIntervalStart", "AccountingSoftLimitHitAt"):
                timestamp_utc = _parse_tor_timestamp(value)

            records.append({
                "state_key": key,
                "state_value": value,
                "timestamp_utc": timestamp_utc,
            })

    # Build summary
    tor_version = entries.get("TorVersion", ["unknown"])[0]
    last_written = entries.get("LastWritten", [None])[0]
    guards = entries.get("Guard", [])

    summary = {
        "entry_count": len(entries),
        "record_count": sum(len(v) for v in entries.values()),
        "tor_version": tor_version,
        "last_written": last_written,
        "last_written_utc": _parse_tor_timestamp(last_written) if last_written else None,
        "guard_count": len(guards),
        "guards": [_parse_guard_entry(g) for g in guards],
        "circuit_build_time_bins": len(entries.get("CircuitBuildTimeBin", [])),
        "has_bandwidth_history": any(k.startswith("BWHistory") for k in found_keys),
        "is_dormant": entries.get("Dormant", ["0"])[0] == "1",
    }

    return {
        "entries": entries,
        "records": records,
        "summary": summary,
        "found_keys": found_keys,
    }


def parse_cached_file(
    file_path: Path,
    source_path: str,
    file_type: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Dict[str, Any]:
    """
    Parse a Tor cached file (cached-microdesc, cached-certs, etc.)

    These files contain relay/circuit data. For now we just extract
    basic metadata (size, entry count) without deep parsing.

    Args:
        file_path: Path to extracted cached file
        source_path: Original source path
        file_type: Type of cached file
        warning_collector: Optional collector for warnings

    Returns:
        Dict with summary info about the cached file
    """
    try:
        content = file_path.read_bytes()
    except Exception as e:
        LOGGER.warning("Failed to read cached file at %s: %s", file_path, e)
        return {
            "summary": {"error": str(e)},
            "records": [],
        }

    size_bytes = len(content)

    # Try to decode as text for analysis
    try:
        text_content = content.decode("utf-8", errors="replace")
    except Exception:
        text_content = ""

    summary = {
        "size_bytes": size_bytes,
        "file_type": file_type,
    }

    # Analyze based on file type
    if "microdesc" in file_type.lower():
        # Count microdescriptor entries (start with "onion-key")
        entry_count = text_content.count("onion-key")
        summary["entry_count"] = entry_count
        summary["entry_type"] = "microdescriptor"
    elif "certs" in file_type.lower():
        # Count certificate entries (start with "dir-key-certificate-version")
        entry_count = text_content.count("dir-key-certificate-version")
        summary["entry_count"] = entry_count
        summary["entry_type"] = "certificate"
    elif "consensus" in file_type.lower():
        # Network consensus
        router_count = text_content.count("\nr ")
        summary["router_count"] = router_count
        summary["entry_type"] = "consensus"

    return {
        "summary": summary,
        "records": [],  # No detailed records for cached files yet
    }


def _parse_tor_timestamp(value: Optional[str]) -> Optional[str]:
    """
    Parse Tor timestamp format to ISO 8601.

    Tor uses format: "YYYY-MM-DD HH:MM:SS"

    Returns:
        ISO 8601 string or None if parsing fails
    """
    if not value:
        return None

    try:
        # Tor timestamp format
        dt = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
        # Tor timestamps are UTC
        dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except (ValueError, AttributeError):
        return None


def _parse_guard_entry(guard_line: str) -> Dict[str, Any]:
    """
    Parse a Guard entry from state file.

    Format varies but typically:
    Guard in=<name> <fingerprint> ... sampled_on=<timestamp> ...

    Returns:
        Dict with parsed guard info
    """
    result = {
        "raw": guard_line,
        "name": None,
        "fingerprint": None,
        "sampled_on": None,
    }

    # Try to extract key fields
    # Format: Guard in=<name> <fingerprint> <flags>
    parts = guard_line.split()

    for i, part in enumerate(parts):
        if part.startswith("in="):
            result["name"] = part[3:]
        elif part.startswith("rsa_id="):
            result["fingerprint"] = part[7:]
        elif part.startswith("sampled_on="):
            result["sampled_on"] = _parse_tor_timestamp(part[11:].replace("T", " ").replace("Z", ""))
        # Old format: second part is fingerprint (40 hex chars)
        elif i == 1 and len(part) == 40 and all(c in "0123456789ABCDEFabcdef" for c in part):
            result["fingerprint"] = part

    return result
