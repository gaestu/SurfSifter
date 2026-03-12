"""
DPAPI evidence collector — gather DPAPI-related files from evidence image.

Collects:
- SYSTEM/SAM/SECURITY registry hives (or reuses from registry extractor output)
- DPAPI master key files from each user profile
- Chromium Local State files for each supported browser
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional

from core.logging import get_logger
from ...callbacks import ExtractorCallbacks

LOGGER = get_logger("extractors.system.dpapi_decrypt._collector")

# --- Hive patterns (case-insensitive leading directories) ---

HIVE_PATTERNS: Dict[str, str] = {
    "SYSTEM": "[Ww][Ii][Nn][Dd][Oo][Ww][Ss]/[Ss][Yy][Ss][Tt][Ee][Mm]32/[Cc][Oo][Nn][Ff][Ii][Gg]/SYSTEM",
    "SAM": "[Ww][Ii][Nn][Dd][Oo][Ww][Ss]/[Ss][Yy][Ss][Tt][Ee][Mm]32/[Cc][Oo][Nn][Ff][Ii][Gg]/SAM",
    "SECURITY": "[Ww][Ii][Nn][Dd][Oo][Ww][Ss]/[Ss][Yy][Ss][Tt][Ee][Mm]32/[Cc][Oo][Nn][Ff][Ii][Gg]/SECURITY",
}

# --- DPAPI master key patterns ---
# Users/*/AppData/Roaming/Microsoft/Protect/<SID>/<GUID>  (master key files inside SID dirs)
DPAPI_PROTECT_PATTERN = (
    "[Uu][Ss][Ee][Rr][Ss]/*/AppData/Roaming/Microsoft/Protect/*/*"
)

# GUID regex: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
_GUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

# --- Chromium Local State paths ---
CHROMIUM_LOCAL_STATE_PATTERNS = [
    ("chrome", "[Uu][Ss][Ee][Rr][Ss]/*/AppData/Local/Google/Chrome/User Data/Local State"),
    ("edge", "[Uu][Ss][Ee][Rr][Ss]/*/AppData/Local/Microsoft/Edge/User Data/Local State"),
    ("brave", "[Uu][Ss][Ee][Rr][Ss]/*/AppData/Local/BraveSoftware/Brave-Browser/User Data/Local State"),
    ("opera", "[Uu][Ss][Ee][Rr][Ss]/*/AppData/Roaming/Opera Software/Opera Stable/Local State"),
    ("vivaldi", "[Uu][Ss][Ee][Rr][Ss]/*/AppData/Local/Vivaldi/User Data/Local State"),
]


# --- Data classes ---


@dataclass
class CollectedChromiumProfile:
    browser: str
    profile_name: str
    profile_root: str
    local_state_bytes: Optional[bytes] = None
    local_state_hash: Optional[str] = None
    local_state_path: str = ""


@dataclass
class CollectedUser:
    sid: str
    username: str
    profile_path: str
    master_key_files: Dict[str, bytes] = field(default_factory=dict)   # GUID → bytes
    master_key_hashes: Dict[str, str] = field(default_factory=dict)    # GUID → sha256 hex
    preferred_file: Optional[bytes] = None
    chromium_profiles: List[CollectedChromiumProfile] = field(default_factory=list)


@dataclass
class CollectedEvidence:
    windows_root: str
    system_hive: Optional[bytes] = None
    system_hive_hash: Optional[str] = None
    sam_hive: Optional[bytes] = None
    sam_hive_hash: Optional[str] = None
    security_hive: Optional[bytes] = None
    security_hive_hash: Optional[str] = None
    users: List[CollectedUser] = field(default_factory=list)
    run_id: str = ""

    def to_manifest(self, version: str) -> Dict[str, Any]:
        """Serialize to manifest.json dict."""
        from datetime import datetime, timezone

        hives: Dict[str, Any] = {}
        for name in ("SYSTEM", "SAM", "SECURITY"):
            data = getattr(self, f"{name.lower()}_hive")
            h = getattr(self, f"{name.lower()}_hive_hash")
            if data is not None:
                hives[name] = {
                    "local_path": f"hives/{name}",
                    "hash": f"sha256:{h}" if h else None,
                }

        users_list: List[Dict[str, Any]] = []
        files_list: List[Dict[str, Any]] = []

        for user in self.users:
            mk_list = []
            for guid, mk_hash in user.master_key_hashes.items():
                local_path = f"master_keys/{user.sid}/{guid}"
                mk_list.append({
                    "guid": guid,
                    "local_path": local_path,
                    "hash": f"sha256:{mk_hash}",
                })
                files_list.append({
                    "source_path": f"{user.profile_path}/AppData/Roaming/Microsoft/Protect/{user.sid}/{guid}",
                    "local_filename": guid,
                    "sha256": mk_hash,
                    "artifact_type": "DPAPI Master Key",
                })

            preferred_info = None
            if user.preferred_file is not None:
                preferred_info = {
                    "local_path": f"master_keys/{user.sid}/Preferred",
                }

            chrom_list = []
            for cp in user.chromium_profiles:
                local_state_rel = f"local_state/{cp.browser}_{user.username}/Local State"
                chrom_list.append({
                    "browser": cp.browser,
                    "profile_root": cp.profile_root,
                    "local_state_path": local_state_rel,
                    "local_state_hash": f"sha256:{cp.local_state_hash}" if cp.local_state_hash else None,
                })
                files_list.append({
                    "source_path": cp.local_state_path,
                    "local_filename": "Local State",
                    "sha256": cp.local_state_hash,
                    "artifact_type": "Chromium Local State",
                })

            users_list.append({
                "sid": user.sid,
                "username": user.username,
                "profile_path": user.profile_path,
                "master_keys": mk_list,
                "preferred": preferred_info,
                "chromium_profiles": chrom_list,
            })

        # Add hive files to files list
        for name, hive_info in hives.items():
            h_val = getattr(self, f"{name.lower()}_hive_hash")
            files_list.append({
                "source_path": f"{self.windows_root}/System32/config/{name}",
                "local_filename": name,
                "sha256": h_val,
                "artifact_type": "Registry Hive",
            })

        return {
            "run_id": self.run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "extractor": "system_dpapi_decrypt",
            "version": version,
            "windows_root": self.windows_root,
            "hives": hives,
            "users": users_list,
            "files": files_list,
        }


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_evidence_file(evidence_fs, path: str) -> bytes:
    """Read a file from evidence_fs into bytes using chunked read."""
    with evidence_fs.open_for_read(path) as f:
        chunks = []
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            chunks.append(chunk)
    return b"".join(chunks)


def _write_to_output(data: bytes, dest: Path) -> None:
    """Write bytes to a local file, creating parent dirs as needed."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)


def _extract_username_from_path(path: str) -> str:
    """Extract the username component from a Users/xxx/... path."""
    parts = PurePosixPath(path).parts
    for i, part in enumerate(parts):
        if part.lower() == "users" and i + 1 < len(parts):
            return parts[i + 1]
    return "unknown"


def _try_reuse_registry_hive(
    output_dir: Path, hive_name: str
) -> Optional[bytes]:
    """Try to reuse a hive from the registry extractor output."""
    registry_hives_dir = output_dir.parent / "registry" / "hives"
    if not registry_hives_dir.exists():
        return None
    # Registry extractor saves as SYSTEM_0.hive, SAM_2.hive, etc.
    for hive_file in registry_hives_dir.iterdir():
        if hive_file.name.startswith(f"{hive_name}_") and hive_file.suffix == ".hive":
            try:
                data = hive_file.read_bytes()
                LOGGER.info(
                    "Reusing %s hive from registry extractor: %s",
                    hive_name, hive_file,
                )
                return data
            except OSError:
                pass
    return None


def collect_dpapi_evidence(
    evidence_fs,
    output_dir: Path,
    run_id: str,
    callbacks: ExtractorCallbacks,
) -> CollectedEvidence:
    """Collect DPAPI-related files from evidence.

    Returns a CollectedEvidence instance with all gathered data.
    """
    collected = CollectedEvidence(windows_root="Windows", run_id=run_id)

    # ------------------------------------------------------------------
    # 1. Collect registry hives (try reuse from registry extractor first)
    # ------------------------------------------------------------------
    callbacks.on_step("Collecting registry hives")
    hives_dir = output_dir / "hives"
    hives_dir.mkdir(parents=True, exist_ok=True)

    for hive_name, pattern in HIVE_PATTERNS.items():
        if callbacks.is_cancelled():
            return collected

        # Try reuse first
        reused = _try_reuse_registry_hive(output_dir, hive_name)
        if reused is not None:
            h = _sha256(reused)
            _write_to_output(reused, hives_dir / hive_name)
            setattr(collected, f"{hive_name.lower()}_hive", reused)
            setattr(collected, f"{hive_name.lower()}_hive_hash", h)
            callbacks.on_log(f"Reused {hive_name} hive from registry extractor", "info")
            continue

        # Read from evidence
        try:
            paths = list(evidence_fs.iter_paths(pattern))
            if paths:
                data = _read_evidence_file(evidence_fs, paths[0])
                h = _sha256(data)
                _write_to_output(data, hives_dir / hive_name)
                setattr(collected, f"{hive_name.lower()}_hive", data)
                setattr(collected, f"{hive_name.lower()}_hive_hash", h)
                callbacks.on_log(f"Collected {hive_name} hive ({len(data):,} bytes)", "info")
            else:
                callbacks.on_log(f"{hive_name} hive not found in evidence", "warning")
        except Exception as e:
            callbacks.on_log(f"Error collecting {hive_name} hive: {e}", "error")
            LOGGER.warning("Error collecting %s hive: %s", hive_name, e)

    # ------------------------------------------------------------------
    # 2. Collect DPAPI master key files per user
    # ------------------------------------------------------------------
    callbacks.on_step("Scanning for DPAPI master key files")

    # Discover all files under Protect directories
    user_map: Dict[str, CollectedUser] = {}  # keyed by SID

    try:
        protect_paths = list(evidence_fs.iter_paths(DPAPI_PROTECT_PATTERN))
    except Exception as e:
        callbacks.on_log(f"Error scanning for DPAPI master keys: {e}", "error")
        LOGGER.warning("Error scanning for DPAPI master keys: %s", e)
        protect_paths = []

    # Group paths by SID directory
    # Expected structure: Users/<user>/AppData/Roaming/Microsoft/Protect/<SID>/<file>
    for ppath in protect_paths:
        if callbacks.is_cancelled():
            return collected

        parts = PurePosixPath(ppath).parts
        # Find the Protect directory and get the SID (next component)
        protect_idx = None
        for i, part in enumerate(parts):
            if part.lower() == "protect" and i + 1 < len(parts):
                protect_idx = i
                break
        if protect_idx is None:
            continue

        sid = parts[protect_idx + 1]
        # SID must start with S-
        if not sid.startswith("S-"):
            continue

        if sid not in user_map:
            username = _extract_username_from_path(ppath)
            # Build profile_path from the user component
            users_idx = None
            for i, part in enumerate(parts):
                if part.lower() == "users" and i + 1 < len(parts):
                    users_idx = i
                    break
            profile_path = f"{parts[users_idx]}/{parts[users_idx + 1]}" if users_idx is not None else f"Users/{username}"
            user_map[sid] = CollectedUser(
                sid=sid,
                username=username,
                profile_path=profile_path,
            )

        user = user_map[sid]
        filename = parts[-1] if len(parts) > protect_idx + 2 else ""

        # Check if this is a GUID-named master key file
        if _GUID_RE.match(filename):
            try:
                data = _read_evidence_file(evidence_fs, ppath)
                h = _sha256(data)
                user.master_key_files[filename] = data
                user.master_key_hashes[filename] = h
                mk_dest = output_dir / "master_keys" / sid / filename
                _write_to_output(data, mk_dest)
                callbacks.on_log(
                    f"Collected master key {filename} for {user.username} ({len(data):,} bytes)",
                    "info",
                )
            except Exception as e:
                callbacks.on_log(
                    f"Error reading master key {filename}: {e}", "error"
                )
                LOGGER.warning("Error reading master key %s: %s", ppath, e)

        elif filename.lower() == "preferred":
            try:
                data = _read_evidence_file(evidence_fs, ppath)
                user.preferred_file = data
                pref_dest = output_dir / "master_keys" / sid / "Preferred"
                _write_to_output(data, pref_dest)
            except Exception as e:
                callbacks.on_log(f"Error reading Preferred file: {e}", "error")

    callbacks.on_log(
        f"Found {len(user_map)} user(s) with DPAPI master keys", "info"
    )

    # ------------------------------------------------------------------
    # 3. Collect Chromium Local State files per user
    # ------------------------------------------------------------------
    callbacks.on_step("Scanning for Chromium Local State files")

    for browser, pattern in CHROMIUM_LOCAL_STATE_PATTERNS:
        if callbacks.is_cancelled():
            return collected

        try:
            ls_paths = list(evidence_fs.iter_paths(pattern))
        except Exception as e:
            callbacks.on_log(
                f"Error scanning for {browser} Local State: {e}", "warning"
            )
            continue

        for ls_path in ls_paths:
            username = _extract_username_from_path(ls_path)

            # Extract profile root (everything up to "User Data")
            ls_parts = PurePosixPath(ls_path).parts
            user_data_idx = None
            for i, part in enumerate(ls_parts):
                if part == "User Data":
                    user_data_idx = i
                    break
            if user_data_idx is not None:
                profile_root = "/".join(ls_parts[: user_data_idx + 1])
            else:
                profile_root = str(PurePosixPath(ls_path).parent)

            try:
                data = _read_evidence_file(evidence_fs, ls_path)
                h = _sha256(data)
                ls_dest = output_dir / "local_state" / f"{browser}_{username}" / "Local State"
                _write_to_output(data, ls_dest)

                profile = CollectedChromiumProfile(
                    browser=browser,
                    profile_name="Default",
                    profile_root=profile_root,
                    local_state_bytes=data,
                    local_state_hash=h,
                    local_state_path=ls_path,
                )

                # Attach to the matching user (by username)
                attached = False
                for user in user_map.values():
                    if user.username.lower() == username.lower():
                        user.chromium_profiles.append(profile)
                        attached = True
                        break

                if not attached:
                    # Create a user entry without SID if no match
                    # This can happen if the user has no DPAPI master keys
                    placeholder_sid = f"UNKNOWN-{username}"
                    if placeholder_sid not in user_map:
                        users_idx = None
                        for i, part in enumerate(ls_parts):
                            if part.lower() == "users" and i + 1 < len(ls_parts):
                                users_idx = i
                                break
                        pp = f"{ls_parts[users_idx]}/{ls_parts[users_idx + 1]}" if users_idx is not None else f"Users/{username}"
                        user_map[placeholder_sid] = CollectedUser(
                            sid=placeholder_sid,
                            username=username,
                            profile_path=pp,
                        )
                    user_map[placeholder_sid].chromium_profiles.append(profile)

                callbacks.on_log(
                    f"Collected {browser} Local State for {username}", "info"
                )
            except Exception as e:
                callbacks.on_log(
                    f"Error reading {browser} Local State for {username}: {e}",
                    "error",
                )
                LOGGER.warning("Error reading Local State %s: %s", ls_path, e)

    collected.users = list(user_map.values())

    total_mk = sum(len(u.master_key_files) for u in collected.users)
    total_ls = sum(len(u.chromium_profiles) for u in collected.users)
    callbacks.on_log(
        f"Collection complete: {len(collected.users)} users, "
        f"{total_mk} master keys, {total_ls} Local State files",
        "info",
    )

    return collected
