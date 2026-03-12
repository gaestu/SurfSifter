"""
DPAPI decryption pipeline — decrypt master keys, Chromium keys, and browser blobs.

Orchestrates the full decryption pipeline:
1. Extract boot key from SYSTEM hive
2. Extract NTLM hashes from SAM hive
3. Decrypt DPAPI master keys using NTLM hashes and/or passwords
4. Unwrap Chromium AES application keys
5. Decrypt v10-encrypted credential, cookie, and credit card blobs
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from core.logging import get_logger
from core.database.helpers.dpapi import (
    insert_windows_user,
    insert_dpapi_master_key,
    update_dpapi_master_key_status,
    update_windows_user_key_counts,
    insert_chromium_app_key,
    update_chromium_app_key_status,
    insert_decrypt_audit,
    update_credential_decrypted,
    update_cookie_decrypted,
    update_credit_card_decrypted,
    batch_update_credentials_decrypted,
    batch_update_cookies_decrypted,
)
from core.database.helpers.process_log import insert_process_log
from core.dpapi import (
    extract_boot_key,
    extract_ntlm_hashes,
    extract_dpapi_system_keys,
    decrypt_master_key_with_ntlm,
    decrypt_master_key_with_password,
    decrypt_master_key_with_key,
    extract_chromium_key,
    unwrap_chromium_key,
    decrypt_v10_blob,
    parse_master_key_file,
    parse_preferred_file,
    BootKeyError,
    SAMError,
    LSAError,
    MasterKeyError,
    ChromiumKeyError,
    IntegrityError,
)
from ...callbacks import ExtractorCallbacks

LOGGER = get_logger("extractors.system.dpapi_decrypt._pipeline")


def run_dpapi_pipeline(
    output_dir: Path,
    evidence_conn,
    evidence_id: int,
    config: Dict[str, Any],
    callbacks: ExtractorCallbacks,
) -> Dict[str, int]:
    """Execute the full DPAPI decryption pipeline.

    Returns:
        Summary dict with counts: users, master_keys_found, master_keys_unlocked,
        chromium_keys, decrypted, failed, no_key.
    """
    run_id = config.get("run_id", "")
    auto_sam = config.get("auto_sam", True)
    user_password = config.get("user_password")
    decrypt_passwords = config.get("decrypt_passwords", True)
    decrypt_cookies = config.get("decrypt_cookies", True)
    decrypt_cards = config.get("decrypt_cards", True)
    started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    summary: Dict[str, int] = {
        "users": 0,
        "master_keys_found": 0,
        "master_keys_unlocked": 0,
        "chromium_keys": 0,
        "decrypted": 0,
        "failed": 0,
        "no_key": 0,
    }

    manifest_path = output_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())

    # ------------------------------------------------------------------
    # Phase 1: Extract boot key
    # ------------------------------------------------------------------
    callbacks.on_step("Phase 1: Extracting boot key from SYSTEM hive")
    boot_key: Optional[bytes] = None

    system_info = manifest.get("hives", {}).get("SYSTEM")
    if system_info:
        system_path = output_dir / system_info["local_path"]
        if system_path.exists():
            try:
                system_bytes = system_path.read_bytes()
                boot_key = extract_boot_key(system_bytes)
                callbacks.on_log("Boot key extracted successfully", "info")
            except BootKeyError as e:
                callbacks.on_log(f"Boot key extraction failed: {e}", "warning")
                LOGGER.warning("Boot key extraction failed: %s", e)
        else:
            callbacks.on_log("SYSTEM hive file missing from output", "warning")
    else:
        callbacks.on_log("SYSTEM hive not collected — cannot extract boot key", "warning")

    # ------------------------------------------------------------------
    # Phase 2: Extract NTLM hashes from SAM
    # ------------------------------------------------------------------
    callbacks.on_step("Phase 2: Extracting NTLM hashes from SAM hive")
    ntlm_results: Dict[str, Any] = {}  # SID → NTLMHashResult

    sam_info = manifest.get("hives", {}).get("SAM")
    if sam_info and boot_key:
        sam_path = output_dir / sam_info["local_path"]
        if sam_path.exists():
            try:
                sam_bytes = sam_path.read_bytes()
                ntlm_results = extract_ntlm_hashes(sam_bytes, boot_key)
                callbacks.on_log(
                    f"Extracted NTLM hashes for {len(ntlm_results)} user(s)", "info"
                )
            except SAMError as e:
                callbacks.on_log(f"SAM hash extraction failed: {e}", "warning")
                LOGGER.warning("SAM hash extraction failed: %s", e)
        else:
            callbacks.on_log("SAM hive file missing from output", "warning")
    elif not boot_key:
        callbacks.on_log("Skipping SAM extraction — no boot key available", "warning")
    else:
        callbacks.on_log("SAM hive not collected", "warning")

    # ------------------------------------------------------------------
    # Phase 2b: Extract DPAPI_SYSTEM keys from SECURITY hive
    # ------------------------------------------------------------------
    callbacks.on_step("Phase 2b: Extracting DPAPI_SYSTEM keys from SECURITY hive")
    dpapi_system_keys = None  # DPAPISystemKeys or None

    security_info = manifest.get("hives", {}).get("SECURITY")
    if security_info and boot_key:
        security_path = output_dir / security_info["local_path"]
        if security_path.exists():
            try:
                security_bytes = security_path.read_bytes()
                dpapi_system_keys = extract_dpapi_system_keys(security_bytes, boot_key)
                callbacks.on_log("DPAPI_SYSTEM keys extracted successfully", "info")
            except LSAError as e:
                callbacks.on_log(f"DPAPI_SYSTEM extraction failed: {e}", "warning")
                LOGGER.warning("DPAPI_SYSTEM extraction failed: %s", e)
        else:
            callbacks.on_log("SECURITY hive file missing from output", "warning")
    elif not boot_key:
        callbacks.on_log("Skipping SECURITY extraction — no boot key available", "warning")
    else:
        callbacks.on_log("SECURITY hive not collected", "warning")

    # ------------------------------------------------------------------
    # Phase 3: Insert users and decrypt master keys
    # ------------------------------------------------------------------
    callbacks.on_step("Phase 3: Decrypting DPAPI master keys")

    # Mapping: GUID → decrypted 64-byte master key (across all users)
    decrypted_master_keys: Dict[str, bytes] = {}

    for user_info in manifest.get("users", []):
        if callbacks.is_cancelled():
            break

        sid = user_info["sid"]
        username = user_info["username"]
        profile_path = user_info.get("profile_path", "")
        mk_list = user_info.get("master_keys", [])

        summary["users"] += 1
        summary["master_keys_found"] += len(mk_list)

        # Find matching NTLM hash
        ntlm_hash_bytes: Optional[bytes] = None
        rid = 0

        # Try to match user by SID
        ntlm_result = ntlm_results.get(sid)
        if ntlm_result is None:
            # Try matching by username
            for ntlm_sid, ntlm_r in ntlm_results.items():
                if ntlm_r.username.lower() == username.lower():
                    ntlm_result = ntlm_r
                    break

        if ntlm_result and ntlm_result.ntlm_hash:
            ntlm_hash_bytes = ntlm_result.ntlm_hash
            rid = ntlm_result.rid

        if rid == 0 and sid.startswith("S-") and not sid.startswith("UNKNOWN-"):
            # Extract RID from last component of SID
            try:
                rid = int(sid.rsplit("-", 1)[-1])
            except (ValueError, IndexError):
                pass

        # Insert windows_user record
        user_kwargs: Dict[str, Any] = {
            "profile_path": profile_path,
            "run_id": run_id,
            "master_keys_found": len(mk_list),
            "master_keys_unlocked": 0,
        }
        if ntlm_hash_bytes:
            user_kwargs["ntlm_hash_available"] = 1

        try:
            user_db_id = insert_windows_user(
                evidence_conn, evidence_id, sid=sid, rid=rid, username=username,
                **user_kwargs,
            )
        except Exception as e:
            callbacks.on_log(f"Failed to insert user {username}: {e}", "error")
            LOGGER.warning("Failed to insert user %s: %s", username, e)
            continue

        # Decrypt each master key
        user_unlocked = 0
        for mk_info in mk_list:
            if callbacks.is_cancelled():
                break

            guid = mk_info["guid"]
            mk_local_path = output_dir / mk_info["local_path"]

            if not mk_local_path.exists():
                callbacks.on_log(f"Master key file missing: {mk_local_path}", "warning")
                continue

            mk_bytes = mk_local_path.read_bytes()

            # Insert master key record (initially locked)
            source_path = f"{profile_path}/AppData/Roaming/Microsoft/Protect/{sid}/{guid}"
            try:
                mk_db_id = insert_dpapi_master_key(
                    evidence_conn, evidence_id,
                    sid=sid, guid=guid, source_path=source_path,
                    run_id=run_id, status="locked",
                    file_hash_sha256=mk_info.get("hash", ""),
                )
            except Exception as e:
                callbacks.on_log(f"Failed to insert master key {guid}: {e}", "error")
                continue

            # Parse the master key file
            try:
                mk_file = parse_master_key_file(mk_bytes)
            except Exception as e:
                callbacks.on_log(f"Failed to parse master key {guid}: {e}", "warning")
                try:
                    update_dpapi_master_key_status(
                        evidence_conn, mk_db_id, "error",
                        error_message=str(e),
                    )
                except Exception:
                    pass
                continue

            # Try decryption: password first, then NTLM
            decrypted = False

            # Try examiner-provided password
            if user_password and not decrypted:
                try:
                    result = decrypt_master_key_with_password(mk_file, user_password, sid)
                    decrypted_master_keys[guid] = result.decrypted_key
                    update_dpapi_master_key_status(
                        evidence_conn, mk_db_id, "unlocked",
                        unlock_method="password",
                    )
                    decrypted = True
                    user_unlocked += 1
                    callbacks.on_log(f"Master key {guid} unlocked with password", "info")
                except MasterKeyError:
                    pass

            # Try NTLM hash (auto-SAM)
            if auto_sam and ntlm_hash_bytes and not decrypted:
                try:
                    result = decrypt_master_key_with_ntlm(mk_file, ntlm_hash_bytes, sid)
                    decrypted_master_keys[guid] = result.decrypted_key
                    update_dpapi_master_key_status(
                        evidence_conn, mk_db_id, "unlocked",
                        unlock_method=result.method,
                    )
                    decrypted = True
                    user_unlocked += 1
                    callbacks.on_log(
                        f"Master key {guid} unlocked with {result.method}", "info"
                    )
                except MasterKeyError:
                    pass

            # Fallback: try empty password for accounts with blank passwords
            # Uses password path (SHA1-based) which differs from NTLM path:
            # SHA1("") ≠ MD4(""), producing different DPAPI derived keys.
            if auto_sam and not decrypted:
                try:
                    result = decrypt_master_key_with_password(mk_file, "", sid)
                    decrypted_master_keys[guid] = result.decrypted_key
                    update_dpapi_master_key_status(
                        evidence_conn, mk_db_id, "unlocked",
                        unlock_method="empty_password",
                    )
                    decrypted = True
                    user_unlocked += 1
                    callbacks.on_log(
                        f"Master key {guid} unlocked with empty password", "info"
                    )
                except MasterKeyError:
                    pass

            # Try DPAPI_SYSTEM keys (machine_key and user_key)
            if dpapi_system_keys and not decrypted:
                for key_name, syskey in [
                    ("dpapi_system_user", dpapi_system_keys.user_key),
                    ("dpapi_system_machine", dpapi_system_keys.machine_key),
                ]:
                    try:
                        result = decrypt_master_key_with_key(mk_file, syskey)
                        decrypted_master_keys[guid] = result.decrypted_key
                        update_dpapi_master_key_status(
                            evidence_conn, mk_db_id, "unlocked",
                            unlock_method=key_name,
                        )
                        decrypted = True
                        user_unlocked += 1
                        callbacks.on_log(
                            f"Master key {guid} unlocked with {key_name}", "info"
                        )
                        break
                    except MasterKeyError:
                        pass

            if not decrypted:
                update_dpapi_master_key_status(
                    evidence_conn, mk_db_id, "locked",
                    error_message="No valid credentials available",
                )
                callbacks.on_log(f"Master key {guid} remains locked", "warning")

        summary["master_keys_unlocked"] += user_unlocked

        # Update user record with unlock counts
        try:
            update_windows_user_key_counts(
                evidence_conn, user_db_id, len(mk_list), user_unlocked
            )
        except Exception:
            pass

    callbacks.on_log(
        f"Master keys: {summary['master_keys_unlocked']}/{summary['master_keys_found']} unlocked",
        "info",
    )

    # ------------------------------------------------------------------
    # Phase 4: Unwrap Chromium application keys
    # ------------------------------------------------------------------
    callbacks.on_step("Phase 4: Unwrapping Chromium application keys")

    # Mapping: (browser, sid) → 32-byte AES key
    chromium_aes_keys: Dict[Tuple[str, str], bytes] = {}

    for user_info in manifest.get("users", []):
        if callbacks.is_cancelled():
            break

        sid = user_info["sid"]
        username = user_info["username"]

        for cp_info in user_info.get("chromium_profiles", []):
            browser = cp_info["browser"]
            ls_local_path = output_dir / cp_info["local_state_path"]

            if not ls_local_path.exists():
                callbacks.on_log(
                    f"Local State missing for {browser}/{username}", "warning"
                )
                continue

            ls_bytes = ls_local_path.read_bytes()

            # Insert chromium_app_key record
            try:
                ck_db_id = insert_chromium_app_key(
                    evidence_conn, evidence_id,
                    sid=sid, browser=browser,
                    profile_root=cp_info.get("profile_root", ""),
                    local_state_path=cp_info.get("local_state_path", ""),
                    run_id=run_id, status="pending",
                )
            except Exception as e:
                callbacks.on_log(
                    f"Failed to insert chromium_app_key for {browser}/{username}: {e}",
                    "error",
                )
                continue

            try:
                dpapi_blob = extract_chromium_key(ls_bytes)
                aes_result = unwrap_chromium_key(
                    dpapi_blob, decrypted_master_keys,
                    local_state_path=cp_info.get("local_state_path", ""),
                )
                chromium_aes_keys[(browser, sid)] = aes_result.aes_key
                update_chromium_app_key_status(
                    evidence_conn, ck_db_id, "decrypted",
                    master_key_guid=aes_result.master_key_guid,
                )
                summary["chromium_keys"] += 1
                callbacks.on_log(
                    f"Unwrapped {browser} AES key for {username} "
                    f"(master key: {aes_result.master_key_guid})",
                    "info",
                )
            except ChromiumKeyError as e:
                update_chromium_app_key_status(
                    evidence_conn, ck_db_id, "failed",
                    error_message=str(e),
                )
                callbacks.on_log(
                    f"Failed to unwrap {browser} key for {username}: {e}", "warning"
                )

    if not chromium_aes_keys:
        callbacks.on_log("No Chromium AES keys available — skipping blob decryption", "warning")
        _write_process_log(
            evidence_conn, evidence_id, run_id, config, started_at, summary
        )
        return summary

    # ------------------------------------------------------------------
    # Phase 5: Decrypt browser blobs
    # ------------------------------------------------------------------
    callbacks.on_step("Phase 5: Decrypting browser secrets")

    # Build SID → username map for multi-user key matching
    sid_username_map: Dict[str, str] = {}
    for user_info in manifest.get("users", []):
        sid_username_map[user_info["sid"]] = user_info["username"]

    if decrypt_passwords:
        _decrypt_credentials(
            evidence_conn, evidence_id, run_id, chromium_aes_keys,
            callbacks, summary, sid_username_map,
        )

    if decrypt_cookies:
        _decrypt_cookies(
            evidence_conn, evidence_id, run_id, chromium_aes_keys,
            callbacks, summary, sid_username_map,
        )

    if decrypt_cards:
        _decrypt_credit_cards(
            evidence_conn, evidence_id, run_id, chromium_aes_keys,
            callbacks, summary, sid_username_map,
        )

    callbacks.on_log(
        f"Decryption complete: {summary['decrypted']} decrypted, "
        f"{summary['failed']} failed, {summary['no_key']} no key",
        "info",
    )

    _write_process_log(
        evidence_conn, evidence_id, run_id, config, started_at, summary
    )
    return summary


def _normalize_browser(browser: str) -> str:
    """Normalize browser name for matching."""
    _BROWSER_ALIASES = {
        "chrome": "chrome",
        "google chrome": "chrome",
        "edge": "edge",
        "microsoft edge": "edge",
        "brave": "brave",
        "brave browser": "brave",
        "opera": "opera",
        "opera stable": "opera",
        "vivaldi": "vivaldi",
    }
    return _BROWSER_ALIASES.get(browser.lower(), browser.lower())


def _extract_username_from_path(source_path: str) -> Optional[str]:
    """Extract Windows username from a source path like C:\\Users\\<name>\\..."""
    if not source_path:
        return None
    # Normalise separators
    normalized = source_path.replace("\\", "/")
    parts = normalized.split("/")
    for i, part in enumerate(parts):
        if part.lower() == "users" and i + 1 < len(parts):
            return parts[i + 1]
    return None


def _find_aes_key(
    browser: str,
    source_path: str,
    chromium_aes_keys: Dict[Tuple[str, str], bytes],
    sid_username_map: Optional[Dict[str, str]] = None,
) -> Optional[bytes]:
    """Find the matching AES key for a browser credential.

    Matching strategy (most specific → least specific):
    1. Match browser + SID via username extracted from source_path
    2. Match browser only (any SID)
    3. Match normalized browser alias (any SID)
    """
    norm_browser = _normalize_browser(browser)
    path_username = _extract_username_from_path(source_path)

    # Strategy 1: match (browser, SID) via username from source_path
    if path_username and sid_username_map:
        path_user_lower = path_username.lower()
        for sid, uname in sid_username_map.items():
            if uname.lower() == path_user_lower:
                # Try exact browser first, then normalized
                key = chromium_aes_keys.get((browser, sid))
                if key is not None:
                    return key
                key = chromium_aes_keys.get((norm_browser, sid))
                if key is not None:
                    return key

    # Strategy 2: match by browser name (any SID)
    for (key_browser, _), aes_key in chromium_aes_keys.items():
        if key_browser == browser:
            return aes_key

    # Strategy 3: match by normalized browser alias
    for (key_browser, _), aes_key in chromium_aes_keys.items():
        if _normalize_browser(key_browser) == norm_browser:
            return aes_key

    return None


def _decrypt_credentials(
    evidence_conn,
    evidence_id: int,
    run_id: str,
    chromium_aes_keys: Dict[Tuple[str, str], bytes],
    callbacks: ExtractorCallbacks,
    summary: Dict[str, int],
    sid_username_map: Optional[Dict[str, str]] = None,
) -> None:
    """Decrypt credential password blobs."""
    callbacks.on_log("Decrypting saved passwords...", "info")

    rows = evidence_conn.execute(
        """SELECT id, password_value_encrypted, browser, source_path
           FROM credentials
           WHERE evidence_id = ? AND password_value_encrypted IS NOT NULL""",
        (evidence_id,),
    ).fetchall()

    if not rows:
        callbacks.on_log("No encrypted credentials found", "info")
        return

    callbacks.on_log(f"Found {len(rows)} encrypted credential(s)", "info")
    cred_updates: List[Dict[str, Any]] = []
    audit_batch: List[Dict[str, Any]] = []

    for row in rows:
        if callbacks.is_cancelled():
            break

        cred_id, encrypted_blob, browser, source_path = row

        if not encrypted_blob or len(encrypted_blob) < 4:
            continue

        aes_key = _find_aes_key(browser or "", source_path or "", chromium_aes_keys, sid_username_map)
        if aes_key is None:
            summary["no_key"] += 1
            audit_batch.append({
                "target_table": "credentials",
                "target_id": cred_id,
                "status": "no_key",
                "run_id": run_id,
            })
            continue

        try:
            result = decrypt_v10_blob(encrypted_blob, aes_key)
            try:
                decrypted_text = result.plaintext.decode("utf-8")
            except UnicodeDecodeError:
                # Non-UTF-8 plaintext likely indicates wrong key or corrupted data
                summary["failed"] += 1
                audit_batch.append({
                    "target_table": "credentials",
                    "target_id": cred_id,
                    "status": "invalid_plaintext",
                    "run_id": run_id,
                    "error_code": "invalid_utf8",
                })
                continue
            cred_updates.append({
                "id": cred_id,
                "decrypted_value": decrypted_text,
                "status": "decrypted",
            })
            summary["decrypted"] += 1
            audit_batch.append({
                "target_table": "credentials",
                "target_id": cred_id,
                "status": "decrypted",
                "run_id": run_id,
            })
        except (ChromiumKeyError, IntegrityError) as e:
            summary["failed"] += 1
            audit_batch.append({
                "target_table": "credentials",
                "target_id": cred_id,
                "status": "failed",
                "run_id": run_id,
                "error_code": str(e),
            })
        except Exception as e:
            summary["failed"] += 1
            audit_batch.append({
                "target_table": "credentials",
                "target_id": cred_id,
                "status": "failed",
                "run_id": run_id,
                "error_code": str(e),
            })

    # Batch updates
    if cred_updates:
        try:
            batch_update_credentials_decrypted(evidence_conn, cred_updates)
        except Exception as e:
            callbacks.on_log(f"Batch credential update failed: {e}", "error")
            # Fall back to individual updates
            for upd in cred_updates:
                try:
                    update_credential_decrypted(
                        evidence_conn, upd["id"], upd["decrypted_value"], upd["status"]
                    )
                except Exception:
                    pass

    # Insert audit records
    for audit in audit_batch:
        try:
            insert_decrypt_audit(
                evidence_conn, evidence_id,
                target_table=audit["target_table"],
                target_id=audit["target_id"],
                status=audit["status"],
                run_id=audit.get("run_id", run_id),
                error_code=audit.get("error_code"),
            )
        except Exception:
            pass

    callbacks.on_log(
        f"Credentials: {sum(1 for u in cred_updates)} decrypted "
        f"of {len(rows)} total",
        "info",
    )


def _decrypt_cookies(
    evidence_conn,
    evidence_id: int,
    run_id: str,
    chromium_aes_keys: Dict[Tuple[str, str], bytes],
    callbacks: ExtractorCallbacks,
    summary: Dict[str, int],
    sid_username_map: Optional[Dict[str, str]] = None,
) -> None:
    """Decrypt cookie encrypted_value blobs."""
    callbacks.on_log("Decrypting cookies...", "info")

    rows = evidence_conn.execute(
        """SELECT id, encrypted_value, browser, source_path
           FROM cookies
           WHERE evidence_id = ? AND encrypted_value IS NOT NULL""",
        (evidence_id,),
    ).fetchall()

    if not rows:
        callbacks.on_log("No encrypted cookies found", "info")
        return

    callbacks.on_log(f"Found {len(rows)} encrypted cookie(s)", "info")
    cookie_updates: List[Dict[str, Any]] = []
    audit_batch: List[Dict[str, Any]] = []

    for row in rows:
        if callbacks.is_cancelled():
            break

        cookie_id, encrypted_blob, browser, source_path = row

        if not encrypted_blob or len(encrypted_blob) < 4:
            continue

        aes_key = _find_aes_key(browser or "", source_path or "", chromium_aes_keys, sid_username_map)
        if aes_key is None:
            summary["no_key"] += 1
            audit_batch.append({
                "target_table": "cookies",
                "target_id": cookie_id,
                "status": "no_key",
                "run_id": run_id,
            })
            continue

        try:
            result = decrypt_v10_blob(encrypted_blob, aes_key)
            try:
                decrypted_text = result.plaintext.decode("utf-8")
            except UnicodeDecodeError:
                summary["failed"] += 1
                audit_batch.append({
                    "target_table": "cookies",
                    "target_id": cookie_id,
                    "status": "invalid_plaintext",
                    "run_id": run_id,
                    "error_code": "invalid_utf8",
                })
                continue
            cookie_updates.append({
                "id": cookie_id,
                "decrypted_value": decrypted_text,
                "status": "decrypted",
            })
            summary["decrypted"] += 1
            audit_batch.append({
                "target_table": "cookies",
                "target_id": cookie_id,
                "status": "decrypted",
                "run_id": run_id,
            })
        except (ChromiumKeyError, IntegrityError) as e:
            summary["failed"] += 1
            audit_batch.append({
                "target_table": "cookies",
                "target_id": cookie_id,
                "status": "failed",
                "run_id": run_id,
                "error_code": str(e),
            })
        except Exception as e:
            summary["failed"] += 1
            audit_batch.append({
                "target_table": "cookies",
                "target_id": cookie_id,
                "status": "failed",
                "run_id": run_id,
                "error_code": str(e),
            })

    if cookie_updates:
        try:
            batch_update_cookies_decrypted(evidence_conn, cookie_updates)
        except Exception as e:
            callbacks.on_log(f"Batch cookie update failed: {e}", "error")
            for upd in cookie_updates:
                try:
                    update_cookie_decrypted(
                        evidence_conn, upd["id"], upd["decrypted_value"], upd["status"]
                    )
                except Exception:
                    pass

    # Insert audit records
    for audit in audit_batch:
        try:
            insert_decrypt_audit(
                evidence_conn, evidence_id,
                target_table=audit["target_table"],
                target_id=audit["target_id"],
                status=audit["status"],
                run_id=audit.get("run_id", run_id),
                error_code=audit.get("error_code"),
            )
        except Exception:
            pass

    callbacks.on_log(
        f"Cookies: {len(cookie_updates)} decrypted of {len(rows)} total",
        "info",
    )


def _decrypt_credit_cards(
    evidence_conn,
    evidence_id: int,
    run_id: str,
    chromium_aes_keys: Dict[Tuple[str, str], bytes],
    callbacks: ExtractorCallbacks,
    summary: Dict[str, int],
    sid_username_map: Optional[Dict[str, str]] = None,
) -> None:
    """Decrypt credit card number blobs."""
    callbacks.on_log("Decrypting credit cards...", "info")

    rows = evidence_conn.execute(
        """SELECT id, card_number_encrypted, browser, source_path
           FROM credit_cards
           WHERE evidence_id = ? AND card_number_encrypted IS NOT NULL""",
        (evidence_id,),
    ).fetchall()

    if not rows:
        callbacks.on_log("No encrypted credit cards found", "info")
        return

    callbacks.on_log(f"Found {len(rows)} encrypted credit card(s)", "info")

    for row in rows:
        if callbacks.is_cancelled():
            break

        card_id, encrypted_blob, browser, source_path = row

        if not encrypted_blob or len(encrypted_blob) < 4:
            continue

        aes_key = _find_aes_key(browser or "", source_path or "", chromium_aes_keys, sid_username_map)
        if aes_key is None:
            summary["no_key"] += 1
            try:
                insert_decrypt_audit(
                    evidence_conn, evidence_id,
                    target_table="credit_cards",
                    target_id=card_id,
                    status="no_key",
                    run_id=run_id,
                )
            except Exception:
                pass
            continue

        try:
            result = decrypt_v10_blob(encrypted_blob, aes_key)
            try:
                decrypted_text = result.plaintext.decode("utf-8")
            except UnicodeDecodeError:
                summary["failed"] += 1
                try:
                    insert_decrypt_audit(
                        evidence_conn, evidence_id,
                        target_table="credit_cards",
                        target_id=card_id,
                        status="invalid_plaintext",
                        run_id=run_id,
                        error_code="invalid_utf8",
                    )
                except Exception:
                    pass
                continue
            update_credit_card_decrypted(
                evidence_conn, card_id, decrypted_text, "decrypted"
            )
            summary["decrypted"] += 1

            insert_decrypt_audit(
                evidence_conn, evidence_id,
                target_table="credit_cards",
                target_id=card_id,
                status="decrypted",
                run_id=run_id,
            )
        except (ChromiumKeyError, IntegrityError) as e:
            summary["failed"] += 1
            try:
                insert_decrypt_audit(
                    evidence_conn, evidence_id,
                    target_table="credit_cards",
                    target_id=card_id,
                    status="failed",
                    run_id=run_id,
                    error_code=str(e),
                )
            except Exception:
                pass
        except Exception as e:
            summary["failed"] += 1

    callbacks.on_log(f"Credit cards: processed {len(rows)} total", "info")


def _write_process_log(
    evidence_conn,
    evidence_id: int,
    run_id: str,
    config: Dict[str, Any],
    started_at: str,
    summary: Dict[str, int],
) -> None:
    """Write a process_log entry for the pipeline run."""
    finished_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    # Summarize config without secrets
    safe_config = {
        "auto_sam": config.get("auto_sam", True),
        "decrypt_passwords": config.get("decrypt_passwords", True),
        "decrypt_cookies": config.get("decrypt_cookies", True),
        "decrypt_cards": config.get("decrypt_cards", True),
        "has_user_password": bool(config.get("user_password")),
    }

    try:
        insert_process_log(
            evidence_conn,
            evidence_id,
            tool_name="system_dpapi_decrypt",
            command_line=f"DPAPI decryption pipeline (config: {json.dumps(safe_config)})",
            started_at=started_at,
            finished_at=finished_at,
            exit_code=0 if summary.get("failed", 0) == 0 else 1,
            run_id=run_id,
            record_count=summary.get("decrypted", 0),
            metadata=json.dumps(summary),
        )
    except Exception as e:
        LOGGER.warning("Failed to write process_log entry: %s", e)
