"""
DPAPI decryption database helper functions.

This module provides CRUD operations for Windows user accounts, DPAPI master keys,
Chromium application keys, and decryption audit records.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_evidence, delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    # Windows users
    "insert_windows_user",
    "insert_windows_users",
    "get_windows_users",
    "delete_windows_users_by_run",
    "update_windows_user_key_counts",
    # DPAPI master keys
    "insert_dpapi_master_key",
    "insert_dpapi_master_keys",
    "get_dpapi_master_keys",
    "update_dpapi_master_key_status",
    "delete_dpapi_master_keys_by_run",
    # Chromium app keys
    "insert_chromium_app_key",
    "insert_chromium_app_keys",
    "get_chromium_app_keys",
    "update_chromium_app_key_status",
    "delete_chromium_app_keys_by_run",
    # Decrypt audit
    "insert_decrypt_audit",
    "insert_decrypt_audits",
    "get_decrypt_audit",
    "get_decrypt_summary",
    "delete_decrypt_audit_by_run",
    # Decrypt result updates
    "update_credential_decrypted",
    "update_cookie_decrypted",
    "update_credit_card_decrypted",
    "batch_update_credentials_decrypted",
    "batch_update_cookies_decrypted",
    # Evidence-wide cleanup
    "delete_windows_users_by_evidence",
    "delete_dpapi_master_keys_by_evidence",
    "delete_chromium_app_keys_by_evidence",
    "delete_decrypt_audit_by_evidence",
    "reset_decrypt_status_by_evidence",
]


# =============================================================================
# Windows Users
# =============================================================================


def insert_windows_user(
    conn: sqlite3.Connection,
    evidence_id: int,
    sid: str,
    rid: int,
    username: str,
    **kwargs: Any,
) -> int:
    """Insert a single Windows user record. Returns lastrowid."""
    record = {"sid": sid, "rid": rid, "username": username, **kwargs}
    return insert_row(conn, TABLE_SCHEMAS["windows_users"], evidence_id, record)


def insert_windows_users(
    conn: sqlite3.Connection,
    evidence_id: int,
    users: Iterable[Dict[str, Any]],
) -> int:
    """Insert multiple Windows user records. Returns count inserted."""
    return insert_rows(conn, TABLE_SCHEMAS["windows_users"], evidence_id, users)


def get_windows_users(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    sid: Optional[str] = None,
    run_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Retrieve Windows users for an evidence."""
    filters: Dict[str, Any] = {}
    if sid:
        filters["sid"] = (FilterOp.EQ, sid)
    if run_id:
        filters["run_id"] = (FilterOp.EQ, run_id)
    return get_rows(
        conn,
        TABLE_SCHEMAS["windows_users"],
        evidence_id,
        filters=filters or None,
        limit=limit or 1000,
    )


def delete_windows_users_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """Delete Windows users by run_id. Returns count deleted."""
    return delete_by_run(conn, TABLE_SCHEMAS["windows_users"], evidence_id, run_id)


def update_windows_user_key_counts(
    conn: sqlite3.Connection,
    user_id: int,
    master_keys_found: int,
    master_keys_unlocked: int,
) -> None:
    """Update master key counts for a Windows user record."""
    conn.execute(
        "UPDATE windows_users SET master_keys_found = ?, master_keys_unlocked = ? WHERE id = ?",
        (master_keys_found, master_keys_unlocked, user_id),
    )
    conn.commit()


# =============================================================================
# DPAPI Master Keys
# =============================================================================


def insert_dpapi_master_key(
    conn: sqlite3.Connection,
    evidence_id: int,
    sid: str,
    guid: str,
    source_path: str,
    **kwargs: Any,
) -> int:
    """Insert a single DPAPI master key record. Returns lastrowid."""
    record = {"sid": sid, "guid": guid, "source_path": source_path, **kwargs}
    return insert_row(conn, TABLE_SCHEMAS["dpapi_master_keys"], evidence_id, record)


def insert_dpapi_master_keys(
    conn: sqlite3.Connection,
    evidence_id: int,
    keys: Iterable[Dict[str, Any]],
) -> int:
    """Insert multiple DPAPI master key records. Returns count inserted."""
    return insert_rows(conn, TABLE_SCHEMAS["dpapi_master_keys"], evidence_id, keys)


def get_dpapi_master_keys(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    sid: Optional[str] = None,
    status: Optional[str] = None,
    run_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Retrieve DPAPI master keys for an evidence."""
    filters: Dict[str, Any] = {}
    if sid:
        filters["sid"] = (FilterOp.EQ, sid)
    if status:
        filters["status"] = (FilterOp.EQ, status)
    if run_id:
        filters["run_id"] = (FilterOp.EQ, run_id)
    return get_rows(
        conn,
        TABLE_SCHEMAS["dpapi_master_keys"],
        evidence_id,
        filters=filters or None,
        limit=limit or 1000,
    )


def update_dpapi_master_key_status(
    conn: sqlite3.Connection,
    key_id: int,
    status: str,
    unlock_method: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Update status of a DPAPI master key."""
    conn.execute(
        "UPDATE dpapi_master_keys SET status = ?, unlock_method = ?, error_message = ? WHERE id = ?",
        (status, unlock_method, error_message, key_id),
    )
    conn.commit()


def delete_dpapi_master_keys_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """Delete DPAPI master keys by run_id. Returns count deleted."""
    return delete_by_run(conn, TABLE_SCHEMAS["dpapi_master_keys"], evidence_id, run_id)


# =============================================================================
# Chromium App Keys
# =============================================================================


def insert_chromium_app_key(
    conn: sqlite3.Connection,
    evidence_id: int,
    sid: str,
    browser: str,
    profile_root: str,
    local_state_path: str,
    **kwargs: Any,
) -> int:
    """Insert a single Chromium app key record. Returns lastrowid."""
    record = {
        "sid": sid,
        "browser": browser,
        "profile_root": profile_root,
        "local_state_path": local_state_path,
        **kwargs,
    }
    return insert_row(conn, TABLE_SCHEMAS["chromium_app_keys"], evidence_id, record)


def insert_chromium_app_keys(
    conn: sqlite3.Connection,
    evidence_id: int,
    keys: Iterable[Dict[str, Any]],
) -> int:
    """Insert multiple Chromium app key records. Returns count inserted."""
    return insert_rows(conn, TABLE_SCHEMAS["chromium_app_keys"], evidence_id, keys)


def get_chromium_app_keys(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    sid: Optional[str] = None,
    browser: Optional[str] = None,
    run_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Retrieve Chromium app keys for an evidence."""
    filters: Dict[str, Any] = {}
    if sid:
        filters["sid"] = (FilterOp.EQ, sid)
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if run_id:
        filters["run_id"] = (FilterOp.EQ, run_id)
    return get_rows(
        conn,
        TABLE_SCHEMAS["chromium_app_keys"],
        evidence_id,
        filters=filters or None,
        limit=limit or 1000,
    )


def update_chromium_app_key_status(
    conn: sqlite3.Connection,
    key_id: int,
    status: str,
    master_key_guid: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Update status of a Chromium app key."""
    conn.execute(
        "UPDATE chromium_app_keys SET status = ?, master_key_guid = ?, error_message = ? WHERE id = ?",
        (status, master_key_guid, error_message, key_id),
    )
    conn.commit()


def delete_chromium_app_keys_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """Delete Chromium app keys by run_id. Returns count deleted."""
    return delete_by_run(conn, TABLE_SCHEMAS["chromium_app_keys"], evidence_id, run_id)


# =============================================================================
# Decrypt Audit
# =============================================================================


def insert_decrypt_audit(
    conn: sqlite3.Connection,
    evidence_id: int,
    target_table: str,
    target_id: int,
    status: str,
    **kwargs: Any,
) -> int:
    """Insert a single decrypt audit record. Returns lastrowid."""
    record = {"target_table": target_table, "target_id": target_id, "status": status, **kwargs}
    return insert_row(conn, TABLE_SCHEMAS["decrypt_audit"], evidence_id, record)


def insert_decrypt_audits(
    conn: sqlite3.Connection,
    evidence_id: int,
    audits: Iterable[Dict[str, Any]],
) -> int:
    """Insert multiple decrypt audit records. Returns count inserted."""
    return insert_rows(conn, TABLE_SCHEMAS["decrypt_audit"], evidence_id, audits)


def get_decrypt_audit(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    target_table: Optional[str] = None,
    status: Optional[str] = None,
    run_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Retrieve decrypt audit records for an evidence."""
    filters: Dict[str, Any] = {}
    if target_table:
        filters["target_table"] = (FilterOp.EQ, target_table)
    if status:
        filters["status"] = (FilterOp.EQ, status)
    if run_id:
        filters["run_id"] = (FilterOp.EQ, run_id)
    return get_rows(
        conn,
        TABLE_SCHEMAS["decrypt_audit"],
        evidence_id,
        filters=filters or None,
        limit=limit or 1000,
    )


def get_decrypt_summary(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> Dict[str, Any]:
    """Get a summary of decryption results across all target tables.

    Returns a dict with totals and per-table breakdowns.
    """
    rows = conn.execute(
        """
        SELECT target_table, status, COUNT(*) as cnt
        FROM decrypt_audit
        WHERE evidence_id = ?
        GROUP BY target_table, status
        """,
        (evidence_id,),
    ).fetchall()

    summary: Dict[str, Any] = {
        "total": 0,
        "decrypted": 0,
        "failed": 0,
        "no_key": 0,
        "not_encrypted": 0,
        "by_table": {},
    }

    for row in rows:
        table = row[0]
        status = row[1]
        count = row[2]

        if table not in summary["by_table"]:
            summary["by_table"][table] = {
                "total": 0,
                "decrypted": 0,
                "failed": 0,
                "no_key": 0,
                "not_encrypted": 0,
            }

        summary["total"] += count
        summary["by_table"][table]["total"] += count

        if status in summary:
            summary[status] += count
        if status in summary["by_table"][table]:
            summary["by_table"][table][status] += count

    return summary


def delete_decrypt_audit_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """Delete decrypt audit records by run_id. Returns count deleted."""
    return delete_by_run(conn, TABLE_SCHEMAS["decrypt_audit"], evidence_id, run_id)


# =============================================================================
# Decrypt Result Updates
# =============================================================================


def update_credential_decrypted(
    conn: sqlite3.Connection,
    credential_id: int,
    decrypted_value: Optional[str],
    status: str,
) -> None:
    """Update decrypted password value and status on a credential row."""
    conn.execute(
        "UPDATE credentials SET password_value_decrypted = ?, decrypt_status = ? WHERE id = ?",
        (decrypted_value, status, credential_id),
    )
    conn.commit()


def update_cookie_decrypted(
    conn: sqlite3.Connection,
    cookie_id: int,
    decrypted_value: Optional[str],
    status: str,
) -> None:
    """Update decrypted value and status on a cookie row."""
    conn.execute(
        "UPDATE cookies SET decrypted_value = ?, decrypt_status = ? WHERE id = ?",
        (decrypted_value, status, cookie_id),
    )
    conn.commit()


def update_credit_card_decrypted(
    conn: sqlite3.Connection,
    credit_card_id: int,
    decrypted_value: Optional[str],
    status: str,
) -> None:
    """Update decrypted card number and status on a credit card row."""
    conn.execute(
        "UPDATE credit_cards SET card_number_decrypted = ?, decrypt_status = ? WHERE id = ?",
        (decrypted_value, status, credit_card_id),
    )
    conn.commit()


def batch_update_credentials_decrypted(
    conn: sqlite3.Connection,
    updates: List[Dict[str, Any]],
) -> int:
    """Batch update decrypted values on credential rows.

    Each update dict must have keys: id, decrypted_value, status.
    Returns count of rows updated.
    """
    if not updates:
        return 0
    params = [
        (u["decrypted_value"], u["status"], u["id"])
        for u in updates
    ]
    conn.executemany(
        "UPDATE credentials SET password_value_decrypted = ?, decrypt_status = ? WHERE id = ?",
        params,
    )
    conn.commit()
    return len(params)


def batch_update_cookies_decrypted(
    conn: sqlite3.Connection,
    updates: List[Dict[str, Any]],
) -> int:
    """Batch update decrypted values on cookie rows.

    Each update dict must have keys: id, decrypted_value, status.
    Returns count of rows updated.
    """
    if not updates:
        return 0
    params = [
        (u["decrypted_value"], u["status"], u["id"])
        for u in updates
    ]
    conn.executemany(
        "UPDATE cookies SET decrypted_value = ?, decrypt_status = ? WHERE id = ?",
        params,
    )
    conn.commit()
    return len(params)


# =============================================================================
# Evidence-wide Cleanup (full re-run support)
# =============================================================================


def delete_windows_users_by_evidence(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> int:
    """Delete all Windows users for an evidence. Returns count deleted."""
    return delete_by_evidence(conn, TABLE_SCHEMAS["windows_users"], evidence_id)


def delete_dpapi_master_keys_by_evidence(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> int:
    """Delete all DPAPI master keys for an evidence. Returns count deleted."""
    return delete_by_evidence(conn, TABLE_SCHEMAS["dpapi_master_keys"], evidence_id)


def delete_chromium_app_keys_by_evidence(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> int:
    """Delete all Chromium app keys for an evidence. Returns count deleted."""
    return delete_by_evidence(conn, TABLE_SCHEMAS["chromium_app_keys"], evidence_id)


def delete_decrypt_audit_by_evidence(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> int:
    """Delete all decrypt audit records for an evidence. Returns count deleted."""
    return delete_by_evidence(conn, TABLE_SCHEMAS["decrypt_audit"], evidence_id)


def reset_decrypt_status_by_evidence(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> None:
    """Reset decrypt_status and decrypted values on all credential/cookie/credit_card rows."""
    conn.execute(
        "UPDATE credentials SET password_value_decrypted = NULL, decrypt_status = NULL "
        "WHERE evidence_id = ?",
        (evidence_id,),
    )
    conn.execute(
        "UPDATE cookies SET decrypted_value = NULL, decrypt_status = NULL "
        "WHERE evidence_id = ?",
        (evidence_id,),
    )
    conn.execute(
        "UPDATE credit_cards SET card_number_decrypted = NULL, decrypt_status = NULL "
        "WHERE evidence_id = ?",
        (evidence_id,),
    )
