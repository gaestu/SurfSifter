"""
Autofill database helper functions.

This module provides CRUD operations for autofill, autofill_profiles,
credentials, and credit_cards tables.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_distinct_values, get_rows, insert_row, insert_rows

__all__ = [
    # Autofill form data
    "insert_autofill_entry",
    "insert_autofill_entries",
    "insert_autofill",  # legacy alias
    "get_autofill_entries",
    "get_autofill",  # legacy alias
    "delete_autofill_by_run",
    # Autofill profiles
    "insert_autofill_profile",
    "insert_autofill_profiles",
    "get_autofill_profiles",
    "delete_autofill_profiles_by_run",
    # Credentials
    "insert_credential",
    "insert_credentials",
    "get_credentials",
    "delete_credentials_by_run",
    # Credit cards
    "insert_credit_card",
    "insert_credit_cards",
    "get_credit_cards",
    "delete_credit_cards_by_run",
]


# ============================================================================
# Autofill Form Data
# ============================================================================

def insert_autofill_entry(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    name: str,
    value: str,
    **kwargs,
) -> None:
    """
    Insert a single autofill form entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        name: Field name
        value: Field value
        **kwargs: Optional fields (profile, date_created_utc, date_last_used_utc, count, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "name": name,
        "value": value,
        "date_created_utc": kwargs.get("date_created_utc"),
        "date_last_used_utc": kwargs.get("date_last_used_utc"),
        "count": kwargs.get("count"),
        "run_id": kwargs.get("run_id"),
        "source_path": kwargs.get("source_path"),
        "discovered_by": kwargs.get("discovered_by"),
        "partition_index": kwargs.get("partition_index"),
        "fs_type": kwargs.get("fs_type"),
        "logical_path": kwargs.get("logical_path"),
        "forensic_path": kwargs.get("forensic_path"),
        "tags": kwargs.get("tags"),
        "notes": kwargs.get("notes"),
    }
    insert_row(conn, TABLE_SCHEMAS["autofill"], evidence_id, record)


def insert_autofill_entries(conn: sqlite3.Connection, evidence_id: int, entries: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple autofill entries in batch.

    Returns:
        Number of entries inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["autofill"], evidence_id, entries)


def get_autofill_entries(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    name: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve autofill entries for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        name: Optional field name filter (partial match)
        limit: Maximum rows to return

    Returns:
        List of autofill records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if name:
        filters["name"] = (FilterOp.LIKE, f"%{name}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["autofill"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def delete_autofill_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete autofill entries from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["autofill"], evidence_id, run_id)


# ============================================================================
# Autofill Profiles
# ============================================================================

def insert_autofill_profile(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    guid: str,
    **kwargs,
) -> None:
    """
    Insert a single autofill profile.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        guid: Profile GUID
        **kwargs: Optional fields (full_name, email, phone, address_*, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "guid": guid,
        "full_name": kwargs.get("full_name"),
        "email": kwargs.get("email"),
        "phone": kwargs.get("phone"),
        "company_name": kwargs.get("company_name"),
        "address_line1": kwargs.get("address_line1"),
        "address_line2": kwargs.get("address_line2"),
        "city": kwargs.get("city"),
        "state": kwargs.get("state"),
        "zipcode": kwargs.get("zipcode"),
        "country_code": kwargs.get("country_code"),
        "date_modified_utc": kwargs.get("date_modified_utc"),
        "use_count": kwargs.get("use_count"),
        "run_id": kwargs.get("run_id"),
        "source_path": kwargs.get("source_path"),
        "discovered_by": kwargs.get("discovered_by"),
        "partition_index": kwargs.get("partition_index"),
        "fs_type": kwargs.get("fs_type"),
        "logical_path": kwargs.get("logical_path"),
        "forensic_path": kwargs.get("forensic_path"),
        "tags": kwargs.get("tags"),
        "notes": kwargs.get("notes"),
    }
    insert_row(conn, TABLE_SCHEMAS["autofill_profiles"], evidence_id, record)


def insert_autofill_profiles(conn: sqlite3.Connection, evidence_id: int, profiles: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple autofill profiles in batch.

    Returns:
        Number of profiles inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["autofill_profiles"], evidence_id, profiles)


def get_autofill_profiles(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve autofill profiles for an evidence.

    Returns:
        List of autofill profile records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)

    return get_rows(
        conn,
        TABLE_SCHEMAS["autofill_profiles"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def delete_autofill_profiles_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete autofill profiles from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["autofill_profiles"], evidence_id, run_id)


# ============================================================================
# Credentials (Login Data)
# ============================================================================

def insert_credential(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    origin_url: str,
    username: str,
    **kwargs,
) -> None:
    """
    Insert a single credential entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        origin_url: Website URL
        username: Username
        **kwargs: Optional fields (signon_realm, encrypted, date_created_utc, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "origin_url": origin_url,
        "action_url": kwargs.get("action_url"),
        "signon_realm": kwargs.get("signon_realm"),
        "username": username,
        "username_element": kwargs.get("username_element"),
        "password_element": kwargs.get("password_element"),
        "encrypted": kwargs.get("encrypted", 0),
        "date_created_utc": kwargs.get("date_created_utc"),
        "date_last_used_utc": kwargs.get("date_last_used_utc"),
        "times_used": kwargs.get("times_used"),
        "run_id": kwargs.get("run_id"),
        "source_path": kwargs.get("source_path"),
        "discovered_by": kwargs.get("discovered_by"),
        "partition_index": kwargs.get("partition_index"),
        "fs_type": kwargs.get("fs_type"),
        "logical_path": kwargs.get("logical_path"),
        "forensic_path": kwargs.get("forensic_path"),
        "tags": kwargs.get("tags"),
        "notes": kwargs.get("notes"),
    }
    insert_row(conn, TABLE_SCHEMAS["credentials"], evidence_id, record)


def insert_credentials(conn: sqlite3.Connection, evidence_id: int, creds: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple credentials in batch.

    Returns:
        Number of credentials inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["credentials"], evidence_id, creds)


def get_credentials(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    origin_url: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve credentials for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        origin_url: Optional URL filter (partial match)
        limit: Maximum rows to return

    Returns:
        List of credential records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if origin_url:
        filters["origin_url"] = (FilterOp.LIKE, f"%{origin_url}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["credentials"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def delete_credentials_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete credentials from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["credentials"], evidence_id, run_id)


# ============================================================================
# Credit Cards
# ============================================================================

def insert_credit_card(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    guid: str,
    **kwargs,
) -> None:
    """
    Insert a single credit card entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        guid: Card GUID
        **kwargs: Optional fields (name_on_card, expiration_*, card_type, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "guid": guid,
        "name_on_card": kwargs.get("name_on_card"),
        "expiration_month": kwargs.get("expiration_month"),
        "expiration_year": kwargs.get("expiration_year"),
        "card_type": kwargs.get("card_type"),
        "encrypted": kwargs.get("encrypted", 0),
        "date_modified_utc": kwargs.get("date_modified_utc"),
        "use_count": kwargs.get("use_count"),
        "run_id": kwargs.get("run_id"),
        "source_path": kwargs.get("source_path"),
        "discovered_by": kwargs.get("discovered_by"),
        "partition_index": kwargs.get("partition_index"),
        "fs_type": kwargs.get("fs_type"),
        "logical_path": kwargs.get("logical_path"),
        "forensic_path": kwargs.get("forensic_path"),
        "tags": kwargs.get("tags"),
        "notes": kwargs.get("notes"),
    }
    insert_row(conn, TABLE_SCHEMAS["credit_cards"], evidence_id, record)


def insert_credit_cards(conn: sqlite3.Connection, evidence_id: int, cards: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple credit cards in batch.

    Returns:
        Number of cards inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["credit_cards"], evidence_id, cards)


def get_credit_cards(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve credit cards for an evidence.

    Returns:
        List of credit card records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)

    return get_rows(
        conn,
        TABLE_SCHEMAS["credit_cards"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def delete_credit_cards_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete credit cards from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["credit_cards"], evidence_id, run_id)


# ============================================================================
# Legacy Aliases (backward compatibility with db.py)
# Deprecated compatibility shims for older import paths.
# Planned removal window: first stable major release after 0.2.x (target ).
# ============================================================================

insert_autofill = insert_autofill_entries
get_autofill = get_autofill_entries
