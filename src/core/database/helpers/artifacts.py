"""
Artifact database helper functions (bulk_extractor output).

This module provides CRUD operations for bulk_extractor-discovered artifacts:
- Bitcoin addresses
- Ethereum addresses
- Email addresses
- Domains
- IP addresses
- Phone numbers

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_rows

__all__ = [
    # Bitcoin
    "insert_bitcoins",
    "insert_bitcoin_addresses",  # legacy alias
    "get_bitcoins",
    "get_bitcoin_addresses",  # legacy alias
    "delete_bitcoins_by_run",
    # Ethereum
    "insert_ethereums",
    "insert_ethereum_addresses",  # legacy alias
    "get_ethereums",
    "get_ethereum_addresses",  # legacy alias
    "delete_ethereums_by_run",
    # Emails
    "insert_emails",
    "get_emails",
    "delete_emails_by_run",
    # Domains
    "insert_domains",
    "get_domains",
    "delete_domains_by_run",
    # IPs
    "insert_ips",
    "insert_ip_addresses",  # legacy alias
    "get_ips",
    "get_ip_addresses",  # legacy alias
    "delete_ips_by_run",
    # Phones
    "insert_phones",
    "insert_telephone_numbers",  # legacy alias
    "get_phones",
    "get_telephone_numbers",  # legacy alias
    "delete_phones_by_run",
]


# ============================================================================
# Bitcoin Addresses
# ============================================================================

def insert_bitcoins(conn: sqlite3.Connection, evidence_id: int, records: Iterable[Dict[str, Any]], run_id: Optional[str] = None) -> int:
    """
    Insert bitcoin addresses discovered by bulk_extractor.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        records: Iterable of bitcoin records (address, offset, source_path, etc.)
        run_id: Optional run_id to inject into all records

    Returns:
        Number of records inserted
    """
    if run_id:
        records = [{**r, "run_id": run_id} for r in records]
    return insert_rows(conn, TABLE_SCHEMAS["bitcoin_addresses"], evidence_id, records)


def get_bitcoins(conn: sqlite3.Connection, evidence_id: int, *, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Retrieve bitcoin addresses for an evidence.

    Returns:
        List of bitcoin records as dicts
    """
    return get_rows(conn, TABLE_SCHEMAS["bitcoin_addresses"], evidence_id, limit=limit)


def delete_bitcoins_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete bitcoin records from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["bitcoin_addresses"], evidence_id, run_id)


# ============================================================================
# Ethereum Addresses
# ============================================================================

def insert_ethereums(conn: sqlite3.Connection, evidence_id: int, records: Iterable[Dict[str, Any]], run_id: Optional[str] = None) -> int:
    """
    Insert ethereum addresses discovered by bulk_extractor.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        records: Iterable of ethereum records (address, offset, source_path, etc.)
        run_id: Optional run_id to inject into all records

    Returns:
        Number of records inserted
    """
    if run_id:
        records = [{**r, "run_id": run_id} for r in records]
    return insert_rows(conn, TABLE_SCHEMAS["ethereum_addresses"], evidence_id, records)


def get_ethereums(conn: sqlite3.Connection, evidence_id: int, *, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Retrieve ethereum addresses for an evidence.

    Returns:
        List of ethereum records as dicts
    """
    return get_rows(conn, TABLE_SCHEMAS["ethereum_addresses"], evidence_id, limit=limit)


def delete_ethereums_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete ethereum records from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["ethereum_addresses"], evidence_id, run_id)


# ============================================================================
# Email Addresses
# ============================================================================

def insert_emails(conn: sqlite3.Connection, evidence_id: int, records: Iterable[Dict[str, Any]], run_id: Optional[str] = None) -> int:
    """
    Insert email addresses discovered by bulk_extractor.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        records: Iterable of email records (email, domain, offset, source_path, etc.)
        run_id: Optional run_id to inject into all records

    Returns:
        Number of records inserted
    """
    if run_id:
        records = [{**r, "run_id": run_id} for r in records]
    return insert_rows(conn, TABLE_SCHEMAS["emails"], evidence_id, records)


def get_emails(conn: sqlite3.Connection, evidence_id: int, *, domain: Optional[str] = None, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Retrieve email addresses for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        domain: Optional domain filter
        limit: Maximum rows to return

    Returns:
        List of email records as dicts
    """
    from ..schema import FilterOp
    filters = {}
    if domain:
        filters["domain"] = (FilterOp.LIKE, f"%{domain}%")
    return get_rows(conn, TABLE_SCHEMAS["emails"], evidence_id, filters=filters or None, limit=limit)


def delete_emails_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete email records from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["emails"], evidence_id, run_id)


# ============================================================================
# Domains
# ============================================================================

def insert_domains(conn: sqlite3.Connection, evidence_id: int, records: Iterable[Dict[str, Any]], run_id: Optional[str] = None) -> int:
    """
    Insert domains discovered by bulk_extractor.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        records: Iterable of domain records (domain, offset, source_path, etc.)
        run_id: Optional run_id to inject into all records

    Returns:
        Number of records inserted
    """
    if run_id:
        records = [{**r, "run_id": run_id} for r in records]
    return insert_rows(conn, TABLE_SCHEMAS["domains"], evidence_id, records)


def get_domains(conn: sqlite3.Connection, evidence_id: int, *, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Retrieve domains for an evidence.

    Returns:
        List of domain records as dicts
    """
    return get_rows(conn, TABLE_SCHEMAS["domains"], evidence_id, limit=limit)


def delete_domains_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete domain records from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["domains"], evidence_id, run_id)


# ============================================================================
# IP Addresses
# ============================================================================

def insert_ips(conn: sqlite3.Connection, evidence_id: int, records: Iterable[Dict[str, Any]], run_id: Optional[str] = None) -> int:
    """
    Insert IP addresses discovered by bulk_extractor.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        records: Iterable of IP records (ip, offset, source_path, etc.)
        run_id: Optional run_id to inject into all records

    Returns:
        Number of records inserted
    """
    if run_id:
        records = [{**r, "run_id": run_id} for r in records]
    return insert_rows(conn, TABLE_SCHEMAS["ip_addresses"], evidence_id, records)


def get_ips(conn: sqlite3.Connection, evidence_id: int, *, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Retrieve IP addresses for an evidence.

    Returns:
        List of IP records as dicts
    """
    return get_rows(conn, TABLE_SCHEMAS["ip_addresses"], evidence_id, limit=limit)


def delete_ips_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete IP records from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["ip_addresses"], evidence_id, run_id)


# ============================================================================
# Phone Numbers
# ============================================================================

def insert_phones(conn: sqlite3.Connection, evidence_id: int, records: Iterable[Dict[str, Any]], run_id: Optional[str] = None) -> int:
    """
    Insert phone numbers discovered by bulk_extractor.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        records: Iterable of phone records (phone, offset, source_path, etc.)
        run_id: Optional run_id to inject into all records

    Returns:
        Number of records inserted
    """
    if run_id:
        records = [{**r, "run_id": run_id} for r in records]
    return insert_rows(conn, TABLE_SCHEMAS["telephone_numbers"], evidence_id, records)


def get_phones(conn: sqlite3.Connection, evidence_id: int, *, limit: int = 1000) -> List[Dict[str, Any]]:
    """
    Retrieve phone numbers for an evidence.

    Returns:
        List of phone records as dicts
    """
    return get_rows(conn, TABLE_SCHEMAS["telephone_numbers"], evidence_id, limit=limit)


def delete_phones_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete phone records from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["telephone_numbers"], evidence_id, run_id)


# ============================================================================
# Legacy Aliases (backward compatibility with db.py)
# Deprecated compatibility shims for older import paths.
# Planned removal window: first stable major release after 0.2.x (target ).
# ============================================================================

# Bitcoin
insert_bitcoin_addresses = insert_bitcoins
get_bitcoin_addresses = get_bitcoins

# Ethereum
insert_ethereum_addresses = insert_ethereums
get_ethereum_addresses = get_ethereums

# IPs
insert_ip_addresses = insert_ips
get_ip_addresses = get_ips

# Phones
insert_telephone_numbers = insert_phones
get_telephone_numbers = get_phones
