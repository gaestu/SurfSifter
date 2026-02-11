"""
Autofill profile tokens database helper functions.

This module provides CRUD operations for the autofill_profile_tokens table,
which stores token-based contact info from Chromium 100+ browsers.

Added for autofill enhancement feature.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    "insert_autofill_profile_token",
    "insert_autofill_profile_tokens",
    "get_autofill_profile_tokens",
    "get_autofill_profile_tokens_by_guid",
    "delete_autofill_profile_tokens_by_run",
]

# Chromium token type mappings (from components/autofill/core/browser/field_types.h)
# Note: The canonical/complete definition is in extractors/browser/chromium/autofill/_schemas.py
# This copy is maintained for UI and test backward compatibility.
CHROMIUM_TOKEN_TYPES = {
    # Unknown/empty
    0: "UNKNOWN_TYPE",

    # Name fields (1-7)
    1: "NAME_FULL",
    2: "NAME_FIRST",
    3: "NAME_MIDDLE",
    4: "NAME_LAST",
    5: "NAME_MIDDLE_INITIAL",
    6: "NAME_PREFIX",
    7: "NAME_SUFFIX",

    # Email (9)
    9: "EMAIL_ADDRESS",

    # Phone fields (10-14)
    10: "PHONE_HOME_NUMBER",
    11: "PHONE_HOME_CITY_CODE",
    12: "PHONE_HOME_COUNTRY_CODE",
    13: "PHONE_HOME_CITY_AND_NUMBER",
    14: "PHONE_HOME_WHOLE_NUMBER",

    # Address fields (30-37, 60-62)
    30: "ADDRESS_HOME_LINE1",
    31: "ADDRESS_HOME_LINE2",
    32: "ADDRESS_HOME_LINE3",
    33: "ADDRESS_HOME_APT_NUM",
    34: "ADDRESS_HOME_CITY",
    35: "ADDRESS_HOME_STATE",
    36: "ADDRESS_HOME_ZIP",
    37: "ADDRESS_HOME_COUNTRY",
    60: "ADDRESS_HOME_STREET_ADDRESS",
    61: "ADDRESS_HOME_SORTING_CODE",
    62: "ADDRESS_HOME_DEPENDENT_LOCALITY",

    # Additional address fields (Chromium 90+)
    63: "ADDRESS_HOME_SUBPREMISE",
    64: "ADDRESS_HOME_PREMISE_NAME",
    65: "ADDRESS_HOME_DEPENDENT_STREET_NAME",
    66: "ADDRESS_HOME_STREET_NAME",
    67: "ADDRESS_HOME_HOUSE_NUMBER",
    68: "ADDRESS_HOME_FLOOR",

    # Company (77)
    77: "COMPANY_NAME",

    # Credit card types (51-59) - for reference, stored in credit_cards table
    51: "CREDIT_CARD_NAME_FULL",
    52: "CREDIT_CARD_NUMBER",
    53: "CREDIT_CARD_EXP_MONTH",
    54: "CREDIT_CARD_EXP_2_DIGIT_YEAR",
    55: "CREDIT_CARD_EXP_4_DIGIT_YEAR",
    56: "CREDIT_CARD_EXP_DATE_2_DIGIT_YEAR",
    57: "CREDIT_CARD_EXP_DATE_4_DIGIT_YEAR",
    58: "CREDIT_CARD_TYPE",
    59: "CREDIT_CARD_VERIFICATION_CODE",

    # Birthdate fields (Chromium 100+)
    78: "BIRTHDATE_DAY",
    79: "BIRTHDATE_MONTH",
    80: "BIRTHDATE_4_DIGIT_YEAR",

    # IBAN (Chromium 110+)
    81: "IBAN_VALUE",
}


def get_token_type_name(token_type: int) -> str:
    """Convert Chromium token type code to human-readable name."""
    return CHROMIUM_TOKEN_TYPES.get(token_type, f"UNKNOWN_{token_type}")


def insert_autofill_profile_token(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    guid: str,
    token_type: int,
    token_value: str,
    **kwargs,
) -> None:
    """
    Insert a single autofill profile token.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        guid: Profile GUID
        token_type: Chromium token type code
        token_value: Token value
        **kwargs: Optional fields (profile, source_table, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "guid": guid,
        "token_type": token_type,
        "token_type_name": get_token_type_name(token_type),
        "token_value": token_value,
        "source_table": kwargs.get("source_table"),
        "parent_table": kwargs.get("parent_table"),
        "parent_use_count": kwargs.get("parent_use_count"),
        "parent_use_date_utc": kwargs.get("parent_use_date_utc"),
        "parent_date_modified_utc": kwargs.get("parent_date_modified_utc"),
        "run_id": kwargs.get("run_id"),
        "source_path": kwargs.get("source_path"),
        "discovered_by": kwargs.get("discovered_by"),
    }
    insert_row(conn, TABLE_SCHEMAS["autofill_profile_tokens"], evidence_id, record)


def insert_autofill_profile_tokens(
    conn: sqlite3.Connection,
    evidence_id: int,
    tokens: Iterable[Dict[str, Any]],
) -> int:
    """
    Insert multiple autofill profile tokens in batch.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        tokens: Iterable of token records

    Returns:
        Number of records inserted
    """
    # Ensure token_type_name is set for each record
    processed_tokens = []
    for token in tokens:
        t = dict(token)
        if "token_type_name" not in t and "token_type" in t:
            t["token_type_name"] = get_token_type_name(t["token_type"])
        processed_tokens.append(t)

    return insert_rows(conn, TABLE_SCHEMAS["autofill_profile_tokens"], evidence_id, processed_tokens)


def get_autofill_profile_tokens(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    guid: Optional[str] = None,
    token_type: Optional[int] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """
    Retrieve autofill profile tokens for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        guid: Optional GUID filter
        token_type: Optional token type filter
        limit: Maximum rows to return

    Returns:
        List of token records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if guid:
        filters["guid"] = (FilterOp.EQ, guid)
    if token_type is not None:
        filters["token_type"] = (FilterOp.EQ, token_type)

    return get_rows(
        conn,
        TABLE_SCHEMAS["autofill_profile_tokens"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def get_autofill_profile_tokens_by_guid(
    conn: sqlite3.Connection,
    evidence_id: int,
    guid: str,
) -> List[Dict[str, Any]]:
    """
    Retrieve all tokens for a specific profile GUID.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        guid: Profile GUID

    Returns:
        List of token records for the given GUID
    """
    return get_autofill_profile_tokens(
        conn, evidence_id, guid=guid, limit=1000
    )


def delete_autofill_profile_tokens_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """Delete autofill profile tokens from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["autofill_profile_tokens"], evidence_id, run_id)
