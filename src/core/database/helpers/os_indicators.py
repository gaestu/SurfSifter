"""
OS indicators database helper functions.

This module provides CRUD operations for os_indicators and platform_detections tables.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    # OS Indicators
    "insert_os_indicator",
    "insert_os_indicators",
    "get_os_indicators",
    "delete_os_indicators_by_run",
    # Platform detections
    "insert_platform_detection",
    "insert_platform_detections",
    "get_platform_detections",
    "delete_platform_detections_by_run",
]


# ============================================================================
# OS Indicators
# ============================================================================

def insert_os_indicator(
    conn: sqlite3.Connection,
    evidence_id: int,
    indicator_type: str,
    indicator_name: str,
    **kwargs,
) -> None:
    """
    Insert a single OS indicator entry.

    OS indicators include Deep Freeze, kiosk mode, enterprise policies, etc.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        indicator_type: Indicator type (deep_freeze, kiosk, enterprise, etc.)
        indicator_name: Indicator name
        **kwargs: Optional fields (value, confidence, source_path, etc.)
    """
    record = {
        "indicator_type": indicator_type,
        "indicator_name": indicator_name,
        "value": kwargs.get("value"),
        "confidence": kwargs.get("confidence"),
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
    insert_row(conn, TABLE_SCHEMAS["os_indicators"], evidence_id, record)


def insert_os_indicators(conn: sqlite3.Connection, evidence_id: int, indicators: Iterable[Dict[str, Any]]) -> int:
    """Insert multiple OS indicators in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["os_indicators"], evidence_id, indicators)


def get_os_indicators(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    indicator_type: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """Retrieve OS indicators for an evidence."""
    filters: Dict[str, Any] = {}
    if indicator_type:
        filters["indicator_type"] = (FilterOp.EQ, indicator_type)
    return get_rows(conn, TABLE_SCHEMAS["os_indicators"], evidence_id, filters=filters or None, limit=limit)


def delete_os_indicators_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete OS indicators from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["os_indicators"], evidence_id, run_id)


# ============================================================================
# Platform Detections
# ============================================================================

def insert_platform_detection(
    conn: sqlite3.Connection,
    evidence_id: int,
    platform_type: str,
    platform_name: str,
    **kwargs,
) -> None:
    """
    Insert a single platform detection entry.

    Platform detections identify gambling sites, gaming platforms, etc.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        platform_type: Platform type (gambling, gaming, social, etc.)
        platform_name: Platform name
        **kwargs: Optional fields (url, confidence, matched_rule, etc.)
    """
    record = {
        "platform_type": platform_type,
        "platform_name": platform_name,
        "url": kwargs.get("url"),
        "domain": kwargs.get("domain"),
        "confidence": kwargs.get("confidence"),
        "matched_rule": kwargs.get("matched_rule"),
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
    insert_row(conn, TABLE_SCHEMAS["platform_detections"], evidence_id, record)


def insert_platform_detections(conn: sqlite3.Connection, evidence_id: int, detections: Iterable[Dict[str, Any]], run_id: Optional[str] = None) -> int:
    """Insert multiple platform detections in batch."""
    if run_id:
        detections = [{**d, "run_id": run_id} for d in detections]
    return insert_rows(conn, TABLE_SCHEMAS["platform_detections"], evidence_id, detections)


def get_platform_detections(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    platform_type: Optional[str] = None,
    platform_name: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """Retrieve platform detections for an evidence."""
    filters: Dict[str, Any] = {}
    if platform_type:
        filters["platform_type"] = (FilterOp.EQ, platform_type)
    if platform_name:
        filters["platform_name"] = (FilterOp.LIKE, f"%{platform_name}%")
    return get_rows(conn, TABLE_SCHEMAS["platform_detections"], evidence_id, filters=filters or None, limit=limit)


def delete_platform_detections_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete platform detections from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["platform_detections"], evidence_id, run_id)
