"""OS Indicators query operations.

This module provides indicator-specific queries for the UI:
- OS indicator listing with type filter
- System/network/startup indicators grouped

Extracted from case_data.py for modular repository pattern.
Removed platform_detections (unused legacy feature).
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional

from ._base import BaseDataAccess


class IndicatorQueryMixin(BaseDataAccess):
    """Mixin providing OS indicator queries.

    Features:
    - OS indicator listing with type filter
    - System information indicator grouping

    All methods operate on the evidence database.

    Extracted from CaseDataAccess for modular architecture.
    Removed platform_detections methods (unused legacy feature).
    """

    # -------------------------------------------------------------------------
    # OS Indicators (Evidence DB)
    # -------------------------------------------------------------------------

    def iter_indicators(
        self,
        evidence_id: int,
        *,
        indicator_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve OS indicators with optional type filter.

        Args:
            evidence_id: Evidence ID
            indicator_type: Optional filter by indicator type

        Returns:
            List of indicator dicts with keys: id, type, name, value, path,
            hive, confidence, detected_at_utc, provenance
        """
        params: List[Any] = [evidence_id]
        where = ["evidence_id = ?"]
        if indicator_type:
            where.append("type = ?")
            params.append(indicator_type)
        sql = f"""
            SELECT id, type, name, value, path, hive, confidence, detected_at_utc, provenance
            FROM os_indicators
            WHERE {' AND '.join(where)}
            ORDER BY detected_at_utc DESC
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, params)
                return [dict(row) for row in cursor.fetchall()]

    def list_indicator_types(self, evidence_id: int) -> List[str]:
        """Get distinct indicator types for filter dropdowns.

        Args:
            evidence_id: Evidence ID

        Returns:
            Sorted list of distinct indicator type strings
        """
        sql = """
            SELECT DISTINCT type
            FROM os_indicators
            WHERE evidence_id = ?
            ORDER BY type
        """
        with self._use_evidence_conn(evidence_id):
            with self._connect() as conn:
                cursor = conn.execute(sql, (evidence_id,))
                return [row[0] for row in cursor.fetchall()]

    def get_system_indicators(self, evidence_id: int) -> Dict[str, Any]:
        """
        Retrieve system information indicators for an evidence.

        Queries indicators with types starting with 'system:', 'network:',
        or 'startup:' and groups them by type.

        Args:
            evidence_id: Evidence ID

        Returns:
            Dictionary grouped by semantic type (e.g. system:os_version).
            Each type maps to a list of dicts with name, value, extra keys.
        """
        label = self._get_evidence_label(evidence_id)
        if not label:
            return {}

        conn = self._db_manager.get_evidence_conn(evidence_id, label)
        conn.row_factory = sqlite3.Row

        try:
            cursor = conn.execute(
                "SELECT type, name, value, extra_json FROM os_indicators WHERE type LIKE 'system:%' OR type LIKE 'network:%' OR type LIKE 'startup:%'"
            )

            results: Dict[str, Any] = {}
            for row in cursor:
                indicator_type = row["type"]
                if indicator_type not in results:
                    results[indicator_type] = []

                item = {
                    "name": row["name"],
                    "value": row["value"],
                    "extra": json.loads(row["extra_json"]) if row["extra_json"] else {}
                }
                results[indicator_type].append(item)

            return results
        except sqlite3.OperationalError:
            # Table might not exist or column missing if migration not run
            return {}
