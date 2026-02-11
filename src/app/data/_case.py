"""Case metadata data access.

This module provides CRUD operations for case metadata
stored in the case database (not evidence databases).

Extracted from case_data.py for modular repository pattern.
Evidence metadata extracted to _evidence.py.
Added get_case_id() from case_data.py.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from ._base import BaseDataAccess


class CaseMetadataMixin(BaseDataAccess):
    """Mixin providing case metadata operations.

    All methods operate on the case database (cases table).

    Extracted from CaseDataAccess for modular architecture.
    Evidence metadata extracted to EvidenceMetadataMixin.
    Added get_case_id() from case_data.py.
    """

    # -------------------------------------------------------------------------
    # Case Metadata (Case DB)
    # -------------------------------------------------------------------------

    def get_case_id(self) -> Optional[int]:
        """Get the case ID from the database.

        Note: cases table is in the case database, not evidence database.
        """
        with self._connect_case() as conn:
            row = conn.execute("SELECT id FROM cases ORDER BY id ASC LIMIT 1").fetchone()
            return row["id"] if row else None

    def get_case_metadata(self) -> Dict[str, Any]:
        """Get case metadata.

        Note: cases table is in the case database, not evidence database.
        """
        with self._connect_case() as conn:
            row = conn.execute(
                """
                SELECT id, case_id, title, investigator, created_at_utc, notes,
                       case_number, case_name
                FROM cases
                ORDER BY id ASC
                LIMIT 1
                """
            ).fetchone()
            return dict(row) if row else {}

    def update_case_metadata(
        self,
        case_number: Optional[str] = None,
        case_name: Optional[str] = None,
        investigator: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> None:
        """Update case metadata fields.

        Note: cases table is in the case database, not evidence database.
        """
        with self._connect_case() as conn:
            conn.execute(
                """
                UPDATE cases
                SET case_number = ?,
                    case_name = ?,
                    investigator = ?,
                    notes = ?
                WHERE id = (SELECT id FROM cases ORDER BY id ASC LIMIT 1)
                """,
                (case_number, case_name, investigator, notes)
            )
            conn.commit()
