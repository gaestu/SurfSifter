"""
Downloads feature helpers.

Shared utility functions for downloads feature.

Extracted from downloads/tab.py
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Optional

from core.database import slugify_label

logger = logging.getLogger(__name__)


def get_downloads_folder(
    case_folder: Path,
    evidence_id: int,
    db_path: Optional[Path] = None,
) -> Optional[Path]:
    """
    Get the downloads folder for an evidence.

    Uses evidences/{slug}/_downloads/ path structure to align with
    other extractors (foremost, scalpel, bulk_extractor, etc.).

    Args:
        case_folder: Path to the case folder
        evidence_id: Evidence ID
        db_path: Path to case database (for looking up evidence label)

    Returns:
        Path to downloads folder, or None if label cannot be determined
    """
    # Look up evidence label from case database
    if db_path is None:
        from core.database import find_case_database, CASE_DB_SUFFIX
        db_path = find_case_database(case_folder)
        if db_path is None:
            # Last resort: try constructing from folder name
            db_path = case_folder / f"{case_folder.name}{CASE_DB_SUFFIX}"

    if not db_path.exists():
        logger.warning("No case database found for downloads folder lookup")
        return None

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT label FROM evidences WHERE id = ?",
                (evidence_id,),
            ).fetchone()

            if not row or not row["label"]:
                logger.warning("No label found for evidence %d", evidence_id)
                return None

            slug = slugify_label(row["label"], evidence_id)
            return case_folder / "evidences" / slug / "_downloads"

    except Exception as e:
        logger.error("Failed to get downloads folder: %s", e)
        return None
