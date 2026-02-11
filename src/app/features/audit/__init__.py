"""Audit feature - tabs for forensic audit data.

Initial implementation with Extraction subtab.
Added Warnings subtab.
Consolidated Statistics subtab (moved from app.features.statistics).
Added Logs subtab (moved from standalone Logs tab).
Added Download Audit subtab.

Subtabs:
- Extraction: View extracted_files table with filtering
- Warnings: View extraction warnings (schema discovery)
- Download Audit: View investigator download outcomes
- Statistics: View extractor run statistics (summary cards)
- Logs: Per-evidence extraction logs
"""

from .tab import AuditTab

__all__ = ["AuditTab"]
