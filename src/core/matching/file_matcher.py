"""
File List Matcher - Match file_list entries against reference lists.

Matches file_list database entries against:
- Hash lists (MD5/SHA1/SHA256)
- Filename pattern lists (wildcard or regex)
"""
from __future__ import annotations

import fnmatch
import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Callable, Optional

from .manager import ReferenceListManager

__all__ = ["ReferenceListMatcher"]

logger = logging.getLogger(__name__)


class ReferenceListMatcher:
    """Match file list entries against reference lists."""

    def __init__(self, evidence_conn: sqlite3.Connection, evidence_id: int):
        """
        Initialize matcher.

        Args:
            evidence_conn: SQLite connection to evidence database
            evidence_id: Evidence ID
        """
        self.evidence_conn = evidence_conn
        self.evidence_id = evidence_id
        self.ref_manager = ReferenceListManager()

    def match_hashlist(
        self,
        hashlist_name: str,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """
        Match file_list entries against hash list.

        Args:
            hashlist_name: Name of hash list (without .txt extension)
            progress_callback: Optional callback(rows_processed, total_rows)

        Returns:
            Number of matches found

        Raises:
            FileNotFoundError: If hash list doesn't exist
        """
        logger.info(f"Matching against hash list '{hashlist_name}'")
        matched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Load hash list
        hashes = self.ref_manager.load_hashlist(hashlist_name)
        if not hashes:
            logger.warning(f"Hash list '{hashlist_name}' is empty")
            return 0

        # Query file_list entries with hashes
        cursor = self.evidence_conn.execute(
            """
            SELECT id, file_path, md5_hash, sha1_hash, sha256_hash
            FROM file_list
            WHERE evidence_id = ?
              AND (md5_hash IS NOT NULL OR sha1_hash IS NOT NULL OR sha256_hash IS NOT NULL)
        """,
            (self.evidence_id,),
        )

        rows = cursor.fetchall()
        total_rows = len(rows)
        match_count = 0

        for idx, row in enumerate(rows):
            file_list_id, file_path, md5, sha1, sha256 = row

            # Check each hash type
            matched_value = None
            if md5 and md5.lower() in hashes:
                matched_value = md5.lower()
            elif sha1 and sha1.lower() in hashes:
                matched_value = sha1.lower()
            elif sha256 and sha256.lower() in hashes:
                matched_value = sha256.lower()

            if matched_value:
                # Insert match record
                try:
                    self.evidence_conn.execute(
                        """
                        INSERT INTO file_list_matches (
                            evidence_id, file_list_id, reference_list_name,
                            match_type, matched_value, matched_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                        (
                            self.evidence_id,
                            file_list_id,
                            hashlist_name,
                            "hash",
                            matched_value,
                            matched_at,
                        ),
                    )
                    match_count += 1
                except sqlite3.IntegrityError:
                    # Duplicate match (already exists)
                    logger.debug(f"Duplicate match for {file_path}")

            # Report progress
            if progress_callback and (idx + 1) % 100 == 0:
                progress_callback(idx + 1, total_rows)

        # Final progress
        if progress_callback:
            progress_callback(total_rows, total_rows)

        self.evidence_conn.commit()
        logger.info(f"Hash list matching complete: {match_count} matches found")

        # Rebuild filter cache after matching
        self._rebuild_filter_cache()

        return match_count

    def match_filelist(
        self,
        filelist_name: str,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """
        Match file_list entries against filename patterns.

        Args:
            filelist_name: Name of file list (without .txt extension)
            progress_callback: Optional callback(rows_processed, total_rows)

        Returns:
            Number of matches found

        Raises:
            FileNotFoundError: If file list doesn't exist
        """
        logger.info(f"Matching against file list '{filelist_name}'")
        matched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Load file list patterns
        patterns, is_regex = self.ref_manager.load_filelist(filelist_name)
        if not patterns:
            logger.warning(f"File list '{filelist_name}' is empty")
            return 0

        # Compile regex patterns if needed
        compiled_patterns = []
        if is_regex:
            for pattern in patterns:
                try:
                    compiled_patterns.append(re.compile(pattern, re.IGNORECASE))
                except re.error as e:
                    logger.warning(f"Invalid regex pattern '{pattern}': {e}")
            patterns = compiled_patterns

        # Query all file_list entries
        cursor = self.evidence_conn.execute(
            """
            SELECT id, file_path, file_name
            FROM file_list
            WHERE evidence_id = ?
        """,
            (self.evidence_id,),
        )

        rows = cursor.fetchall()
        total_rows = len(rows)
        match_count = 0

        for idx, row in enumerate(rows):
            file_list_id, file_path, file_name = row

            # Try matching against each pattern
            for pattern in patterns:
                matched = False
                matched_value = None

                if is_regex:
                    # Regex matching (pattern is compiled regex object)
                    if pattern.search(file_name):
                        matched = True
                        matched_value = pattern.pattern
                else:
                    # Wildcard matching (case-insensitive)
                    if self._match_wildcard(file_name, pattern):
                        matched = True
                        matched_value = pattern

                if matched:
                    # Insert match record
                    try:
                        self.evidence_conn.execute(
                            """
                            INSERT INTO file_list_matches (
                                evidence_id, file_list_id, reference_list_name,
                                match_type, matched_value, matched_at
                            ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                            (
                                self.evidence_id,
                                file_list_id,
                                filelist_name,
                                "filename",
                                matched_value,
                                matched_at,
                            ),
                        )
                        match_count += 1
                        break  # One match per file is enough
                    except sqlite3.IntegrityError:
                        # Duplicate match
                        logger.debug(f"Duplicate match for {file_path}")
                        break

            # Report progress
            if progress_callback and (idx + 1) % 100 == 0:
                progress_callback(idx + 1, total_rows)

        # Final progress
        if progress_callback:
            progress_callback(total_rows, total_rows)

        self.evidence_conn.commit()
        logger.info(f"File list matching complete: {match_count} matches found")

        # Rebuild filter cache after matching
        self._rebuild_filter_cache()

        return match_count

    def _match_wildcard(self, filename: str, pattern: str) -> bool:
        """
        Case-insensitive wildcard matching.

        Args:
            filename: Filename to test
            pattern: Wildcard pattern (* and ? supported)

        Returns:
            True if filename matches pattern
        """
        return fnmatch.fnmatch(filename.lower(), pattern.lower())

    def _rebuild_filter_cache(self) -> None:
        """
        Rebuild filter cache for this evidence.

        Updates the 'match' filter values after reference list matching completes.
        This ensures filter dropdowns stay up-to-date without expensive DISTINCT queries.
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        # Check if cache table exists
        cache_exists = self.evidence_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='file_list_filter_cache'"
        ).fetchone()

        if not cache_exists:
            logger.debug("Filter cache table not available, skipping cache rebuild")
            return

        with self.evidence_conn:
            # Remove old match cache entries for this evidence
            self.evidence_conn.execute(
                "DELETE FROM file_list_filter_cache WHERE evidence_id = ? AND filter_type = 'match'",
                (self.evidence_id,)
            )

            # Rebuild match cache
            self.evidence_conn.execute(
                """
                INSERT INTO file_list_filter_cache (evidence_id, filter_type, filter_value, count, last_updated)
                SELECT ?, 'match', reference_list_name, COUNT(DISTINCT file_list_id), ?
                FROM file_list_matches
                WHERE evidence_id = ?
                GROUP BY reference_list_name
                """,
                (self.evidence_id, timestamp, self.evidence_id)
            )

        logger.debug(f"Filter cache updated for evidence {self.evidence_id}")
