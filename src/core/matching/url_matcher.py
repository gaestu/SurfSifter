"""
URL Matcher - Match discovered URLs against reference URL lists.

Supports matching modes:
1. Wildcard: Pattern matching with * wildcards (default)
2. Regex: Full regular expression support

Reference lists stored in: ~/.config/surfsifter/reference_lists/urllists/
"""
from __future__ import annotations

import fnmatch
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

__all__ = ["URLMatcher"]

logger = logging.getLogger(__name__)


class URLMatcher:
    """Match discovered URLs against URL reference lists."""

    def __init__(self, evidence_conn: sqlite3.Connection, evidence_id: int):
        """
        Initialize URL matcher.

        Args:
            evidence_conn: SQLite connection to evidence database
            evidence_id: Evidence ID
        """
        self.evidence_conn = evidence_conn
        self.evidence_id = evidence_id

    def load_list(self, list_path: Union[str, Path]) -> Tuple[List[str], bool]:
        """
        Load URL list from file.

        Args:
            list_path: Path to URL list file (str or Path)

        Returns:
            Tuple of (patterns, is_regex) where:
                - patterns: List of URL patterns/domains/IPs
                - is_regex: True if patterns are regex, False for wildcards

        Raises:
            FileNotFoundError: If list file doesn't exist
        """
        # Convert to Path if string
        if isinstance(list_path, str):
            list_path = Path(list_path)

        if not list_path.exists():
            raise FileNotFoundError(f"URL list not found: {list_path}")

        patterns = []
        is_regex = False

        with open(list_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()

                # Skip comments and empty lines
                if not line or line.startswith("#"):
                    # Check for REGEX flag in comments
                    if line.startswith("# REGEX:") and "true" in line.lower():
                        is_regex = True
                    continue

                patterns.append(line)

        logger.info(f"Loaded {len(patterns)} patterns from {list_path.name} (regex={is_regex})")
        return patterns, is_regex

    def match_pattern(self, url: str, pattern: str, mode: str = "wildcard") -> bool:
        """
        Check if URL matches pattern.

        Args:
            url: URL to test
            pattern: Pattern to match against (domain, URL fragment, IP)
            mode: Matching mode ('wildcard' or 'regex')

        Returns:
            True if URL matches pattern (case-insensitive)

        Examples:
            Wildcard mode:
                match_pattern("https://www.example.com/page", "example", "wildcard") → True
                match_pattern("https://example.com/test", "example.com", "wildcard") → True

            Strict substring matching:
                Pattern "example" → *example* (matches anywhere in URL)
                Pattern "example.com" → *example.com* (matches exact domain)
                Pattern "192.168.1.1" → exact IP match
        """
        # Normalize for case-insensitive matching
        url_lower = url.lower()
        pattern_lower = pattern.lower()

        if mode == "regex":
            try:
                return bool(re.search(pattern_lower, url_lower, re.IGNORECASE))
            except re.error as e:
                logger.warning(f"Invalid regex pattern '{pattern}': {e}")
                return False
        else:
            # Wildcard mode: strict substring matching
            # Pattern "example" → matches if "example" appears anywhere in URL
            # Pattern "example.com" → matches if "example.com" appears anywhere

            # If pattern looks like a wildcard pattern (* or ?), use fnmatch
            if '*' in pattern_lower or '?' in pattern_lower:
                return fnmatch.fnmatch(url_lower, pattern_lower)
            else:
                # Simple substring match (strict)
                return pattern_lower in url_lower

    def match_urls(
        self,
        list_name: str,
        list_path: Path,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        should_interrupt: Optional[Callable[[], bool]] = None,
    ) -> Dict[str, int]:
        """
        Match all URLs in database against URL list.

        Args:
            list_name: Name of URL list (for display, typically filename without .txt)
            list_path: Path to URL list file
            progress_callback: Optional callback(rows_processed, total_rows)
            should_interrupt: Optional callback returning True when matching should stop

        Returns:
            Dictionary with keys:
                - 'matched': Number of matches found
                - 'total': Total URLs processed
                - 'list_name': Name of list

        Raises:
            FileNotFoundError: If list doesn't exist
            InterruptedError: If should_interrupt callback requests cancellation

        Side Effect:
            Inserts matches into url_matches table
        """
        logger.info(f"Matching URLs against list '{list_name}'")

        # Load URL list patterns
        patterns, is_regex = self.load_list(list_path)
        if not patterns:
            logger.warning(f"URL list '{list_name}' is empty")
            return {"matched": 0, "total": 0, "list_name": list_name}

        # Determine matching mode
        mode = "regex" if is_regex else "wildcard"

        # Query all URLs for this evidence
        cursor = self.evidence_conn.execute(
            """
            SELECT id, url, domain
            FROM urls
            WHERE evidence_id = ?
        """,
            (self.evidence_id,),
        )

        rows = cursor.fetchall()
        total_rows = len(rows)
        match_count = 0

        logger.info(f"Processing {total_rows} URLs with {len(patterns)} patterns (mode={mode})")

        for idx, row in enumerate(rows):
            if should_interrupt and should_interrupt():
                logger.info("URL matching interrupted at %d/%d URLs", idx, total_rows)
                raise InterruptedError("URL matching interrupted")

            url_id, url, domain = row

            # Try matching against each pattern
            for pattern in patterns:
                if should_interrupt and should_interrupt():
                    logger.info("URL matching interrupted while processing patterns")
                    raise InterruptedError("URL matching interrupted")

                if self.match_pattern(url, pattern, mode):
                    # Insert match record
                    try:
                        self.evidence_conn.execute(
                            """
                            INSERT INTO url_matches (
                                evidence_id, url_id, list_name,
                                match_type, matched_pattern
                            ) VALUES (?, ?, ?, ?, ?)
                        """,
                            (
                                self.evidence_id,
                                url_id,
                                list_name,
                                mode,
                                pattern,
                            ),
                        )
                        match_count += 1
                        break  # One match per URL is enough (first pattern wins)
                    except sqlite3.IntegrityError:
                        # Duplicate match (already exists)
                        logger.debug(f"Duplicate match for URL: {url}")
                        break

            # Report progress
            if progress_callback and (idx + 1) % 100 == 0:
                progress_callback(idx + 1, total_rows)

        # Final progress
        if progress_callback:
            progress_callback(total_rows, total_rows)

        self.evidence_conn.commit()
        logger.info(f"URL matching complete: {match_count}/{total_rows} matches found")

        return {
            "matched": match_count,
            "total": total_rows,
            "list_name": list_name,
        }

    def clear_matches(self, list_name: Optional[str] = None) -> int:
        """
        Clear URL matches for this evidence.

        Args:
            list_name: Optional list name to clear (if None, clears all URL matches)

        Returns:
            Number of matches removed
        """
        if list_name:
            cursor = self.evidence_conn.execute(
                """
                DELETE FROM url_matches
                WHERE evidence_id = ? AND list_name = ?
            """,
                (self.evidence_id, list_name),
            )
        else:
            cursor = self.evidence_conn.execute(
                """
                DELETE FROM url_matches
                WHERE evidence_id = ?
            """,
                (self.evidence_id,),
            )

        removed = cursor.rowcount
        self.evidence_conn.commit()
        logger.info(f"Cleared {removed} URL matches")
        return removed

    def get_match_stats(self) -> Dict[str, Any]:
        """
        Get URL matching statistics for this evidence.

        Returns:
            Dictionary with keys:
                - 'total_urls': Total URLs in database
                - 'matched_urls': Count of URLs with matches
                - 'match_count': Total match records
                - 'lists': Dict mapping list_name → match_count
        """
        # Total URLs
        total_urls = self.evidence_conn.execute(
            "SELECT COUNT(*) FROM urls WHERE evidence_id = ?",
            (self.evidence_id,),
        ).fetchone()[0]

        # Matched URLs (distinct)
        matched_urls = self.evidence_conn.execute(
            """
            SELECT COUNT(DISTINCT url_id)
            FROM url_matches
            WHERE evidence_id = ?
        """,
            (self.evidence_id,),
        ).fetchone()[0]

        # Total match records
        match_count = self.evidence_conn.execute(
            """
            SELECT COUNT(*)
            FROM url_matches
            WHERE evidence_id = ?
        """,
            (self.evidence_id,),
        ).fetchone()[0]

        # Matches per list
        cursor = self.evidence_conn.execute(
            """
            SELECT list_name, COUNT(*)
            FROM url_matches
            WHERE evidence_id = ?
            GROUP BY list_name
        """,
            (self.evidence_id,),
        )

        lists = {row[0]: row[1] for row in cursor.fetchall()}

        return {
            "total_urls": total_urls,
            "matched_urls": matched_urls,
            "match_count": match_count,
            "lists": lists,
        }
