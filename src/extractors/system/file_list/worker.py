"""
File-List Extractor Worker - Import file lists from CSV exports into database.

Supports importing FTK, EnCase, and generic CSV file lists with:
- Batched database writes (1000 rows per transaction)
- Progress reporting
- Error handling and logging
- Automatic parser detection
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .parser import detect_parser, BaseFileListParser

__all__ = ["FileListExtractor"]

logger = logging.getLogger(__name__)


class FileListExtractor:
    """Worker for importing file lists from CSV files into evidence database."""

    BATCH_SIZE = 1000  # Insert 1000 rows per transaction

    def __init__(
        self,
        evidence_conn: sqlite3.Connection,
        evidence_id: int,
        csv_path: Path,
        import_source: str = "auto",
        parser: Optional[BaseFileListParser] = None,
    ):
        """
        Initialize file list extractor.

        Args:
            evidence_conn: SQLite connection to evidence database
            evidence_id: Evidence ID
            csv_path: Path to CSV file to import
            import_source: Source type ('ftk', 'encase', 'generic', or 'auto' for detection)
            parser: Optional pre-configured parser (if None, auto-detect)
        """
        self.evidence_conn = evidence_conn
        self.evidence_id = evidence_id
        self.csv_path = Path(csv_path)
        self.import_source = import_source
        self.parser = parser

        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")

    def run(
        self, progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> Dict[str, Any]:
        """
        Execute file list import.

        Args:
            progress_callback: Optional callback(rows_processed, total_rows)

        Returns:
            Statistics dictionary with:
                - total_rows: Total entries parsed
                - inserted_rows: Successfully inserted entries
                - skipped_rows: Entries skipped due to errors
                - duration_seconds: Import duration
                - import_timestamp: UTC timestamp of import
        """
        start_time = datetime.now(timezone.utc)
        import_timestamp = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        logger.info(f"Starting file list import from {self.csv_path}")

        # Auto-detect parser if not provided
        if self.parser is None:
            self.parser = detect_parser(self.csv_path)
            logger.info(f"Auto-detected parser: {type(self.parser).__name__}")

        # Set import source based on parser type if auto
        if self.import_source == "auto":
            parser_name = type(self.parser).__name__.lower()
            if "ftk" in parser_name:
                self.import_source = "ftk"
            elif "encase" in parser_name:
                self.import_source = "encase"
            else:
                self.import_source = "generic"

        # Parse CSV
        try:
            entries = self.parser.parse(self.csv_path)
            total_rows = len(entries)
            logger.info(f"Parsed {total_rows} entries from CSV")
        except Exception as e:
            logger.error(f"Failed to parse CSV: {e}")
            raise

        # Insert into database in batches
        inserted_rows = 0
        skipped_rows = 0

        for batch_start in range(0, total_rows, self.BATCH_SIZE):
            batch_end = min(batch_start + self.BATCH_SIZE, total_rows)
            batch = entries[batch_start:batch_end]

            try:
                inserted = self._insert_batch(batch, import_timestamp)
                inserted_rows += inserted
                skipped_rows += len(batch) - inserted

                # Report progress
                if progress_callback:
                    progress_callback(batch_end, total_rows)

                logger.debug(
                    f"Inserted batch {batch_start}-{batch_end}: {inserted}/{len(batch)} rows"
                )

            except Exception as e:
                logger.error(f"Failed to insert batch {batch_start}-{batch_end}: {e}")
                skipped_rows += len(batch)
                continue

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        # Phase 2: Rebuild filter cache after import
        logger.info("Rebuilding filter cache...")
        self._rebuild_filter_cache()

        stats = {
            "total_rows": total_rows,
            "inserted_rows": inserted_rows,
            "skipped_rows": skipped_rows,
            "duration_seconds": duration,
            "import_timestamp": import_timestamp,
            "import_source": self.import_source,
            "csv_path": str(self.csv_path),
        }

        logger.info(
            f"File list import complete: {inserted_rows}/{total_rows} rows inserted "
            f"in {duration:.2f}s ({skipped_rows} skipped)"
        )

        return stats

    def _insert_batch(
        self, entries: List[Dict[str, Any]], import_timestamp: str
    ) -> int:
        """
        Insert a batch of file list entries into database.

        Phase 2 optimization: Use executemany() for 10x faster batch inserts.

        Args:
            entries: List of parsed file entries
            import_timestamp: ISO timestamp for this import

        Returns:
            Number of rows successfully inserted
        """
        if not entries:
            return 0

        # Prepare data for executemany()
        rows = [
            (
                self.evidence_id,
                entry.get("file_path"),
                entry.get("file_name"),
                entry.get("extension"),
                entry.get("size_bytes"),
                entry.get("created_ts"),
                entry.get("modified_ts"),
                entry.get("accessed_ts"),
                entry.get("md5_hash"),
                entry.get("sha1_hash"),
                entry.get("sha256_hash"),
                entry.get("file_type"),
                1 if entry.get("deleted") else 0,
                entry.get("metadata"),
                self.import_source,
                import_timestamp,
                entry.get("partition_index"),
                entry.get("inode"),
            )
            for entry in entries
        ]

        # Try bulk insert first (fast path)
        try:
            with self.evidence_conn:
                self.evidence_conn.executemany(
                    """
                    INSERT INTO file_list (
                        evidence_id, file_path, file_name, extension,
                        size_bytes, created_ts, modified_ts, accessed_ts,
                        md5_hash, sha1_hash, sha256_hash, file_type,
                        deleted, metadata, import_source, import_timestamp,
                        partition_index, inode
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    rows,
                )
            logger.debug(f"Bulk insert succeeded: {len(rows)} rows")
            return len(rows)
        except sqlite3.IntegrityError:
            # Duplicates exist - fall back to individual inserts with duplicate handling
            logger.debug(
                f"Bulk insert failed (duplicates), falling back to individual inserts"
            )
            inserted = self._insert_batch_with_duplicates(entries, import_timestamp)
            logger.debug(f"Individual insert fallback: {inserted} rows inserted")
            return inserted

    def _insert_batch_with_duplicates(
        self, entries: List[Dict[str, Any]], import_timestamp: str
    ) -> int:
        """
        Insert batch with individual duplicate handling (slower fallback).

        Args:
            entries: List of parsed file entries
            import_timestamp: ISO timestamp for this import

        Returns:
            Number of rows successfully inserted
        """
        inserted = 0

        # Use transaction for batch
        with self.evidence_conn:
            for entry in entries:
                try:
                    self.evidence_conn.execute(
                        """
                        INSERT INTO file_list (
                            evidence_id, file_path, file_name, extension,
                            size_bytes, created_ts, modified_ts, accessed_ts,
                            md5_hash, sha1_hash, sha256_hash, file_type,
                            deleted, metadata, import_source, import_timestamp,
                            partition_index, inode
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            self.evidence_id,
                            entry.get("file_path"),
                            entry.get("file_name"),
                            entry.get("extension"),
                            entry.get("size_bytes"),
                            entry.get("created_ts"),
                            entry.get("modified_ts"),
                            entry.get("accessed_ts"),
                            entry.get("md5_hash"),
                            entry.get("sha1_hash"),
                            entry.get("sha256_hash"),
                            entry.get("file_type"),
                            1 if entry.get("deleted") else 0,
                            entry.get("metadata"),
                            self.import_source,
                            import_timestamp,
                            entry.get("partition_index"),
                            entry.get("inode"),
                        ),
                    )
                    inserted += 1
                except sqlite3.IntegrityError as e:
                    # Duplicate entry (same file path) - skip
                    logger.debug(
                        f"Skipping duplicate entry: {entry.get('file_path')} ({e})"
                    )
                    continue
                except Exception as e:
                    logger.warning(
                        f"Failed to insert entry {entry.get('file_path')}: {e}"
                    )
                    continue

        return inserted

    def get_import_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about existing file list data in database.

        Returns:
            Statistics dictionary with counts by extension, size distribution, etc.
        """
        cursor = self.evidence_conn.execute(
            """
            SELECT
                COUNT(*) as total_files,
                COUNT(DISTINCT extension) as unique_extensions,
                SUM(size_bytes) as total_size_bytes,
                AVG(size_bytes) as avg_size_bytes,
                MAX(size_bytes) as max_size_bytes,
                COUNT(CASE WHEN deleted = 1 THEN 1 END) as deleted_count
            FROM file_list
            WHERE evidence_id = ?
        """,
            (self.evidence_id,),
        )

        row = cursor.fetchone()
        if not row:
            return {}

        return {
            "total_files": row[0] or 0,
            "unique_extensions": row[1] or 0,
            "total_size_bytes": row[2] or 0,
            "avg_size_bytes": row[3] or 0,
            "max_size_bytes": row[4] or 0,
            "deleted_count": row[5] or 0,
        }

    def get_top_extensions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get most common file extensions.

        Args:
            limit: Maximum number of extensions to return

        Returns:
            List of dicts with 'extension' and 'count' keys
        """
        cursor = self.evidence_conn.execute(
            """
            SELECT extension, COUNT(*) as count
            FROM file_list
            WHERE evidence_id = ? AND extension IS NOT NULL AND extension != ''
            GROUP BY extension
            ORDER BY count DESC
            LIMIT ?
        """,
            (self.evidence_id, limit),
        )

        return [{"extension": row[0], "count": row[1]} for row in cursor.fetchall()]

    def _rebuild_filter_cache(self) -> None:
        """
        Rebuild filter cache for this evidence (Phase 2 optimization).

        Pre-computes filter values to avoid expensive DISTINCT queries on large datasets.
        This is called after import completion and should also be called after
        reference list matching or tagging operations.
        """
        timestamp = datetime.now(timezone.utc).isoformat()

        with self.evidence_conn:
            # Clear old cache for this evidence
            self.evidence_conn.execute(
                "DELETE FROM file_list_filter_cache WHERE evidence_id = ?",
                (self.evidence_id,)
            )

            # Cache extensions
            self.evidence_conn.execute(
                """
                INSERT INTO file_list_filter_cache (evidence_id, filter_type, filter_value, count, last_updated)
                SELECT ?, 'extension', extension, COUNT(*), ?
                FROM file_list
                WHERE evidence_id = ? AND extension IS NOT NULL AND extension != ''
                GROUP BY extension
                """,
                (self.evidence_id, timestamp, self.evidence_id)
            )

            # Cache tags (if any exist)
            self.evidence_conn.execute(
                """
                INSERT INTO file_list_filter_cache (evidence_id, filter_type, filter_value, count, last_updated)
                SELECT ?, 'tag', t.name, COUNT(DISTINCT ta.artifact_id), ?
                FROM tag_associations ta
                JOIN tags t ON ta.tag_id = t.id
                WHERE ta.evidence_id = ? AND ta.artifact_type = 'file_list'
                GROUP BY t.name
                """,
                (self.evidence_id, timestamp, self.evidence_id)
            )

            # Cache matches (if any exist)
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

        logger.info(f"Filter cache rebuilt for evidence {self.evidence_id}")
