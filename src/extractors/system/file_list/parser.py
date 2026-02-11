"""
File list parsers for importing forensic tool exports (FTK, EnCase, etc.).

Supports:
- FTK CSV exports (tab-delimited, UTF-16 LE)
- EnCase CSV exports
- Generic CSV with user-defined column mapping

Handles encoding detection (UTF-8, UTF-16, Latin-1) and malformed data gracefully.
"""
from __future__ import annotations

import csv
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import chardet

__all__ = [
    "BaseFileListParser",
    "FTKParser",
    "EnCaseParser",
    "GenericParser",
    "detect_parser",
    "_auto_detect_column_mapping",
]

logger = logging.getLogger(__name__)


class BaseFileListParser(ABC):
    """Base class for file list parsers."""

    def detect_encoding(self, file_path: Path) -> str:
        """
        Auto-detect file encoding using chardet.

        Args:
            file_path: Path to CSV file

        Returns:
            Detected encoding (e.g., 'utf-8', 'utf-16-le', 'latin-1')
        """
        with open(file_path, "rb") as f:
            raw_data = f.read(100000)  # Read first 100KB for detection
            result = chardet.detect(raw_data)
            encoding = result["encoding"]
            confidence = result["confidence"]

            logger.info(
                f"Detected encoding: {encoding} (confidence: {confidence:.2f})"
            )

            # Map common aliases
            if encoding and encoding.lower().startswith("utf-16"):
                # chardet returns 'UTF-16' but Python needs 'utf-16-le' or 'utf-16-be'
                # Check BOM to determine byte order
                if raw_data[:2] == b"\xff\xfe":
                    return "utf-16-le"
                elif raw_data[:2] == b"\xfe\xff":
                    return "utf-16-be"
                return "utf-16"

            return encoding if encoding else "utf-8"

    @abstractmethod
    def parse(self, csv_path: Path) -> List[Dict[str, Any]]:
        """
        Parse CSV file and return list of file entries.

        Args:
            csv_path: Path to CSV file

        Returns:
            List of dictionaries with standardized keys:
                - file_path: str (full path)
                - file_name: str (filename only)
                - extension: str (file extension, including dot)
                - size_bytes: int (file size in bytes)
                - created_ts: str (ISO 8601 UTC timestamp)
                - modified_ts: str (ISO 8601 UTC timestamp)
                - accessed_ts: str (ISO 8601 UTC timestamp)
                - deleted: bool (whether file is deleted)
                - md5_hash: str (optional)
                - sha1_hash: str (optional)
                - sha256_hash: str (optional)
                - file_type: str (optional)
                - metadata: dict (extra columns as JSON)
        """
        pass

    def _extract_filename(self, full_path: str) -> str:
        """Extract filename from full path."""
        if not full_path:
            return ""
        # Handle both Windows and Unix paths
        if "\\" in full_path:
            return full_path.rsplit("\\", 1)[-1]
        return full_path.rsplit("/", 1)[-1]

    def _extract_extension(self, filename: str) -> str:
        """Extract file extension (including dot)."""
        if not filename or "." not in filename:
            return ""
        return "." + filename.rsplit(".", 1)[-1].lower()

    def _parse_timestamp(self, timestamp_str: str) -> Optional[str]:
        """
        Parse timestamp string to ISO 8601 UTC format.

        Handles various formats:
        - 2004-Aug-19 16:57:43.694987 UTC
        - 2004-08-19 16:57:43
        - 2004-08-19T16:57:43Z
        - Unix timestamps

        Returns:
            ISO 8601 UTC timestamp string, or None if parsing fails
        """
        if not timestamp_str or timestamp_str.strip() == "":
            return None

        timestamp_str = timestamp_str.strip()

        # Try various formats
        formats = [
            "%Y-%b-%d %H:%M:%S.%f UTC",  # FTK: 2004-Aug-19 16:57:43.694987 UTC
            "%Y-%b-%d %H:%M:%S UTC",  # FTK without microseconds
            "%Y-%m-%d %H:%M:%S.%f",  # Standard with microseconds
            "%Y-%m-%d %H:%M:%S",  # Standard
            "%Y-%m-%dT%H:%M:%SZ",  # ISO 8601
            "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO 8601 with microseconds
            "%m/%d/%Y %H:%M:%S",  # US format
            "%d/%m/%Y %H:%M:%S",  # EU format
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(timestamp_str, fmt)
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                continue

        logger.warning(f"Failed to parse timestamp: {timestamp_str}")
        return None

    def _parse_bool(self, value: str) -> bool:
        """Parse boolean value from string."""
        if not value:
            return False
        value_lower = value.strip().lower()
        return value_lower in ("yes", "true", "1", "y", "t")


class FTKParser(BaseFileListParser):
    """Parser for FTK (Forensic Toolkit) CSV exports."""

    # Expected FTK column names (tab-delimited)
    EXPECTED_COLUMNS = [
        "Filename",
        "Full Path",
        "Size (bytes)",
        "Created",
        "Modified",
        "Accessed",
        "Is Deleted",
    ]

    def parse(self, csv_path: Path) -> List[Dict[str, Any]]:
        """Parse FTK CSV file."""
        encoding = self.detect_encoding(csv_path)
        entries = []
        skipped_rows = 0

        try:
            with open(csv_path, "r", encoding=encoding, newline="") as f:
                # FTK uses tab delimiter
                reader = csv.DictReader(f, delimiter="\t")

                # Verify headers
                if not reader.fieldnames:
                    raise ValueError("CSV file has no headers")

                logger.info(f"FTK CSV headers: {reader.fieldnames}")

                for row_num, row in enumerate(reader, start=2):
                    try:
                        entry = self._parse_ftk_row(row)
                        if entry:
                            entries.append(entry)
                    except Exception as e:
                        logger.warning(
                            f"Skipping row {row_num} due to error: {e}"
                        )
                        skipped_rows += 1
                        continue

            logger.info(
                f"Parsed {len(entries)} entries from FTK CSV (skipped {skipped_rows} rows)"
            )
            return entries

        except Exception as e:
            logger.error(f"Failed to parse FTK CSV: {e}")
            raise

    def _parse_ftk_row(self, row: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Parse a single FTK CSV row."""
        filename = row.get("Filename", "").strip()
        full_path = row.get("Full Path", "").strip()

        # Skip empty rows or special entries
        if not filename and not full_path:
            return None

        # If no filename, extract from path
        if not filename and full_path:
            filename = self._extract_filename(full_path)

        # Parse size
        size_str = row.get("Size (bytes)", "").strip()
        try:
            size_bytes = int(size_str) if size_str else None
        except ValueError:
            size_bytes = None

        # Parse timestamps
        created_ts = self._parse_timestamp(row.get("Created", ""))
        modified_ts = self._parse_timestamp(row.get("Modified", ""))
        accessed_ts = self._parse_timestamp(row.get("Accessed", ""))

        # Parse deleted flag
        deleted = self._parse_bool(row.get("Is Deleted", ""))

        # Extract extension
        extension = self._extract_extension(filename)

        # Collect extra metadata (any columns not mapped to standard fields)
        metadata = {}
        standard_fields = {
            "Filename",
            "Full Path",
            "Size (bytes)",
            "Created",
            "Modified",
            "Accessed",
            "Is Deleted",
        }
        for key, value in row.items():
            if key not in standard_fields and value:
                metadata[key] = value

        return {
            "file_path": full_path,
            "file_name": filename,
            "extension": extension,
            "size_bytes": size_bytes,
            "created_ts": created_ts,
            "modified_ts": modified_ts,
            "accessed_ts": accessed_ts,
            "deleted": deleted,
            "md5_hash": None,
            "sha1_hash": None,
            "sha256_hash": None,
            "file_type": None,
            "metadata": json.dumps(metadata) if metadata else None,
        }


class EnCaseParser(BaseFileListParser):
    """Parser for EnCase CSV exports."""

    # Expected EnCase column names
    EXPECTED_COLUMNS = [
        "File Name",
        "Full Path",
        "File Size",
        "Created Date",
        "Modified Date",
        "Accessed Date",
        "Deleted",
    ]

    def parse(self, csv_path: Path) -> List[Dict[str, Any]]:
        """Parse EnCase CSV file."""
        encoding = self.detect_encoding(csv_path)
        entries = []
        skipped_rows = 0

        try:
            with open(csv_path, "r", encoding=encoding, newline="") as f:
                # EnCase typically uses comma delimiter
                reader = csv.DictReader(f)

                if not reader.fieldnames:
                    raise ValueError("CSV file has no headers")

                logger.info(f"EnCase CSV headers: {reader.fieldnames}")

                for row_num, row in enumerate(reader, start=2):
                    try:
                        entry = self._parse_encase_row(row)
                        if entry:
                            entries.append(entry)
                    except Exception as e:
                        logger.warning(
                            f"Skipping row {row_num} due to error: {e}"
                        )
                        skipped_rows += 1
                        continue

            logger.info(
                f"Parsed {len(entries)} entries from EnCase CSV (skipped {skipped_rows} rows)"
            )
            return entries

        except Exception as e:
            logger.error(f"Failed to parse EnCase CSV: {e}")
            raise

    def _parse_encase_row(self, row: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Parse a single EnCase CSV row."""
        filename = row.get("File Name", "").strip()
        full_path = row.get("Full Path", "").strip()

        if not filename and not full_path:
            return None

        if not filename and full_path:
            filename = self._extract_filename(full_path)

        # Parse size (EnCase uses "File Size")
        size_str = row.get("File Size", "").strip()
        try:
            size_bytes = int(size_str) if size_str else None
        except ValueError:
            size_bytes = None

        # Parse timestamps (EnCase uses "Created Date", "Modified Date", etc.)
        created_ts = self._parse_timestamp(row.get("Created Date", ""))
        modified_ts = self._parse_timestamp(row.get("Modified Date", ""))
        accessed_ts = self._parse_timestamp(row.get("Accessed Date", ""))

        # Parse deleted flag
        deleted = self._parse_bool(row.get("Deleted", ""))

        # Extract extension
        extension = self._extract_extension(filename)

        # Parse hashes if present
        md5_hash = row.get("MD5 Hash", "").strip() or None
        sha1_hash = row.get("SHA1 Hash", "").strip() or None
        sha256_hash = row.get("SHA256 Hash", "").strip() or None

        # Collect extra metadata
        metadata = {}
        standard_fields = {
            "File Name",
            "Full Path",
            "File Size",
            "Created Date",
            "Modified Date",
            "Accessed Date",
            "Deleted",
            "MD5 Hash",
            "SHA1 Hash",
            "SHA256 Hash",
        }
        for key, value in row.items():
            if key not in standard_fields and value:
                metadata[key] = value

        return {
            "file_path": full_path,
            "file_name": filename,
            "extension": extension,
            "size_bytes": size_bytes,
            "created_ts": created_ts,
            "modified_ts": modified_ts,
            "accessed_ts": accessed_ts,
            "deleted": deleted,
            "md5_hash": md5_hash,
            "sha1_hash": sha1_hash,
            "sha256_hash": sha256_hash,
            "file_type": None,
            "metadata": json.dumps(metadata) if metadata else None,
        }


class GenericParser(BaseFileListParser):
    """Parser for generic CSV with user-defined column mapping."""

    def __init__(self, column_mapping: Dict[str, str]):
        """
        Initialize generic parser with column mapping.

        Args:
            column_mapping: Maps standard field names to CSV column names
                Example: {
                    'file_name': 'Name',
                    'file_path': 'Path',
                    'size_bytes': 'Size',
                    'modified_ts': 'Date Modified',
                }
        """
        self.column_mapping = column_mapping

    def parse(self, csv_path: Path) -> List[Dict[str, Any]]:
        """Parse generic CSV file using column mapping."""
        encoding = self.detect_encoding(csv_path)
        entries = []
        skipped_rows = 0

        try:
            with open(csv_path, "r", encoding=encoding, newline="") as f:
                reader = csv.DictReader(f)

                if not reader.fieldnames:
                    raise ValueError("CSV file has no headers")

                logger.info(f"Generic CSV headers: {reader.fieldnames}")

                for row_num, row in enumerate(reader, start=2):
                    try:
                        entry = self._parse_generic_row(row)
                        if entry:
                            entries.append(entry)
                    except Exception as e:
                        logger.warning(
                            f"Skipping row {row_num} due to error: {e}"
                        )
                        skipped_rows += 1
                        continue

            logger.info(
                f"Parsed {len(entries)} entries from generic CSV (skipped {skipped_rows} rows)"
            )
            return entries

        except Exception as e:
            logger.error(f"Failed to parse generic CSV: {e}")
            raise

    def _parse_generic_row(self, row: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Parse a single generic CSV row using column mapping."""
        # Map columns according to user-defined mapping
        filename = row.get(self.column_mapping.get("file_name", ""), "").strip()
        full_path = row.get(self.column_mapping.get("file_path", ""), "").strip()

        if not filename and not full_path:
            return None

        if not filename and full_path:
            filename = self._extract_filename(full_path)

        # Parse size
        size_col = self.column_mapping.get("size_bytes", "")
        size_str = row.get(size_col, "").strip() if size_col else ""
        try:
            size_bytes = int(size_str) if size_str else None
        except ValueError:
            size_bytes = None

        # Parse timestamps
        created_col = self.column_mapping.get("created_ts", "")
        modified_col = self.column_mapping.get("modified_ts", "")
        accessed_col = self.column_mapping.get("accessed_ts", "")

        created_ts = (
            self._parse_timestamp(row.get(created_col, "")) if created_col else None
        )
        modified_ts = (
            self._parse_timestamp(row.get(modified_col, ""))
            if modified_col
            else None
        )
        accessed_ts = (
            self._parse_timestamp(row.get(accessed_col, ""))
            if accessed_col
            else None
        )

        # Parse deleted flag
        deleted_col = self.column_mapping.get("deleted", "")
        deleted = self._parse_bool(row.get(deleted_col, "")) if deleted_col else False

        # Extract extension
        extension = self._extract_extension(filename)

        # Parse hashes if mapped
        md5_col = self.column_mapping.get("md5_hash", "")
        sha1_col = self.column_mapping.get("sha1_hash", "")
        sha256_col = self.column_mapping.get("sha256_hash", "")

        md5_hash = row.get(md5_col, "").strip() if md5_col else None
        sha1_hash = row.get(sha1_col, "").strip() if sha1_col else None
        sha256_hash = row.get(sha256_col, "").strip() if sha256_col else None

        # Parse partition_index and inode if mapped
        partition_col = self.column_mapping.get("partition_index", "")
        inode_col = self.column_mapping.get("inode", "")

        partition_index = None
        if partition_col:
            partition_str = row.get(partition_col, "").strip()
            if partition_str:
                try:
                    partition_index = int(partition_str)
                except ValueError:
                    partition_index = None

        inode = row.get(inode_col, "").strip() if inode_col else None
        if inode == "":
            inode = None

        # Collect extra metadata
        metadata = {}
        mapped_columns = set(self.column_mapping.values())
        for key, value in row.items():
            if key not in mapped_columns and value:
                metadata[key] = value

        return {
            "file_path": full_path,
            "file_name": filename,
            "extension": extension,
            "size_bytes": size_bytes,
            "created_ts": created_ts,
            "modified_ts": modified_ts,
            "accessed_ts": accessed_ts,
            "deleted": deleted,
            "md5_hash": md5_hash,
            "sha1_hash": sha1_hash,
            "sha256_hash": sha256_hash,
            "file_type": None,
            "metadata": json.dumps(metadata) if metadata else None,
            "partition_index": partition_index,
            "inode": inode,
        }


def detect_parser(csv_path: Path) -> BaseFileListParser:
    """
    Auto-detect parser type from CSV headers.

    Args:
        csv_path: Path to CSV file

    Returns:
        Appropriate parser instance (FTK, EnCase, or Generic with best-guess mapping)
    """
    # Detect encoding first
    with open(csv_path, "rb") as f:
        raw_data = f.read(100000)
        result = chardet.detect(raw_data)
        encoding = result["encoding"]

        if encoding and encoding.lower().startswith("utf-16"):
            if raw_data[:2] == b"\xff\xfe":
                encoding = "utf-16-le"
            elif raw_data[:2] == b"\xfe\xff":
                encoding = "utf-16-be"
            else:
                encoding = "utf-16"
        encoding = encoding if encoding else "utf-8"

    # Read first row to check headers
    try:
        # Try tab delimiter first (FTK)
        with open(csv_path, "r", encoding=encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            headers = reader.fieldnames or []

            # Check for FTK headers
            ftk_match = sum(
                1 for col in FTKParser.EXPECTED_COLUMNS if col in headers
            )
            if ftk_match >= 4:  # At least 4 columns match
                logger.info("Detected FTK CSV format")
                return FTKParser()

        # Try comma delimiter (EnCase)
        with open(csv_path, "r", encoding=encoding, newline="") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []

            # Check for EnCase headers
            encase_match = sum(
                1 for col in EnCaseParser.EXPECTED_COLUMNS if col in headers
            )
            if encase_match >= 4:
                logger.info("Detected EnCase CSV format")
                return EnCaseParser()

        # Fallback to generic parser with best-guess mapping
        logger.info(
            "Could not detect known format, using generic parser with auto-mapping"
        )
        column_mapping = _auto_detect_column_mapping(headers)
        return GenericParser(column_mapping)

    except Exception as e:
        logger.error(f"Failed to detect parser: {e}")
        # Return FTK parser as default fallback
        return FTKParser()


def _auto_detect_column_mapping(headers: List[str]) -> Dict[str, str]:
    """
    Auto-detect column mapping for generic CSV.

    Args:
        headers: List of CSV column names

    Returns:
        Best-guess column mapping
    """
    mapping = {}

    # Common patterns for filename
    for header in headers:
        header_lower = header.lower()
        if "name" in header_lower and "file" in header_lower:
            mapping["file_name"] = header
        elif "path" in header_lower:
            mapping["file_path"] = header
        elif "size" in header_lower:
            mapping["size_bytes"] = header
        elif "created" in header_lower or "create" in header_lower:
            mapping["created_ts"] = header
        elif "modified" in header_lower or "modify" in header_lower:
            mapping["modified_ts"] = header
        elif "accessed" in header_lower or "access" in header_lower:
            mapping["accessed_ts"] = header
        elif "deleted" in header_lower or "delete" in header_lower:
            mapping["deleted"] = header
        elif "md5" in header_lower:
            mapping["md5_hash"] = header
        elif "sha1" in header_lower:
            mapping["sha1_hash"] = header
        elif "sha256" in header_lower or "sha-256" in header_lower:
            mapping["sha256_hash"] = header
        elif "partition" in header_lower:
            mapping["partition_index"] = header
        elif "inode" in header_lower:
            mapping["inode"] = header

    logger.info(f"Auto-detected column mapping: {mapping}")
    return mapping
