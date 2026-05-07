"""
Reference Lists Manager - Manage reference list files (hashlists, filelists, urllists).

This module handles file I/O for reference lists stored in:
    ~/.config/surfsifter/reference_lists/

Supports three types of reference lists:
1. Hash Lists: MD5/SHA1/SHA256 hashes for exact matching
2. File Lists: Filename patterns (wildcard or regex) for flexible matching
3. URL Lists: URL/domain patterns (wildcard or regex) for URL matching
"""
from __future__ import annotations

import logging
import hashlib
import os
import re
import shutil
import stat
import tempfile
import unicodedata
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, IO, List, Optional, Set, Tuple

__all__ = [
    "ReferenceListManager",
    "ConflictPolicy",
    "ImportResult",
    "install_predefined_lists",
    "MAX_HASHLIST_SIZE",
    "MAX_URLLIST_SIZE",
]

logger = logging.getLogger(__name__)

# Maximum file size for hash list import (100 MB)
MAX_HASHLIST_SIZE = 100 * 1024 * 1024

# Maximum file size for URL list import (100 MB)
MAX_URLLIST_SIZE = 100 * 1024 * 1024


class ConflictPolicy(Enum):
    """Policy for handling name collisions during batch import."""
    SKIP = "skip"
    OVERWRITE = "overwrite"
    RENAME = "rename"  # Appends _1, _2, etc.


@dataclass
class ImportResult:
    """Result of a single file import operation."""
    source_path: Path
    dest_name: str
    status: str  # "imported", "skipped", "overwritten", "renamed", "error", "cancelled"
    error: Optional[str] = None


class ReferenceListManager:
    """Manage reference lists (hash lists, file name lists, and URL lists)."""

    def __init__(self, base_path: Optional[Path] = None, *, create_dirs: bool = True):
        """
        Initialize reference list manager.

        Args:
            base_path: Custom base path (default: ~/.config/surfsifter/reference_lists)
            create_dirs: Create missing reference-list directories when True.
        """
        if base_path is None:
            primary = Path.home() / ".config" / "surfsifter" / "reference_lists"
            legacy = Path.home() / ".config" / "web-and-browser-analyzer" / "reference_lists"
            # Use legacy path if it exists and primary doesn't, for backward compat
            if not primary.exists() and legacy.exists():
                base_path = legacy
                logger.info("Using legacy reference lists path: %s", legacy)
            else:
                base_path = primary

        self.base_path = Path(base_path).expanduser().resolve()
        self.hashlists_dir = self.base_path / "hashlists"
        self.filelists_dir = self.base_path / "filelists"
        self.urllists_dir = self.base_path / "urllists"

        if create_dirs:
            self.hashlists_dir.mkdir(parents=True, exist_ok=True)
            self.filelists_dir.mkdir(parents=True, exist_ok=True)
            self.urllists_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Reference lists base path: {self.base_path}")

    # -------------------------------------------------------------------------
    # Load Methods
    # -------------------------------------------------------------------------

    def load_hashlist(self, name: str) -> Set[str]:
        """
        Load hash list from file.

        Args:
            name: Hash list name (without .txt extension)

        Returns:
            Set of hashes (normalized to lowercase)

        Raises:
            FileNotFoundError: If hash list doesn't exist
        """
        hashes, _version = self.load_hashlist_with_version(name)
        return hashes

    def load_hashlist_with_version(self, name: str) -> Tuple[Set[str], str]:
        """
        Load a hash list from one file snapshot and return its SHA-256 version.

        Args:
            name: Hash list name (without .txt extension)

        Returns:
            Tuple of normalized hash set and SHA-256 of the file bytes.

        Raises:
            FileNotFoundError: If hash list doesn't exist
            UnicodeDecodeError: If the hash list is not UTF-8 text
        """
        list_path = self._get_list_path("hashlist", name)
        if not list_path.exists():
            raise FileNotFoundError(f"Hash list not found: {name}")

        contents = self._read_hashlist_bytes(list_path)
        version = hashlib.sha256(contents).hexdigest()

        hashes = set()
        for line in contents.decode("utf-8").splitlines():
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith("#"):
                continue
            # Normalize to lowercase for case-insensitive matching
            hashes.add(line.lower())

        logger.info(f"Loaded {len(hashes)} hashes from '{name}'")
        return hashes, version

    def load_filelist(self, name: str) -> Tuple[List[str], bool]:
        """
        Load file list patterns.

        Args:
            name: File list name (without .txt extension)

        Returns:
            Tuple of (patterns, is_regex) where:
                - patterns: List of filename patterns
                - is_regex: True if patterns are regex, False for wildcards

        Raises:
            FileNotFoundError: If file list doesn't exist
        """
        list_path = self._get_list_path("filelist", name)
        if not list_path.exists():
            raise FileNotFoundError(f"File list not found: {name}")

        patterns = []
        is_regex = False
        metadata = self.get_metadata("filelist", name)

        # Check REGEX flag in metadata
        if metadata.get("REGEX", "false").lower() in ("true", "yes", "1"):
            is_regex = True

        with self._open_hashlist_text(list_path) as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith("#"):
                    continue
                patterns.append(line)

        logger.info(f"Loaded {len(patterns)} patterns from '{name}' (regex={is_regex})")
        return patterns, is_regex

    def load_urllist(self, name: str) -> Tuple[List[str], bool]:
        """
        Load URL list patterns.

        Args:
            name: URL list name (without .txt extension)

        Returns:
            Tuple of (patterns, is_regex) where:
                - patterns: List of URL patterns/domains
                - is_regex: True if patterns are regex, False for wildcards

        Raises:
            FileNotFoundError: If URL list doesn't exist
        """
        list_path = self._get_list_path("urllist", name)
        if not list_path.exists():
            raise FileNotFoundError(f"URL list not found: {name}")

        patterns = []
        is_regex = False
        metadata = self.get_metadata("urllist", name)

        # Check REGEX flag in metadata
        if metadata.get("REGEX", "false").lower() in ("true", "yes", "1"):
            is_regex = True

        with self._open_urllist_text(list_path) as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith("#"):
                    continue
                patterns.append(line)

        logger.info(f"Loaded {len(patterns)} URL patterns from '{name}' (regex={is_regex})")
        return patterns, is_regex

    # -------------------------------------------------------------------------
    # List & Metadata Methods
    # -------------------------------------------------------------------------

    def list_available(self) -> Dict[str, List[str]]:
        """
        List all available reference lists.

        Returns:
            Dictionary with keys 'hashlists', 'filelists', 'urllists',
            values are list names (without .txt)
        """
        hashlists = self._list_safe_stems(self.hashlists_dir)
        filelists = self._list_safe_stems(self.filelists_dir)
        urllists = self._list_safe_stems(self.urllists_dir)

        return {
            "hashlists": sorted(hashlists),
            "filelists": sorted(filelists),
            "urllists": sorted(urllists),
        }

    def get_metadata(self, list_type: str, name: str) -> Dict[str, str]:
        """
        Extract metadata from reference list header.

        Args:
            list_type: 'hashlist', 'filelist', or 'urllist'
            name: List name (without .txt extension)

        Returns:
            Dictionary of metadata (NAME, CATEGORY, DESCRIPTION, UPDATED, AUTHOR, TYPE, REGEX)

        Raises:
            FileNotFoundError: If list doesn't exist
        """
        list_path = self._get_list_path(list_type, name)

        if not list_path.exists():
            raise FileNotFoundError(f"{list_type} not found: {name}")

        metadata = {}
        if list_type == "urllist":
            opener = self._open_urllist_text
        else:
            opener = self._open_hashlist_text
        with opener(list_path) as f:
            for line in f:
                line = line.strip()
                # Stop at first non-comment line
                if not line.startswith("#"):
                    break
                # Parse metadata: # KEY: Value
                if ":" in line:
                    key, value = line[1:].split(":", 1)
                    metadata[key.strip()] = value.strip()

        return metadata

    # -------------------------------------------------------------------------
    # Import/Export Methods
    # -------------------------------------------------------------------------

    def import_list(self, source_path: Path, list_type: str, name: str) -> None:
        """
        Import external text file as reference list.

        Args:
            source_path: Path to source text file
            list_type: 'hashlist', 'filelist', or 'urllist'
            name: Name for imported list (without .txt extension)

        Raises:
            FileNotFoundError: If source file doesn't exist
            ValueError: If list_type is invalid
        """
        source_path = Path(source_path)
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        dest_path = self._get_list_path(list_type, name)
        if list_type == "hashlist":
            self._atomic_copy(source_path, dest_path)
        elif list_type == "urllist":
            self._atomic_copy_urllist(source_path, dest_path)
        else:
            shutil.copy2(source_path, dest_path)
        logger.info(f"Imported {list_type} '{name}' from {source_path}")

    def read_list_text(self, list_type: str, name: str) -> str:
        """Read a stored reference list with validation and no-follow checks."""
        list_path = self._get_list_path(list_type, name)
        if list_type == "hashlist":
            return self._read_hashlist_bytes(list_path).decode("utf-8")
        if list_type == "filelist":
            return self._read_filelist_bytes(list_path).decode("utf-8")
        if list_type == "urllist":
            return self._read_urllist_bytes(list_path).decode("utf-8")
        raise ValueError(f"Invalid list_type: {list_type}")

    def create_list(
        self,
        list_type: str,
        name: str,
        metadata: Dict[str, str],
        entries: List[str],
    ) -> None:
        """
        Create new reference list from scratch.

        Args:
            list_type: 'hashlist', 'filelist', or 'urllist'
            name: Name for new list (without .txt extension)
            metadata: Dictionary of metadata (NAME, CATEGORY, DESCRIPTION, etc.)
            entries: List of hashes, patterns, or URLs

        Raises:
            ValueError: If list_type is invalid
        """
        dest_path = self._get_list_path(list_type, name)

        # Write file with metadata header
        with open(dest_path, "w", encoding="utf-8") as f:
            # Write metadata
            for key, value in metadata.items():
                f.write(f"# {key}: {value}\n")
            f.write("\n")

            # Write entries
            for entry in entries:
                f.write(f"{entry}\n")

        logger.info(f"Created {list_type} '{name}' with {len(entries)} entries")

    def delete_list(self, list_type: str, name: str) -> None:
        """
        Delete reference list.

        Args:
            list_type: 'hashlist', 'filelist', or 'urllist'
            name: List name (without .txt extension)

        Raises:
            FileNotFoundError: If list doesn't exist
            ValueError: If list_type is invalid
        """
        list_path = self._get_list_path(list_type, name)

        if not list_path.exists():
            raise FileNotFoundError(f"{list_type} not found: {name}")

        list_path.unlink()
        logger.info(f"Deleted {list_type} '{name}'")

    # -------------------------------------------------------------------------
    # Batch Import Methods
    # -------------------------------------------------------------------------

    def check_exists(self, list_type: str, name: str) -> bool:
        """
        Check if a reference list already exists.

        Args:
            list_type: 'hashlist', 'filelist', or 'urllist'
            name: List name (without .txt extension)

        Returns:
            True if list exists, False otherwise
        """
        try:
            list_path = self._get_list_path(list_type, name)
            return list_path.exists()
        except ValueError:
            return False

    def generate_unique_name(self, list_type: str, base_name: str) -> str:
        """
        Generate unique name by appending _1, _2, etc. if name already exists.

        Args:
            list_type: 'hashlist', 'filelist', or 'urllist'
            base_name: Base name to use

        Returns:
            Unique name (may be same as base_name if no conflict)
        """
        if not self.check_exists(list_type, base_name):
            return base_name

        counter = 1
        while True:
            candidate = f"{base_name}_{counter}"
            if not self.check_exists(list_type, candidate):
                return candidate
            counter += 1
            # Safety limit
            if counter > 1000:
                raise RuntimeError(f"Could not generate unique name for {base_name}")

    def import_hashlist_batch(
        self,
        files: List[Path],
        conflict_policy: ConflictPolicy = ConflictPolicy.SKIP,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> List[ImportResult]:
        """
        Import multiple hash list files.

        Uses atomic copy (temp file + rename) to prevent partial files on cancel.
        Validates each file before import (UTF-8, non-empty, valid content).

        Args:
            files: List of file paths to import
            conflict_policy: How to handle name collisions (SKIP, OVERWRITE, RENAME)
            progress_callback: Optional callback(current, total, filename) for progress
            cancel_check: Optional callback() -> bool to check if cancelled

        Returns:
            List of ImportResult for each file
        """
        results: List[ImportResult] = []
        total = len(files)

        for idx, file_path in enumerate(files):
            # Check for cancellation
            if cancel_check and cancel_check():
                logger.info(f"Batch import cancelled at file {idx + 1}/{total}")
                # Add remaining files as cancelled
                for remaining in files[idx:]:
                    results.append(ImportResult(
                        source_path=remaining,
                        dest_name=remaining.stem,
                        status="cancelled",
                    ))
                break

            # Report progress
            if progress_callback:
                progress_callback(idx + 1, total, file_path.name)

            # Derive name from filename (without extension)
            base_name = file_path.stem
            try:
                base_name = self.validate_list_name(base_name)
            except ValueError as e:
                results.append(ImportResult(
                    source_path=file_path,
                    dest_name=base_name,
                    status="error",
                    error=str(e),
                ))
                continue

            # Validate file
            is_valid, error_msg = self._validate_hashlist_file(file_path)
            if not is_valid:
                results.append(ImportResult(
                    source_path=file_path,
                    dest_name=base_name,
                    status="error",
                    error=error_msg,
                ))
                continue

            # Check for conflicts
            exists = self.check_exists("hashlist", base_name)

            if exists:
                if conflict_policy == ConflictPolicy.SKIP:
                    results.append(ImportResult(
                        source_path=file_path,
                        dest_name=base_name,
                        status="skipped",
                    ))
                    continue
                elif conflict_policy == ConflictPolicy.RENAME:
                    dest_name = self.generate_unique_name("hashlist", base_name)
                    status = "renamed"
                else:  # OVERWRITE
                    dest_name = base_name
                    status = "overwritten"
            else:
                dest_name = base_name
                status = "imported"

            try:
                dest_path = self._get_list_path("hashlist", dest_name)
                self._atomic_copy(file_path, dest_path)
                results.append(ImportResult(
                    source_path=file_path,
                    dest_name=dest_name,
                    status=status,
                ))
                logger.debug(f"Imported hashlist '{dest_name}' from {file_path}")
            except Exception as e:
                results.append(ImportResult(
                    source_path=file_path,
                    dest_name=dest_name,
                    status="error",
                    error=str(e),
                ))
                logger.error(f"Failed to import {file_path}: {e}")

        # Summary logging
        imported = sum(1 for r in results if r.status in ("imported", "overwritten", "renamed"))
        skipped = sum(1 for r in results if r.status == "skipped")
        errors = sum(1 for r in results if r.status == "error")
        cancelled = sum(1 for r in results if r.status == "cancelled")

        logger.info(
            f"Batch import complete: {imported} imported, {skipped} skipped, "
            f"{errors} errors, {cancelled} cancelled"
        )

        return results

    def import_urllist_batch(
        self,
        files: List[Path],
        conflict_policy: ConflictPolicy = ConflictPolicy.SKIP,
        category: str = "Custom",
        description: str = "",
        is_regex: bool = False,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> List[ImportResult]:
        """
        Import multiple URL list files.

        Plain text inputs receive generated URL-list metadata. Inputs that
        already declare SurfSifter URL-list metadata are copied as-is.

        Args:
            files: List of file paths to import
            conflict_policy: How to handle name collisions (SKIP, OVERWRITE, RENAME)
            category: Shared category for generated metadata
            description: Shared description for generated metadata
            is_regex: Shared regex flag for generated metadata
            progress_callback: Optional callback(current, total, filename) for progress
            cancel_check: Optional callback() -> bool to check if cancelled

        Returns:
            List of ImportResult for each file
        """
        results: List[ImportResult] = []
        total = len(files)

        for idx, file_path in enumerate(files):
            if cancel_check and cancel_check():
                logger.info(f"URL-list batch import cancelled at file {idx + 1}/{total}")
                for remaining in files[idx:]:
                    results.append(ImportResult(
                        source_path=remaining,
                        dest_name=remaining.stem,
                        status="cancelled",
                    ))
                break

            if progress_callback:
                progress_callback(idx + 1, total, file_path.name)

            base_name = file_path.stem
            try:
                base_name = self.validate_list_name(base_name)
            except ValueError as e:
                results.append(ImportResult(
                    source_path=file_path,
                    dest_name=base_name,
                    status="error",
                    error=str(e),
                ))
                continue

            is_valid, error_msg = self._validate_urllist_file(file_path)
            if not is_valid:
                results.append(ImportResult(
                    source_path=file_path,
                    dest_name=base_name,
                    status="error",
                    error=error_msg,
                ))
                continue

            exists = self.check_exists("urllist", base_name)
            if exists:
                if conflict_policy == ConflictPolicy.SKIP:
                    results.append(ImportResult(
                        source_path=file_path,
                        dest_name=base_name,
                        status="skipped",
                    ))
                    continue
                elif conflict_policy == ConflictPolicy.RENAME:
                    dest_name = self.generate_unique_name("urllist", base_name)
                    status = "renamed"
                else:
                    dest_name = base_name
                    status = "overwritten"
            else:
                dest_name = base_name
                status = "imported"

            try:
                dest_path = self._get_list_path("urllist", dest_name)
                if self._has_urllist_metadata(file_path):
                    self._atomic_copy_urllist(file_path, dest_path)
                else:
                    self._atomic_write_urllist(
                        file_path,
                        dest_path,
                        name=dest_name,
                        category=category,
                        description=description,
                        is_regex=is_regex,
                    )
                results.append(ImportResult(
                    source_path=file_path,
                    dest_name=dest_name,
                    status=status,
                ))
                logger.debug(f"Imported URL list '{dest_name}' from {file_path}")
            except Exception as e:
                results.append(ImportResult(
                    source_path=file_path,
                    dest_name=dest_name,
                    status="error",
                    error=str(e),
                ))
                logger.error(f"Failed to import URL list {file_path}: {e}")

        imported = sum(1 for r in results if r.status in ("imported", "overwritten", "renamed"))
        skipped = sum(1 for r in results if r.status == "skipped")
        errors = sum(1 for r in results if r.status == "error")
        cancelled = sum(1 for r in results if r.status == "cancelled")

        logger.info(
            f"URL-list batch import complete: {imported} imported, {skipped} skipped, "
            f"{errors} errors, {cancelled} cancelled"
        )

        return results

    # -------------------------------------------------------------------------
    # Private Helper Methods
    # -------------------------------------------------------------------------

    def _list_safe_stems(self, directory: Path) -> List[str]:
        """List regular, non-symlink .txt reference-list stems with safe names."""
        stems: List[str] = []
        for path in directory.glob("*.txt"):
            if path.is_symlink() or not path.is_file():
                continue
            try:
                stems.append(self.validate_list_name(path.stem))
            except ValueError:
                logger.warning("Ignoring unsafe reference list name: %s", path.name)
        return stems

    def _get_list_path(self, list_type: str, name: str) -> Path:
        """Get the file path for a reference list."""
        safe_name = self.validate_list_name(name)
        if list_type == "hashlist":
            list_dir = self.hashlists_dir
        elif list_type == "filelist":
            list_dir = self.filelists_dir
        elif list_type == "urllist":
            list_dir = self.urllists_dir
        else:
            raise ValueError(f"Invalid list_type: {list_type}")

        path = list_dir / f"{safe_name}.txt"
        try:
            base_resolved = self.base_path.resolve(strict=False)
            list_dir_resolved = list_dir.resolve(strict=False)
            list_dir_resolved.relative_to(base_resolved)
            path.parent.resolve(strict=False).relative_to(list_dir_resolved)
        except ValueError as exc:
            raise ValueError(f"Invalid reference list path: {name!r}") from exc
        return path

    @staticmethod
    def validate_list_name(name: str) -> str:
        """Validate a reference-list name before turning it into a path."""
        if not isinstance(name, str):
            raise ValueError("Reference list name must be text")
        name = unicodedata.normalize("NFKC", name)
        if not name or not name.strip():
            raise ValueError("Reference list name cannot be empty")
        if name in {".", ".."} or ".." in name:
            raise ValueError(f"Invalid reference list name: {name!r}")
        if Path(name).is_absolute() or "/" in name or "\\" in name:
            raise ValueError(f"Invalid reference list name: {name!r}")
        if any(ord(char) < 32 or ord(char) == 127 for char in name):
            raise ValueError("Reference list name contains control characters")
        if not re.fullmatch(r"[A-Za-z0-9._\- ]+", name):
            raise ValueError(f"Invalid reference list name: {name!r}")
        return name

    def _validate_hashlist_file(self, path: Path) -> Tuple[bool, Optional[str]]:
        """
        Validate a hash list file before import.

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            with self._open_hashlist_text(path) as f:
                has_content = False
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        has_content = True
                        break
                if not has_content:
                    return False, "No valid hash entries found (only comments or empty lines)"
        except FileNotFoundError:
            return False, f"File not found: {path}"
        except IsADirectoryError:
            return False, f"Not a file: {path}"
        except UnicodeDecodeError as e:
            return False, f"Invalid UTF-8 encoding: {e}"
        except ValueError as e:
            return False, str(e)
        except OSError as e:
            return False, f"Cannot read file: {e}"

        return True, None

    def _validate_urllist_file(self, path: Path) -> Tuple[bool, Optional[str]]:
        """
        Validate a URL list file before import.

        URL list entries are patterns, so this intentionally does not perform
        URL syntax validation.
        """
        try:
            with self._open_urllist_text(path) as f:
                has_content = False
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        has_content = True
                        break
                if not has_content:
                    return False, "No valid URL patterns found (only comments or empty lines)"
        except FileNotFoundError:
            return False, f"File not found: {path}"
        except IsADirectoryError:
            return False, f"Not a file: {path}"
        except UnicodeDecodeError as e:
            return False, f"Invalid UTF-8 encoding: {e}"
        except ValueError as e:
            return False, str(e)
        except OSError as e:
            return False, f"Cannot read file: {e}"

        return True, None

    def _has_urllist_metadata(self, path: Path) -> bool:
        """Return True when a source file declares SurfSifter URL-list metadata."""
        metadata: Dict[str, str] = {}
        with self._open_urllist_text(path) as f:
            for line in f:
                line = line.strip()
                if not line.startswith("#"):
                    break
                if ":" in line:
                    key, value = line[1:].split(":", 1)
                    metadata[key.strip().upper()] = value.strip()

        return metadata.get("TYPE", "").lower() == "urllist"

    def _atomic_write_urllist(
        self,
        source_path: Path,
        dest_path: Path,
        name: str,
        category: str,
        description: str,
        is_regex: bool,
    ) -> None:
        """Write a URL list with generated metadata using temp file + rename."""
        temp_name: Optional[str] = None
        try:
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                delete=False,
                dir=dest_path.parent,
                prefix=f".{dest_path.name}.",
                suffix=".tmp",
            ) as f:
                temp_name = f.name
                f.write(f"# NAME: {self._metadata_value(name)}\n")
                f.write(f"# CATEGORY: {self._metadata_value(category)}\n")
                f.write(f"# DESCRIPTION: {self._metadata_value(description)}\n")
                f.write("# TYPE: urllist\n")
                f.write(f"# REGEX: {'true' if is_regex else 'false'}\n")
                f.write("\n")
                with self._open_urllist_text(source_path) as source:
                    for line in source:
                        entry = line.strip()
                        if entry and not entry.startswith("#"):
                            f.write(f"{entry}\n")
            Path(temp_name).replace(dest_path)
        except Exception:
            if temp_name and Path(temp_name).exists():
                try:
                    Path(temp_name).unlink()
                except OSError:
                    pass
            raise

    @staticmethod
    def _metadata_value(value: str) -> str:
        """Normalize metadata values to one line; colons remain allowed in values."""
        return " ".join(str(value).splitlines())

    def _open_urllist_text(self, path: Path) -> IO[str]:
        """Open a URL-list source as a regular UTF-8 file without following symlinks."""
        fd = self._open_urllist_binary_fd(path)
        try:
            return os.fdopen(fd, "r", encoding="utf-8")
        except Exception:
            os.close(fd)
            raise

    def _open_hashlist_text(self, path: Path) -> IO[str]:
        """Open a hash-list source as a regular UTF-8 file without following symlinks."""
        fd = self._open_hashlist_binary_fd(path)
        try:
            return os.fdopen(fd, "r", encoding="utf-8")
        except Exception:
            os.close(fd)
            raise

    def _open_urllist_binary_fd(self, path: Path) -> int:
        """Open a URL-list source fd with no-follow semantics and size checks."""
        return self._open_regular_binary_fd(path, MAX_URLLIST_SIZE)

    def _open_hashlist_binary_fd(self, path: Path) -> int:
        """Open a hash-list source fd with no-follow semantics and size checks."""
        return self._open_regular_binary_fd(path, MAX_HASHLIST_SIZE)

    def _read_hashlist_bytes(self, path: Path) -> bytes:
        fd = self._open_regular_binary_fd(path, MAX_HASHLIST_SIZE, allow_empty=True)
        return self._read_regular_bytes(fd, MAX_HASHLIST_SIZE)

    def _read_filelist_bytes(self, path: Path) -> bytes:
        fd = self._open_regular_binary_fd(path, MAX_HASHLIST_SIZE, allow_empty=True)
        return self._read_regular_bytes(fd, MAX_HASHLIST_SIZE)

    def _read_urllist_bytes(self, path: Path) -> bytes:
        fd = self._open_regular_binary_fd(path, MAX_URLLIST_SIZE, allow_empty=True)
        return self._read_regular_bytes(fd, MAX_URLLIST_SIZE)

    def _read_regular_bytes(self, fd: int, max_size: int) -> bytes:
        """Read an fd in bounded chunks so growth after open cannot exceed limits."""
        chunks: List[bytes] = []
        total = 0
        with os.fdopen(fd, "rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_size:
                    raise ValueError(
                        f"File too large: {total / (1024*1024):.1f} MB (max 100 MB)"
                    )
                chunks.append(chunk)
        return b"".join(chunks)

    def _open_regular_binary_fd(self, path: Path, max_size: int, *, allow_empty: bool = False) -> int:
        """Open a source fd with no-follow semantics and stable regular-file checks."""
        if path.is_symlink():
            raise ValueError(f"Symlinked files are not supported: {path}")

        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW

        try:
            fd = os.open(path, flags)
        except OSError as e:
            if path.is_symlink():
                raise ValueError(f"Symlinked files are not supported: {path}") from e
            raise

        try:
            source_stat = os.fstat(fd)
            path_stat = path.lstat()
            # Keep source imports inside the user-selected regular file even if
            # the import folder changes concurrently between path checks and open.
            if stat.S_ISLNK(path_stat.st_mode):
                raise ValueError(f"Symlinked files are not supported: {path}")
            if (
                source_stat.st_dev != path_stat.st_dev
                or source_stat.st_ino != path_stat.st_ino
            ):
                raise ValueError(f"File changed during import validation: {path}")
            if not stat.S_ISREG(source_stat.st_mode):
                raise ValueError(f"Not a file: {path}")
            if source_stat.st_size > max_size:
                raise ValueError(
                    f"File too large: {source_stat.st_size / (1024*1024):.1f} MB (max 100 MB)"
                )
            if source_stat.st_size == 0 and not allow_empty:
                raise ValueError("File is empty")
            return fd
        except Exception:
            os.close(fd)
            raise

    def _atomic_copy_urllist(self, src: Path, dest: Path) -> None:
        """Copy a URL-list source without following symlinks using temp file + replace."""
        temp_name: Optional[str] = None
        try:
            source_fd = self._open_urllist_binary_fd(src)
            with os.fdopen(source_fd, "rb") as source:
                with tempfile.NamedTemporaryFile(
                    "wb",
                    delete=False,
                    dir=dest.parent,
                    prefix=f".{dest.name}.",
                    suffix=".tmp",
                ) as temp:
                    temp_name = temp.name
                    shutil.copyfileobj(source, temp)
            Path(temp_name).replace(dest)
        except Exception:
            if temp_name and Path(temp_name).exists():
                try:
                    Path(temp_name).unlink()
                except OSError:
                    pass
            raise

    def _atomic_copy(self, src: Path, dest: Path) -> None:
        """Copy file atomically using temp file + replace."""
        temp_name: Optional[str] = None
        try:
            source_fd = self._open_hashlist_binary_fd(src)
            with os.fdopen(source_fd, "rb") as source:
                with tempfile.NamedTemporaryFile(
                    "wb",
                    delete=False,
                    dir=dest.parent,
                    prefix=f".{dest.name}.",
                    suffix=".tmp",
                ) as temp:
                    temp_name = temp.name
                    shutil.copyfileobj(source, temp)
            Path(temp_name).replace(dest)
        except Exception:
            if temp_name and Path(temp_name).exists():
                try:
                    Path(temp_name).unlink()
                except OSError:
                    pass
            raise


def install_predefined_lists(dest_base_path: Optional[Path] = None) -> List[str]:
    """
    Install predefined reference lists from the application to user config.

    Copies predefined lists from reference_lists/ in the app directory to the
    user's config directory (~/.config/surfsifter/reference_lists/).

    Args:
        dest_base_path: Destination base path (default: ~/.config/surfsifter/reference_lists)

    Returns:
        List of installed list names
    """
    # Determine source directory (repository reference_lists/)
    import sys
    if getattr(sys, "frozen", False):
        repo_root = Path(getattr(sys, "_MEIPASS", "."))
    else:
        # src/core/matching/manager.py -> 4 parents -> repo root
        repo_root = Path(__file__).resolve().parent.parent.parent.parent
    source_dir = repo_root / "reference_lists"

    if not source_dir.exists():
        logger.warning(f"Predefined reference lists directory not found: {source_dir}")
        return []

    # Determine destination
    if dest_base_path is None:
        dest_base_path = Path.home() / ".config" / "surfsifter" / "reference_lists"
    else:
        dest_base_path = Path(dest_base_path)

    dest_base_path.mkdir(parents=True, exist_ok=True)

    installed = []

    # Copy hashlists
    source_hashlists = source_dir / "hashlists"
    dest_hashlists = dest_base_path / "hashlists"
    dest_hashlists.mkdir(exist_ok=True)

    if source_hashlists.exists():
        for hashlist_file in source_hashlists.glob("*.txt"):
            dest_file = dest_hashlists / hashlist_file.name
            if not dest_file.exists():  # Don't overwrite existing
                shutil.copy2(hashlist_file, dest_file)
                installed.append(f"hashlist/{hashlist_file.stem}")
                logger.info(f"Installed predefined hashlist: {hashlist_file.stem}")

    # Copy filelists
    source_filelists = source_dir / "filelists"
    dest_filelists = dest_base_path / "filelists"
    dest_filelists.mkdir(exist_ok=True)

    if source_filelists.exists():
        for filelist_file in source_filelists.glob("*.txt"):
            dest_file = dest_filelists / filelist_file.name
            if not dest_file.exists():
                shutil.copy2(filelist_file, dest_file)
                installed.append(f"filelist/{filelist_file.stem}")
                logger.info(f"Installed predefined filelist: {filelist_file.stem}")

    # Copy urllists
    source_urllists = source_dir / "urllists"
    dest_urllists = dest_base_path / "urllists"
    dest_urllists.mkdir(exist_ok=True)

    if source_urllists.exists():
        for urllist_file in source_urllists.glob("*.txt"):
            dest_file = dest_urllists / urllist_file.name
            if not dest_file.exists():
                shutil.copy2(urllist_file, dest_file)
                installed.append(f"urllist/{urllist_file.stem}")
                logger.info(f"Installed predefined urllist: {urllist_file.stem}")

    return installed
