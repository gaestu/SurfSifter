"""
Base extractor interface for modular extraction system.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from pathlib import Path
from dataclasses import dataclass, field
from PySide6.QtWidgets import QWidget

from .callbacks import ExtractorCallbacks
from core.app_version import get_app_version


@dataclass
class ExtractorMetadata:
    """
    Metadata about an extractor module.

    Attributes:
        name: Internal identifier (e.g., "bulk_extractor")
        display_name: UI display name (e.g., "bulk_extractor")
        description: Short description for UI
        category: Category for grouping ("forensic_tools" | "browser" | "system")
        version: Module version string
        requires_tools: External tools needed (e.g., ["bulk_extractor", "foremost"])
        can_extract: Whether module has extraction phase
        can_ingest: Whether module has ingestion phase
    """
    name: str
    display_name: str
    description: str
    category: str
    requires_tools: List[str]
    can_extract: bool
    can_ingest: bool
    version: str = field(default_factory=get_app_version)


class BaseExtractor(ABC):
    """
    Base class for all extractor modules.

    Each module is responsible for:
    1. Declaring capabilities and requirements (metadata)
    2. Providing configuration UI (get_config_widget)
    3. Running extraction phase - write files (run_extraction)
    4. Running ingestion phase - load database (run_ingestion)
    5. Reporting status (get_status_widget)

    Lifecycle:
        1. Registry discovers module
        2. UI requests status widget → display current state
        3. User clicks "Run Extraction" → run_extraction() → write files
        4. User clicks "Ingest Results" → run_ingestion() → load database

    Example:
        class MyExtractor(BaseExtractor):
            @property
            def metadata(self):
                return ExtractorMetadata(
                    name="my_extractor",
                    display_name="My Extractor",
                    description="Does something useful",
                    category="forensic_tools",
                    requires_tools=["mytool"],
                    can_extract=True,
                    can_ingest=True
                )

            def can_run_extraction(self, evidence_fs):
                # Check if mytool is installed
                return True, ""

            def run_extraction(self, evidence_fs, output_dir, config, callbacks):
                # Run mytool, write output to output_dir
                callbacks.on_step("Running mytool")
                # ...
                return True

            def run_ingestion(self, output_dir, evidence_conn, evidence_id, config, callbacks):
                # Parse output files, insert into database
                callbacks.on_step("Parsing results")
                # ...
                return {"artifacts": 123}
    """

    @property
    @abstractmethod
    def metadata(self) -> ExtractorMetadata:
        """
        Return module metadata.

        Returns:
            ExtractorMetadata describing this module
        """
        pass

    @abstractmethod
    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """
        Check if extraction can run on this evidence.

        Verifies:
        - Required tools are installed
        - Evidence format is supported
        - Dependencies are met

        Args:
            evidence_fs: Evidence filesystem (pytsk3 FS or mounted path)

        Returns:
            Tuple of (can_run, reason_if_not)

        Example:
            return True, ""
            return False, "bulk_extractor not installed"
            return False, "Evidence is not a disk image"
        """
        pass

    @abstractmethod
    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """
        Check if ingestion can run (output files exist).

        Verifies:
        - Output directory exists
        - Expected output files are present
        - Files are not corrupted

        Args:
            output_dir: Directory where extraction wrote output files

        Returns:
            Tuple of (can_run, reason_if_not)

        Example:
            return True, ""
            return False, "No output files found"
            return False, "url.txt is empty"
        """
        pass

    @abstractmethod
    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """
        Return configuration widget or None if no config needed.

        Widget should:
        - Allow user to configure extractor settings
        - Emit configChanged signal when settings change
        - Provide get_config() method returning dict

        Args:
            parent: Parent widget for Qt hierarchy

        Returns:
            Configuration widget or None

        Example:
            return BulkExtractorConfigWidget(parent)
            return None  # No configuration needed
        """
        pass

    @abstractmethod
    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """
        Return widget showing current status.

        Widget should display:
        - Extraction status (not run | running | complete | failed)
        - Output file counts and sizes
        - Database record counts
        - Last run timestamp
        - Any warnings or errors

        Args:
            parent: Parent widget for Qt hierarchy
            output_dir: Directory where output files are/will be
            evidence_conn: SQLite connection to evidence database
            evidence_id: Evidence ID in database

        Returns:
            Status widget

        Example:
            return BulkExtractorStatusWidget(parent, output_dir, evidence_conn, evidence_id)
        """
        pass

    @abstractmethod
    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """
        Return output directory for this extractor's files.

        Convention:
            {case_root}/evidences/{evidence_label}/{extractor_name}/

        Some extractors support custom extractor names via config to differentiate
        between multiple runs (e.g., image_carving_foremost, image_carving_scalpel).

        Args:
            case_root: Root directory of case workspace
            evidence_label: Evidence label/slug (e.g., "test-local-win11", "4dell-latitude-cpi")
            config: Optional configuration dict (may contain 'extractor_name' key)

        Returns:
            Path to output directory

        Example:
            return case_root / "evidences" / evidence_label / "bulk_extractor"
        """
        pass

    @abstractmethod
    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Run extraction phase (write output files only, no database writes).

        Responsibilities:
        - Run external tools or internal logic
        - Write output files to output_dir
        - Report progress via callbacks
        - Handle cancellation
        - Clean up on failure

        Args:
            evidence_fs: Evidence filesystem (pytsk3 FS or mounted path)
            output_dir: Where to write output files (already created)
            config: Configuration dict from get_config_widget()
            callbacks: Progress/log/error callbacks

        Returns:
            True if successful, False if failed

        Example:
            callbacks.on_step("Running bulk_extractor")
            callbacks.on_log("Starting extraction with 16 threads")

            # Run tool
            result = subprocess.run([...])

            if callbacks.is_cancelled():
                callbacks.on_log("Cancelled by user", "warning")
                return False

            if result.returncode != 0:
                callbacks.on_error("Tool failed", result.stderr)
                return False

            callbacks.on_log(f"Wrote {count} files to {output_dir}")
            return True
        """
        pass

    @abstractmethod
    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """
        Run ingestion phase (load files into database).

        Responsibilities:
        - Parse output files from output_dir
        - Insert records into evidence database
        - Report progress via callbacks
        - Handle cancellation
        - Rollback on failure

        Args:
            output_dir: Where output files are located
            evidence_conn: SQLite connection to evidence database
            evidence_id: Evidence ID to associate records with
            config: Configuration dict (which artifacts to ingest, etc.)
            callbacks: Progress/log/error callbacks

        Returns:
            Dictionary of artifact counts ingested

        Example:
            callbacks.on_step("Parsing url.txt")

            urls = []
            with open(output_dir / "url.txt") as f:
                for line in f:
                    if callbacks.is_cancelled():
                        evidence_conn.rollback()
                        return {}
                    urls.append(parse_url(line))

            callbacks.on_step("Writing to database")
            cursor = evidence_conn.cursor()
            for url in urls:
                cursor.execute("INSERT INTO urls (...) VALUES (...)", url)
            evidence_conn.commit()

            callbacks.on_log(f"Ingested {len(urls)} URLs")
            return {"urls": len(urls)}
        """
        pass
