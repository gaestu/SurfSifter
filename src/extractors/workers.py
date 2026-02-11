"""
Qt worker threads for extraction and ingestion.
"""

from PySide6.QtCore import QObject, Signal, QThread
from pathlib import Path
from typing import Dict, Any, Optional, TYPE_CHECKING
import traceback
import inspect
import time
import uuid

from .base import BaseExtractor

if TYPE_CHECKING:
    from core.audit_logging import EvidenceLogger


class WorkerCallbacks(QObject):
    """
    Qt-compatible callbacks that emit signals AND write to EvidenceLogger.

    Implements ExtractorCallbacks protocol via Qt signals.
    Integration point: on_log/on_step write to persistent log FIRST,
    then emit UI signals.

    Signals:
        progress(int, int, str): current, total, message
        log_message(str, str): message, level
        error(str, str): error, details
        step(str): step_name

    Usage:
        callbacks = WorkerCallbacks()
        callbacks.progress.connect(progress_bar.setValue)
        callbacks.log_message.connect(log_widget.append)

        extractor.run_extraction(..., callbacks=callbacks)
    """

    progress = Signal(int, int, str)  # current, total, message
    log_message = Signal(str, str)    # message, level
    error = Signal(str, str)          # error, details
    step = Signal(str)                # step_name

    def __init__(
        self,
        parent=None,
        evidence_logger: Optional["EvidenceLogger"] = None,
        extractor_name: str = "unknown"
    ):
        super().__init__(parent)
        self._cancelled = False
        self._evidence_logger = evidence_logger
        self._extractor_name = extractor_name

    def on_progress(self, current: int, total: int, message: str = ""):
        """Emit progress signal."""
        self.progress.emit(current, total, message)

    def on_log(self, message: str, level: str = "info"):
        """Emit log message signal AND write to persistent log."""
        # Write to file/DB first (persistent)
        if self._evidence_logger:
            self._evidence_logger.log_message(message, level, self._extractor_name)
        # Then emit UI signal
        self.log_message.emit(message, level)

    def on_error(self, error: str, details: str = ""):
        """Emit error signal AND write to persistent log."""
        # Write to file/DB first (persistent)
        if self._evidence_logger:
            self._evidence_logger.log_error(self._extractor_name, f"{error}: {details}" if details else error)
        self.error.emit(error, details)

    def on_step(self, step_name: str):
        """Emit step signal AND write to persistent log."""
        if self._evidence_logger:
            self._evidence_logger.log_step(step_name, self._extractor_name)
        self.step.emit(step_name)

    def is_cancelled(self) -> bool:
        """Check if cancelled."""
        return self._cancelled

    def cancel(self):
        """Mark as cancelled."""
        self._cancelled = True

    def set_extractor_name(self, name: str):
        """Update the extractor name for logging context."""
        self._extractor_name = name


class ExtractionWorker(QThread):
    """
    Worker thread for extraction phase.

    Runs extractor.run_extraction() in background thread and reports results.

    Signals:
        finished(bool): success
        error(str): error message

    Usage:
        worker = ExtractionWorker(extractor, evidence_fs, output_dir, config)
        worker.callbacks.progress.connect(lambda c, t, m: progress_bar.setValue(c/t*100))
        worker.finished.connect(on_extraction_finished)
        worker.start()
    """

    finished = Signal(bool)  # success
    error = Signal(str)      # error message

    def __init__(
        self,
        extractor: BaseExtractor,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        db_manager=None,
        evidence_id: int = None,
        evidence_label: str = None,
        evidence_source_path=None,
        evidence_logger: Optional["EvidenceLogger"] = None,
        parent=None
    ):
        """
        Initialize extraction worker.

        Args:
            extractor: Extractor module to run
            evidence_fs: Evidence filesystem (pytsk3, mounted, etc.) - may be None
            output_dir: Where to write output files
            config: Configuration dict from config widget
            db_manager: DatabaseManager instance for creating thread-local connections
            evidence_id: Evidence ID
            evidence_label: Evidence label
            evidence_source_path: Path to evidence source (E01/EWF file)
            evidence_logger: EvidenceLogger for persistent logging
            parent: Parent QObject
        """
        super().__init__(parent)
        self.extractor = extractor
        self.evidence_fs = evidence_fs
        self.output_dir = output_dir
        self.config = config
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self.evidence_source_path = evidence_source_path
        self._evidence_logger = evidence_logger
        self.callbacks = WorkerCallbacks(
            evidence_logger=evidence_logger,
            extractor_name=extractor.metadata.name if extractor and extractor.metadata else "unknown"
        )

    def _generate_run_id(self) -> str:
        """Generate unique run ID for process_log tracking."""
        return f"{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    def run(self):
        """Run extraction in background thread."""
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"ExtractionWorker.run() started for {self.extractor.metadata.name}")

        # Ensure config includes evidence_id and evidence_label for statistics tracking
        # Make a copy to avoid modifying the original
        config = dict(self.config)
        if self.evidence_id is not None:
            config.setdefault("evidence_id", self.evidence_id)
        if self.evidence_label:
            config.setdefault("evidence_label", self.evidence_label)
        if self.db_manager:
            # Some extractors (file_list) need db_manager to open their own connections
            config.setdefault("db_manager", self.db_manager)

        # Generate run_id and log extraction start
        run_id = self._generate_run_id()
        process_log_id = None
        start_time = time.time()

        if self._evidence_logger:
            try:
                process_log_id = self._evidence_logger.log_extraction_start(
                    extractor=self.extractor.metadata.name,
                    run_id=run_id,
                    config=config
                )
            except Exception as e:
                logger.warning(f"Failed to log extraction start: {e}")

        records_count = 0
        errors_count = 0

        try:
            # Call extractor's run_extraction with appropriate parameters
            # Different extractors need different parameters:
            # - evidence_conn: Database-based extractors (file_list_importer)
            # - evidence_source_path: Path-based extractors (bulk_extractor)
            # - evidence_fs: Filesystem-based extractors (browser_history, cache, etc.)

            import inspect
            sig = inspect.signature(self.extractor.run_extraction)
            params = list(sig.parameters.keys())

            logger.info(f"Extractor {self.extractor.metadata.name} parameters: {params}")

            # Determine extractor type by first parameter
            if 'evidence_conn' in params:
                # Database-based extractor (file_list_importer)
                # Create thread-local database connection
                if not self.db_manager:
                    raise ValueError("db_manager required for database-based extractors")

                logger.info(f"Creating thread-local evidence connection for {self.extractor.metadata.name}")
                evidence_conn = self.db_manager.get_evidence_conn(
                    self.evidence_id,
                    self.evidence_label
                )

                try:
                    logger.info(f"Calling {self.extractor.metadata.name}.run_extraction with evidence_conn")
                    success = self.extractor.run_extraction(
                        evidence_conn,
                        self.evidence_id,
                        self.output_dir,
                        config,
                        self.callbacks
                    )
                finally:
                    # Close thread-local connection
                    evidence_conn.close()

            elif 'evidence_source_path' in params:
                # Path-based extractor (bulk_extractor - works directly with E01 files)
                logger.info(f"Calling {self.extractor.metadata.name}.run_extraction with evidence_source_path")
                success = self.extractor.run_extraction(
                    self.evidence_source_path,
                    self.output_dir,
                    config,
                    self.callbacks
                )

            else:
                # Filesystem-based extractor (browser_history, cache parsers, etc.)
                # Create thread-local evidence_conn for multi-partition file_list queries
                evidence_conn = None
                if self.db_manager and self.evidence_id and self.evidence_label:
                    try:
                        evidence_conn = self.db_manager.get_evidence_conn(
                            self.evidence_id,
                            self.evidence_label
                        )
                        config["evidence_conn"] = evidence_conn
                        logger.info(f"Added evidence_conn to config for {self.extractor.metadata.name}")
                    except Exception as e:
                        logger.warning(f"Failed to create evidence_conn for config: {e}")

                try:
                    logger.info(f"Calling {self.extractor.metadata.name}.run_extraction with evidence_fs")
                    success = self.extractor.run_extraction(
                        self.evidence_fs,
                        self.output_dir,
                        config,
                        self.callbacks
                    )
                finally:
                    # Close thread-local connection if created
                    if evidence_conn is not None:
                        try:
                            evidence_conn.close()
                        except Exception:
                            pass

            logger.info(f"Extractor {self.extractor.metadata.name} returned: {success}")

            # Log extraction result with timing
            elapsed = time.time() - start_time
            if self._evidence_logger:
                try:
                    self._evidence_logger.log_extraction_result(
                        extractor=self.extractor.metadata.name,
                        run_id=run_id,
                        records=records_count,
                        errors=0 if success else 1,
                        elapsed_sec=elapsed,
                        process_log_id=process_log_id
                    )
                except Exception as e:
                    logger.warning(f"Failed to log extraction result: {e}")

            self.finished.emit(success)
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"

            # Log extraction failure
            elapsed = time.time() - start_time
            if self._evidence_logger:
                try:
                    self._evidence_logger.log_extraction_result(
                        extractor=self.extractor.metadata.name,
                        run_id=run_id,
                        records=0,
                        errors=1,
                        elapsed_sec=elapsed,
                        process_log_id=process_log_id
                    )
                except Exception as log_e:
                    logger.warning(f"Failed to log extraction error: {log_e}")

            self.error.emit(error_msg)
            self.finished.emit(False)

    def cancel(self):
        """Request cancellation."""
        self.callbacks.cancel()


class IngestionWorker(QThread):
    """
    Worker thread for ingestion phase.

    Runs extractor.run_ingestion() in background thread and reports results.

    Signals:
        finished(bool, dict): success, counts
        error(str): error message

    Usage:
        worker = IngestionWorker(extractor, output_dir, db_manager, evidence_id, evidence_label, config)
        worker.callbacks.progress.connect(lambda c, t, m: progress_bar.setValue(c/t*100))
        worker.finished.connect(on_ingestion_finished)
        worker.start()
    """

    finished = Signal(bool, dict)  # success, counts
    error = Signal(str)            # error message

    def __init__(
        self,
        extractor: BaseExtractor,
        output_dir: Path,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        config: Dict[str, Any],
        evidence_logger: Optional["EvidenceLogger"] = None,
        parent=None
    ):
        """
        Initialize ingestion worker.

        Args:
            extractor: Extractor module to run
            output_dir: Where output files are
            db_manager: DatabaseManager for creating thread-local connections
            evidence_id: Evidence ID
            evidence_label: Evidence label
            config: Configuration dict (which artifacts to ingest, etc.)
            evidence_logger: EvidenceLogger for persistent logging
            parent: Parent QObject
        """
        super().__init__(parent)
        self.extractor = extractor
        self.output_dir = output_dir
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self.config = config
        self._evidence_logger = evidence_logger
        self.callbacks = WorkerCallbacks(
            evidence_logger=evidence_logger,
            extractor_name=extractor.metadata.name if extractor and extractor.metadata else "unknown"
        )

    def _generate_run_id(self) -> str:
        """Generate unique run ID for process_log tracking."""
        return f"{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    def run(self):
        """Run ingestion in background thread."""
        import logging
        logger = logging.getLogger(__name__)

        # Ensure config includes evidence_id and evidence_label for statistics tracking
        # Make a copy to avoid modifying the original
        config = dict(self.config)
        if self.evidence_id is not None:
            config.setdefault("evidence_id", self.evidence_id)
        if self.evidence_label:
            config.setdefault("evidence_label", self.evidence_label)

        # Generate run_id and track timing
        run_id = self._generate_run_id()
        process_log_id = None
        start_time = time.time()

        if self._evidence_logger:
            try:
                process_log_id = self._evidence_logger.log_extraction_start(
                    extractor=f"{self.extractor.metadata.name}:ingest",
                    run_id=run_id,
                    config=config
                )
            except Exception as e:
                logger.warning(f"Failed to log ingestion start: {e}")

        try:
            # Create thread-local database connection
            evidence_conn = self.db_manager.get_evidence_conn(
                self.evidence_id,
                self.evidence_label
            )

            try:
                result = self.extractor.run_ingestion(
                    self.output_dir,
                    evidence_conn,
                    self.evidence_id,
                    config,
                    self.callbacks
                )
                # Handle extractors that return bool vs dict
                # Some extractors return True/False, others return Dict[str, int]
                if isinstance(result, bool):
                    success = result
                    counts = {}
                elif isinstance(result, dict):
                    success = True
                    counts = result
                else:
                    # Unexpected return type, treat as success with empty counts
                    success = bool(result)
                    counts = {}

                # Log ingestion result
                elapsed = time.time() - start_time
                # Sum only integer values (some extractors include 'errors' list in counts)
                records_ingested = sum(v for v in counts.values() if isinstance(v, int)) if counts else 0
                if self._evidence_logger:
                    try:
                        self._evidence_logger.log_ingestion_complete(
                            extractor=f"{self.extractor.metadata.name}:ingest",
                            run_id=run_id,
                            records_ingested=records_ingested,
                            errors=0 if success else 1,
                            elapsed_sec=elapsed,
                            process_log_id=process_log_id
                        )
                    except Exception as e:
                        logger.warning(f"Failed to log ingestion result: {e}")

                self.finished.emit(success, counts)
            finally:
                # Close thread-local connection
                evidence_conn.close()
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"

            # Log ingestion failure
            elapsed = time.time() - start_time
            if self._evidence_logger:
                try:
                    self._evidence_logger.log_ingestion_complete(
                        extractor=f"{self.extractor.metadata.name}:ingest",
                        run_id=run_id,
                        records_ingested=0,
                        errors=1,
                        elapsed_sec=elapsed,
                        process_log_id=process_log_id
                    )
                except Exception as log_e:
                    logger.warning(f"Failed to log ingestion error: {log_e}")

            self.error.emit(error_msg)
            self.finished.emit(False, {})

    def cancel(self):
        """Request cancellation."""
        self.callbacks.cancel()
