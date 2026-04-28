from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict, Callable, Any

from .logging import get_logger
from .database import slugify_label
from .database.helpers.statistics import sync_process_log_counters

from extractors import ExtractorRegistry
from extractors.callbacks import ExtractorCallbacks

LOGGER = get_logger("core.extraction_orchestrator")


# =============================================================================
# Extractor Tool Requirements
# =============================================================================

# Tool requirements for each extractor
# Maps extractor names to required external tools
# Empty list means Python-only, no external tools required
EXTRACTOR_TOOL_REQUIREMENTS: Dict[str, List[str]] = {
    # Forensic Tools (require external binaries)
    "bulk_extractor": ["bulk_extractor"],
    "foremost_carver": ["foremost"],
    "scalpel": ["scalpel"],
    "swiftbeaver": ["swiftbeaver"],

    # Python-based Extractors (no external tools)
    "sqlite_browser_history": [],
    "cache_simple": [],
    "cache_indexed": [],
    "cache_firefox": [],
    "registry_offline": [],  # Uses regipy (Python package, not external tool)
    "regex_text_scanner": [],
}

# Extractors that receive the raw evidence source path rather than an EvidenceFS.
# Add new path-based extractors here when they are introduced.
_PATH_BASED_EXTRACTORS: frozenset = frozenset({"bulk_extractor", "swiftbeaver"})


def get_extractor_tool_requirements(extractor: str) -> List[str]:
    """
    Get list of required external tools for an extractor.

    Args:
        extractor: Extractor name (e.g., "bulk_extractor", "foremost_carver")

    Returns:
        List of tool names required (empty if Python-only)

    Example:
        >>> get_extractor_tool_requirements("bulk_extractor")
        ["bulk_extractor"]
        >>> get_extractor_tool_requirements("sqlite_browser_history")
        []
    """
    return EXTRACTOR_TOOL_REQUIREMENTS.get(extractor, [])


# =============================================================================
# Legacy data structures removed cleanup
# RegexUrlFinding, RegexExecutionResult, ExecutionSummary were removed as they are no longer used.
# =============================================================================

class BridgeCallbacks(ExtractorCallbacks):
    def __init__(self, log_cb, step_cb, cancellation_check):
        self.log_cb = log_cb
        self.step_cb = step_cb
        self.cancellation_check = cancellation_check
        self._cancelled = False

    def on_log(self, message: str, level: str = "info"):
        if self.log_cb:
            self.log_cb(message)

    def on_step(self, message: str):
        if self.step_cb:
            self.step_cb("extraction", "running", message)

    def on_progress(self, current: int, total: int, message: str = ""):
        # Legacy pipeline doesn't have fine-grained progress per extractor easily mapped
        pass

    def on_error(self, message: str, details: str = ""):
        if self.log_cb:
            self.log_cb(f"ERROR: {message} {details}")

    def is_cancelled(self) -> bool:
        if self.cancellation_check:
            return self.cancellation_check()
        return self._cancelled

    def cancel(self):
        self._cancelled = True


@dataclass(frozen=True)
class ExtractorFailure:
    extractor: str
    phase: str
    message: str


@dataclass(frozen=True)
class PipelineSummary:
    total_extractors: int
    selected_extractors: int
    failed_extractors: List[ExtractorFailure]
    skipped_extractors: List[str]


def run_extraction_pipeline(
    fs: Any,
    *,
    case_conn: sqlite3.Connection,
    evidence_conn: sqlite3.Connection,
    evidence_id: int,
    case_root: Path,
    selected_extractors: Optional[List[str]] = None,
    bulk_extractor_scanners: Optional[List[str]] = None,
    bulk_extractor_threads: Optional[int] = None,
    bulk_extractor_existing_policy: str = "overwrite",
    bulk_extractor_delete_existing: bool = False,
    evidence_db_path: Optional[Path] = None,
    log_cb: Optional[Callable[[str], None]] = None,
    process_cb: Optional[Callable[[Any], None]] = None,
    step_cb: Optional[Callable[[str, str, str], None]] = None,
    cancellation_check: Optional[Callable[[], bool]] = None,
) -> PipelineSummary:
    """
    Execute the full extraction pipeline using modular extractors.

    Replaces legacy worker-based pipeline with ExtractorRegistry-based execution.
    """
    if log_cb is None:
        log_cb = LOGGER.info

    registry = ExtractorRegistry()
    extractors = registry.get_all()
    failed_extractors: List[ExtractorFailure] = []
    skipped_extractors: List[str] = []
    total_extractors = len(extractors)

    # Filter extractors - warn if no explicit selection
    if selected_extractors is None:
        LOGGER.warning(
            "selected_extractors is None — running ALL extractors. "
            "Consider passing explicit list to avoid unintended long runs."
        )
    else:
        # Map legacy names if needed
        filtered = []
        selected_set = set(selected_extractors)
        for ext in extractors:
            name = ext.metadata.name
            if name in selected_set:
                filtered.append(ext)
            elif name == "bulk_extractor" and "url_discovery" in selected_set:
                filtered.append(ext)
            elif name == "browser_history" and "sqlite_browser_history" in selected_set:
                filtered.append(ext)
            elif name == "registry" and ("registry_offline" in selected_set or "registry_reading" in selected_set):
                filtered.append(ext)
        extractors = filtered

    # Get evidence label for output paths
    case_conn.row_factory = sqlite3.Row
    row = case_conn.execute("SELECT label FROM evidences WHERE id = ?", (evidence_id,)).fetchone()
    evidence_label = row["label"] if row else f"evidence_{evidence_id}"
    evidence_slug = slugify_label(evidence_label, evidence_id)

    callbacks = BridgeCallbacks(log_cb, step_cb, cancellation_check)

    for extractor in extractors:
        if callbacks.is_cancelled():
            LOGGER.info("Extraction cancelled by user")
            break

        name = extractor.metadata.name
        LOGGER.info(f"Running extractor: {name}")

        # Build config
        config = {
            "evidence_id": evidence_id,
            "evidence_label": evidence_label,
            "evidence_conn": evidence_conn,
        }
        if name == "bulk_extractor":
            config["scanners"] = bulk_extractor_scanners
            config["num_threads"] = bulk_extractor_threads
            config["output_reuse_policy"] = bulk_extractor_existing_policy
            if bulk_extractor_delete_existing:
                config["overwrite_mode"] = "overwrite"
            else:
                config["overwrite_mode"] = "append"

            # Default to ingesting all supported artifact types
            # This ensures backward compatibility with legacy worker which ingested everything found
            config["artifact_types"] = ["url", "email", "domain", "ip", "telephone", "ccn", "bitcoin", "ether"]

        # Determine output dir
        output_dir = extractor.get_output_dir(case_root, evidence_slug)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Run Extraction
        extraction_succeeded = False
        if extractor.metadata.can_extract:
            # Determine input (fs or path) based on extractor requirements
            # Most modular extractors expect evidence_fs, but bulk_extractor expects path
            # We check signature or metadata if possible, or try both

            if name in _PATH_BASED_EXTRACTORS:
                # source_path-based extractor
                source_path = getattr(fs, "source_path", None)
                if source_path:
                    can_run, reason = extractor.can_run_extraction(source_path)
                    if can_run:
                        try:
                            extraction_succeeded = extractor.run_extraction(
                                source_path, output_dir, config, callbacks
                            ) is not False
                        except Exception as e:
                            LOGGER.error(f"Extractor {name} failed extraction: {e}")
                            failed_extractors.append(
                                ExtractorFailure(extractor=name, phase="extraction", message=str(e))
                            )
                    else:
                        LOGGER.warning(f"Skipping {name} extraction: {reason}")
                        skipped_extractors.append(f"{name} extraction: {reason}")
                else:
                    LOGGER.warning(f"Skipping {name}: EvidenceFS has no source_path")
                    skipped_extractors.append(f"{name} extraction: missing source_path")
            else:
                # Others use fs
                can_run, reason = extractor.can_run_extraction(fs)
                if can_run:
                    try:
                        extraction_succeeded = extractor.run_extraction(
                            fs, output_dir, config, callbacks
                        ) is not False
                    except Exception as e:
                        LOGGER.error(f"Extractor {name} failed extraction: {e}")
                        failed_extractors.append(
                            ExtractorFailure(extractor=name, phase="extraction", message=str(e))
                        )
                else:
                    LOGGER.warning(f"Skipping {name} extraction: {reason}")
                    skipped_extractors.append(f"{name} extraction: {reason}")

            # After successful extraction, trigger downstream ingest-only extractors
            downstream_names = getattr(extractor, 'downstream_extractors', [])
            if extraction_succeeded and downstream_names and extractor.metadata.can_extract and not extractor.metadata.can_ingest:
                for ds_name in downstream_names:
                    if callbacks.is_cancelled():
                        break
                    ds_ext = registry.get(ds_name)
                    if ds_ext is None:
                        LOGGER.warning("Downstream extractor %s not found", ds_name)
                        continue
                    if not ds_ext.metadata.can_ingest:
                        continue
                    # Use parent extractor's output dir — downstream ingesters
                    # read manifest.json from the parent's output
                    ingest_dir = output_dir
                    can_ingest, reason = ds_ext.can_run_ingestion(ingest_dir)
                    if can_ingest:
                        try:
                            LOGGER.info("Auto-triggering downstream ingestion: %s", ds_name)
                            callbacks.on_log(f"Auto-triggering {ds_name} ingestion")
                            ds_ext.run_ingestion(ingest_dir, evidence_conn, evidence_id, config, callbacks)
                            # Sync process_log counters for downstream extractor
                            try:
                                sync_process_log_counters(evidence_conn, evidence_id, ds_name)
                            except Exception as sync_err:
                                LOGGER.debug("process_log sync for %s: %s", ds_name, sync_err)
                        except Exception as e:
                            LOGGER.error("Downstream %s ingestion failed: %s", ds_name, e)
                            failed_extractors.append(
                                ExtractorFailure(extractor=ds_name, phase="ingestion", message=str(e))
                            )
                    else:
                        LOGGER.debug("Downstream %s cannot ingest: %s", ds_name, reason)

        # Run Ingestion
        if extractor.metadata.can_ingest:
             if callbacks.is_cancelled():
                 break

             can_run, reason = extractor.can_run_ingestion(output_dir)
             if can_run:
                 try:
                     extractor.run_ingestion(output_dir, evidence_conn, evidence_id, config, callbacks)
                     # Sync process_log counters from extractor_statistics
                     try:
                         sync_process_log_counters(evidence_conn, evidence_id, name)
                     except Exception as sync_err:
                         LOGGER.debug("process_log sync for %s: %s", name, sync_err)
                 except Exception as e:
                     LOGGER.error(f"Extractor {name} failed ingestion: {e}")
                     failed_extractors.append(
                         ExtractorFailure(extractor=name, phase="ingestion", message=str(e))
                     )
             else:
                 skipped_extractors.append(f"{name} ingestion: {reason}")

    summary = PipelineSummary(
        total_extractors=total_extractors,
        selected_extractors=len(extractors),
        failed_extractors=failed_extractors,
        skipped_extractors=skipped_extractors,
    )
    if failed_extractors:
        failed_names = ", ".join(
            f"{item.extractor} ({item.phase})" for item in failed_extractors
        )
        message = f"Extraction pipeline had {len(failed_extractors)} failure(s): {failed_names}"
        LOGGER.error(message)
        if log_cb:
            log_cb(f"ERROR: {message}")
        raise RuntimeError(message)
    return summary
