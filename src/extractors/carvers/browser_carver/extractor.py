"""
Browser Carver Extractor

Deep scan unallocated space for browser artifacts using foremost/scalpel.
Recovers browser SQLite databases from deleted/unallocated areas.

Current capabilities:
- SQLite database recovery (History, Cookies, Login Data, Form History, etc.)
- URL extraction from corrupted/partial SQLite files
- Raw URL pattern scanning in binary data
- Post-carving pruning of non-ingested file types
- Active limit enforcement with process termination during carving
- O(n) limit checking with early exit for performance

Future expansion (not yet implemented):
- Chrome cache block files
- LevelDB files
- OLE compound files (would require integration with jump_lists)
- Compressed artifacts (gzip, mozLz4)

This is a user-initiated, long-running operation - not automatic.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QCheckBox, QGroupBox,
    QComboBox, QHBoxLayout
)

from ...base import BaseExtractor, ExtractorMetadata
from ...callbacks import ExtractorCallbacks
from core.logging import get_logger
from core.tool_discovery import discover_tools
from core.database import (
    insert_urls,
    insert_browser_history_rows,
    insert_cookies,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)
from core.statistics_collector import StatisticsCollector
from .sqlite_validator import identify_browser_db, BROWSER_SCHEMAS
from .resilient_sqlite import parse_sqlite_best_effort, scan_for_urls

LOGGER = get_logger("extractors.browser_carver")

# Default safety caps to prevent disk exhaustion
DEFAULT_MAX_CARVED_SIZE_MB = 10000  # 10 GB total carved output
DEFAULT_MAX_CARVED_FILES = 50000   # 50,000 files maximum

# File types we actually process (prune everything else)
INGESTED_EXTENSIONS = {'.sqlite', '.db', ''}  # Empty string = no extension (SQLite files)
# Directories created by carving tools that we don't process
PRUNABLE_DIRS = {'ldblog', 'sst', 'ldb'}


class BrowserCarverConfigWidget(QWidget):
    """Configuration widget for browser carver."""

    def __init__(self, parent=None, *, available_tools: Dict[str, bool] = None):
        super().__init__(parent)
        self.available_tools = available_tools or {}
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Warning label
        warning = QLabel(
            "⚠️ Deep scan of unallocated space.\n"
            "This operation may take several hours on large images."
        )
        warning.setStyleSheet("color: orange; font-weight: bold;")
        layout.addWidget(warning)

        # Tool selection
        tool_group = QGroupBox("Carving Tool")
        tool_layout = QVBoxLayout(tool_group)

        self.tool_combo = QComboBox()

        if self.available_tools.get("foremost"):
            self.tool_combo.addItem("foremost", "foremost")
        if self.available_tools.get("scalpel"):
            self.tool_combo.addItem("scalpel", "scalpel")

        if self.tool_combo.count() == 0:
            self.tool_combo.addItem("No carving tools available", None)
            self.tool_combo.setEnabled(False)

        tool_layout.addWidget(self.tool_combo)
        layout.addWidget(tool_group)

        # Artifact types
        artifacts_group = QGroupBox("Artifacts to Recover")
        artifacts_layout = QVBoxLayout(artifacts_group)

        self.cb_history = QCheckBox("Browser History (SQLite)")
        self.cb_history.setChecked(True)
        artifacts_layout.addWidget(self.cb_history)

        self.cb_cookies = QCheckBox("Cookies (SQLite)")
        self.cb_cookies.setChecked(True)
        artifacts_layout.addWidget(self.cb_cookies)

        self.cb_cache = QCheckBox("Cache entries (Chrome blockfile)")
        self.cb_cache.setChecked(False)
        artifacts_layout.addWidget(self.cb_cache)

        layout.addWidget(artifacts_group)

        # Options
        options_group = QGroupBox("Options")
        options_layout = QVBoxLayout(options_group)

        self.cb_raw_scan = QCheckBox("Include raw URL scanning (slower but more thorough)")
        self.cb_raw_scan.setChecked(True)
        options_layout.addWidget(self.cb_raw_scan)

        self.cb_prune = QCheckBox("Auto-prune non-ingested file types (recommended)")
        self.cb_prune.setChecked(True)
        self.cb_prune.setToolTip(
            "Automatically removes carved files we don't process (e.g., .ldblog, .sst)\n"
            "to prevent disk exhaustion. Disable only for debugging."
        )
        options_layout.addWidget(self.cb_prune)

        layout.addWidget(options_group)

        # Safety limits
        limits_group = QGroupBox("Safety Limits")
        limits_layout = QVBoxLayout(limits_group)

        limits_note = QLabel(
            "⚠️ Carving can generate massive output. Set caps to prevent disk exhaustion."
        )
        limits_note.setWordWrap(True)
        limits_note.setStyleSheet("color: #888; font-size: 10px;")
        limits_layout.addWidget(limits_note)

        # Max size
        size_layout = QHBoxLayout()
        size_layout.addWidget(QLabel("Max carved output (GB):"))
        from PySide6.QtWidgets import QSpinBox
        self.max_size_spin = QSpinBox()
        self.max_size_spin.setRange(1, 1000)
        self.max_size_spin.setValue(10)  # 10 GB default
        self.max_size_spin.setToolTip(
            "Terminate carving and clean up if total output exceeds this size.\n"
            "The carving process is monitored and killed if the limit is reached."
        )
        size_layout.addWidget(self.max_size_spin)
        limits_layout.addLayout(size_layout)

        # Max files
        files_layout = QHBoxLayout()
        files_layout.addWidget(QLabel("Max carved files:"))
        self.max_files_spin = QSpinBox()
        self.max_files_spin.setRange(100, 500000)
        self.max_files_spin.setValue(50000)  # 50,000 files default
        self.max_files_spin.setSingleStep(1000)
        self.max_files_spin.setToolTip(
            "Terminate carving and clean up if file count exceeds this limit.\n"
            "The carving process is monitored and killed if the limit is reached."
        )
        files_layout.addWidget(self.max_files_spin)
        limits_layout.addLayout(files_layout)

        layout.addWidget(limits_group)
        layout.addStretch()

    def get_config(self) -> Dict[str, Any]:
        """Get current configuration."""
        tool = self.tool_combo.currentData()
        return {
            "tool": tool,
            "recover_history": self.cb_history.isChecked(),
            "recover_cookies": self.cb_cookies.isChecked(),
            "recover_cache": self.cb_cache.isChecked(),
            "raw_url_scan": self.cb_raw_scan.isChecked(),
            "prune_non_ingested": self.cb_prune.isChecked(),
            "max_carved_size_mb": self.max_size_spin.value() * 1024,  # Convert GB to MB
            "max_carved_files": self.max_files_spin.value(),
        }


class BrowserCarverExtractor(BaseExtractor):
    """
    Deep scan extractor for browser artifacts in unallocated space.

    Uses foremost or scalpel to carve SQLite databases, then validates
    and parses browser-related files.
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata."""
        return ExtractorMetadata(
            name="browser_carver",
            display_name="Browser Carver (Deep Scan)",
            description="Recover browser data from unallocated space (Deep Freeze recovery)",
            category="forensic_tools",
            requires_tools=["foremost", "scalpel"],  # Optional tools
            can_extract=True,
            can_ingest=True
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        if evidence_fs is None:
            return False, "No evidence filesystem mounted."

        # Check for carving tools
        tools = discover_tools()
        foremost_info = tools.get("foremost")
        scalpel_info = tools.get("scalpel")
        has_foremost = foremost_info.available if foremost_info else False
        has_scalpel = scalpel_info.available if scalpel_info else False

        if not has_foremost and not has_scalpel:
            return False, "No carving tools available. Install foremost or scalpel."

        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if ingestion can run."""
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run extraction first"
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output directory has existing extraction output."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return configuration widget."""
        tools = discover_tools()
        foremost_info = tools.get("foremost")
        scalpel_info = tools.get("scalpel")
        available = {
            "foremost": foremost_info.available if foremost_info else False,
            "scalpel": scalpel_info.available if scalpel_info else False,
        }
        return BrowserCarverConfigWidget(parent, available_tools=available)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget."""
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            carved_count = data.get("carved_count", 0)
            browser_count = len(data.get("browser_files", []))
            status_text = (
                f"Browser Carver (Deep Scan)\n"
                f"Files carved: {carved_count}\n"
                f"Browser DBs found: {browser_count}\n"
                f"Run ID: {data.get('run_id', 'N/A')}"
            )
        else:
            status_text = "Browser Carver (Deep Scan)\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory."""
        return case_root / "evidences" / evidence_label / "browser_carver"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Run browser carving extraction.

        1. Get raw disk image path
        2. Run foremost/scalpel with browser-specific config
        3. Validate carved SQLite files
        4. Generate manifest
        """
        callbacks.on_step("Initializing browser carver extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")

        # Start statistics tracking (may be None in tests)
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting browser carver extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        tool = config.get("tool", "foremost")

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "tool": tool,
            "config": config,
            "carved_count": 0,
            "browser_files": [],
            "status": "ok",
            "notes": [],
        }

        # Get image path
        image_path = self._get_image_path(evidence_fs)
        if not image_path:
            manifest_data["status"] = "error"
            manifest_data["notes"].append("Could not determine disk image path")
            self._write_manifest(manifest_data, output_dir)
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "failed")
            return False

        callbacks.on_step(f"Running {tool} on disk image")

        # Run carving tool
        carve_output = output_dir / "carved"
        carve_output.mkdir(exist_ok=True)

        # Get safety limits from config
        max_size_mb = config.get("max_carved_size_mb", DEFAULT_MAX_CARVED_SIZE_MB)
        max_files = config.get("max_carved_files", DEFAULT_MAX_CARVED_FILES)
        prune_non_ingested = config.get("prune_non_ingested", True)

        success, carved_files, carve_notes = self._run_carving_tool(
            tool, image_path, carve_output, config, callbacks,
            max_size_mb=max_size_mb, max_files=max_files
        )

        manifest_data["notes"].extend(carve_notes)

        # Check if run was limited (vs actual error)
        # Limited runs have "Carving terminated:" in notes, regardless of carved_files count
        # (early limit hits may produce no SQLite files before termination)
        run_was_limited = not success and any(
            "Carving terminated:" in note for note in carve_notes
        )

        if not success and not run_was_limited:
            # Actual error (tool failed, not found, etc.)
            manifest_data["status"] = "error"
            manifest_data["notes"].append(f"{tool} execution failed")
            self._write_manifest(manifest_data, output_dir)
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "failed")
            return False

        if run_was_limited:
            # Run was truncated due to limits - mark status clearly
            manifest_data["status"] = "limited"
            callbacks.on_log("⚠️ Extraction was truncated due to safety limits", "warning")

        # Prune non-ingested file types if enabled
        if prune_non_ingested:
            callbacks.on_step("Pruning non-ingested file types")
            pruned_count, pruned_bytes = self._prune_non_ingested(carve_output, callbacks)
            if pruned_count > 0:
                manifest_data["pruned_files"] = pruned_count
                manifest_data["pruned_bytes"] = pruned_bytes
                manifest_data["notes"].append(
                    f"Pruned {pruned_count} non-ingested files ({pruned_bytes / (1024*1024):.1f} MB)"
                )

        manifest_data["carved_count"] = len(carved_files)

        # Validate carved SQLite files
        callbacks.on_step("Validating carved files")
        browser_files = []

        for i, carved_file in enumerate(carved_files):
            if callbacks.is_cancelled():
                manifest_data["status"] = "cancelled"
                break

            callbacks.on_progress(i + 1, len(carved_files), f"Validating {carved_file.name}")

            db_type = identify_browser_db(carved_file)
            if db_type:
                md5 = hashlib.md5(carved_file.read_bytes()).hexdigest()
                sha256 = hashlib.sha256(carved_file.read_bytes()).hexdigest()

                browser_files.append({
                    "path": str(carved_file),
                    "filename": carved_file.name,
                    "db_type": db_type,
                    "size_bytes": carved_file.stat().st_size,
                    "md5": md5,
                    "sha256": sha256,
                })

                callbacks.on_log(f"Found {db_type} database: {carved_file.name}", "info")

        manifest_data["browser_files"] = browser_files

        # Report discovery count (browser DBs found)
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=len(browser_files))

        # Also scan for raw URLs if enabled
        if config.get("raw_url_scan", True):
            callbacks.on_step("Scanning carved files for raw URLs")
            raw_urls = self._scan_raw_urls(carved_files, callbacks)
            manifest_data["raw_urls_found"] = len(raw_urls)

            # Save raw URLs to file
            if raw_urls:
                raw_urls_path = output_dir / "raw_urls.txt"
                raw_urls_path.write_text('\n'.join(sorted(set(raw_urls))))

        callbacks.on_step("Writing manifest")
        self._write_manifest(manifest_data, output_dir)

        LOGGER.info(
            "Browser carver extraction complete: %d carved, %d browser DBs",
            manifest_data["carved_count"],
            len(browser_files),
        )

        if manifest_data["status"] != "error":
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "ok")
        else:
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "failed")
        return manifest_data["status"] != "error"

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """
        Ingest carved browser artifacts.

        Parses validated SQLite files and inserts into appropriate tables
        with discovered_by="browser_carver".
        """
        callbacks.on_step("Reading manifest")

        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"urls": 0, "history": 0, "cookies": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        browser_files = manifest_data.get("browser_files", [])

        # Start statistics tracking for ingestion - use run_id from manifest
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        results = {"urls": 0, "history": 0, "cookies": 0}

        if not browser_files:
            callbacks.on_log("No browser databases to ingest", "warning")

            # Still check for raw URLs
            raw_urls_path = output_dir / "raw_urls.txt"
            if raw_urls_path.exists():
                results["urls"] = self._ingest_raw_urls(
                    raw_urls_path, evidence_id, run_id, evidence_conn, callbacks
                )

            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, records=results["urls"], urls=results["urls"])
                stats.finish_run(evidence_id, self.metadata.name, "ok")
            return results

        discovered_by = f"browser_carver:{self.metadata.version}:{run_id}"

        callbacks.on_progress(0, len(browser_files), "Ingesting browser databases")

        for i, file_entry in enumerate(browser_files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(i + 1, len(browser_files), f"Parsing {file_entry['filename']}")

            file_path = Path(file_entry["path"])
            db_type = file_entry["db_type"]

            try:
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser="unknown",  # Carved from unallocated
                    artifact_type=db_type,
                    run_id=run_id,
                    extracted_path=str(file_path),
                    extraction_status="carved",
                    extraction_timestamp_utc=manifest_data["extraction_timestamp_utc"],
                    logical_path=f"[carved]/{file_entry['filename']}",
                    file_size_bytes=file_entry["size_bytes"],
                    file_md5=file_entry["md5"],
                    file_sha256=file_entry["sha256"],
                )

                # Parse based on DB type
                if db_type == "history":
                    count = self._ingest_history_db(
                        file_path, evidence_id, run_id, discovered_by, evidence_conn, callbacks
                    )
                    results["history"] += count
                    update_inventory_ingestion_status(
                        evidence_conn, inventory_id, "ok", records_parsed=count
                    )

                elif db_type == "cookies":
                    count = self._ingest_cookies_db(
                        file_path, evidence_id, run_id, discovered_by, evidence_conn, callbacks
                    )
                    results["cookies"] += count
                    update_inventory_ingestion_status(
                        evidence_conn, inventory_id, "ok", records_parsed=count
                    )

                elif db_type == "places":
                    # Firefox places.sqlite - contains history and bookmarks
                    count = self._ingest_places_db(
                        file_path, evidence_id, run_id, discovered_by, evidence_conn, callbacks
                    )
                    results["history"] += count
                    update_inventory_ingestion_status(
                        evidence_conn, inventory_id, "ok", records_parsed=count
                    )

                else:
                    callbacks.on_log(f"Unknown DB type: {db_type}", "warning")

            except Exception as e:
                LOGGER.error("Failed to ingest %s: %s", file_path, e, exc_info=True)
                callbacks.on_error(f"Ingestion failed: {file_entry['filename']}", str(e))

        # Ingest raw URLs
        raw_urls_path = output_dir / "raw_urls.txt"
        if raw_urls_path.exists():
            results["urls"] += self._ingest_raw_urls(
                raw_urls_path, evidence_id, run_id, evidence_conn, callbacks
            )

        evidence_conn.commit()

        total_records = sum(results.values())
        if stats:
            stats.report_ingested(
                evidence_id, self.metadata.name,
                records=total_records,
                urls=results["urls"],
                history=results["history"],
                cookies=results["cookies"]
            )
            stats.finish_run(evidence_id, self.metadata.name, "ok")
        return results

    # Helper Methods

    def _generate_run_id(self) -> str:
        """Generate run ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _write_manifest(self, data: dict, output_dir: Path) -> None:
        """Write manifest file."""
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(data, indent=2))

    def _get_image_path(self, evidence_fs) -> Optional[Path]:
        """Get disk image path from evidence filesystem."""
        try:
            source_path = getattr(evidence_fs, 'source_path', None)
            if source_path:
                return Path(source_path) if isinstance(source_path, str) else source_path

            image_path = getattr(evidence_fs, 'image_path', None)
            if image_path:
                return Path(image_path) if isinstance(image_path, str) else image_path
        except Exception:
            pass
        return None

    def _run_carving_tool(
        self,
        tool: str,
        image_path: Path,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
        *,
        max_size_mb: int = DEFAULT_MAX_CARVED_SIZE_MB,
        max_files: int = DEFAULT_MAX_CARVED_FILES
    ) -> tuple[bool, List[Path], List[str]]:
        """Run foremost or scalpel on disk image with active limit monitoring.

        Monitors output directory during carving and terminates the process
        if size or file count limits are exceeded.

        Returns:
            Tuple of (success, carved_files, notes)
        """
        notes: List[str] = []

        # Get config file
        config_path = self._get_carving_config()

        if tool == "foremost":
            cmd = ["foremost", "-i", str(image_path), "-o", str(output_dir)]
            if config_path:
                cmd.extend(["-c", str(config_path)])
        elif tool == "scalpel":
            cmd = ["scalpel", "-c", str(config_path), "-o", str(output_dir), str(image_path)]
            if not config_path:
                callbacks.on_error("scalpel requires config file", "")
                return False, [], ["scalpel requires config file"]
        else:
            callbacks.on_error(f"Unknown carving tool: {tool}", "")
            return False, [], [f"Unknown carving tool: {tool}"]

        callbacks.on_log(f"Running: {' '.join(cmd)}", "info")
        callbacks.on_log(f"Limits: {max_size_mb} MB, {max_files} files", "info")

        try:
            import time

            # Start process
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            max_size_bytes = max_size_mb * 1024 * 1024
            limit_exceeded = False
            limit_reason = ""
            check_interval = 2.0  # Check every 2 seconds

            # Initial check before monitoring loop (catches pre-existing output)
            exceeded, reason = self._check_carve_limits(
                output_dir, max_size_bytes, max_files
            )
            if exceeded:
                limit_exceeded = True
                limit_reason = reason
                callbacks.on_log(f"⚠️ Limit exceeded before carving started: {reason}", "warning")
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

            # Monitor output directory while process runs
            if not limit_exceeded:
                while process.poll() is None:
                    time.sleep(check_interval)

                    # Check limits with single-pass counting
                    exceeded, reason = self._check_carve_limits(
                        output_dir, max_size_bytes, max_files
                    )

                    if exceeded:
                        limit_exceeded = True
                        limit_reason = reason
                        callbacks.on_log(f"⚠️ Limit exceeded: {reason} - terminating {tool}", "warning")
                        process.terminate()
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                        break

                    # Check if cancelled
                    if callbacks.is_cancelled():
                        process.terminate()
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                        notes.append("Carving cancelled by user")
                        break

            # Get any remaining output
            stdout, stderr = process.communicate()

            # Final post-process check (catches fast runs that complete between checks)
            if not limit_exceeded and not callbacks.is_cancelled():
                exceeded, reason = self._check_carve_limits(
                    output_dir, max_size_bytes, max_files
                )
                if exceeded:
                    limit_exceeded = True
                    limit_reason = reason
                    callbacks.on_log(f"⚠️ Limit exceeded after carving completed: {reason}", "warning")

            if limit_exceeded:
                notes.append(f"Carving terminated: {limit_reason}")
                # Return False to indicate the run was truncated/limited
                # Collect what we carved before limit was hit for partial ingestion decision
                carved_files = []
                for carved in output_dir.rglob("*"):
                    if carved.is_file() and carved.suffix in ['.sqlite', '.db', '']:
                        try:
                            header = carved.read_bytes()[:16]
                            if header.startswith(b"SQLite format 3"):
                                carved_files.append(carved)
                        except Exception:
                            pass
                # Return False (limited) with carved files and notes
                return False, carved_files, notes
            elif process.returncode != 0 and not callbacks.is_cancelled():
                callbacks.on_error(f"{tool} failed", stderr.decode('utf-8', errors='replace') if stderr else "")
                return False, [], [f"{tool} failed with return code {process.returncode}"]

            # Collect carved SQLite files (single pass, O(n))
            carved_files = []
            for carved in output_dir.rglob("*"):
                if carved.is_file() and carved.suffix in ['.sqlite', '.db', '']:
                    try:
                        header = carved.read_bytes()[:16]
                        if header.startswith(b"SQLite format 3"):
                            carved_files.append(carved)
                    except Exception:
                        pass

            return True, carved_files, notes

        except FileNotFoundError:
            callbacks.on_error(f"{tool} not found", f"Install with: apt install {tool}")
            return False, [], [f"{tool} not found"]
        except Exception as e:
            callbacks.on_error(f"{tool} execution error", str(e))
            return False, [], [f"{tool} error: {str(e)[:200]}"]

    def _check_carve_limits(
        self,
        output_dir: Path,
        max_size_bytes: int,
        max_files: int
    ) -> tuple[bool, str]:
        """Check if carving output exceeds limits.

        Single-pass O(n) check of the output directory.

        Returns:
            Tuple of (exceeded: bool, reason: str)
        """
        total_size = 0
        file_count = 0

        try:
            for item in output_dir.rglob("*"):
                if item.is_file():
                    file_count += 1
                    try:
                        total_size += item.stat().st_size
                    except OSError:
                        pass  # File may have been deleted

                    # Check limits during iteration for early exit
                    if file_count > max_files:
                        return True, f"File count ({file_count}) exceeded limit ({max_files})"
                    if total_size > max_size_bytes:
                        size_mb = total_size / (1024 * 1024)
                        limit_mb = max_size_bytes / (1024 * 1024)
                        return True, f"Size ({size_mb:.0f} MB) exceeded limit ({limit_mb:.0f} MB)"
        except Exception as e:
            LOGGER.debug("Error checking carve limits: %s", e)

        return False, ""

    def _get_carving_config(self) -> Optional[Path]:
        """Get browser artifacts carving config file."""
        config_paths = [
            # Config file in extractor directory
            Path(__file__).parent / "browser_artifacts.conf",
        ]

        for path in config_paths:
            if path.exists():
                return path

        return None

    def _prune_non_ingested(
        self,
        carve_output: Path,
        callbacks: ExtractorCallbacks
    ) -> tuple[int, int]:
        """
        Remove carved files/directories that we don't process.

        This prevents disk exhaustion from LevelDB/SST files that carving tools
        create but we don't actually ingest.

        Returns:
            Tuple of (files_removed, bytes_freed)
        """
        files_removed = 0
        bytes_freed = 0

        # First, remove entire directories we know we don't process
        for dir_name in PRUNABLE_DIRS:
            target_dir = carve_output / dir_name
            if target_dir.exists() and target_dir.is_dir():
                try:
                    dir_size = sum(f.stat().st_size for f in target_dir.rglob("*") if f.is_file())
                    dir_count = len(list(target_dir.rglob("*")))
                    shutil.rmtree(target_dir)
                    bytes_freed += dir_size
                    files_removed += dir_count
                    callbacks.on_log(
                        f"Pruned directory: {dir_name} ({dir_count} files, {dir_size / (1024*1024):.1f} MB)",
                        "info"
                    )
                except Exception as e:
                    LOGGER.warning("Failed to prune directory %s: %s", target_dir, e)

        # Then, remove individual files with extensions we don't process
        non_ingested_extensions = {'.ldblog', '.sst', '.ldb', '.log'}
        for carved in carve_output.rglob("*"):
            if carved.is_file():
                suffix = carved.suffix.lower()
                # Keep SQLite files and files without extension (potential SQLite)
                if suffix in non_ingested_extensions:
                    try:
                        file_size = carved.stat().st_size
                        carved.unlink()
                        bytes_freed += file_size
                        files_removed += 1
                    except Exception as e:
                        LOGGER.debug("Failed to prune file %s: %s", carved, e)

        if files_removed > 0:
            LOGGER.info(
                "Pruned %d non-ingested files (%.1f MB freed)",
                files_removed, bytes_freed / (1024*1024)
            )

        return files_removed, bytes_freed

    def _scan_raw_urls(
        self,
        carved_files: List[Path],
        callbacks: ExtractorCallbacks
    ) -> List[str]:
        """Scan carved files for raw URL strings."""
        all_urls = []

        for carved in carved_files:
            try:
                data = carved.read_bytes()
                urls = scan_for_urls(data)
                all_urls.extend(urls)
            except Exception as e:
                LOGGER.debug("Failed to scan %s for URLs: %s", carved, e)

        callbacks.on_log(f"Found {len(all_urls)} raw URLs in carved files", "info")
        return all_urls

    def _ingest_raw_urls(
        self,
        urls_path: Path,
        evidence_id: int,
        run_id: str,
        evidence_conn,
        callbacks: ExtractorCallbacks
    ) -> int:
        """Ingest raw URLs from scan."""
        discovered_by = f"browser_carver:raw_scan:{run_id}"

        try:
            urls_text = urls_path.read_text()
            urls = [u.strip() for u in urls_text.splitlines() if u.strip()]
        except (IOError, OSError, UnicodeDecodeError):
            return 0

        records = []
        for url in urls:
            domain = self._extract_domain(url)
            scheme = url.split("://")[0] if "://" in url else None

            records.append({
                "url": url,
                "domain": domain,
                "scheme": scheme,
                "discovered_by": discovered_by,
                "source_path": "[carved/unallocated]",
                "notes": "Recovered from unallocated space",
                "run_id": run_id,
            })

        if records:
            insert_urls(evidence_conn, evidence_id, records)
            callbacks.on_log(f"Ingested {len(records)} raw URLs", "info")

        return len(records)

    def _ingest_history_db(
        self,
        db_path: Path,
        evidence_id: int,
        run_id: str,
        discovered_by: str,
        evidence_conn,
        callbacks: ExtractorCallbacks
    ) -> int:
        """Ingest Chromium History database."""
        data = parse_sqlite_best_effort(db_path)

        if not data:
            return 0

        records = []

        # Try standard Chromium history format
        if "urls" in data:
            for row in data["urls"]:
                records.append({
                    "browser": "unknown",
                    "url": row.get("url", ""),
                    "title": row.get("title"),
                    "visit_time_utc": None,
                    "visit_count": row.get("visit_count"),
                    "typed_count": row.get("typed_count"),
                    "source_path": "[carved/unallocated]",
                    "discovered_by": discovered_by,
                    "run_id": run_id,
                    "notes": "Recovered from unallocated space",
                })

        if records:
            insert_browser_history_rows(evidence_conn, evidence_id, records)
            callbacks.on_log(f"Ingested {len(records)} history records", "info")

        return len(records)

    def _ingest_cookies_db(
        self,
        db_path: Path,
        evidence_id: int,
        run_id: str,
        discovered_by: str,
        evidence_conn,
        callbacks: ExtractorCallbacks
    ) -> int:
        """Ingest Chromium Cookies database."""
        data = parse_sqlite_best_effort(db_path)

        if not data or "cookies" not in data:
            return 0

        records = []
        for row in data["cookies"]:
            records.append({
                "browser": "unknown",
                "profile": None,
                "name": row.get("name", ""),
                "value": row.get("value"),
                "domain": row.get("host_key", row.get("domain", "")),
                "path": row.get("path"),
                "expires_utc": None,
                "is_secure": row.get("is_secure", 0),
                "is_httponly": row.get("is_httponly", 0),
                "run_id": run_id,
                "source_path": "[carved/unallocated]",
                "discovered_by": discovered_by,
            })

        if records:
            insert_cookies(evidence_conn, evidence_id, records)
            callbacks.on_log(f"Ingested {len(records)} cookie records", "info")

        return len(records)

    def _ingest_places_db(
        self,
        db_path: Path,
        evidence_id: int,
        run_id: str,
        discovered_by: str,
        evidence_conn,
        callbacks: ExtractorCallbacks
    ) -> int:
        """Ingest Firefox places.sqlite database."""
        data = parse_sqlite_best_effort(db_path)

        if not data:
            return 0

        records = []

        if "moz_places" in data:
            for row in data["moz_places"]:
                records.append({
                    "browser": "firefox",
                    "url": row.get("url", ""),
                    "title": row.get("title"),
                    "visit_time_utc": None,
                    "visit_count": row.get("visit_count"),
                    "source_path": "[carved/unallocated]",
                    "discovered_by": discovered_by,
                    "run_id": run_id,
                    "notes": "Recovered from unallocated space",
                })

        if records:
            insert_browser_history_rows(evidence_conn, evidence_id, records)
            callbacks.on_log(f"Ingested {len(records)} Firefox history records", "info")

        return len(records)

    def _extract_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc.lower() if parsed.netloc else None
        except Exception:
            return None
