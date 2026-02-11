"""
Image Carving Worker - Extraction Logic

Handles actual carving using foremost/scalpel tools.
Reuses logic from src/workers/carving_worker.py with extractor adaptations.
"""

from __future__ import annotations

import shutil
import tempfile
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Any, Generator, Tuple

from extractors.callbacks import ExtractorCallbacks
from core.logging import get_logger

LOGGER = get_logger("extractors._shared.carving.worker")


@dataclass(slots=True)
class CarvedFile:
    """Metadata for a carved file."""
    path: Path
    md5: Optional[str] = None
    sha256: Optional[str] = None


@dataclass(slots=True)
class CarvingRunResult:
    """Result metadata for a carving run."""

    carved_files: List[Path]
    stdout: str
    stderr: str
    returncode: int
    command: List[str]
    input_source: str
    input_type: str
    audit_path: Optional[Path] = None


@contextmanager
def _resolve_input_source(evidence_fs) -> Generator[Tuple[str, str], None, None]:
    """
    Resolve the input source for carving, mounting E01s if necessary.

    Yields:
        (input_path, input_type)
    """
    # Try mount point first (MountedFS)
    mount_root = getattr(evidence_fs, "mount_point", None)
    if isinstance(mount_root, (str, Path)) and Path(mount_root).exists():
        yield str(mount_root), "mount"
        return

    # Try E01 segments (PyEwfTskFS)
    ewf_paths = getattr(evidence_fs, "ewf_paths", None)
    if ewf_paths and isinstance(ewf_paths, (list, tuple)) and ewf_paths:
        # Check for ewfmount
        ewfmount_path = shutil.which("ewfmount")
        if ewfmount_path:
            temp_mount = Path(tempfile.mkdtemp(prefix="waba_ewf_"))
            try:
                LOGGER.info("Mounting E01 for carving: %s -> %s", ewf_paths[0], temp_mount)
                subprocess.run(
                    [ewfmount_path, str(ewf_paths[0]), str(temp_mount)],
                    check=True,
                    capture_output=True
                )

                # Find raw image (usually 'ewf1')
                raw_files = list(temp_mount.iterdir())
                if raw_files:
                    # Prefer 'ewf1' if present, else first file
                    ewf1 = temp_mount / "ewf1"
                    input_path = str(ewf1) if ewf1.exists() else str(raw_files[0])
                    LOGGER.info("Using mounted raw image: %s", input_path)
                    yield input_path, "ewf_mount"
                else:
                    raise RuntimeError("ewfmount created empty directory")
            except subprocess.CalledProcessError as exc:
                LOGGER.error("Failed to mount E01: %s", exc.stderr)
                # Fallback to direct E01
                yield str(ewf_paths[0]), "ewf"
            finally:
                # Unmount
                subprocess.run(["fusermount", "-u", str(temp_mount)], check=False)
                try:
                    temp_mount.rmdir()
                except OSError:
                    pass
        else:
            LOGGER.warning("ewfmount not found. Foremost may fail on E01 files.")
            yield str(ewf_paths[0]), "ewf"
        return

    # Error if no source found
    fs_type = type(evidence_fs).__name__
    available_attrs = [attr for attr in dir(evidence_fs) if not attr.startswith('_')]
    LOGGER.error(
        "No valid input source for carving. Evidence filesystem type: %s, available attributes: %s",
        fs_type, available_attrs
    )
    raise ValueError(
        f"No valid input source for carving (evidence type: {fs_type}). "
        "Expected 'mount_point' (MountedFS) or 'ewf_paths' (PyEwfTskFS)."
    )


def _generate_config_file(path: Path, file_types: Dict[str, Any], tool: str):
    """Generate carving configuration file from file_types dict."""
    lines = []
    # Default size if not specified (10MB)
    default_size = "10000000"

    for name, conf in file_types.items():
        if not conf.get("enabled", True):
            continue

        ext = conf.get("extension", name)
        case = "y"
        size = str(conf.get("max_size", default_size))
        header = conf.get("header", "")
        footer = conf.get("footer", "")

        # Format: extension  case_sensitive  size  header  footer
        lines.append(f"{ext}\t{case}\t{size}\t{header}\t{footer}")

    path.write_text("\n".join(lines), encoding="utf-8")


# Default timeout: 12 hours (carving large forensic images can take many hours)
DEFAULT_CARVING_TIMEOUT = 12 * 60 * 60  # 43200 seconds


def run_carving_extraction(
    evidence_fs,
    output_dir: Path,
    carving_tool: str,
    tool_path: Path,
    callbacks: ExtractorCallbacks,
    config_file: Optional[Path] = None,
    file_types: Optional[Dict[str, Any]] = None,
    timeout: Optional[int] = None,
) -> CarvingRunResult:
    """
    Run forensic carving tool on evidence image.

    Args:
        evidence_fs: Mounted evidence filesystem (PyEwfTskFS or MountedFS)
        output_dir: Directory to write carved files
        carving_tool: Tool name ("foremost" or "scalpel")
        tool_path: Path to carving tool executable
        callbacks: Progress callbacks
        config_file: Path to existing config file (optional)
        file_types: Dict of file type configs to generate config (optional)
        timeout: Timeout in seconds (default: 12 hours)

    Returns:
        CarvingRunResult with carved file paths and process metadata
    """
    if timeout is None:
        timeout = DEFAULT_CARVING_TIMEOUT
    callbacks.on_step(f"Configuring {carving_tool}")

    with _resolve_input_source(evidence_fs) as (input_source, input_type):
        # Create output subdirectory for carved files
        carved_dir = output_dir / "carved"

        # Scalpel requires an empty output directory for forensic soundness
        # Clean existing carved directory if it exists
        if carved_dir.exists():
            LOGGER.info("Cleaning existing carved directory: %s", carved_dir)
            shutil.rmtree(carved_dir)
        carved_dir.mkdir(parents=True, exist_ok=True)

        # Prepare config file
        final_config_path = output_dir / "carver.conf"

        if config_file:
            import shutil
            shutil.copy2(config_file, final_config_path)
            LOGGER.info("Copied carving config from %s to %s", config_file, final_config_path)
        elif file_types:
            _generate_config_file(final_config_path, file_types, carving_tool)
            LOGGER.info("Generated carving config at %s", final_config_path)
        else:
            # Fallback to default if nothing provided (though caller should provide one)
            LOGGER.warning("No config provided, using tool defaults")
            final_config_path = None

        # Build command
        cmd = [str(tool_path)]

        # Add verbose flag for foremost
        if carving_tool == "foremost":
            cmd.append("-v")
            cmd.append("-q")  # Enable quick mode (sector alignment) to reduce false positives

        cmd.extend(["-o", str(carved_dir)])

        if final_config_path:
            cmd += ["-c", str(final_config_path)]
        cmd.append(input_source)

        timeout_hours = timeout / 3600
        callbacks.on_step(f"Running {carving_tool} (this may take several hours, timeout: {timeout_hours:.1f}h)")
        LOGGER.info("Running carving command: %s (timeout: %ds)", " ".join(cmd), timeout)

        # Execute carving tool
        try:
            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",  # Handle non-utf8 output from tools
                check=False,
                timeout=timeout,
            )

            if completed.returncode != 0:
                LOGGER.warning("Carving tool exited with code %d: %s", completed.returncode, completed.stderr)
                # Don't fail - some tools return non-zero even on partial success

            LOGGER.info("Carving complete (exit code %d)", completed.returncode)

        except subprocess.TimeoutExpired:
            callbacks.on_error(f"Carving operation timed out after {timeout_hours:.1f} hours")
            raise RuntimeError(f"Carving operation timed out after {timeout_hours:.1f} hours")
        except OSError as exc:
            callbacks.on_error(f"Failed to execute {carving_tool}: {exc}")
            raise

        # Collect carved files
        callbacks.on_step("Collecting carved files")
        carved_files = _collect_carved_files(carved_dir)

        LOGGER.info("Collected %d carved files", len(carved_files))
        audit_path = carved_dir / "audit.txt"
        return CarvingRunResult(
            carved_files=carved_files,
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
            returncode=completed.returncode,
            command=cmd,
            input_source=input_source,
            input_type=input_type,
            audit_path=audit_path if audit_path.exists() else None,
        )


def _collect_carved_files(carved_dir: Path) -> List[Path]:
    """
    Recursively collect all carved image files from output directory.

    Args:
        carved_dir: Directory where carver wrote output

    Returns:
        List of paths to carved files
    """
    files: List[Path] = []

    # Carving tools create subdirectories per file type (e.g., jpg/, png/)
    for path in carved_dir.rglob("*"):
        if path.is_file() and path.suffix.lower() in {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif"}:
            files.append(path)

    return sorted(files)  # Deterministic ordering


def parse_foremost_audit(audit_path: Path) -> List[Dict[str, Any]]:
    """
    Parse foremost audit.txt entries (format).

    Format:
        <num>: <name(bs=512)> <size> <offset> <comment?>
    """
    import re

    if not audit_path.exists():
        return []

    entries: List[Dict[str, Any]] = []
    line_re = re.compile(r"^\s*(\d+):\s+(\S+)\s+(\d+)\s+(\d+)")

    for line in audit_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        match = line_re.match(line)
        if not match:
            continue
        _, name, size, offset = match.groups()
        entries.append(
            {
                "name": name,
                "size": int(size),
                "offset": int(offset),
                "comment": None,
            }
        )

    return entries
