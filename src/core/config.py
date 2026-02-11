from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

import yaml


@dataclass(slots=True)
class LoggingConfig:
    """Logging configuration from config.yml."""

    level: str = "INFO"
    app_log_max_mb: int = 50
    app_log_backup_count: int = 10
    case_log_max_mb: int = 50
    case_log_backup_count: int = 5
    evidence_log_max_mb: int = 100
    evidence_log_backup_count: int = 5


@dataclass(slots=True)
class ExtractionConfig:
    """Extraction configuration from config.yml."""

    auto_generate_file_list: bool = True  # Auto-run fls on E01 evidence addition


@dataclass(slots=True)
class AppConfig:
    """Top-level configuration resolved from disk."""

    base_dir: Path
    tool_paths: Dict[str, Path]
    rules_dir: Path
    logs_dir: Path
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    extraction: ExtractionConfig = field(default_factory=ExtractionConfig)

    def to_json(self) -> str:
        """Serialize the configuration into a JSON string for manifest outputs."""
        data = {
            "tool_paths": {name: str(path) for name, path in self.tool_paths.items()},
            "rules_dir": str(self.rules_dir),
            "logs_dir": str(self.logs_dir),
        }
        return json.dumps(data, indent=2, sort_keys=True)


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        content = yaml.safe_load(handle) or {}
        if not isinstance(content, dict):
            raise ValueError(f"Config file {path} must contain a mapping at the top level.")
        return content


def load_app_config(base_dir: Path) -> AppConfig:
    """Load application configuration from disk, providing sensible defaults."""

    config_dir = base_dir / "config"
    config_yaml = config_dir / "config.yml"
    config_overrides = _load_yaml(config_yaml)

    tool_paths_cfg = config_overrides.get("tool_paths", {})
    tool_paths: Dict[str, Path] = {}
    for tool_name, path_str in tool_paths_cfg.items():
        tool_paths[tool_name] = Path(path_str)

    rules_dir = base_dir / "rules"

    # Logs must go to a persistent, writable location — not the ephemeral
    # _MEIPASS temp directory used by PyInstaller.
    import sys
    if getattr(sys, 'frozen', False):
        logs_dir = Path.home() / ".config" / "surfsifter" / "logs"
    else:
        logs_dir = base_dir / "logs"
    # Ensure critical directories exist; they are safe to create because they live under the workspace.
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Load logging configuration
    logging_cfg = config_overrides.get("logging", {})
    logging_config = LoggingConfig(
        level=logging_cfg.get("level", "INFO"),
        app_log_max_mb=logging_cfg.get("app_log_max_mb", 50),
        app_log_backup_count=logging_cfg.get("app_log_backup_count", 10),
        case_log_max_mb=logging_cfg.get("case_log_max_mb", 50),
        case_log_backup_count=logging_cfg.get("case_log_backup_count", 5),
        evidence_log_max_mb=logging_cfg.get("evidence_log_max_mb", 100),
        evidence_log_backup_count=logging_cfg.get("evidence_log_backup_count", 5),
    )

    # Load extraction configuration
    extraction_cfg = config_overrides.get("extraction", {})
    extraction_config = ExtractionConfig(
        auto_generate_file_list=extraction_cfg.get("auto_generate_file_list", True),
    )

    return AppConfig(
        base_dir=base_dir,
        tool_paths=tool_paths,
        rules_dir=rules_dir,
        logs_dir=logs_dir,
        logging=logging_config,
        extraction=extraction_config,
    )


@dataclass
class ParallelConfig:
    """Configuration for parallel extraction."""

    max_workers: int = field(default_factory=lambda: max(1, os.cpu_count() or 4))
    batch_size: int = 1000
    enable_parallel: bool = True

    def __post_init__(self) -> None:
        # Respect environment variable overrides
        if "VMGO_MAX_WORKERS" in os.environ:
            try:
                self.max_workers = int(os.environ["VMGO_MAX_WORKERS"])
            except ValueError:
                pass
        if "VMGO_BATCH_SIZE" in os.environ:
            try:
                self.batch_size = int(os.environ["VMGO_BATCH_SIZE"])
            except ValueError:
                pass
        if "VMGO_PARALLEL_IMAGES" in os.environ:
            val = os.environ["VMGO_PARALLEL_IMAGES"].lower()
            if val in ("0", "false", "no", "off"):
                self.enable_parallel = False
            elif val in ("1", "true", "yes", "on"):
                self.enable_parallel = True

    @classmethod
    def from_environment(cls) -> ParallelConfig:
        """Create config with environment variable overrides applied."""
        return cls()
