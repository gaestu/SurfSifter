from __future__ import annotations

import json
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Dict, List


# Expanded content types for downloads
DEFAULT_CONTENT_TYPES = [
    "image/*",
    "video/*",
    "audio/*",
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.*",
    "application/vnd.ms-excel",
    "application/vnd.ms-powerpoint",
    "application/zip",
    "application/x-rar-compressed",
    "application/x-7z-compressed",
    "application/gzip",
    "application/x-tar",
    "text/plain",
    "text/html",
]


@dataclass
class NetworkSettings:
    concurrency: int = 2
    timeout_s: int = 10
    retries: int = 1
    max_bytes: int = 200 * 1024 * 1024
    allowed_content_types: List[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.allowed_content_types is None:
            self.allowed_content_types = list(DEFAULT_CONTENT_TYPES)
        self.concurrency = max(1, min(self.concurrency, 4))
        self.timeout_s = max(5, min(self.timeout_s, 60))
        self.retries = max(0, min(self.retries, 5))
        self.max_bytes = max(1 * 1024 * 1024, self.max_bytes)
        self.allowed_content_types = [ctype for ctype in self.allowed_content_types if ctype]
        if not self.allowed_content_types:
            self.allowed_content_types = list(DEFAULT_CONTENT_TYPES)


@dataclass
class ToolPaths:
    bulk_extractor: str = ""
    foremost: str = ""
    exiftool: str = ""
    ewfmount: str = ""


@dataclass
class GeneralSettings:
    thumbnail_size: int = 180


@dataclass
class HashSettings:
    db_path: str = ""


@dataclass
class SandboxSettings:
    """Settings for sandbox browser behavior."""

    # Prefer external browser with Firejail (Linux only)
    prefer_external: bool = False

    # JavaScript enabled in embedded viewer (security vs functionality tradeoff)
    javascript_enabled: bool = False

    # External browser command (auto-detected if empty)
    external_browser: str = ""

    # Audit logging for sandbox opens
    log_opens: bool = True


@dataclass
class ReportSettings:
    """Default branding settings for reports.

    These are used as defaults when opening a new case that has no
    per-evidence settings saved yet.
    """

    # Default author info
    default_author_function: str = ""
    default_author_name: str = ""

    # Default branding
    default_org_name: str = ""
    default_department: str = ""
    default_footer_text: str = ""
    default_logo_path: str = ""  # Relative to config dir (e.g., "branding/logo.png")

    # Default preferences
    default_locale: str = "en"
    default_date_format: str = "eu"

    # Default title-page visibility
    default_show_title_case_number: bool = True
    default_show_title_evidence: bool = True
    default_show_title_investigator: bool = True
    default_show_title_date: bool = True

    # Default footer / appendix options
    default_show_footer_date: bool = True
    default_hide_appendix_page_numbers: bool = False


@dataclass
class AppSettings:
    general: GeneralSettings = field(default_factory=GeneralSettings)
    tools: ToolPaths = field(default_factory=ToolPaths)
    network: NetworkSettings = field(default_factory=NetworkSettings)
    hash: HashSettings = field(default_factory=HashSettings)
    sandbox: SandboxSettings = field(default_factory=SandboxSettings)
    reports: ReportSettings = field(default_factory=ReportSettings)

    @classmethod
    def load(cls, path: Path) -> "AppSettings":
        if not path.exists():
            return cls()
        data = json.loads(path.read_text(encoding="utf-8"))
        # Filter unknown keys to support loading old configs (e.g., with 'language' field)
        general_data = data.get("general", {})
        general_fields = {f.name for f in GeneralSettings.__dataclass_fields__.values()}
        general = GeneralSettings(**{k: v for k, v in general_data.items() if k in general_fields})
        tools = ToolPaths(**data.get("tools", {}))
        network = NetworkSettings(**data.get("network", {}))
        hash_cfg = HashSettings(**data.get("hash", {}))
        sandbox_data = data.get("sandbox", {})
        sandbox_fields = {f.name for f in SandboxSettings.__dataclass_fields__.values()}
        sandbox = SandboxSettings(**{k: v for k, v in sandbox_data.items() if k in sandbox_fields})
        reports_data = data.get("reports", {})
        reports_fields = {f.name for f in ReportSettings.__dataclass_fields__.values()}
        reports = ReportSettings(**{k: v for k, v in reports_data.items() if k in reports_fields})
        return cls(general=general, tools=tools, network=network, hash=hash_cfg, sandbox=sandbox, reports=reports)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload: Dict[str, object] = {
            "general": asdict(self.general),
            "tools": asdict(self.tools),
            "network": asdict(self.network),
            "hash": asdict(self.hash),
            "sandbox": asdict(self.sandbox),
            "reports": asdict(self.reports),
        }
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def settings_path(base_dir: Path) -> Path:
    import shutil
    import sys
    if getattr(sys, 'frozen', False):
        # Frozen binary: write settings to a persistent user config directory,
        # not the ephemeral _MEIPASS temp dir.
        config_dir = Path.home() / ".config" / "surfsifter"
    else:
        config_dir = base_dir / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    settings_file = config_dir / "settings.json"
    if not settings_file.exists():
        # Seed from shipped defaults template (tracked in version control)
        defaults_file = config_dir / "settings.defaults.json"
        if defaults_file.exists():
            shutil.copy2(defaults_file, settings_file)
    return settings_file
