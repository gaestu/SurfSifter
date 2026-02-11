"""
Tool Registry and Discovery System

Centralized registry for all forensic tools used by extractors.
Auto-discovers tools on PATH and allows manual path overrides.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict
from pathlib import Path
import subprocess
import shutil
import json
import logging

LOGGER = logging.getLogger(__name__)


@dataclass
class ToolInfo:
    """Information about a discovered forensic tool."""
    name: str
    path: Optional[Path]
    version: Optional[str]
    status: str  # "found", "missing", "error"
    capabilities: List[str]
    error_message: Optional[str] = None


class ToolRegistry:
    """
    Central registry for all forensic tools used by extractors.
    Auto-discovers tools on PATH and allows manual path overrides.
    """

    KNOWN_TOOLS = {
        "bulk_extractor": {
            "executable": "bulk_extractor",
            "min_version": "1.6.0",
            "version_command": ["-V"],
            "version_parser": lambda out: out.split()[1] if len(out.split()) > 1 else None,
            "capabilities": [
                "url_extraction",
                "email_discovery",
                "ip_extraction",
                "bitcoin_addresses",
                "ethereum_addresses",
                "domain_extraction",
                "phone_numbers",
                "credit_card_numbers"
            ],
            "required_by": ["bulk_extractor extractor"],
        },
        "foremost": {
            "executable": "foremost",
            "min_version": "1.5.0",
            "version_command": ["-V"],
            "version_parser": lambda out: out.split()[1] if "version" in out.lower() and len(out.split()) > 1 else out.split()[0],
            "capabilities": [
                "file_carving",
                "jpg_recovery",
                "png_recovery",
                "gif_recovery",
                "bmp_recovery",
            ],
            "required_by": ["foremost_carver extractor"],
        },
        "scalpel": {
            "executable": "scalpel",
            "min_version": "1.0.0",
            "version_command": ["-V"],
            "version_parser": lambda out: out.split()[1] if len(out.split()) > 1 else None,
            "capabilities": [
                "file_carving",
                "jpg_recovery",
                "png_recovery",
                "gif_recovery",
                "advanced_carving",
            ],
            "required_by": ["foremost_carver extractor (alternative to foremost)"],
        },
        "exiftool": {
            "executable": "exiftool",
            "min_version": "12.0.0",
            "version_command": ["-ver"],
            "version_parser": lambda out: out.strip(),
            "capabilities": [
                "exif_extraction",
                "metadata_reading",
                "image_analysis",
            ],
            "required_by": ["image postprocessing"],
        },
        "ewfmount": {
            "executable": "ewfmount",
            "min_version": "20140608",
            "version_command": ["-V"],
            "version_parser": lambda out: out.split()[1] if len(out.split()) > 1 else out.strip(),
            "capabilities": [
                "ewf_mount",
                "mounted_fs",
            ],
            "required_by": ["image carving worker (E01 mount fallback)"],
        },
        "pytsk3": {
            "import_check": "pytsk3",
            "min_version": "20140506",
            "version_check": lambda: __import__("pytsk3").get_version(),
            "capabilities": [
                "filesystem_access",
                "sleuth_kit_bindings",
                "partition_reading",
            ],
            "required_by": ["evidence filesystem access"],
        },
        "pyewf": {
            "import_check": "pyewf",
            "min_version": "20140608",
            "version_check": lambda: __import__("pyewf").get_version(),
            "capabilities": [
                "ewf_reading",
                "e01_support",
            ],
            "required_by": ["EWF image reading"],
        },
        "firejail": {
            "executable": "firejail",
            "min_version": "0.9.60",
            "version_command": ["--version"],
            "version_parser": lambda out: out.split()[2] if "firejail" in out.lower() and len(out.split()) > 2 else out.strip(),
            "capabilities": [
                "sandbox_browser",
                "process_isolation",
                "namespace_isolation",
            ],
            "required_by": ["sandbox browser feature"],
        },
    }

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize tool registry.

        Args:
            config_path: Path to JSON config file for custom tool paths.
                        Defaults to ~/.config/surfsifter/tool_paths.json
        """
        self._legacy_config_path: Optional[Path] = None
        if config_path is None:
            config_dir = Path.home() / ".config" / "surfsifter"
            config_dir.mkdir(parents=True, exist_ok=True)
            config_path = config_dir / "tool_paths.json"
            self._legacy_config_path = Path.home() / ".config" / "web-and-browser-analyzer" / "tool_paths.json"

        self.config_path = config_path
        self._tools: Dict[str, ToolInfo] = {}
        self._custom_paths: Dict[str, Path] = {}

        # Load custom paths from config
        self._load_custom_paths()

    def discover_all_tools(self) -> Dict[str, ToolInfo]:
        """
        Discover all known tools.

        Returns:
            Dictionary mapping tool name to ToolInfo
        """
        LOGGER.info("Discovering all forensic tools...")

        for tool_name in self.KNOWN_TOOLS.keys():
            self._tools[tool_name] = self.discover_tool(tool_name)

        # Log summary
        found = sum(1 for t in self._tools.values() if t.status == "found")
        missing = sum(1 for t in self._tools.values() if t.status == "missing")
        error = sum(1 for t in self._tools.values() if t.status == "error")

        LOGGER.info(f"Tool discovery complete: {found} found, {missing} missing, {error} errors")

        return self._tools

    def discover_tool(self, tool_name: str) -> ToolInfo:
        """
        Discover a single tool.

        Args:
            tool_name: Name of tool from KNOWN_TOOLS

        Returns:
            ToolInfo with discovery results
        """
        if tool_name not in self.KNOWN_TOOLS:
            return ToolInfo(
                name=tool_name,
                path=None,
                version=None,
                status="error",
                capabilities=[],
                error_message=f"Unknown tool: {tool_name}"
            )

        tool_spec = self.KNOWN_TOOLS[tool_name]

        # Check if this is a Python module
        if "import_check" in tool_spec:
            return self._check_python_module(tool_name, tool_spec)

        # Check for custom path first
        if tool_name in self._custom_paths:
            tool_path = self._custom_paths[tool_name]
            if tool_path.exists():
                return self._check_tool_version(tool_name, tool_path, tool_spec)
            else:
                LOGGER.warning(f"Custom path for {tool_name} does not exist: {tool_path}")

        # Try to find executable on PATH
        executable = tool_spec["executable"]
        tool_path = shutil.which(executable)

        if tool_path is None:
            return ToolInfo(
                name=tool_name,
                path=None,
                version=None,
                status="missing",
                capabilities=tool_spec.get("capabilities", []),
                error_message=f"Not found on PATH"
            )

        return self._check_tool_version(tool_name, Path(tool_path), tool_spec)

    def _check_tool_version(self, tool_name: str, tool_path: Path, tool_spec: dict) -> ToolInfo:
        """Check tool version and validate against minimum version."""
        try:
            # Run version command
            version_cmd = [str(tool_path)] + tool_spec.get("version_command", ["--version"])
            result = subprocess.run(
                version_cmd,
                capture_output=True,
                text=True,
                timeout=5,
                check=False
            )

            # Parse version from output
            output = result.stdout or result.stderr
            version_parser = tool_spec.get("version_parser")

            if version_parser:
                try:
                    version = version_parser(output)
                except Exception as e:
                    LOGGER.warning(f"Failed to parse version for {tool_name}: {e}")
                    version = "unknown"
            else:
                version = output.strip().split()[0] if output else "unknown"

            # Check minimum version
            min_version = tool_spec.get("min_version")
            if min_version and version != "unknown":
                if self._version_compare(version, min_version) < 0:
                    return ToolInfo(
                        name=tool_name,
                        path=tool_path,
                        version=version,
                        status="error",
                        capabilities=tool_spec.get("capabilities", []),
                        error_message=f"Version {version} is below minimum {min_version}"
                    )

            return ToolInfo(
                name=tool_name,
                path=tool_path,
                version=version,
                status="found",
                capabilities=tool_spec.get("capabilities", []),
            )

        except subprocess.TimeoutExpired:
            return ToolInfo(
                name=tool_name,
                path=tool_path,
                version=None,
                status="error",
                capabilities=tool_spec.get("capabilities", []),
                error_message="Version check timed out"
            )
        except Exception as e:
            return ToolInfo(
                name=tool_name,
                path=tool_path,
                version=None,
                status="error",
                capabilities=tool_spec.get("capabilities", []),
                error_message=f"Version check failed: {e}"
            )

    def _check_python_module(self, tool_name: str, tool_spec: dict) -> ToolInfo:
        """Check if Python module is importable and get version."""
        module_name = tool_spec["import_check"]

        try:
            # Try to import module
            module = __import__(module_name)

            # Get version
            version_check = tool_spec.get("version_check")
            if version_check:
                try:
                    version = version_check()
                except Exception as e:
                    LOGGER.warning(f"Failed to get version for {module_name}: {e}")
                    version = "unknown"
            else:
                version = getattr(module, "__version__", "unknown")

            return ToolInfo(
                name=tool_name,
                path=None,  # Python modules don't have paths
                version=str(version),
                status="found",
                capabilities=tool_spec.get("capabilities", []),
            )

        except ImportError:
            return ToolInfo(
                name=tool_name,
                path=None,
                version=None,
                status="missing",
                capabilities=tool_spec.get("capabilities", []),
                error_message=f"Python module '{module_name}' not installed"
            )
        except Exception as e:
            return ToolInfo(
                name=tool_name,
                path=None,
                version=None,
                status="error",
                capabilities=tool_spec.get("capabilities", []),
                error_message=f"Import check failed: {e}"
            )

    def _version_compare(self, v1: str, v2: str) -> int:
        """
        Compare semantic versions.

        Returns:
            -1 if v1 < v2
            0 if v1 == v2
            1 if v1 > v2
        """
        def normalize(v):
            parts = []
            for part in v.split('.'):
                try:
                    parts.append(int(part))
                except ValueError:
                    # Handle non-numeric parts (like "1.6.0-beta")
                    parts.append(0)
            return parts

        v1_parts = normalize(v1)
        v2_parts = normalize(v2)

        # Pad shorter version with zeros
        max_len = max(len(v1_parts), len(v2_parts))
        v1_parts.extend([0] * (max_len - len(v1_parts)))
        v2_parts.extend([0] * (max_len - len(v2_parts)))

        for p1, p2 in zip(v1_parts, v2_parts):
            if p1 < p2:
                return -1
            elif p1 > p2:
                return 1

        return 0

    def set_custom_path(self, tool_name: str, tool_path: Path):
        """Set custom path for a tool and save to config."""
        if tool_name not in self.KNOWN_TOOLS:
            raise ValueError(f"Unknown tool: {tool_name}")

        self._custom_paths[tool_name] = tool_path
        self._save_custom_paths()

        # Re-discover tool with new path
        self._tools[tool_name] = self.discover_tool(tool_name)

    def _load_custom_paths(self):
        """Load custom tool paths from config file."""
        source_path = self.config_path
        if not source_path.exists() and self._legacy_config_path and self._legacy_config_path.exists():
            source_path = self._legacy_config_path
            LOGGER.info("Loading custom tool paths from legacy location: %s", source_path)
        elif not source_path.exists():
            return

        try:
            with open(source_path, 'r') as f:
                data = json.load(f)
                self._custom_paths = {
                    name: Path(path) for name, path in data.items()
                }
            LOGGER.info(f"Loaded {len(self._custom_paths)} custom tool paths")
            if source_path != self.config_path:
                # Migrate legacy config to current location after successful load.
                self._save_custom_paths()
        except Exception as e:
            LOGGER.error(f"Failed to load custom tool paths: {e}")

    def _save_custom_paths(self):
        """Save custom tool paths to config file."""
        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_path, 'w') as f:
                data = {
                    name: str(path) for name, path in self._custom_paths.items()
                }
                json.dump(data, f, indent=2)
            LOGGER.info(f"Saved {len(self._custom_paths)} custom tool paths")
        except Exception as e:
            LOGGER.error(f"Failed to save custom tool paths: {e}")

    def get_tool_info(self, tool_name: str) -> Optional[ToolInfo]:
        """Get cached tool info. Call discover_tool() to refresh."""
        return self._tools.get(tool_name)

    def get_custom_paths(self) -> Dict[str, Path]:
        """Return a copy of persisted custom tool-path overrides."""
        return dict(self._custom_paths)

    def get_missing_tools(self) -> List[ToolInfo]:
        """Get list of missing or error tools."""
        return [
            tool for tool in self._tools.values()
            if tool.status in ("missing", "error")
        ]

    def test_tool(self, tool_name: str) -> tuple[bool, str]:
        """
        Test tool execution with simple command.

        Returns:
            (success: bool, message: str)
        """
        tool_info = self._tools.get(tool_name)
        if not tool_info or tool_info.status != "found":
            return False, f"Tool not found or unavailable"

        # Python modules can't be tested this way
        if "import_check" in self.KNOWN_TOOLS[tool_name]:
            return True, f"Python module {tool_name} is importable"

        # Run version command as test
        try:
            tool_spec = self.KNOWN_TOOLS[tool_name]
            version_cmd = [str(tool_info.path)] + tool_spec.get("version_command", ["--version"])
            result = subprocess.run(
                version_cmd,
                capture_output=True,
                text=True,
                timeout=5,
                check=True
            )
            return True, f"Successfully executed {tool_name} v{tool_info.version}"

        except Exception as e:
            return False, f"Test failed: {e}"
