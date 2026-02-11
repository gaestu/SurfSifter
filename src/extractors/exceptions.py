"""
Exceptions for extractor modules.
"""


class ExtractorError(Exception):
    """Base exception for extractor errors."""
    pass


class ExtractionFailedError(ExtractorError):
    """Raised when extraction phase fails."""
    pass


class IngestionFailedError(ExtractorError):
    """Raised when ingestion phase fails."""
    pass


class ConfigurationError(ExtractorError):
    """Raised when extractor configuration is invalid."""
    pass


class MissingToolError(ExtractorError):
    """Raised when required external tool is not found."""

    def __init__(self, tool_name: str, install_hint: str = ""):
        self.tool_name = tool_name
        self.install_hint = install_hint
        message = f"Required tool '{tool_name}' not found"
        if install_hint:
            message += f"\n{install_hint}"
        super().__init__(message)
