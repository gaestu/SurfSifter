"""
Callback interface for extractor progress reporting.
"""

from typing import Protocol


class ExtractorCallbacks(Protocol):
    """
    Callback interface for extractor progress reporting.

    Modules call these methods to report progress, logs, and errors.
    Implementations can be synchronous (for testing) or signal-based (for Qt UI).
    """

    def on_progress(self, current: int, total: int, message: str = "") -> None:
        """
        Report progress.

        Args:
            current: Current item/step (0-based)
            total: Total items/steps
            message: Optional status message

        Example:
            callbacks.on_progress(50, 100, "Processing file 50/100")
        """
        ...

    def on_log(self, message: str, level: str = "info") -> None:
        """
        Log a message.

        Args:
            message: Log message
            level: "debug" | "info" | "warning" | "error"

        Example:
            callbacks.on_log("Starting extraction", "info")
            callbacks.on_log("No files found", "warning")
        """
        ...

    def on_error(self, error: str, details: str = "") -> None:
        """
        Report an error.

        Args:
            error: Short error message
            details: Detailed error information (traceback, etc.)

        Example:
            callbacks.on_error("Failed to parse file", traceback.format_exc())
        """
        ...

    def on_step(self, step_name: str) -> None:
        """
        Report entering a new processing step.

        Args:
            step_name: Name of the step

        Example:
            callbacks.on_step("Parsing url.txt")
            callbacks.on_step("Writing to database")
        """
        ...

    def is_cancelled(self) -> bool:
        """
        Check if user cancelled the operation.

        Returns:
            True if operation should stop

        Example:
            if callbacks.is_cancelled():
                return False
        """
        ...
