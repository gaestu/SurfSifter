from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from logging import Logger
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

LOG_FILE_NAME = "processing.log"


class UtcFormatter(logging.Formatter):
    """Formatter that renders timestamps in UTC using ISO-8601."""

    converter = time.gmtime

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat(timespec="seconds")


def configure_logging(
    log_dir: Path,
    level: int = logging.INFO,
    max_bytes: int = 50 * 1024 * 1024,  # 50 MB default
    backup_count: int = 10,
) -> Logger:
    """
    Configure the root logger with console and rotating file handlers.

    Args:
        log_dir: Directory for log files
        level: Logging level (default: INFO)
        max_bytes: Maximum size per log file before rotation (default: 50 MB)
        backup_count: Number of backup files to keep (default: 10)

    Returns:
        Configured root logger
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / LOG_FILE_NAME

    formatter = UtcFormatter(
        fmt="%(asctime)sZ %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    root_logger = logging.getLogger("surfsifter")
    root_logger.setLevel(level)
    root_logger.handlers.clear()

    # Use RotatingFileHandler for size-based log rotation
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    root_logger.debug("Logging configured. File: %s (max %d MB, %d backups)",
                      log_path, max_bytes // (1024 * 1024), backup_count)
    return root_logger


def get_logger(name: Optional[str] = None) -> Logger:
    """Return a child logger under the application namespace."""
    base = logging.getLogger("surfsifter")
    if name:
        return base.getChild(name)
    return base
