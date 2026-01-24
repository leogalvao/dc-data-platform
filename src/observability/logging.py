"""
Structured logging configuration for the scraper framework.

Provides:
- JSON-formatted logs for machine parsing
- Console output for development
- Run ID correlation across all log messages
- Per-source log files
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

# Try to import rich for pretty console output
try:
    from rich.logging import RichHandler
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class JSONFormatter(logging.Formatter):
    """Formatter that outputs JSON for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add extra fields
        for key in ["run_id", "source_name", "record_count", "duration_ms"]:
            if hasattr(record, key):
                log_data[key] = getattr(record, key)

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


class StructuredLogger(logging.LoggerAdapter):
    """
    Logger adapter that adds structured context to all log messages.

    Usage:
        logger = StructuredLogger(logging.getLogger(__name__))
        logger.bind(run_id="abc123", source_name="contracts")
        logger.info("Processing records", record_count=100)
    """

    def __init__(self, logger: logging.Logger, extra: dict[str, Any] | None = None):
        super().__init__(logger, extra or {})

    def bind(self, **kwargs: Any) -> StructuredLogger:
        """Add context that will be included in all subsequent logs."""
        self.extra.update(kwargs)
        return self

    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """Add extra context to log record."""
        extra = kwargs.get("extra", {})
        extra.update(self.extra)
        kwargs["extra"] = extra
        return msg, kwargs


def setup_logging(
    level: str = "INFO",
    log_dir: Path | None = None,
    json_output: bool = False,
    rich_console: bool = True,
) -> None:
    """
    Configure logging for the scraper framework.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        log_dir: Directory for log files (None = no file logging)
        json_output: Use JSON format for file logs
        rich_console: Use rich for pretty console output
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers
    root_logger.handlers.clear()

    # Console handler
    if rich_console and RICH_AVAILABLE:
        console_handler = RichHandler(
            rich_tracebacks=True,
            show_time=True,
            show_path=False,
        )
        console_handler.setFormatter(logging.Formatter("%(message)s"))
    else:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    console_handler.setLevel(getattr(logging, level.upper()))
    root_logger.addHandler(console_handler)

    # File handler
    if log_dir:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"scraper_{timestamp}.log"

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # Capture everything

        if json_output:
            file_handler.setFormatter(JSONFormatter())
        else:
            file_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
                )
            )

        root_logger.addHandler(file_handler)

    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def get_logger(name: str, run_id: UUID | None = None, source_name: str | None = None) -> StructuredLogger:
    """
    Get a structured logger with optional context.

    Args:
        name: Logger name (typically __name__)
        run_id: Optional run ID for correlation
        source_name: Optional source name

    Returns:
        StructuredLogger instance
    """
    logger = StructuredLogger(logging.getLogger(name))

    if run_id:
        logger.bind(run_id=str(run_id))
    if source_name:
        logger.bind(source_name=source_name)

    return logger
