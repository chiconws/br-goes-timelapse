from __future__ import annotations

import logging
import sys
from collections.abc import Mapping
from typing import Any, Final, cast


TRACE_LEVEL: Final[int] = 5
TRACE_LEVEL_NAME: Final[str] = "TRACE"
RESET: Final[str] = "\x1b[0m"
LEVEL_COLORS: Final[dict[int, str]] = {
    TRACE_LEVEL: "\x1b[2;37m",
    logging.DEBUG: "\x1b[36m",
    logging.INFO: "\x1b[32m",
    logging.WARNING: "\x1b[33m",
    logging.ERROR: "\x1b[31m",
    logging.CRITICAL: "\x1b[1;91m",
}
LEVEL_NAMES: Final[dict[str, int]] = {
    TRACE_LEVEL_NAME: TRACE_LEVEL,
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


class SeverityColorFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        timestamp = self.formatTime(record, self.datefmt)
        color = LEVEL_COLORS.get(record.levelno, "")
        prefix = f"{timestamp} {record.levelname:<8} [{record.name}]"
        rendered = f"{color}{prefix}{RESET} {message}" if color else f"{prefix} {message}"

        if record.exc_info:
            rendered += "\n" + self.formatException(record.exc_info)
        if record.stack_info:
            rendered += "\n" + self.formatStack(record.stack_info)
        return rendered


class TraceLogger(logging.Logger):
    def trace(self, message: str, *args: object, **kwargs: object) -> None:
        ...


def _logger_trace(self: logging.Logger, message: str, *args: object, **kwargs: object) -> None:
    if self.isEnabledFor(TRACE_LEVEL):
        self._log(
            TRACE_LEVEL,
            message,
            args,
            exc_info=cast(Any, kwargs.get("exc_info")),
            extra=cast(Mapping[str, object] | None, kwargs.get("extra")),
            stack_info=cast(bool, kwargs.get("stack_info", False)),
            stacklevel=cast(int, kwargs.get("stacklevel", 1)),
        )


def _module_trace(message: str, *args: object, **kwargs: object) -> None:
    logging.log(
        TRACE_LEVEL,
        message,
        *args,
        exc_info=cast(Any, kwargs.get("exc_info")),
        extra=cast(Mapping[str, object] | None, kwargs.get("extra")),
        stack_info=cast(bool, kwargs.get("stack_info", False)),
        stacklevel=cast(int, kwargs.get("stacklevel", 1)),
    )


def register_trace_level() -> None:
    if getattr(logging, TRACE_LEVEL_NAME, None) != TRACE_LEVEL:
        setattr(logging, TRACE_LEVEL_NAME, TRACE_LEVEL)
    logging.addLevelName(TRACE_LEVEL, TRACE_LEVEL_NAME)

    if not hasattr(logging.Logger, "trace"):
        logging.Logger.trace = _logger_trace  # type: ignore[attr-defined]

    if not hasattr(logging, "trace"):
        logging.trace = _module_trace  # type: ignore[attr-defined]


def get_logger(name: str) -> TraceLogger:
    register_trace_level()
    return cast(TraceLogger, logging.getLogger(name))


def resolve_log_level(value: str | int | None, default: int = logging.INFO) -> int:
    register_trace_level()
    if isinstance(value, int):
        return value
    if value is None:
        return default
    normalized = value.strip().upper()
    if not normalized:
        return default
    if normalized.isdigit():
        return int(normalized)
    return LEVEL_NAMES.get(normalized, default)


def configure_logging(level: int) -> None:
    register_trace_level()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(SeverityColorFormatter(datefmt="%Y-%m-%d %H:%M:%S"))
    setattr(handler, "_goes_timelapse_handler", True)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers = [
        existing
        for existing in root_logger.handlers
        if not getattr(existing, "_goes_timelapse_handler", False)
    ]
    root_logger.addHandler(handler)

    for logger_name in ("uvicorn", "uvicorn.error"):
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(level)

    access_logger = logging.getLogger("uvicorn.access")
    access_logger.handlers.clear()
    access_logger.propagate = False
    access_logger.disabled = True
