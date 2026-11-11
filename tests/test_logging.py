"""Tests for logging setup."""

from __future__ import annotations

from dataclasses import replace
import logging

from fastapi.testclient import TestClient

from goes_timelapse.app import create_app
from goes_timelapse.core.config import Settings
from goes_timelapse.core.logging_utils import (
    RESET,
    TRACE_LEVEL,
    SeverityColorFormatter,
    configure_logging,
    register_trace_level,
    resolve_log_level,
)


def test_trace_level_resolves_from_env(monkeypatch):
    """Test trace level resolves from env."""
    monkeypatch.setenv("GOES_LOG_LEVEL", "trace")

    settings = Settings.from_env()

    assert settings.log_level == TRACE_LEVEL


def test_logger_trace_emits_records(caplog):
    """Test logger trace emits records."""
    register_trace_level()
    logger = logging.getLogger("goes.trace.test")

    with caplog.at_level(TRACE_LEVEL, logger="goes.trace.test"):
        logger.trace("trace payload")  # type: ignore[attr-defined]

    assert any(record.levelno == TRACE_LEVEL for record in caplog.records)
    assert "trace payload" in caplog.text


def test_color_formatter_colors_prefix_only():
    """Test color formatter colors prefix only."""
    formatter = SeverityColorFormatter(datefmt="%Y-%m-%d %H:%M:%S")
    record = logging.LogRecord(
        name="goes.trace.test",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg="payload-sentinel",
        args=(),
        exc_info=None,
    )

    rendered = formatter.format(record)

    assert "\x1b[" in rendered
    assert RESET in rendered
    assert rendered.split(RESET)[-1] == " payload-sentinel"


def test_configure_logging_sets_uvicorn_loggers_to_selected_level():
    """Test configure logging sets uvicorn loggers to selected level."""
    configure_logging(TRACE_LEVEL)

    root_logger = logging.getLogger()
    uvicorn_logger = logging.getLogger("uvicorn")
    access_logger = logging.getLogger("uvicorn.access")
    error_logger = logging.getLogger("uvicorn.error")
    goes_handlers = [
        handler
        for handler in root_logger.handlers
        if getattr(handler, "_goes_timelapse_handler", False)
    ]

    assert root_logger.level == TRACE_LEVEL
    assert len(goes_handlers) == 1
    assert isinstance(goes_handlers[0].formatter, SeverityColorFormatter)
    assert uvicorn_logger.level == TRACE_LEVEL
    assert error_logger.level == TRACE_LEVEL
    assert uvicorn_logger.propagate is True
    assert error_logger.propagate is True
    assert access_logger.propagate is False
    assert access_logger.disabled is True


def test_create_app_starts_with_trace_logging(settings):
    """Test create app starts with trace logging."""
    trace_settings = replace(settings, log_level=resolve_log_level("trace"))

    app = create_app(settings=trace_settings, start_background_tasks=False)

    with TestClient(app) as client:
        response = client.get("/api/status")

    assert response.status_code == 200
