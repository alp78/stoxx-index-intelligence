"""Structured logging for the STOXX ingestion pipeline.

Console output is human-readable. Datadog gets JSON in logs/pipeline.jsonl.

Usage:
    from logger import get_logger, log_info, log_warning, log_error
    logger = get_logger(__name__)
    log_info(logger, "Load complete", step="load", index="euro_stoxx", records_inserted=50)

Console:
    INFO  | Load complete | step=load index=euro_stoxx records_inserted=50

Datadog (logs/pipeline.jsonl):
    {"timestamp":"...","level":"INFO","logger":"loaders.load_index_dim",
     "message":"Load complete","service":"esg-ingestion",
     "step":"load","index":"euro_stoxx","records_inserted":50}
"""

import logging
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_LOG_DIR = _PROJECT_ROOT / "logs"
_LOG_FILE = _LOG_DIR / "pipeline.jsonl"

_LEVEL_TAG = {
    "DEBUG": "DEBUG",
    "INFO": "INFO ",
    "WARNING": "WARN ",
    "ERROR": "ERROR",
    "CRITICAL": "CRIT ",
}


class ConsoleFormatter(logging.Formatter):
    """Human-readable single-line format for terminal output."""

    def format(self, record):
        tag = _LEVEL_TAG.get(record.levelname, record.levelname)
        parts = [f"{tag} | {record.getMessage()}"]
        if hasattr(record, "_fields") and record._fields:
            attrs = " ".join(f"{k}={v}" for k, v in record._fields.items())
            parts.append(attrs)
        line = " | ".join(parts)
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            line += "\n" + record.exc_text
        return line


class DatadogJsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON for Datadog log management."""

    def format(self, record):
        log = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": "esg-ingestion",
        }
        # Inject trace correlation IDs for Datadog log-to-trace linking
        try:
            from ddtrace import tracer
            span = tracer.current_span()
            if span:
                log["dd.trace_id"] = str(span.trace_id)
                log["dd.span_id"] = str(span.span_id)
        except ImportError:
            pass
        if hasattr(record, "_fields"):
            log.update(record._fields)
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            log["error.stack"] = record.exc_text
        return json.dumps(log, default=str)


def get_logger(name, level=logging.INFO):
    """Returns a logger with readable console output + JSON file for Datadog.

    Set LOG_FORMAT=json to get JSON on stdout instead (e.g. in containers).
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        # JSON file for Datadog
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(_LOG_FILE, encoding="utf-8")
        file_handler.setFormatter(DatadogJsonFormatter())
        logger.addHandler(file_handler)

        # Console for the developer
        stdout_handler = logging.StreamHandler(sys.stdout)
        if os.environ.get("LOG_FORMAT", "").strip().lower() == "json":
            stdout_handler.setFormatter(DatadogJsonFormatter())
        else:
            stdout_handler.setFormatter(ConsoleFormatter())
        logger.addHandler(stdout_handler)

        logger.setLevel(level)
        logger.propagate = False
    return logger


def log_info(logger, msg, **kwargs):
    """Log INFO with structured Datadog attributes."""
    logger.info(msg, extra={"_fields": kwargs})


def log_warning(logger, msg, **kwargs):
    """Log WARNING with structured Datadog attributes."""
    logger.warning(msg, extra={"_fields": kwargs})


def log_error(logger, msg, exc_info=False, **kwargs):
    """Log ERROR with structured Datadog attributes."""
    logger.error(msg, exc_info=exc_info, extra={"_fields": kwargs})


class StepTimer:
    """Context manager to measure step duration in milliseconds."""

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.duration_ms = int((time.perf_counter() - self.start) * 1000)
