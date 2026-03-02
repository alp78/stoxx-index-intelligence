"""Datadog-compatible structured JSON logging for the ESG ingestion pipeline.

Usage:
    from logger import get_logger, log_info, log_warning, log_error
    logger = get_logger(__name__)
    log_info(logger, "Load complete", step="load", index="euro_stoxx", records_inserted=50)

Output (one JSON object per line, Datadog auto-parses):
    {"timestamp":"...","level":"INFO","logger":"loaders.load_index_dim",
     "message":"Load complete","service":"esg-ingestion",
     "step":"load","index":"euro_stoxx","records_inserted":50}
"""

import logging
import json
import sys
import time
from datetime import datetime, timezone


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
        # Merge structured fields passed via extra
        if hasattr(record, "_fields"):
            log.update(record._fields)
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            log["error.stack"] = record.exc_text
        return json.dumps(log, default=str)


def get_logger(name, level=logging.INFO):
    """Returns a logger configured with Datadog JSON output."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(DatadogJsonFormatter())
        logger.addHandler(handler)
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
