from __future__ import annotations

import logging
from typing import Any, Dict

import structlog


def configure_logging(level: str = "INFO") -> None:
    """Structured JSON logs. Production systems rely on this for tracing & alerts."""
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper(), logging.INFO)),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def log_event(logger: structlog.BoundLogger, event: str, **fields: Dict[str, Any]) -> None:
    logger.info(event, **fields)
