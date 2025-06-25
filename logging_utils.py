#!/usr/bin/env python3
"""Simple logging helpers for the portfolio analytics package."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured with console and rotating file handlers.

    Parameters
    ----------
    name: str
        Name for the logger and the associated log file ``<name>.log``.

    The logger writes to ``<name>.log`` with a 10 MB limit and keeps three
    backups. A stream handler is also attached for console output. Handlers are
    added only once per logger instance.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    fmt = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # console
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    # rotating file handler
    log_file = Path(f"{name}.log")
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=3
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger
