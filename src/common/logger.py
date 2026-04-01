"""
logger.py — shared logger factory.

Writes to both stdout and a file under LOG_DIR (from paths.py).
All modules should call get_logger(__name__) at the top.
"""

import logging
import sys

from src.common.paths import LOG_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.propagate = False

    # Avoid adding duplicate handlers if get_logger is called multiple times
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console — reconfigure stdout to UTF-8 so box-drawing chars survive
    # Windows defaults to cp1252 which cannot encode them
    sh = logging.StreamHandler(
        stream=open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1, closefd=False)
    )
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    # File — anchored to LOG_DIR, not the calling directory
    fh = logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger
