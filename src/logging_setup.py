"""Logging terpusat: konsol + file logs/pipeline.log."""
import logging, sys
from .config import LOG_DIR


def get_logger(name: str = "pipeline") -> logging.Logger:
    LOG_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
    for h in (logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8")):
        h.setFormatter(fmt)
        logger.addHandler(h)
    return logger
