import logging
import sys
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler


def get_logger(name: str, log_dir: Path = None) -> logging.Logger:

    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers on re-import
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── Console handler ───────────────────────────────────────
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    logger.addHandler(console)

    # ── File handler ──────────────────────────────────────────
    if log_dir:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        today = datetime.now().strftime("%Y%m%d")
        log_file = log_dir / f"{name}_{today}.log"

        file_handler = RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=3
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

    return logger
