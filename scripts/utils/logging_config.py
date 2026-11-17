import logging
from pathlib import Path


def configure_ingestion_logger(name: str, log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)
        logger.propagate = False

    return logger
