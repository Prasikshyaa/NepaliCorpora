import logging
from datetime import datetime
from scripts.utils import paths

def get_logger(name: str, log_type: str):
    """
    log_type: 'ingestion', 'preprocessing', or 'deduplication'
    """
    log_dir_map = {
        "ingestion": paths.LOGS_INGESTION,
        "preprocessing": paths.LOGS_PREPROCESSING,
        "deduplication": paths.LOGS_DEDUP,
        "wikipedia": paths.LOGS_WIKIPEDIA,
        "automation": paths.LOGS_AUTOMATION,
    }

    log_dir = log_dir_map.get(log_type)
    if log_dir is None:
        raise ValueError("Invalid log_type")

    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"{name}_{datetime.now().strftime('%Y-%m-%d')}.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
