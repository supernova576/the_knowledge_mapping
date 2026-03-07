import json
import logging
from pathlib import Path


def _resolve_log_path() -> Path:
    config_path = Path(__file__).resolve().parent.parent / "conf.json"
    default_path = Path(__file__).resolve().parent.parent / "output" / "app.log"

    try:
        config_data = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return default_path

    relative_path = config_data.get("log", {}).get("log_file_path", "output/app.log")
    return Path(__file__).resolve().parent.parent / relative_path


def get_logger(name: str) -> logging.Logger:
    log_path = _resolve_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False

    return logger
