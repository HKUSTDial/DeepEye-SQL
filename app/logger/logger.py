import sys
import tomllib
from pathlib import Path
from loguru import logger as _logger

from app.config import PROJECT_ROOT


_print_level = "INFO"


def _load_logger_config():
    """Load logger configuration from config.toml."""
    config_path = PROJECT_ROOT / "config" / "config.toml"
    if config_path.exists():
        try:
            with open(config_path, "rb") as f:
                config = tomllib.load(f)
            logger_config = config.get("logger", {})
            return {"print_level": logger_config.get("print_level", "INFO")}
        except Exception:
            pass
    return {"print_level": "INFO"}


def define_log_level(print_level="INFO"):
    """Adjust the log level to above level"""
    global _print_level
    _print_level = print_level

    _logger.remove()
    _logger.add(sys.stderr, level=print_level)
    return _logger


# Load config and initialize logger
_logger_config = _load_logger_config()
logger = define_log_level(print_level=_logger_config["print_level"])


if __name__ == "__main__":
    logger.info("Starting application")
    logger.debug("Debug message")
    logger.warning("Warning message")
    logger.error("Error message")
    logger.critical("Critical message")

    try:
        raise ValueError("Test error")
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
