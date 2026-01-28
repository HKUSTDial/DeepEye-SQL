import sys
import os
import tomllib
from pathlib import Path
from loguru import logger as _logger


# Get project root to avoid circular dependency with app.config
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _load_logger_config():
    """Load logger configuration from the config file."""
    # Respect CONFIG_PATH environment variable if set
    env_config_path = os.environ.get("CONFIG_PATH")
    if env_config_path:
        config_path = Path(env_config_path)
        # If relative, make it relative to project root
        if not config_path.is_absolute():
            config_path = PROJECT_ROOT / config_path
    else:
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
    """Adjust the log level to the specified level."""
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
