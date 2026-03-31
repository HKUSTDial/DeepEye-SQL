import sys
import threading

from loguru import logger as _logger


_logger_instance = None
_logger_lock = threading.Lock()


def define_log_level(print_level: str = "INFO"):
    """Adjust the log level to the specified level."""
    _logger.remove()
    _logger.add(sys.stderr, level=print_level)
    return _logger


def configure_logger(print_level: str = "INFO"):
    global _logger_instance
    with _logger_lock:
        _logger_instance = define_log_level(print_level=print_level)
        return _logger_instance


def get_logger():
    global _logger_instance
    if _logger_instance is None:
        with _logger_lock:
            if _logger_instance is None:
                _logger_instance = define_log_level(print_level="INFO")
    return _logger_instance


def reset_logger():
    global _logger_instance
    with _logger_lock:
        _logger_instance = None


class _LazyLoggerProxy:
    def __getattr__(self, name):
        return getattr(get_logger(), name)

    def __repr__(self):
        return repr(get_logger())


logger = _LazyLoggerProxy()


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
