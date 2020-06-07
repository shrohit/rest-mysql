
import os
import sys
import logging
from ..config import Config


config = Config()

# Taking log format from LOG_FORMAT environment variable
LOG_FORMAT = os.environ.get("LOG_FORMAT",
                            # Default log format
                            "%(asctime)s | %(levelname)s | %(message)s")


class InfoFilter(logging.Filter):
    """Filters only informational messages"""

    def filter(self, record):
        return record.levelno in (logging.DEBUG, logging.INFO)


# -------------- Creating Logger -------------------------

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

logger = root_logger.getChild(config.PROJECT_NAME)    # Child logger for app
formatter = logging.Formatter(LOG_FORMAT)

# STDOUT handler
# Includes: logger.info, logger.debug
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(formatter)
stdout_handler.addFilter(InfoFilter())

# STDERR handler
# Includes: logger.warning, logger.error, logger.critical
stderr_handler = logging.StreamHandler()
stderr_handler.setLevel(logging.WARNING)
stderr_handler.setFormatter(formatter)

# Adding handlers
if not logger.handlers:
    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)
