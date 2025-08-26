from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import os


def setup_logging() -> logging.Logger:
    # Get the current directory or a suitable place in the user's environment
    log_dir = os.path.join(os.path.expanduser("~"), ".spinta_logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = f"spinta_{datetime.now().strftime('%Y-%m-%d')}.log"
    log_path = os.path.join(log_dir, log_file)

    logger: logging.Logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create a timed rotating file handler that rotates every day and keeps logs for 7 days
    file_handler = TimedRotatingFileHandler(log_path, when="midnight", interval=1, backupCount=7)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(file_formatter)

    # Create a console handler that only logs WARNING and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_formatter = logging.Formatter("%(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
