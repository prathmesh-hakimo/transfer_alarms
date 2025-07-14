import logging
import os
from datetime import datetime

def get_logger(name=__name__):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        # Console: no logger name
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


def add_file_handler(logger, raw_alarm_id):
    """
    Adds a file handler to the logger for the given raw_alarm_id.
    Logs will be stored in duplication_logs/<raw_alarm_id>_<timestamp>.log
    """
    log_dir = 'duplication_logs'
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = os.path.join(log_dir, f'{raw_alarm_id}_{timestamp}.log')
    file_handler = logging.FileHandler(file_path)
    # File: include logger name
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    return file_path 