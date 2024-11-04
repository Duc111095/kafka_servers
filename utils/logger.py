import logging
import os.path
from logging import Logger
from logging.handlers import TimedRotatingFileHandler


def get_app_logger() -> Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = TimedRotatingFileHandler(os.path.dirname(os.path.dirname(__file__)) + "/log/app.log", when="midnight", interval=1)
    handler.suffix = "%Y-%m-%d"
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("{asctime} : {name} - {levelname} - {message}",
                                  style="{",
                                  datefmt="%Y-%m-%d %H:%M"
                                  )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
