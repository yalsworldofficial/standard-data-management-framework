# logging.py
import logging
import sys
from sdmf.config.LoggingPrettyFormatter import LoggingPrettyFormatter



class LoggingConfig:
    """Configures the application's global logging settings."""

    def __init__(self, level: int = logging.INFO):
        self.level = level

    def configure(self):
        root = logging.getLogger()
        root.setLevel(self.level)

        if root.handlers:
            root.handlers.clear()
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(LoggingPrettyFormatter())
        root.addHandler(handler)
