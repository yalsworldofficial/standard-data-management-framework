# logging.py
import logging
import sys
from sdmf.config.LoggingPrettyFormatter import LoggingPrettyFormatter


class LoggingConfig:
    """Configures the application's global logging settings.

    Databricks-safe:
    - Does NOT clear existing handlers
    - Does NOT break Databricks logging pipeline
    - Idempotent (safe to call multiple times)
    """

    def __init__(self, level: int = logging.INFO):
        self.level = level

    def configure(self):
        root = logging.getLogger()
        root.setLevel(self.level)

        # Check if our handler is already installed
        for handler in root.handlers:
            if isinstance(handler, logging.StreamHandler):
                formatter = handler.formatter
                if isinstance(formatter, LoggingPrettyFormatter):
                    return  # already configured, do nothing

        # Add our handler without removing Databricks handlers
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(LoggingPrettyFormatter())
        root.addHandler(handler)
