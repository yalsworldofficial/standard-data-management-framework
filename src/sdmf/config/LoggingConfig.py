# inbuilt
import os
import sys
import logging
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

class LoggingConfig:
    """Configures global logging with console + rotating file handler and auto cleanup."""

    def __init__(self, retention_days: int, log_dir: str, run_id: str, level=logging.INFO):
        self.level = level
        self.log_dir = log_dir
        self.run_id = run_id
        self.retention_days = retention_days
        os.makedirs(self.log_dir, exist_ok=True)

    def configure(self):
        root = logging.getLogger()
        root.setLevel(self.level)
        if any(isinstance(h, logging.StreamHandler) for h in root.handlers):
            return
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] - %(message)s"))
        root.addHandler(console_handler)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(self.log_dir, f"sdmf_log_{self.run_id}_{timestamp}.log")
        file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=0, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("[%(asctime)s] [%(name)s] [%(levelname)s] - %(message)s"))
        root.addHandler(file_handler)
        self.cleanup_old_logs()

    def cleanup_old_logs(self):
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        for filename in os.listdir(self.log_dir):
            file_path = os.path.join(self.log_dir, filename)
            if os.path.isfile(file_path):
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                if file_time < cutoff_date:
                    os.remove(file_path)
                    print(f"Deleted old log file: {file_path}")
