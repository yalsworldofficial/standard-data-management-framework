import uuid
from datetime import datetime, timezone
import socket

class MetadataMixin:
    """
    A mixin that provides standardized metadata for any Spark job
    or data processing task.
    """

    def __init__(self, job_name: str = "unknown_job", params: dict = None, env: str = "dev", *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.job_name = job_name
        self.params = params or {}
        self.env = env

        # auto metadata
        self.job_id = str(uuid.uuid4())
        self.start_time = datetime.now(timezone.utc).isoformat()
        self.host = socket.gethostname()

    def to_metadata_dict(self):
        """Return metadata as a dictionary (ready for logging, MLflow, DB)."""
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "env": self.env,
            "params": self.params,
            "start_time": self.start_time,
            "host": self.host
        }

    def log_metadata(self, logger=None):
        data = self.to_metadata_dict()

        if logger:
            logger.info(f"Metadata: {data}")
        else:
            print("=== METADATA ===")
            for k, v in data.items():
                print(f"{k}: {v}")
