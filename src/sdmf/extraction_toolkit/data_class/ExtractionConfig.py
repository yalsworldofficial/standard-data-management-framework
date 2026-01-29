# inbuilt
from dataclasses import dataclass

@dataclass
class ExtractionConfig:
    ingestion_config: dict
    target_table: str