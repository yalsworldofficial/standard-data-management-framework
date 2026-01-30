# inbuilt
from dataclasses import dataclass

@dataclass
class ExtractionConfig:
    master_specs: dict
    feed_specs: dict
    config: dict
    target_table: str