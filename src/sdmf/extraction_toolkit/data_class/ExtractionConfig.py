# inbuilt
from dataclasses import dataclass

@dataclass
class ExtractionConfig:
    config: dict
    target_table: str