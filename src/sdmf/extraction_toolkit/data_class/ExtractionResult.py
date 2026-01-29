# inbuilt
from dataclasses import dataclass
from typing import Optional

@dataclass
class ExtractionResult:
    feed_id: int
    success: bool
    skipped: bool = False
    start_epoch: float = 0.0
    end_epoch: float = 0.0
    total_human_readable_time: str = ""
    target_table_path: str = ""
    total_rows_inserted: int = 0
    total_rows_updated: int = 0
    total_rows_deleted: int = 0
    exception_if_any: Optional[Exception] = None