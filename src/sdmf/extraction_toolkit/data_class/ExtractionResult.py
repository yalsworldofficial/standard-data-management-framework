# inbuilt
from dataclasses import dataclass
from typing import Optional

# external
from pyspark.sql import DataFrame

@dataclass
class ExtractionResult:
    feed_id: int
    success: bool
    skipped: bool = False
    start_epoch: float = 0.0
    end_epoch: float = 0.0
    total_human_readable_time: str = ""
    target_table_path: str = ""
    data_frame: Optional[DataFrame] = None
    total_rows_inserted: int = 0
    exception_if_any: Optional[Exception] = None