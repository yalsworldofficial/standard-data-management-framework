# inbuilt
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class LoadResult:
    success: bool
    start_epoch: float = field(default_factory=time.time)
    end_epoch: float = 0.0
    total_human_readable_time: str = ""
    source_table_path: str = ""
    target_table_path: str = ""
    transfer_direction: str = ""
    total_rows_inserted: int = 0
    total_rows_updated: int = 0
    total_rows_deleted: int = 0
    exception_if_any: Optional[Exception] = None

