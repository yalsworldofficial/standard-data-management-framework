from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class DQResult:
    feed_ok: bool
    standard_passed: bool
    comprehensive_errors: bool
    warnings: List[str] = field(default_factory=list)
    metrics: Dict[str, int] = field(default_factory=dict)
