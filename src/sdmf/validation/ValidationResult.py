from dataclasses import dataclass
from typing import List
from sdmf.exception.ValidationError import ValidationError

@dataclass
class ValidationResult:
    passed: bool
    passed_rules: int
    total_rules: int
    errors: List[ValidationError]

    @property
    def score(self) -> float:
        if self.total_rules == 0:
            return 100.0
        return (self.passed_rules / self.total_rules) * 100
