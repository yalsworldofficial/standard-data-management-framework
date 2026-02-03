# inbuilt
import logging
import pandas as pd
from typing import Sequence

# internal
from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.validation.ValidationResult import ValidationResult


class Validator:
    def __init__(self, rules: Sequence[ValidationRule], fail_fast: bool = True):
        self.rules = rules
        self.fail_fast = fail_fast
        self.logger = logging.getLogger(__name__)

    def validate(self, context: ValidationContext) -> ValidationResult:
        errors = []
        rows = []
        for idx, rule in enumerate(self.rules, start=1):
            try:
                rule.validate(context)
                self.logger.info(f"Rule {idx}: {rule.name} — PASSED")
                rows.append(
                    {
                        "rule_index": idx,
                        "rule_name": rule.name,
                        "status": "PASSED",
                        "error_message": None,
                    }
                )
            except Exception as e:
                self.logger.error(f"Rule {idx}: {rule.name} — FAILED: {e}")
                errors.append(e)
                rows.append(
                    {
                        "rule_index": idx,
                        "rule_name": rule.name,
                        "status": "FAILED",
                        "error_message": str(e),
                    }
                )
                if self.fail_fast:
                    break
        results_df = pd.DataFrame(rows)
        result = ValidationResult(
            passed=len(errors) == 0,
            passed_rules=len(rows) - len(errors),
            total_rules=len(self.rules),
            errors=errors,
            results_df=results_df,
        )
        self._log_summary(result)
        return result

    def _log_summary(self, result: ValidationResult):
        if result.passed:
            self.logger.info(
                f"SYSTEM VALIDATION SUCCESS: {result.passed_rules}/{result.total_rules} "
                f"INTEGRITY ({result.score:.2f}%)"
            )
        else:
            self.logger.error(
                f"SYSTEM VALIDATION FAILED: {result.passed_rules}/{result.total_rules} "
                f"INTEGRITY ({result.score:.2f}%)"
            )
