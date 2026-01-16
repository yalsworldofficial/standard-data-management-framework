import logging
from typing import List
from typing import Sequence
from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.validation.ValidationResult import ValidationResult
from sdmf.exception.ValidationError import ValidationError

class Validator:
    def __init__(self, rules: Sequence[ValidationRule], fail_fast: bool = True):
        self.rules = rules
        self.fail_fast = fail_fast
        self.logger = logging.getLogger(__name__)

    def validate(self, context: ValidationContext) -> ValidationResult:
        errors = []
        passed = 0

        ctr = 1

        for rule in self.rules:
            try:
                rule.validate(context)
                passed += 1
                self.logger.info(f"Rule {ctr}: {rule.name} — PASSED")
            except ValidationError as e:
                e.rule_name = rule.name
                errors.append(e)
                # self.logger.error(f"Rule {ctr}: {rule.name} — FAILED: {e.message}")

                if self.fail_fast:
                    break
            ctr+=1

        result = ValidationResult(
            passed=len(errors) == 0,
            passed_rules=passed,
            total_rules=len(self.rules),
            errors=errors
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
