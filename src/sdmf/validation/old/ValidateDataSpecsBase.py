import logging
import pandas as pd
from sdmf.exception.DataSpecValidationError import DataSpecValidationError
from sdmf.exception.DataSpecRuleExecutionError import DataSpecRuleExecutionError


class ValidateDataSpecsBase:
    """
    Base class for validating pandas DataFrames using rule-based validation.
    Subclasses implement specific rules.

    validate() returns True or False only.
    Logs include rule status + overall integrity score.
    """

    def __init__(self, df: pd.DataFrame, context=None):
        self.df = df
        self.context = context or {}
        self.results = []
        self.logger = logging.getLogger(__name__)

    def _rules(self):
        """Return list of validation rule callables. Override in subclass."""
        return []

    def validate(self):
        rules = self._rules()
        total_rules = len(rules)
        passed = 0

        self.logger.info(f"Starting validation using {self.__class__.__name__} ...")
        self.logger.info(f"Total rules to evaluate: {total_rules}")

        for rule in rules:
            rule_name = rule.__name__

            try:
                self.logger.info(f"Running rule: {rule_name}")

                # RULE RETURNS -> (status: bool, message: str)
                status, msg = rule()

                # Log rule status
                status_str = "PASS" if status else "FAIL"
                self.logger.info(f"Rule: {rule_name} | Status: {status_str} | Message: {msg}")

                # Count passed rules
                if status:
                    passed += 1

                # Save rule results
                self.results.append({
                    "rule": rule_name,
                    "status": status_str,
                    "message": msg
                })

            except DataSpecValidationError:
                # Rule-specific validation error -> FAIL but continue execution
                self.logger.error(f"Rule: {rule_name} | Status: FAIL | ValidationError occurred")
                self.results.append({
                    "rule": rule_name,
                    "status": "FAIL",
                    "message": "ValidationError thrown"
                })

            except Exception as e:
                # Unexpected exception -> STOP and throw
                raise DataSpecRuleExecutionError(
                    message=f"Unexpected error while executing rule: {rule_name}",
                    details=str(e),
                    context=self.context,
                    original_exception=e
                ) from e

        # Compute integrity score
        integrity_score = round((passed / total_rules) * 100, 2) if total_rules else 0.0
        self.logger.info(f"Validation Complete: {passed}/{total_rules} rules passed")
        self.logger.info(f"Overall Integrity Score: {integrity_score}%")

        # Store score if needed externally
        self.integrity_score = integrity_score

        # Overall success = all rules passed
        overall_status = passed == total_rules
        self.logger.info(f"Overall Validation Result: {overall_status}")

        return overall_status
