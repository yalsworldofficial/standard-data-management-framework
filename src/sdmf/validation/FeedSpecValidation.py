import json
import logging
from sdmf.exception.FeedSpecValidationError import FeedSpecValidationError


class FeedSpecValidation:
    """Validates metadata JSON with rule-by-rule tracking and final integrity score."""

    TOTAL_RULES = 7  # used for final score

    def __init__(self, json_str: str):
        self.json_str = json_str
        self.data = None
        self.rules_passed = 0
        self.logger = logging.getLogger(__name__)

    # ----------------------------------------------------
    # PUBLIC API
    # ----------------------------------------------------
    def validate(self) -> dict:
        """Run all validation rules in sequence and log final summary."""

        try:
            self._rule_1_valid_json()
            self._rule_2_primary_key()
            self._rule_3_composite_key()
            self._rule_4_partition_keys()
            self._rule_5_vacuum_hours()
            self._rule_6_checks_list()
            self._rule_7_checks_structure()

        except FeedSpecValidationError as e:
            # Compute how many rules passed before failure
            score = (self.rules_passed / self.TOTAL_RULES) * 100
            self.logger.error(
                f"Metadata Validation FAILED after rule {self.rules_passed}/{self.TOTAL_RULES}. "
                f"Integrity Score: {score:.2f}%"
            )
            raise

        # If all rules passed
        self._log_success_summary()
        return self.data

    # ----------------------------------------------------
    # INTERNAL SUCCESS LOGGING
    # ----------------------------------------------------
    def _log_success_summary(self):
        score = (self.rules_passed / self.TOTAL_RULES) * 100
        self.logger.info(
            f"Metadata Validation SUCCESS: {self.rules_passed}/{self.TOTAL_RULES} rules passed. "
            f"Integrity Score: {score:.2f}%"
        )

    # ----------------------------------------------------
    # RULE IMPLEMENTATIONS
    # ----------------------------------------------------

    def _rule_1_valid_json(self):
        try:
            self.data = json.loads(self.json_str)
            self.rules_passed += 1
        except json.JSONDecodeError as e:
            raise FeedSpecValidationError(
                message="Rule 1 Failed: Invalid JSON",
                original_exception=e
            )

    def _rule_2_primary_key(self):
        if "primary_key" not in self.data:
            raise FeedSpecValidationError(message="Rule 2 Failed: Missing 'primary_key'")

        pk = self.data["primary_key"]
        if not isinstance(pk, str) or not pk.strip():
            raise FeedSpecValidationError(
                message="Rule 2 Failed: 'primary_key' must be a non-empty string"
            )

        self.rules_passed += 1

    def _rule_3_composite_key(self):
        if "composite_key" not in self.data:
            raise FeedSpecValidationError(message="Rule 3 Failed: Missing 'composite_key'")

        ck = self.data["composite_key"]
        if not isinstance(ck, list):
            raise FeedSpecValidationError(message="Rule 3 Failed: 'composite_key' must be a list")

        for key in ck:
            if not isinstance(key, str):
                raise FeedSpecValidationError(
                    message="Rule 3 Failed: All items in 'composite_key' must be strings"
                )

        self.rules_passed += 1

    def _rule_4_partition_keys(self):
        if "partition_keys" not in self.data:
            raise FeedSpecValidationError(message="Rule 4 Failed: Missing 'partition_keys'")

        pk = self.data["partition_keys"]
        if not isinstance(pk, list):
            raise FeedSpecValidationError(
                message="Rule 4 Failed: 'partition_keys' must be a list"
            )

        for key in pk:
            if not isinstance(key, str):
                raise FeedSpecValidationError(
                    message="Rule 4 Failed: All items in 'partition_keys' must be strings"
                )

        self.rules_passed += 1

    def _rule_5_vacuum_hours(self):
        if "vacuum_hours" not in self.data:
            raise FeedSpecValidationError(message="Rule 5 Failed: Missing 'vacuum_hours'")

        if not isinstance(self.data["vacuum_hours"], int):
            raise FeedSpecValidationError(
                message="Rule 5 Failed: 'vacuum_hours' must be an integer"
            )

        self.rules_passed += 1

    def _rule_6_checks_list(self):
        if "checks" not in self.data:
            raise FeedSpecValidationError(message="Rule 6 Failed: Missing 'checks'")

        if not isinstance(self.data["checks"], list):
            raise FeedSpecValidationError(
                message="Rule 6 Failed: 'checks' must be a list"
            )

        self.rules_passed += 1

    def _rule_7_checks_structure(self):
        required_fields = ["check_sequence", "column_name", "threshold"]

        for idx, check in enumerate(self.data["checks"]):
            if not isinstance(check, dict):
                raise FeedSpecValidationError(
                    message=f"Rule 7 Failed: checks[{idx}] must be an object"
                )

            # Required fields
            for f in required_fields:
                if f not in check:
                    raise FeedSpecValidationError(
                        message=f"Rule 7 Failed: checks[{idx}] missing '{f}'"
                    )

            # check_sequence must be list[str]
            if not isinstance(check["check_sequence"], list):
                raise FeedSpecValidationError(
                    message=f"Rule 7 Failed: checks[{idx}].check_sequence must be a list"
                )

            for seq in check["check_sequence"]:
                if not isinstance(seq, str):
                    raise FeedSpecValidationError(
                        message=f"Rule 7 Failed: checks[{idx}].check_sequence must contain only strings"
                    )

            # column_name must be string
            if not isinstance(check["column_name"], str):
                raise FeedSpecValidationError(
                    message=f"Rule 7 Failed: checks[{idx}].column_name must be a string"
                )

            # threshold must be int
            if not isinstance(check["threshold"], int):
                raise FeedSpecValidationError(
                    message=f"Rule 7 Failed: checks[{idx}].threshold must be an integer"
                )

        self.rules_passed += 1
