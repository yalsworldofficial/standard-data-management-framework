import json
import logging
from sdmf.exception.FeedSpecValidationError import FeedSpecValidationError


class FeedSpecValidation:
    """Validates metadata JSON with rule-by-rule tracking and final integrity score."""

    TOTAL_RULES = 7

    def __init__(self, json_str: str):
        self.json_str = json_str
        self.data = None
        self.rules_passed = 0
        self.logger = logging.getLogger(__name__)

    # --------------------------------------------------------------------
    # PUBLIC API
    # --------------------------------------------------------------------
    def validate(self) -> dict:
        """Run all validation rules in sequence and log all results."""

        rule_methods = [
            ("Rule 1: JSON Format", self._rule_1_valid_json),
            ("Rule 2: primary_key", self._rule_2_primary_key),
            ("Rule 3: composite_key", self._rule_3_composite_key),
            ("Rule 4: partition_keys", self._rule_4_partition_keys),
            ("Rule 5: vacuum_hours", self._rule_5_vacuum_hours),
            ("Rule 6: checks list", self._rule_6_checks_list),
            ("Rule 7: checks structure", self._rule_7_checks_structure),
        ]

        for name, fn in rule_methods:
            try:
                fn()
                self.rules_passed += 1
                self.logger.info(f"✔ {name} — PASSED")
            except FeedSpecValidationError as e:
                self.logger.error(f"✖ {name} — FAILED: {e}")
                self._log_failure_summary()
                raise

        self._log_success_summary()
        return self.data

    # --------------------------------------------------------------------
    # INTERNAL LOGGING HELPERS
    # --------------------------------------------------------------------
    def _log_failure_summary(self):
        score = (self.rules_passed / self.TOTAL_RULES) * 100
        self.logger.error(
            f"Metadata Validation FAILED: {self.rules_passed}/{self.TOTAL_RULES} rules passed. "
            f"Integrity Score: {score:.2f}%"
        )

    def _log_success_summary(self):
        score = (self.rules_passed / self.TOTAL_RULES) * 100
        self.logger.info(
            f"Metadata Validation SUCCESS: {self.rules_passed}/{self.TOTAL_RULES} rules passed. "
            f"Integrity Score: {score:.2f}%"
        )

    # --------------------------------------------------------------------
    # RULE IMPLEMENTATIONS
    # --------------------------------------------------------------------
    def _rule_1_valid_json(self):
        try:
            self.data = json.loads(self.json_str)
        except json.JSONDecodeError as e:
            raise FeedSpecValidationError(
                message="Invalid JSON format",
                original_exception=e
            )

    def _rule_2_primary_key(self):
        if "primary_key" not in self.data:
            raise FeedSpecValidationError(message="Missing 'primary_key'")

        pk = self.data["primary_key"]
        if not isinstance(pk, str) or not pk.strip():
            raise FeedSpecValidationError(message="'primary_key' must be a non-empty string")

    def _rule_3_composite_key(self):
        if "composite_key" not in self.data:
            raise FeedSpecValidationError(message="Missing 'composite_key'")

        ck = self.data["composite_key"]
        if not isinstance(ck, list):
            raise FeedSpecValidationError(message="'composite_key' must be a list")

        for key in ck:
            if not isinstance(key, str):
                raise FeedSpecValidationError(
                    message="'composite_key' must contain only strings"
                )

    def _rule_4_partition_keys(self):
        if "partition_keys" not in self.data:
            raise FeedSpecValidationError(message="Missing 'partition_keys'")

        pk = self.data["partition_keys"]
        if not isinstance(pk, list):
            raise FeedSpecValidationError(message="'partition_keys' must be a list")

        for key in pk:
            if not isinstance(key, str):
                raise FeedSpecValidationError(
                    message="'partition_keys' must contain only strings"
                )

    def _rule_5_vacuum_hours(self):
        if "vacuum_hours" not in self.data:
            raise FeedSpecValidationError(message="Missing 'vacuum_hours'")

        if not isinstance(self.data["vacuum_hours"], int):
            raise FeedSpecValidationError(message="'vacuum_hours' must be an integer")

    def _rule_6_checks_list(self):
        if "checks" not in self.data:
            raise FeedSpecValidationError(message="Missing 'checks'")

        if not isinstance(self.data["checks"], list):
            raise FeedSpecValidationError(message="'checks' must be a list")

    def _rule_7_checks_structure(self):
        required_fields = ["check_sequence", "column_name", "threshold"]

        for idx, check in enumerate(self.data["checks"]):
            if not isinstance(check, dict):
                raise FeedSpecValidationError(
                    message=f"checks[{idx}] must be an object"
                )

            for f in required_fields:
                if f not in check:
                    raise FeedSpecValidationError(
                        message=f"checks[{idx}] missing '{f}'"
                    )

            # check_sequence must be list[str]
            if not isinstance(check["check_sequence"], list):
                raise FeedSpecValidationError(
                    message=f"checks[{idx}].check_sequence must be a list"
                )

            for seq in check["check_sequence"]:
                if not isinstance(seq, str):
                    raise FeedSpecValidationError(
                        message=f"checks[{idx}].check_sequence must contain only strings"
                    )

            # column_name
            if not isinstance(check["column_name"], str):
                raise FeedSpecValidationError(
                    message=f"checks[{idx}].column_name must be a string"
                )

            # threshold
            if not isinstance(check["threshold"], int):
                raise FeedSpecValidationError(
                    message=f"checks[{idx}].threshold must be an integer"
                )
