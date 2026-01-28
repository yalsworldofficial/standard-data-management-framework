import json
import logging
from sdmf.exception.FeedSpecValidationError import FeedSpecValidationError


class FeedSpecValidation:
    """Validates metadata JSON with rule-by-rule tracking and final integrity score."""


    def __init__(self, json_str: str, spark):
        self.json_str = json_str
        self.data = {}
        self.rules_passed = 0
        self.logger = logging.getLogger(__name__)
        self.spark = spark

    # --------------------------------------------------------------------
    # PUBLIC API
    # --------------------------------------------------------------------
    def _validate(self) -> dict:
        """Run all validation rules in sequence and log all results."""

        rule_methods = [
            ("Rule 1: JSON Format", self._rule_1_valid_json),
            ("Rule 2: primary_key check", self._rule_2_primary_key),
            ("Rule 3: composite_key check", self._rule_3_composite_key),
            ("Rule 4: partition_keys check", self._rule_4_partition_keys),
            ("Rule 5: vacuum_hours check", self._rule_5_vacuum_hours),
            ("Rule 6: standard checks list", self._rule_6_checks_list),
            ("Rule 7: checks structure", self._rule_7_checks_structure),
            ("Rule 8: selection column available", self._rule_8_columns_exist_in_selection),
            ("Rule 9: dependency data set check", self._rule_9_dependency_datasets_exist_spark)
        ]

        self.TOTAL_RULES = len(rule_methods)

        for name, fn in rule_methods:
            try:
                fn()
                self.rules_passed += 1
                self.logger.info(f"{name} — PASSED")
            except FeedSpecValidationError as e:
                self.logger.error(f"{name} — FAILED: {e}")
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
        
        table_columns = self._get_table_columns()

        if pk not in table_columns:
            raise FeedSpecValidationError(
                f"'primary_key' column '{pk}' not found in table '{self.data['source_table_name']}'"
            )

    def _rule_3_composite_key(self):
        if "composite_key" not in self.data: 
            raise FeedSpecValidationError(message="Missing 'composite_key'")

        ck = self.data["composite_key"] 
        if not isinstance(ck, list):
            raise FeedSpecValidationError(message="'composite_key' must be a list")

        table_columns = self._get_table_columns()

        for key in ck:
            if not isinstance(key, str):
                raise FeedSpecValidationError("'composite_key' must contain only strings")

            if key not in table_columns:
                raise FeedSpecValidationError(
                    f"'composite_key' column '{key}' not found in table "
                    f"'{self.data['source_table_name']}'"
                )

    def _rule_4_partition_keys(self):
        if "partition_keys" not in self.data: 
            raise FeedSpecValidationError(message="Missing 'partition_keys'")

        pk = self.data["partition_keys"] 
        if not isinstance(pk, list):
            raise FeedSpecValidationError(message="'partition_keys' must be a list")
        table_columns = self._get_table_columns()
        for key in pk:
            if not isinstance(key, str):
                raise FeedSpecValidationError(
                    message="'partition_keys' must contain only strings"
                )
            if key not in table_columns:
                raise FeedSpecValidationError(
                    f"'partition_keys' column '{key}' not found in table "
                    f"'{self.data['source_table_name']}'"
                )

    def _rule_5_vacuum_hours(self):
        if "vacuum_hours" not in self.data: 
            raise FeedSpecValidationError(message="Missing 'vacuum_hours'")

        if not isinstance(self.data["vacuum_hours"], int): 
            raise FeedSpecValidationError(message="'vacuum_hours' must be an integer")

    def _rule_6_checks_list(self):
        if "standard_checks" not in self.data: 
            raise FeedSpecValidationError(message="Missing 'checks'")

        if not isinstance(self.data["standard_checks"], list): 
            raise FeedSpecValidationError(message="'standard_checks' must be a list")

    def _rule_7_checks_structure(self):
        required_fields = ["check_sequence", "column_name", "threshold"]

        for idx, check in enumerate(self.data["standard_checks"]): 
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
            
    def _rule_8_columns_exist_in_selection(self):
        if "source_table_name" not in self.data:
            raise FeedSpecValidationError("Missing 'source_table_name'")

        if "selection_schema" not in self.data:
            raise FeedSpecValidationError("Missing 'selection_schema'")

        fields = self.data["selection_schema"].get("fields")
        if not isinstance(fields, list):
            raise FeedSpecValidationError("'selection_schema.fields' must be a list")

        table_name = self.data["source_table_name"]

        # --- Get table schema from Spark (no data read) ---
        try:
            table_df = self.spark.table(table_name)
            table_columns = {col.name for col in table_df.schema.fields}
        except Exception as e:
            raise FeedSpecValidationError(
                message=f"Unable to read schema for table '{table_name}'",
                original_exception=e
            )

        # --- Validate selection_schema columns ---
        for idx, field in enumerate(fields):
            if not isinstance(field, dict) or "name" not in field:
                raise FeedSpecValidationError(
                    f"selection_schema.fields[{idx}] must contain 'name'"
                )

            column_name = field["name"]

            if column_name not in table_columns:
                raise FeedSpecValidationError(
                    message=(
                        f"selection_schema.fields[{idx}].name '{column_name}' "
                        f"not found in table '{table_name}'"
                    )
                )
            
    def _rule_9_dependency_datasets_exist_spark(self):
        if "comprehensive_checks" not in self.data:
            return  # nothing to validate

        for idx, check in enumerate(self.data["comprehensive_checks"]):
            deps = check.get("dependency_dataset", [])

            if not isinstance(deps, list):
                raise FeedSpecValidationError(
                    f"comprehensive_checks[{idx}].dependency_dataset must be a list"
                )

            for dep in deps:
                if not self.spark.catalog.tableExists(dep):
                    raise FeedSpecValidationError(
                        message=(
                            f"comprehensive_checks[{idx}].dependency_dataset "
                            f"table does not exist: '{dep}'"
                        )
                    )

    def _get_table_columns(self) -> set:
        if "source_table_name" not in self.data:
            raise FeedSpecValidationError("Missing 'source_table_name'")

        table_name = self.data["source_table_name"]

        try:
            df = self.spark.table(table_name)
            return {field.name for field in df.schema.fields}
        except Exception as e:
            raise FeedSpecValidationError(
                message=f"Unable to read schema for table '{table_name}'",
                original_exception=e
            )

