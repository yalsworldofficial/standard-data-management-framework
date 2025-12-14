from sdmf.validation.ValidateDataSpecsBase import ValidateDataSpecsBase
import json

class ValidateGold(ValidateDataSpecsBase):

    def _rules(self):
        return [
            self.check_required_columns,
            self.check_non_nullable_columns,
            self.check_data_types,
            self.check_feed_specs_json,
            self.check_load_type_enum,
            self.validate_feed_specs
        ]

    # ------------ RULE 1: Required Columns ----------------
    def check_required_columns(self):
        required = {
            "id",
            "feed_name",
            "feed_specs",
            "load_type",
            "dpendency_dataset",
            "dependency_key",
            "target_unity_catalog",
            "target_schema_name",
            "target_table_name",
            "is_active"
        }

        missing = required - set(self.df.columns)
        if missing:
            return False, f"Missing required columns: {missing}"

        return True, "All required columns are present."

    # ------------ RULE 2: Non-nullable Columns ----------------
    def check_non_nullable_columns(self):
        non_nullable = {
            "id",
            "feed_name",
            "feed_specs",
            "load_type",
            "dpendency_dataset",
            "dependency_key",
            "target_unity_catalog",
            "target_schema_name",
            "target_table_name",
            "is_active"
        }

        nulls = [col for col in non_nullable if self.df[col].isnull().any()]
        if nulls:
            return False, f"Non-nullable columns contain NULL values: {nulls}"

        return True, "All non-nullable columns passed NULL check."

    # ------------ RULE 3: Data Type Validation ----------------
    def check_data_types(self):
        expected_types = {
            "id": "int",
            "feed_name": "str",
            "feed_specs": "str",
            "load_type": "str",
            "dpendency_dataset": "str",
            "dependency_key": "int",
            "target_unity_catalog": "str",
            "target_schema_name": "str",
            "target_table_name": "str",
            "is_active": "int",
        }

        bad_types = []

        for col, expected in expected_types.items():
            if col not in self.df.columns:
                continue

            series = self.df[col]

            if expected == "int" and not series.map(lambda x: isinstance(x, int) or x is None).all():
                bad_types.append((col, "int"))

            if expected == "str" and not series.map(lambda x: isinstance(x, str) or x is None).all():
                bad_types.append((col, "string"))

        if bad_types:
            return False, f"Datatype mismatches: {bad_types}"

        return True, "All datatype validations passed."

    # ------------ RULE 4: feed_specs must be valid JSON ----------------
    def check_feed_specs_json(self):
        invalid_rows = []

        for idx, raw in self.df["feed_specs"].items():
            try:
                json.loads(raw)
            except Exception:
                invalid_rows.append(idx)

        if invalid_rows:
            return False, f"feed_specs contains invalid JSON at rows: {invalid_rows}"

        return True, "feed_specs column contains valid JSON."

    # ------------ RULE 5: load_type ENUM validation ----------------
    def check_load_type_enum(self):
        allowed = {"FULL", "APPEND_LOAD", "INCREMENTAL_CDC", "SCD_TYPE_2"}

        invalid = self.df[~self.df["load_type"].isin(allowed)]
        if not invalid.empty:
            uniq = invalid["load_type"].unique().tolist()
            return False, f"load_type contains invalid values: {uniq}"

        return True, "load_type values are valid."
    

    def validate_feed_specs(self):
        """
        Validates that:
        - feed_specs column contains valid JSON strings
        - JSON parses successfully
        - JSON is of expected structure (dict)
        - Contains required keys
        """

        required_keys = {
            "primary_key",
            "composite_key",
            "partition_keys",
            "selection_schema",
            "standard_checks",
            "comprehensive_checks"
        }

        for idx, feed_spec in enumerate(self.df["feed_specs"].to_list()):
            try:
                feed_spec_json = json.loads(feed_spec)
            except Exception as e:
                return (
                    False,
                    f"Row {idx}: Invalid JSON in feed_specs. Error: {str(e)}"
                )

            if not isinstance(feed_spec_json, dict):
                return (
                    False,
                    f"Row {idx}: feed_specs must be a JSON object, found {type(feed_spec_json).__name__}."
                )

            missing = required_keys - feed_spec_json.keys()
            if missing:
                return (
                    False,
                    f"Row {idx}: feed_specs missing required keys: {missing}"
                )
            
        return True, "All feed_specs entries are valid JSON with the required structure."
