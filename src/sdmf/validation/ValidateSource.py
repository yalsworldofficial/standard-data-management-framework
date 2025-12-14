import json
from sdmf.validation.ValidateDataSpecsBase import ValidateDataSpecsBase

class ValidateSource(ValidateDataSpecsBase):

    def _rules(self):
        return [
            self.check_required_columns,
            self.check_non_nullable_columns,
            self.check_data_types,
            self.check_feed_specs_json,
            self.check_storage_type_enum,
            self.check_dump_type_enum,
        ]

    # ------------ RULE 1: Required Columns ----------------
    def check_required_columns(self):
        required = {
            "id", "solution_name", "description", "category_1",
            "feed_name", "feed_specs", "storage_type",
            "common_save_path", "solution_directory",
            "dump_type", "is_active"
        }

        missing = required - set(self.df.columns)
        if missing:
            return False, f"Missing required columns: {missing}"

        return True, "All required columns are present."

    # ------------ RULE 2: Non-nullable Columns ----------------
    def check_non_nullable_columns(self):
        non_nullable = {
            "id", "solution_name", "description", "category_1",
            "feed_specs", "storage_type", "common_save_path",
            "solution_directory", "dump_type", "is_active"
        }

        nulls = [col for col in non_nullable if self.df[col].isnull().any()]
        if nulls:
            return False, f"Non-nullable columns contain NULL values: {nulls}"

        return True, "All non-nullable columns passed NULL check."

    # ------------ RULE 3: Data Type Validation ----------------
    def check_data_types(self):
        expected_types = {
            "id": "int",
            "solution_name": "str",
            "description": "str",
            "category_1": "str",
            "category_2": "str",
            "category_3": "str",
            "tags": "str",
            "feed_name": "str",
            "feed_specs": "str",
            "storage_type": "str",
            "common_save_path": "str",
            "solution_directory": "str",
            "dump_type": "str",
            "base_path": "str",
            "is_active": "int",
        }

        bad_types = []
        for col, expected in expected_types.items():
            if col in self.df.columns:
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

    # ------------ RULE 5: storage_type ENUM validation ----------------
    def check_storage_type_enum(self):
        allowed = {"MEMORY", "LOCAL", "S3", "ADLS"}

        invalid = self.df[~self.df["storage_type"].isin(allowed)]
        if not invalid.empty:
            return (
                False,
                f"storage_type contains invalid values: {invalid['storage_type'].unique().tolist()}"
            )

        return True, "storage_type values are valid."

    # ------------ RULE 6: dump_type ENUM validation ----------------
    def check_dump_type_enum(self):
        allowed = {"MEDALLION", "DIRECT"}

        invalid = self.df[~self.df["dump_type"].isin(allowed)]
        if not invalid.empty:
            return (
                False,
                f"dump_type contains invalid values: {invalid['dump_type'].unique().tolist()}"
            )

        return True, "dump_type values are valid."
