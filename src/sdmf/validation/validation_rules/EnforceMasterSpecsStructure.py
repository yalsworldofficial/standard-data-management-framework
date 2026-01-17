import os
import pandas as pd
from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class EnforceMasterSpecsStructure(ValidationRule):
    name = "Enforce master spec structure"

    def validate(self, context: ValidationContext):
        master_specs_path = os.path.join(context.file_hunt_path, context.master_spec_name)

        if os.path.exists(master_specs_path) == False:
            raise ValidationError(
                message = f"System can't find the Master Specs File at [{context.file_hunt_path}]. Terminating process", 
                original_exception=None,
                rule_name=self.name
            )

        context.master_specs_dataframe = pd.read_excel(master_specs_path, sheet_name="master_specs")

        cols = context.master_specs_dataframe.columns.tolist()
        
        required_columns = [
            "feed_id",
            "system_name",
            "subsystem_name",
            "category",
            "sub_category",
            "data_flow_direction",
            "residing_layer",
            "feed_name",
            "feed_type",
            "feed_specs",
            "load_type",
            "target_table_name",
            "suggested_feed_name",
            "is_active",
        ]
        expected = set(required_columns)
        actual = set(context.master_specs_dataframe.columns)

        missing = expected - actual
        extra = actual - expected

        if missing or extra:
            raise ValidationError(
                message = f"Missing columns: {sorted(missing)} | Extra columns: {sorted(extra)}", 
                original_exception=None,
                rule_name=self.name
            )

        subset = context.master_specs_dataframe[required_columns].replace(r"^\s*$", pd.NA, regex=True)
        null_mask = subset.isna()

        if null_mask.any().any():
            errors = (
                null_mask
                .stack()                    # (row, column) pairs
                .reset_index()
            )
            errors.columns = ["row_index", "column", "is_null"]

            # keep only failing cells
            errors = errors[errors["is_null"]]

            # make Excel-friendly row numbers
            errors["row_number"] = errors["row_index"] + 2

            error_lines = []

            grouped = (
                errors[["row_number", "column"]]
                .groupby("row_number")["column"]
                .apply(list)
            )

            for row_number, columns in grouped.items():
                cols = ", ".join(columns)
                error_lines.append(f"  • Row {row_number}: {cols}")

            message = (
                "Validation failed: required columns contain null or blank values\n\n"
                "Affected rows and columns:\n"
                + "\n".join(error_lines) + '\n'
            )

            raise ValidationError(
                message=message,
                original_exception=None,
                rule_name=self.name,
            )



