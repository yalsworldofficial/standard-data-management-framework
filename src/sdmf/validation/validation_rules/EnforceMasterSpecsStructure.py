# inbuilt
import os
import json

# external
import pandas as pd

# internal
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
                original_exception=None
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
            "target_unity_catalog",
            "target_schema_name",
            "target_table_name",
            "suggested_feed_name",
            "parallelism_group_number",
            "parent_feed_id",
            "is_active",
        ]
        expected = set(required_columns)
        actual = set(context.master_specs_dataframe.columns)
        missing = expected - actual
        extra = actual - expected
        if missing or extra:
            raise ValidationError(
                message = f"Missing columns: {sorted(missing)} | Extra columns: {sorted(extra)}", 
                original_exception=None
            )
        subset = context.master_specs_dataframe[required_columns].replace(r"^\s*$", pd.NA, regex=True)
        null_mask = subset.isna()
        if null_mask.any().any():
            errors = (
                null_mask
                .stack()
                .reset_index()
            )
            errors.columns = ["row_index", "column", "is_null"]
            errors = errors[errors["is_null"]]
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
                original_exception=None            
            )
        else:

            filtered_df = context.master_specs_dataframe[
                context.master_specs_dataframe['data_flow_direction'] != 'SOURCE_TO_BRONZE'
            ]

            all_feed_specs = filtered_df['feed_specs'].to_list()
            for feed_specs in all_feed_specs:
                feed_specs_dict = json.loads(feed_specs)
                required_keys = [
                    "primary_key",
                    "composite_key",
                    "partition_keys",
                    "vacuum_hours",
                    "source_table_name",
                    "selection_query",
                    "selection_schema",
                    "standard_checks",
                    "comprehensive_checks"
                ]
                top_level_keys = list(feed_specs_dict.keys())
                if set(required_keys) != set(top_level_keys):
                    missing_keys = set(required_keys) - set(top_level_keys)
                    extra_keys = set(top_level_keys) - set(required_keys)
                    message = f"Validation failed! Missing keys: {missing_keys}"
                    if extra_keys:
                        message += f"; Unexpected keys: {extra_keys}"
                    raise ValidationError(
                        message=message,
                        original_exception=None
                    )








