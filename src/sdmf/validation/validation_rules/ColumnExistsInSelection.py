from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError


class ColumnExistsInSelection(ValidationRule):
    name = "Feed spec column exists in selection check"

    def validate(self, context: ValidationContext):
        if context.mdf_feed_specs_array is None:
            raise ValidationError(
                message="JSON has not been parsed yet",
                original_exception=None,
                rule_name=self.name,
            )

        for json_dict in context.mdf_feed_specs_array:
            if json_dict["data_flow_direction"] != "EXTRACTION":
                data = json_dict["feed_specs_dict"]
                if "source_table_name" not in data:
                    raise ValidationError(
                        f"Missing 'source_table_name' for feed id {json_dict['feed_id']}",
                        original_exception=None,
                        rule_name=self.name,
                    )
                if "selection_schema" not in data:
                    raise ValidationError(
                        f"Missing 'selection_schema' for feed id {json_dict['feed_id']}",
                        original_exception=None,
                        rule_name=self.name,
                    )
                fields = data["selection_schema"].get("fields")
                if not isinstance(fields, list):
                    raise ValidationError(
                        f"'selection_schema.fields' must be a list for feed id {json_dict['feed_id']}",
                        original_exception=None,
                        rule_name=self.name,
                    )
                table_name = data["source_table_name"]
                try:
                    table_df = context.spark.table(table_name)
                    table_columns = {col.name for col in table_df.schema.fields}
                except Exception as e:
                    raise ValidationError(
                        message=f"Unable to read schema for table '{table_name}' for feed id {json_dict['feed_id']}",
                        original_exception=e,
                        rule_name=self.name,
                    )
                for idx, field in enumerate(fields):
                    if not isinstance(field, dict) or "name" not in field:
                        raise ValidationError(
                            f"selection_schema.fields[{idx}] must contain 'name' for feed id {json_dict['feed_id']}",
                            original_exception=None,
                            rule_name=self.name,
                        )
                    column_name = field["name"]
                    if column_name not in table_columns:
                        raise ValidationError(
                            message=(
                                f"selection_schema.fields[{idx}].name '{column_name}' "
                                f"not found in table '{table_name}'"
                                f" for feed id {json_dict['feed_id']}"
                            ),
                            original_exception=None,
                            rule_name=self.name,
                        )
