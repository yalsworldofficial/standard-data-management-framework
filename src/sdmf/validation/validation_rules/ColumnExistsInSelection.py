from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class ColumnExistsInSelection(ValidationRule):
    name = "Column Exists in Selection Check"

    def validate(self, context: ValidationContext):
        if context.data is None:
            raise ValidationError(
                message="JSON has not been parsed yet",
                original_exception=None,
                rule_name=self.name
            )

        if "source_table_name" not in context.data:
            raise ValidationError("Missing 'source_table_name'",
                original_exception=None,
                rule_name=self.name)

        if "selection_schema" not in context.data:
            raise ValidationError("Missing 'selection_schema'",
                original_exception=None,
                rule_name=self.name)

        fields = context.data["selection_schema"].get("fields")
        if not isinstance(fields, list):
            raise ValidationError("'selection_schema.fields' must be a list",
                original_exception=None,
                rule_name=self.name)

        table_name = context.data["source_table_name"]

        # --- Get table schema from Spark (no data read) ---
        try:
            table_df = context.spark.table(table_name)
            table_columns = {col.name for col in table_df.schema.fields}
        except Exception as e:
            raise ValidationError(
                message=f"Unable to read schema for table '{table_name}'",
                original_exception=e,
                rule_name=self.name
            )

        # --- Validate selection_schema columns ---
        for idx, field in enumerate(fields):
            if not isinstance(field, dict) or "name" not in field:
                raise ValidationError(
                    f"selection_schema.fields[{idx}] must contain 'name'",
                    original_exception=None,
                    rule_name=self.name
                )

            column_name = field["name"]

            if column_name not in table_columns:
                raise ValidationError(
                    message=(
                        f"selection_schema.fields[{idx}].name '{column_name}' "
                        f"not found in table '{table_name}'"
                    ),
                    original_exception=None,
                    rule_name=self.name
                )
