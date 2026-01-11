import json
from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class PrimaryKeyRule(ValidationRule):
    name = "Primary Key Check"

    def validate(self, context: ValidationContext):
        if context.data is None:
            raise ValidationError(
                message="JSON has not been parsed yet",
                original_exception=None,
                rule_name=self.name
            )

        data = context.data  # now narrowed to dict[str, Any]

        if "primary_key" not in data:
            raise ValidationError(
                message="Missing 'primary_key'",
                original_exception=None,
                rule_name=self.name
            )

        pk = data["primary_key"]
        if not isinstance(pk, str) or not pk.strip():
            raise ValidationError(
                message="'primary_key' must be a non-empty string",
                original_exception=None,
                rule_name=self.name
            )

        table_columns = self._get_table_columns(context)
        if pk not in table_columns:
            raise ValidationError(
                message=f"primary_key '{pk}' not found in table '{data['source_table_name']}'",
                original_exception=None,
                rule_name=self.name
            )
        

    def _get_table_columns(self, context: ValidationContext) -> set:
        if context.data is None:
            raise ValidationError(
                message="JSON has not been parsed yet",
                original_exception=None,
                rule_name=self.name
            )

        data = context.data

        if "source_table_name" not in data:
            raise ValidationError(
                message="Missing 'source_table_name'",
                original_exception=None,
                rule_name=self.name
            )

        table_name = data["source_table_name"]

        try:
            df = context.spark.table(table_name)
            return {field.name for field in df.schema.fields}
        except Exception as e:
            raise ValidationError(
                message=f"Unable to read schema for table '{table_name}'",
                original_exception=None,
                rule_name=self.name
            )

