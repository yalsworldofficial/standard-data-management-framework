from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class PrimaryKey(ValidationRule):
    name = "Primary Key Check"

    def validate(self, context: ValidationContext):
        if context.mdf_feed_specs_array is None:
            raise ValidationError(
                message="JSON list has not been parsed yet",
                original_exception=None,
                rule_name=self.name
            )
        
        for json_dict in context.mdf_feed_specs_array:

            feed_specs_dict = json_dict['feed_specs_dict']

            if "primary_key" not in feed_specs_dict:
                raise ValidationError(
                    message="Missing 'primary_key'",
                    original_exception=None,
                    rule_name=self.name
                )

            pk = feed_specs_dict["primary_key"]
            if not isinstance(pk, str) or not pk.strip():
                raise ValidationError(
                    message="'primary_key' must be a non-empty string",
                    original_exception=None,
                    rule_name=self.name
                )

            table_columns = self._get_table_columns(feed_specs_dict, context)
            if pk not in table_columns:
                raise ValidationError(
                    message=f"primary_key '{pk}' not found in table '{feed_specs_dict['source_table_name']}'",
                    original_exception=None,
                    rule_name=self.name
                )
        

    def _get_table_columns(self, data: dict, context: ValidationContext) -> set:
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

