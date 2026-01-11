import json
from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class ValidateJSON(ValidationRule):
    name = "JSON Format Check"

    def validate(self, context: ValidationContext):
        try:
            context.data = json.loads(context.raw_json)
        except json.JSONDecodeError as e:
            raise ValidationError(
                message = "Invalid JSON format, please make sure the JSON string provided is a valid JSON.", 
                original_exception=e,
                rule_name=self.name
            )
