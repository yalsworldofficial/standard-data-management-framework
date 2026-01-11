from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class EnforceStandardChecks(ValidationRule):
    name = "Standard Checks Enforcement"

    def validate(self, context: ValidationContext):
        if context.data is None:
            raise ValidationError(
                message="JSON has not been parsed yet",
                original_exception=None,
                rule_name=self.name
            )
        
        if "standard_checks" not in context.data: 
            raise ValidationError(
                message="Missing 'checks'",
                original_exception=None,
                rule_name=self.name
            )

        if not isinstance(context.data["standard_checks"], list): 
            raise ValidationError(
                message="'standard_checks' must be a list",
                original_exception=None,
                rule_name=self.name
            )
