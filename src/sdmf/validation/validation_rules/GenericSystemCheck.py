from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class VacuumHoursRule(ValidationRule):
    name = "Generic System Check"

    def validate(self, context: ValidationContext):
        if context.data is None:
            raise ValidationError(
                message="JSON has not been parsed yet",
                original_exception=None,
                rule_name=self.name
            )

        value = context.data.get("vacuum_hours")

        if not isinstance(value, int):
            raise ValidationError(
                message="'vacuum_hours' must be an integer",
                original_exception=None,
                rule_name=self.name
            )
