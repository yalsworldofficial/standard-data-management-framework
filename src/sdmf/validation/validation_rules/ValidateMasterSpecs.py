import os
from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class ValidateMasterSpecs(ValidationRule):
    name = "Master Spec Check"

    def validate(self, context: ValidationContext):
        if os.path.exists(os.path.join(context.file_hunt_path, "master_specs.xlsx")) == False:
            raise ValidationError(
                message = f"System can't find the Master Specs File at [{context.file_hunt_path}]. Terminating process", 
                original_exception=None,
                rule_name=self.name
            )
