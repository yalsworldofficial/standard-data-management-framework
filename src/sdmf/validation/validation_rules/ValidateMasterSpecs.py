import os
from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class ValidateMasterSpecs(ValidationRule):
    name = "Master spec check"

    def validate(self, context: ValidationContext):
        master_specs = os.path.join(context.file_hunt_path, context.master_spec_name)
        if os.path.exists(master_specs) == False:
            raise ValidationError(
                message = f"System can't find the Master Specs File at [{context.file_hunt_path}]. Terminating process", 
                original_exception=None
            )
