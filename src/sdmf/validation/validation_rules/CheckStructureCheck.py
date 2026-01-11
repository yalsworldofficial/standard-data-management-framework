from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class CheckStructureCheck(ValidationRule):
    name = "Check Structure Check"

    def validate(self, context: ValidationContext):
        if context.data is None:
            raise ValidationError(
                message="JSON has not been parsed yet",
                original_exception=None,
                rule_name=self.name
            )
        
        required_fields = ["check_sequence", "column_name", "threshold"]

        for idx, check in enumerate(context.data["standard_checks"]): 
            if not isinstance(check, dict):
                raise ValidationError(
                    message=f"checks[{idx}] must be an object",
                    original_exception=None,
                    rule_name=self.name
                )

            for f in required_fields:
                if f not in check:
                    raise ValidationError(
                        message=f"checks[{idx}] missing '{f}'",
                        original_exception=None,
                        rule_name=self.name
                    )

            # check_sequence must be list[str]
            if not isinstance(check["check_sequence"], list):
                raise ValidationError(
                    message=f"checks[{idx}].check_sequence must be a list",
                    original_exception=None,
                    rule_name=self.name
                )

            for seq in check["check_sequence"]:
                if not isinstance(seq, str):
                    raise ValidationError(
                        message=f"checks[{idx}].check_sequence must contain only strings",
                        original_exception=None,
                        rule_name=self.name
                    )

            # column_name
            if not isinstance(check["column_name"], str):
                raise ValidationError(
                    message=f"checks[{idx}].column_name must be a string",
                    original_exception=None,
                    rule_name=self.name
                )

            # threshold
            if not isinstance(check["threshold"], int):
                raise ValidationError(
                    message=f"checks[{idx}].threshold must be an integer",
                    original_exception=None,
                    rule_name=self.name
                )
