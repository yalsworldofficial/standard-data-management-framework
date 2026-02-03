from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError


class EnforceStandardChecks(ValidationRule):
    name = "Feed spec standard checks enforcement"

    def validate(self, context: ValidationContext):

        if context.mdf_feed_specs_array is None:
            raise ValidationError(
                message="JSON has not been parsed yet",
                original_exception=None
            )

        for json_dict in context.mdf_feed_specs_array:
            if json_dict["data_flow_direction"] != "SOURCE_TO_BRONZE":
                data = json_dict["feed_specs_dict"]
                if data is None:
                    raise ValidationError(
                        message=f"JSON has not been parsed yet for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                if "standard_checks" not in data:
                    raise ValidationError(
                        message=f"Missing 'checks' for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )

                if not isinstance(data["standard_checks"], list):
                    raise ValidationError(
                        message=f"'standard_checks' must be a list for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
