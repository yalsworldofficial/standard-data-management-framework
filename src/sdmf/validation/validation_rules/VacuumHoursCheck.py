from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class VacuumHoursCheck(ValidationRule):
    name = "Vacuum Hours Check"

    def validate(self, context: ValidationContext):

        if context.mdf_feed_specs_array is None:
            raise ValidationError(
                message="JSON list has not been parsed yet",
                original_exception=None
            )
        
        for json_dict in context.mdf_feed_specs_array:

            if json_dict['data_flow_direction'] != 'SOURCE_TO_BRONZE':

                value = json_dict['feed_specs_dict'].get("vacuum_hours")
                if not isinstance(value, int):
                    raise ValidationError(
                        message=f"'vacuum_hours' must be an integer for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
