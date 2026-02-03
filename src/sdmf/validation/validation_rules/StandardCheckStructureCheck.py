from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class StandardCheckStructureCheck(ValidationRule):
    name = "Feed spec standard checks structure check"

    def validate(self, context: ValidationContext):

        if context.mdf_feed_specs_array is None:
            raise ValidationError(
                message="JSON has not been parsed yet",
                original_exception=None
            )
        

        for json_dict in context.mdf_feed_specs_array:
            if json_dict['data_flow_direction'] != 'SOURCE_TO_BRONZE':
                data = json_dict['feed_specs_dict']
                if data is None:
                    raise ValidationError(
                        message=f"JSON has not been parsed yet for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                required_fields = ["check_sequence", "column_name", "threshold"]
                for idx, check in enumerate(data["standard_checks"]): 
                    if not isinstance(check, dict):
                        raise ValidationError(
                            message=f"checks[{idx}] must be an object for feed id {json_dict['feed_id']}",
                            original_exception=None
                        )
                    for f in required_fields:
                        if f not in check:
                            raise ValidationError(
                                message=f"checks[{idx}] missing '{f}' for feed id {json_dict['feed_id']}",
                                original_exception=None
                            )
                    if not isinstance(check["check_sequence"], list):
                        raise ValidationError(
                            message=f"checks[{idx}].check_sequence must be a list for feed id {json_dict['feed_id']}",
                            original_exception=None
                        )
                    for seq in check["check_sequence"]:
                        if not isinstance(seq, str):
                            raise ValidationError(
                                message=f"checks[{idx}].check_sequence must contain only strings for feed id {json_dict['feed_id']}",
                                original_exception=None
                            )
                    if not (isinstance(check["column_name"], str) or isinstance(check["column_name"], list)):
                        raise ValidationError(
                            message=f"checks[{idx}].column_name must be a string for feed id {json_dict['feed_id']}",
                            original_exception=None
                        )
                    if not isinstance(check["threshold"], int):
                        raise ValidationError(
                            message=f"checks[{idx}].threshold must be an integer for feed id {json_dict['feed_id']}",
                            original_exception=None
                        )
            