from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class PrimaryKey(ValidationRule):
    name = "Feed spec primary key check"

    def validate(self, context: ValidationContext):
        if context.mdf_feed_specs_array is None:
            raise ValidationError(
                message="JSON list has not been parsed yet",
                original_exception=None
            )
        
        
        for json_dict in context.mdf_feed_specs_array:

            if json_dict['data_flow_direction'] != 'SOURCE_TO_BRONZE':

                feed_specs_dict = json_dict['feed_specs_dict']

                if "primary_key" not in feed_specs_dict:
                    raise ValidationError(
                        message=f"Missing 'primary_key' for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )

                pk = feed_specs_dict["primary_key"]
                if not isinstance(pk, str) or not pk.strip():
                    raise ValidationError(
                        message=f"'primary_key' must be a non-empty string for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                
                if context.spark.catalog.tableExists(feed_specs_dict['source_table_name']):

                    table_columns = context._get_table_columns(feed_specs_dict, self.name)
                    if pk not in table_columns:
                        raise ValidationError(
                            message=f"primary_key '{pk}' not found in table '{feed_specs_dict['source_table_name']}' for feed id {json_dict['feed_id']}",
                            original_exception=None
                        )                
        


