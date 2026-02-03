from sdmf.validation.ValidationRule import ValidationRule
from sdmf.validation.ValidationContext import ValidationContext
from sdmf.exception.ValidationError import ValidationError

class PartitionKeysCheck(ValidationRule):
    name = "Partition keys check"

    def validate(self, context: ValidationContext):

        if context.mdf_feed_specs_array is None:
            raise ValidationError(
                message="JSON list has not been parsed yet",
                original_exception=None
            )
        
        for json_dict in context.mdf_feed_specs_array:

            if json_dict['data_flow_direction'] != 'SOURCE_TO_BRONZE':
                
                data = json_dict['feed_specs_dict']
                value = data.get("partition_keys")
                if "partition_keys" not in data:
                    raise ValidationError(
                        message=f"Missing 'partition_keys' for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )
                if not isinstance(value, list):
                    raise ValidationError(
                        message=f"'partition_keys' must be an list for feed id {json_dict['feed_id']}",
                        original_exception=None
                    )

                table_name = data["source_table_name"]
                if context.spark.catalog.tableExists(table_name):
                    table_columns = context._get_table_columns(data, self.name)
                    for key in value:
                        if not isinstance(key, str):
                            raise ValidationError(
                                message=f"'partition_keys' must contain only strings for feed id {json_dict['feed_id']}",
                                original_exception=None
                            )
                        if key not in table_columns:
                            raise ValidationError(
                                message=f"'partition_keys' column '{key}' not found in table "
                                        f"'{data['source_table_name']}'  for feed id {json_dict['feed_id']}",
                                original_exception=None
                            )
