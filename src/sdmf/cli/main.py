import traceback
from sdmf.config.LoggingConfig import LoggingConfig
from sdmf.data_quality.runner.FeedDataQualityRunner import FeedDataQualityRunner
from sdmf.validation.FeedSpecValidation import FeedSpecValidation



from pyspark.sql import SparkSession



spark = SparkSession.builder.getOrCreate()



LoggingConfig().configure()












obj = FeedSpecValidation("""{"primary_key": "id", "composite_key": ["id"], "partition_keys": [], "vacuum_hours": 168, "checks": [{"check_sequence": ["_check_primary_key"], "column_name": "id", "threshold": 0}, {"check_sequence": ["_check_nulls", "_check_duplicates"], "column_name": "name", "threshold": 0}, {"check_sequence": ["_check_nulls"], "column_name": "value", "threshold": 0}]}""")



out = obj.validate()


print(out)

try:

    obj = FeedDataQualityRunner(spark, [out])

    obj.run()
    print('ok')
except Exception as e:
    print(e)
    print(traceback.format_exc())
    print("error detected")
