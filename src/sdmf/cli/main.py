import traceback
from sdmf.config.LoggingConfig import LoggingConfig
from sdmf.data_quality.runner.FeedDataQualityRunner import FeedDataQualityRunner
from sdmf.validation.FeedSpecValidation import FeedSpecValidation
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


LoggingConfig().configure()



spark = SparkSession.builder \
    .appName("SDMF") \
    .enableHiveSupport() \
    .getOrCreate()

# Generate 100 records
df = spark.range(1, 101).toDF("row_id")

df = (
    df
    # primary key column WITH HYPHEN (matches FeedSpec exactly)
    .withColumn("alpha3-b", expr("concat('USA', row_id)"))
    .withColumn("alpha3_t", expr("concat('US', row_id)"))
    .withColumn("alpha2", expr("substring('US', 1, 2)"))
    .withColumn(
        "english",
        expr("""
            CASE
                WHEN row_id % 4 = 0 THEN 'United States'
                WHEN row_id % 4 = 1 THEN 'Germany'
                WHEN row_id % 4 = 2 THEN 'India'
                ELSE 'Canada'
            END
        """)
    )
    .drop("row_id")
)

# Recreate table
spark.sql("CREATE DATABASE IF NOT EXISTS demo")
spark.sql("DROP TABLE IF EXISTS demo.customers")

df.write \
    .mode("overwrite") \
    .saveAsTable("demo.customers")










obj = FeedSpecValidation("""

{
    "primary_key": "alpha3-b",
    "composite_key": [],
    "partition_keys": [],
    "vacuum_hours": 168,
    "movement_type": "BRONZE_TO_SILVER",
    "source_table_name":"demo.customers",
    "selection_schema": {
        "type": "struct",
        "fields": [
            {
                "name": "alpha3_b",
                "type": "string",
                "nullable": true
            },
            {
                "name": "alpha3_t",
                "type": "string",
                "nullable": true
            },
            {
                "name": "alpha2",
                "type": "string",
                "nullable": true
            },
            {
                "name": "english",
                "type": "string",
                "nullable": true
            }
        ]
    },
    "standard_checks": [
        {
            "check_sequence": [
                "_check_primary_key"
            ],
            "column_name": "alpha3-b",
            "threshold": 0
        },
        {
            "check_sequence": [
                "_check_nulls"
            ],
            "column_name": "english",
            "threshold": 0
        }
    ],
    "comprehensive_checks": [
        {
            "check_name":"Some unique check name",
            "query":"Select 1;",
            "severity":"ERROR",
            "threshold": 0,
            "load_stage":"PRE_LOAD",
            "dependency_dataset":[]
        },
        {
            "check_name":"Some unique check name 1",
            "query":"Select 1;",
            "severity":"WARNING",
            "threshold": 0,
            "load_stage":"POST_LOAD",
            "dependency_dataset":["uc_name.schema_name.some_table_name1", "uc_name.schema_name.some_table_name2"]
        }
    ]
}

""")



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
