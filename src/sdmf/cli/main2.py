import configparser
from sdmf.orchestrator.Orchestrator import Orchestrator
from sdmf.data_movement_framework.DataLoadController import DataLoadController
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = (
    SparkSession.builder
    .appName("sdmf")
    .enableHiveSupport()
    .config("spark.scheduler.mode", "FAIR")
    .config(
        "spark.jars.packages",
        ",".join([
            "io.delta:delta-spark_2.12:3.1.0",
            "com.databricks:spark-xml_2.12:0.17.0"
        ])
    )
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

# spark.sql(
#     f"ALTER TABLE demo.customers SET TBLPROPERTIES ('data.load_type' = 'test')"
# )
# spark.sql("describe history demo.customers").show()


# spark.sql('select * from bronze.t_iso_language_codes').show()
# spark.sql('describe table  bronze.t_iso_language_codes').show()
# spark.sql('describe history  bronze.t_iso_language_codes').show()
# spark.sql('select count(*) from bronze.t_iso_language_codes').show()


# spark.sql('select * from bronze.t_country_codes').show()
# spark.sql('describe table  bronze.t_country_codes').show()
# spark.sql('describe history  bronze.t_country_codes').show()
# spark.sql('select count(*) from bronze.t_country_codes').show()



spark.sql('select * from bronze.t_test2').show(truncate=False)

