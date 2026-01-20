import configparser
from sdmf.config.LoggingConfig import LoggingConfig
from sdmf.orchestrator.Orchestrator import Orchestrator
LoggingConfig().configure()
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = (
    SparkSession.builder
    .appName("sdmf")
    .enableHiveSupport()
    .getOrCreate()
)


# Generate 100 records
df = spark.range(1, 101).toDF("row_id")

df = (
    df
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


# df.show()

# Recreate table
spark.sql("CREATE DATABASE IF NOT EXISTS demo")
spark.sql("DROP TABLE IF EXISTS demo.customers")

df.write \
    .mode("overwrite") \
    .saveAsTable("demo.customers")


cfg = configparser.ConfigParser()
cfg.read("files_dev/config.ini")


myOrchestrator = Orchestrator(
    spark,
    config=cfg
)


myOrchestrator.run()