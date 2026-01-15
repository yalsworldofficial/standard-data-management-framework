import logging
from sdmf.config.LoggingConfig import LoggingConfig
from sdmf.orchestrator.Orchestrator import Orchestrator

import     os


pth  = os.path.join('../../')
print(pth)
print(os.path.exists(pth))
print(os.path.dir(pth))

# LoggingConfig().configure()

# is_a = True


# if is_a:
#   print('and')
#   import os
  
#   os.environ["SPARK_SUBMIT_OPTS"] = (
#       "-Dorg.xerial.snappy.useNative=false "
#       "-Dsnappy.disable=true "
#       "-Dio.netty.tryReflectionSetAccessible=true"
#   )
  
#   os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
  
#   from pyspark.sql import SparkSession
#   from pyspark.sql.functions import expr
  
  
#   spark = (
#       SparkSession.builder
#       .appName("SDMF")
#       .config("spark.io.compression.codec", "lz4")
#       .config("spark.sql.parquet.compression.codec", "uncompressed")
#       .config("spark.sql.orc.compression.codec", "uncompressed")
#       .config("spark.hadoop.io.native.lib.available", "false")
#       .getOrCreate()
#   )


# else:
#   from pyspark.sql import SparkSession
#   from pyspark.sql.functions import expr
#   spark = SparkSession.builder \
#       .appName("SDMF") \
#       .enableHiveSupport() \
#       .getOrCreate()

# # Generate 100 records
# df = spark.range(1, 101).toDF("row_id")

# df = (
#     df
#     # primary key column WITH HYPHEN (matches FeedSpec exactly)
#     .withColumn("alpha3-b", expr("concat('USA', row_id)"))
#     .withColumn("alpha3_t", expr("concat('US', row_id)"))
#     .withColumn("alpha2", expr("substring('US', 1, 2)"))
#     .withColumn(
#         "english",
#         expr("""
#             CASE
#                 WHEN row_id % 4 = 0 THEN 'United States'
#                 WHEN row_id % 4 = 1 THEN 'Germany'
#                 WHEN row_id % 4 = 2 THEN 'India'
#                 ELSE 'Canada'
#             END
#         """)
#     )
#     .drop("row_id")
# )

# # Recreate table
# spark.sql("CREATE DATABASE IF NOT EXISTS demo")
# spark.sql("DROP TABLE IF EXISTS demo.customers")

# df.write \
#     .mode("overwrite") \
#     .saveAsTable("demo.customers")



# myOrchestrator = Orchestrator(
#     spark,
#     file_hunt_path="../../../files/"
# )