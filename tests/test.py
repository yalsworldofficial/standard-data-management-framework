import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from sdmf import Orchestrator

config = configparser.ConfigParser()
config.read("files_dev/config.ini")

spark = (
    SparkSession.builder
    .appName("sdmf")
    .enableHiveSupport()
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.1.0"
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

# Generate 100 records
df = spark.range(1, 150).toDF("row_id")
df = (
    df
    .withColumn("alpha3_b", expr("concat('USA', row_id)"))
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
print(df.count())
# Recreate table
spark.sql("CREATE DATABASE IF NOT EXISTS demo")
spark.sql("DROP TABLE IF EXISTS demo.customers")
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("demo.customers")


import random
import string

# Function to generate random values
def random_alpha3():
    return ''.join(random.choices(string.ascii_uppercase, k=3))

def random_alpha2():
    return ''.join(random.choices(string.ascii_uppercase, k=2))

def random_english_word():
    return ''.join(random.choices(string.ascii_lowercase, k=6))

# Generate random values
alpha3_b = random_alpha3()
alpha3_t = random_alpha3()
alpha2 = random_alpha2()
english = random_english_word()

# Insert into Spark SQL
query = f"""
INSERT INTO demo.customers (alpha3_b, alpha3_t, alpha2, english)
VALUES ('{alpha3_b}', '{alpha3_t}', '{alpha2}', '{english}')
"""

spark.sql(query)



obj = Orchestrator(config=config, spark=spark)

obj.run()