from typing import Any
from pyspark.sql import SparkSession

class ValidationContext:
    raw_json: str
    file_hunt_path: str
    data: dict[str, Any] | None
    spark: SparkSession

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
