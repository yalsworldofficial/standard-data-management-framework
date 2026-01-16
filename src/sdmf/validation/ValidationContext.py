from typing import Any
from pyspark.sql import SparkSession
import pandas as pd
class ValidationContext:
    spark: SparkSession
    file_hunt_path: str
    raw_json: str
    master_specs_dataframe: pd.DataFrame
    mdf_feed_specs_array: list[dict] | None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def get_master_specs(self) -> pd.DataFrame:
        return self.master_specs_dataframe
