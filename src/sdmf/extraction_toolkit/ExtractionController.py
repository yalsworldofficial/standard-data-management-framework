# inbuilt
import os
import logging
import configparser
from concurrent.futures import ThreadPoolExecutor, as_completed


# external
import pandas as pd
from pyspark.sql import SparkSession

# internal
from sdmf.extraction_toolkit.data_class.ExtractionConfig import ExtractionConfig
from sdmf.extraction_toolkit.data_class.ExtractionResult import ExtractionResult


class ExtractionController():

    def __init__(
            self,
            spark: SparkSession,
            allowed_df: pd.DataFrame,
            config: configparser.ConfigParser
        ) -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.info("Extraction Controller has been initialized...")
        self.master_specs_df = allowed_df
        self.spark = spark
        self.extraction_results_list = []
        self.config = config


        
