# inbuilt
import logging
from abc import ABC, abstractmethod

# external
from pyspark.sql import SparkSession, DataFrame

# internal
from sdmf.data_movement_framework.data_class.LoadConfig import LoadConfig
from sdmf.data_movement_framework.data_class.LoadResult import LoadResult

class BaseExtractor(ABC):
    def __init__(self, config: LoadConfig, spark: SparkSession) -> None:
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.spark = spark

    @abstractmethod
    def extract(self) -> LoadResult:
        """
        Core load logic implemented by subclass.
        Should return IngestionResult on success.
        """