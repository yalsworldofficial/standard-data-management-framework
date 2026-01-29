# inbuilt
import logging
from abc import ABC, abstractmethod

# external
from pyspark.sql import SparkSession

# internal
from sdmf.extraction_toolkit.data_class.ExtractionConfig import ExtractionConfig
from sdmf.extraction_toolkit.data_class.ExtractionResult import ExtractionResult

class BaseExtractor(ABC):
    def __init__(self, extraction_config: ExtractionConfig, spark: SparkSession) -> None:
        self.logger = logging.getLogger(__name__)
        self.extraction_config = extraction_config
        self.spark = spark

    @abstractmethod
    def extract(self) -> ExtractionResult:
        """
        Core load logic implemented by subclass.
        Should return IngestionResult on success.
        """