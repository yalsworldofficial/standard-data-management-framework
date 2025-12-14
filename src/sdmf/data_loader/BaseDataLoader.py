from abc import ABC, abstractmethod
from sdmf.data_quality.StandardDataQualityChecks import StandardDataQualityChecks

class BaseDataLoader(ABC):
    """
    Abstract base class for all data loaders (CDC, SCD2, Snapshot, etc).

    This class:
    - Does NOT import SparkSession
    - Accepts any 'spark-like' object (Databricks, local, tests)
    - Defines the required methods child loaders must implement
    - Provides lifecycle hooks (pre/transform/load/post)
    """

    def __init__(
        self,
        spark
    ):
        super().__init__(job_name=self.__class__.__name__)
        self.spark = spark



    @abstractmethod
    def load_type_dispatcher(self) -> bool:
        """Extract raw data (only child class knows how)."""
        pass


    def pre_load(self):
        """Hook for child class to run before extraction (optional)."""
        pass

    def post_load(self):
        """Hook for child class to run after load (optional)."""
        pass



    def run(self):
        """Master pipeline runner followed by all child loaders."""

        try:

            self.pre_load()


            self.post_load()
        except Exception as e:
            pass


