from abc import ABC, abstractmethod
from sdmf.mixins import MetadataMixin


class BaseDataLoader(MetadataMixin, ABC):
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
        spark,
        load_type: str,
        load_strategy: str,
        source_unity_catalog: str,
        source_schema_name: str,
        source_table_name: str,
        target_unity_catalog: str,
        target_schema_name: str,
        target_table_name: str,
        feed_specs: str,
        source_query: str = None
    ):
        super().__init__(job_name=self.__class__.__name__)
        self.spark = spark
        self.load_type = load_type
        self.load_strategy = load_strategy
        self.source_unity_catalog = source_unity_catalog
        self.source_schema_name = source_schema_name
        self.source_table_name = source_table_name
        self.target_unity_catalog = target_unity_catalog
        self.target_schema_name = target_schema_name
        self.target_table_name = target_table_name
        self.feed_specs = feed_specs
        self.source_query = source_query




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


