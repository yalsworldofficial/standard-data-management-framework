# inbuilt
import logging
from abc import ABC, abstractmethod

# external
import pyspark.sql.functions as F
from pyspark.sql import SparkSession 

# internal
from sdmf.data_movement_framework.data_class.LoadConfig import LoadConfig
from sdmf.data_movement_framework.data_class.LoadResult import LoadResult
from sdmf.exception.DataLoadException import DataLoadException


class BaseLoadStrategy(ABC):
    """
    Template method for data loads without retries.
    Subclasses must implement extract and load.
    """

    def __init__(self, config: LoadConfig, spark: SparkSession):
        self.config = config
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def execute(self) -> LoadResult:
        """
        Orchestrates the load lifecycle:
        """
        try:
            result = self._perform_load()
            self.post_load_actions(result)
            return result
        except Exception as e:
            raise DataLoadException(
                "Somethine went wrong while executing data load",
                load_type=self.config.load_specs['load_type'],
                original_exception=e
            )
    def _perform_load(self) -> LoadResult:
        """
        Calls the subclass load implementation and ensures commit/rollback semantics.
        No retries are performed here; any exception will bubble up to execute.
        """
        try:
            return self.load()
        except Exception as e:
            return LoadResult(
                success=False
            )

    @abstractmethod
    def load(self) -> LoadResult:
        """W
        Core load logic implemented by subclass.W
        Should return LoadResult on success.
        """
        

    def post_load_actions(self, result: LoadResult) -> None:
        """
        Emit metrics, notifications, or cleanup.
        Override to push metrics or alerts.
        """

    def _enforce_load_type_consistency(self) -> None:
        """
        Enforces that a target Delta table can only ever be loaded with a single load_type.
        Once a load_type has been applied, switching to another type is disallowed.
        This is enforced using Delta table properties.
        """
        target_table = f"{"" if self.config.target_unity_catalog == None else f"{self.config.target_unity_catalog}."}{self.config.target_schema_name}.{self.config.target_table_name}"
        current_type = self.config.load_specs['load_type']
        try:
            if self.spark.catalog.tableExists(target_table):
                props_df = self.spark.sql(f"SHOW TBLPROPERTIES {target_table}")
                existing_type_row = (
                    props_df.filter(F.col("key") == "data.load_type").select("value").collect()
                )

                if existing_type_row:
                    existing_type = existing_type_row[0]["value"]
                    if existing_type.upper() != current_type:
                        raise DataLoadException(
                            message=(
                                f"Load type conflict for {target_table}. "
                                f"Existing load_type: '{existing_type}', "
                                f"Attempted: '{current_type}'. "
                                f"Switching load types is not permitted."
                            ),
                            load_type=self.config.load_specs['load_type'],
                            original_exception=None
                        )
                    else:
                        self.logger.info(f"Verified consistent load_type '{existing_type}' for {target_table}.")
                else:
                    self.spark.sql(
                        f"ALTER TABLE {target_table} SET TBLPROPERTIES ('data.load_type' = '{current_type}')"
                    )
                    self.logger.info(f"Registered load_type '{current_type}' for existing table {target_table}.")
            else:
                self.logger.info(f"Target table {target_table} not found yet — will set load_type on creation.")

        except Exception as e:
            raise DataLoadException(
                message="Something went wrong while enforcing load type consistency",
                load_type=self.config.load_specs['load_type'],
                original_exception=e
            )