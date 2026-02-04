# inbuilt
import logging

# external
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# internal
from sdmf.data_movement_framework.data_class.LoadResult import LoadResult
from sdmf.data_movement_framework.data_class.LoadConfig import LoadConfig
from sdmf.data_movement_framework.BaseLoadStrategy import BaseLoadStrategy
from sdmf.exception.DataLoadException import DataLoadException

class FullLoad(BaseLoadStrategy):

    def __init__(self, config: LoadConfig, spark: SparkSession) -> None:
        super().__init__(config=config, spark=spark)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing FULL_LOAD data transfer component...")

    def load(self) -> LoadResult:
        """
        Perform a FULL overwrite load into the target table in Databricks Unity Catalog.
        This replaces all existing data in the target with data from the staging layer.

        Returns:
            LoadResult: LoadResult.
        """
        try:
            self._enforce_load_type_consistency()
            is_staging_layer_created = self._create_staging_layer()
            
            if not is_staging_layer_created:
                self.logger.error(
                    f"FULL LOAD failed for {self._current_target_table_name}: Staging Layer wasn't created."
                )
                return LoadResult(
                    feed_id = self.config.master_specs['feed_id'], 
                    success=False,
                    total_rows_inserted=0, 
                    total_rows_deleted=0, 
                    total_rows_updated=0
                )
            full_staging_df = self._current_staging_table_df
            record_count = full_staging_df.count()
            if full_staging_df is None or record_count == 0:
                self.logger.info(
                    f"FULL LOAD skipped: staging DataFrame is empty for {self._current_target_table_name}."
                )
                return LoadResult(
                    feed_id = self.config.master_specs['feed_id'], 
                    success=True, 
                    skipped=True, 
                    total_rows_inserted=0, 
                    total_rows_deleted=0,
                    total_rows_updated=0
                )
            if self.spark.catalog.tableExists(self._current_target_table_name) == True:
                previous_count = self.spark.table(self._current_target_table_name).count()
            else:
                previous_count = 0
            full_staging_df = self._enforce_schema(full_staging_df, StructType.fromJson(self.config.feed_specs['selection_schema']))
            (
                full_staging_df.write.
                format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(self._current_target_table_name)
            )
            self.logger.info(
                f"FULL LOAD completed successfully for {self._current_target_table_name} "
                f"({record_count} records loaded)."
            )
            return LoadResult(
                feed_id = self.config.master_specs['feed_id'],
                success=True, 
                total_rows_inserted=record_count,
                total_rows_updated=0,
                total_rows_deleted=previous_count
            )
        except Exception as e:
            raise DataLoadException(
                message=f"Feed ID: {self.config.master_specs['feed_id']}, Error during FULL LOAD for {self._current_target_table_name}: {str(e)}",
                original_exception=e
            )
