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


class AppendLoad(BaseLoadStrategy):

    def __init__(self, config: LoadConfig, spark: SparkSession) -> None:
        super().__init__(config=config, spark=spark)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing APPEND_LOAD data transfer component...")

    def load(self):
        try:
            self.logger.info("Perform a Append load into the target table in Databricks Unity Catalog.",)
            self._enforce_load_type_consistency()

            is_staging_layer_created = self._create_staging_layer()
            if not is_staging_layer_created:
                self.logger.error(f"APPEND LOAD failed for {self._current_target_table_name}: Staging Layer wasn't created.")
                return LoadResult(
                    feed_id = self.config.master_specs['feed_id'], 
                    success=False,
                    total_rows_inserted=0, 
                    total_rows_deleted=0, 
                    total_rows_updated=0
                )

            incremental_df = self._current_staging_incremental_table_df
            incremental_df = incremental_df.filter("_x_operation = 'insert'")


            inc_count = incremental_df.count()

            if incremental_df is None or inc_count == 0:
                self.logger.info(f"No new data found to append for {self._current_target_table_name}.")
                return LoadResult(
                    feed_id = self.config.master_specs['feed_id'], 
                    success=False,
                    skipped=True,
                    total_rows_inserted=0, 
                    total_rows_deleted=0, 
                    total_rows_updated=0
                )
            
            self.logger.info(f"Found [{inc_count}] new records to append for {self._current_target_table_name}.")

            incremental_df = self._enforce_schema(incremental_df, StructType.fromJson(self.config.feed_specs['selection_schema']))
            incremental_df.write.format("delta").mode("append").saveAsTable(self._current_target_table_name)
            self.spark.sql(
                f"ALTER TABLE {self._current_target_table_name} SET TBLPROPERTIES ('data.load_type' = '{self.config.master_specs["load_type"]}')"
            )

            self.logger.info(f"APPEND LOAD completed successfully for {self._current_target_table_name}")
            return LoadResult(
                feed_id = self.config.master_specs['feed_id'], 
                success=False,
                total_rows_inserted=inc_count, 
                total_rows_deleted=0, 
                total_rows_updated=0
            )

        except Exception as e:
            raise DataLoadException(
                original_exception=e,
                message=f"Error during APPEND_LOAD for {self._current_target_table_name}: {str(e)}"           
            )