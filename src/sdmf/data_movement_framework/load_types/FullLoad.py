# inbuilt
import logging

# internal
from sdmf.data_movement_framework.data_class.LoadConfig import LoadConfig
from sdmf.data_movement_framework.data_class.LoadResult import LoadResult
from sdmf.data_movement_framework.BaseLoadStrategy import BaseLoadStrategy
from sdmf.exception.DataLoadException import DataLoadException

class FullLoad(BaseLoadStrategy):

    def __init__(self) -> None:
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
                logger.error(f"❌ FULL LOAD failed for {self._current_target_table_name}: Staging Layer wasn't created.")
                return False

            # Step 2: Retrieve the staging DataFrame
            full_staging_df = self._current_staging_table_df
            if full_staging_df is None or full_staging_df.count() == 0:
                logger.info(f"⚠️ FULL LOAD skipped: staging DataFrame is empty for {self._current_target_table_name}.")
                return True
    

            (
                full_staging_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")  # allow schema evolution during full refresh
                .saveAsTable(self._current_target_table_name)
            )
            self.spark.sql(
                f"ALTER TABLE {self._current_target_table_name} SET TBLPROPERTIES ('data.load_type' = '{self.load_type}')"
            )

            record_count = full_staging_df.count()
            logger.info(f"✅ FULL LOAD completed successfully for {self._current_target_table_name} "
                f"({record_count} records loaded).")
            return True

        except Exception as e:
            raise FullLoadError(
                target_table=self._current_target_table_name, 
                original_exception=e, 
                message = f"Error during FULL LOAD for {self._current_target_table_name}: {str(e)}"
            )
        return LoadResult(
            success=True
        )