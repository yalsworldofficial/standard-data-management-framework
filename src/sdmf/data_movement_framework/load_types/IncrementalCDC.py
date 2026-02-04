# inbuilt
import logging

# external
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

# internal
from sdmf.data_movement_framework.data_class.LoadResult import LoadResult
from sdmf.data_movement_framework.data_class.LoadConfig import LoadConfig
from sdmf.data_movement_framework.BaseLoadStrategy import BaseLoadStrategy
from sdmf.exception.DataLoadException import DataLoadException


class IncrementalCDC(BaseLoadStrategy):
    def __init__(self, config: LoadConfig, spark: SparkSession) -> None:
        super().__init__(config=config, spark=spark)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing INCREMENTAL_LOAD data transfer component...")

    def load(self) -> LoadResult:
        """
        Perform an Incremental Change Data Capture (CDC) load into the target Delta table.
        Uses primary/composite keys and a derived row hash to identify inserts, updates, and deletes.
        Merges incremental data (from staging layer) into the target Delta table.

        Returns:
            bool: True if successful, False otherwise.
        """

        self.logger.info(
            " ".join(
                [
                    "Perform an Incremental Change Data Capture (CDC) load into the target Delta table.",
                    "Uses primary/composite keys and a derived row hash to identify inserts, updates, and deletes.",
                    "Merges incremental data (from staging layer) into the target Delta table.",
                ]
            )
        )
        try:
            self._enforce_load_type_consistency()
            is_staging_layer_created = self._create_staging_layer()
            if not is_staging_layer_created:
                self.logger.warning(
                    "Incremental CDC aborted: staging layer not created."
                )
                return LoadResult(
                    feed_id = self.config.master_specs['feed_id'], 
                    success=False, 
                    skipped=False,
                    total_rows_inserted=0, 
                    total_rows_deleted=0, 
                    total_rows_updated=0
                )
            incr_df = self._current_staging_incremental_table_df
            incr_df = incr_df.drop(
                "_x_row_hash", "_x_commit_version", "_x_commit_timestamp"
            )
            if incr_df is None or incr_df.count() == 0:
                self.logger.warning(
                    f"No incremental changes found for {self._current_target_table_name}."
                )
                return LoadResult(
                    feed_id = self.config.master_specs['feed_id'], 
                    success=False, 
                    skipped=True,
                    total_rows_inserted=0, 
                    total_rows_deleted=0, 
                    total_rows_updated=0
                )
            target_table = self._current_target_table_name
            primary_key = self.config.feed_specs.get("primary_key")
            composite_keys = self.config.feed_specs.get("composite_key", [])
            all_keys = [primary_key] if primary_key else []
            all_keys.extend([k for k in composite_keys if k not in all_keys])
            if not all_keys:
                self.logger.error(
                    f"CDC Load failed: No keys (primary/composite) defined in feed_specs for {target_table}."
                )
                return LoadResult(
                    feed_id = self.config.master_specs['feed_id'], 
                    success=False, 
                    skipped=False,
                    total_rows_inserted=0, 
                    total_rows_deleted=0, 
                    total_rows_updated=0
                )
            non_key_columns = [
                c
                for c in incr_df.columns
                if c not in all_keys
                and c
                not in [
                    "_x_operation",
                    "_x_row_hash",
                    "_x_commit_version",
                    "_x_commit_timestamp",
                ]
            ]
            incr_df = incr_df.withColumn(
                "_x_row_hash",
                F.sha2(
                    F.concat_ws(
                        "||", *[F.col(c).cast("string") for c in non_key_columns]
                    ),
                    256,
                ),
            )
            if not self.spark.catalog.tableExists(target_table):
                self.logger.info(
                    f"Target table {target_table} not found — creating new table via CDC snapshot."
                )
                incr_df = self._enforce_schema(incr_df, StructType.fromJson(self.config.feed_specs['selection_schema']))
                (
                    incr_df.write.format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(target_table)
                )
                self.logger.info(
                    f"Created target table {target_table} with initial CDC data."
                )
                return LoadResult(
                    feed_id = self.config.master_specs['feed_id'], 
                    success=True, 
                    skipped=False,
                    total_rows_inserted=0, 
                    total_rows_deleted=0, 
                    total_rows_updated=0
                )
            delta_target = DeltaTable.forName(self.spark, target_table)
            target_df = delta_target.toDF()
            if target_df.columns != incr_df.columns:
                raise DataLoadException(
                    original_exception=None,
                    message=f"Target table {target_table} schema [{target_df.columns}] does not match incremental data schema [{incr_df.columns}]."
                )
            key_condition = " AND ".join([f"target.{k} = source.{k}" for k in all_keys])
            merge_condition = (
                f"{key_condition} AND target._x_row_hash <> source._x_row_hash"
            )
            self.logger.info(
                f"Performing CDC MERGE on [{target_table}] using merge condition [{merge_condition}]"
            )
            incr_df = self._enforce_schema(incr_df, StructType.fromJson(self.config.feed_specs['selection_schema']))
            (
                delta_target.alias("target")
                .merge(incr_df.alias("source"), merge_condition)
                .whenMatchedUpdate(
                    condition="source._x_operation IN ('update', 'insert') AND target._x_row_hash <> source._x_row_hash",
                    set={
                        c: f"source.{c}"
                        for c in incr_df.columns
                        if c
                        not in [
                            "_x_operation",
                            "_x_row_hash",
                            "_x_commit_version",
                            "_x_commit_timestamp",
                        ]
                    },
                )
                .whenMatchedDelete(condition="source._x_operation = 'delete'")
                .whenNotMatchedInsert(
                    values={
                        c: f"source.{c}"
                        for c in incr_df.columns
                        if c not in ["_x_commit_version", "_x_commit_timestamp"]
                    }
                )
                .execute()
            )
            self.spark.sql(
                f"ALTER TABLE {self._current_target_table_name} "
                f"SET TBLPROPERTIES ('data.load_type' = '{self.config.feed_specs['load_type']}')"
            )
            change_count = incr_df.count()
            self.logger.info(
                f"Incremental CDC MERGE completed for {target_table} ({change_count} records processed)."
            )
            return LoadResult(
                feed_id = self.config.master_specs['feed_id'], 
                success=True, 
                skipped=False,
                total_rows_inserted=0, 
                total_rows_deleted=0, 
                total_rows_updated=0
            )
        except Exception as e:
            raise DataLoadException(
                original_exception=e,
                message=f"Error during Incremental CDC load for {self._current_target_table_name}: {str(e)}"           
            )
