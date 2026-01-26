# inbuilt
import logging

# external
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# internal
from sdmf.data_movement_framework.data_class.LoadResult import LoadResult
from sdmf.data_movement_framework.BaseLoadStrategy import BaseLoadStrategy
from sdmf.exception.DataLoadException import DataLoadException


class SCDType2(BaseLoadStrategy):

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing APPEND_LOAD data transfer component...")

    def load(self):
        try:
            self.logger.info("Perform SCD Type-2 load using a true Delta IDENTITY surrogate key.")
            self.logger.info("Executing SCD Type-2 load.")
            self._enforce_load_type_consistency()
            if not self._create_staging_layer():
                self.logger.warning("Incremental CDC aborted: staging layer not created.")
                return LoadResult(
                    feed_id = self.config.master_specs['feed_id'], 
                    success=False,
                    total_rows_inserted=0, 
                    total_rows_deleted=0, 
                    total_rows_updated=0
                )
            incr_df = self._current_staging_incremental_table_df
            incr_df = incr_df.drop("_x_row_hash", "_x_commit_version", "_x_commit_timestamp")
            if incr_df is None or incr_df.count() == 0:
                self.logger.warning(f"No CDC changes found for {self._current_target_table_name}.")
                return LoadResult(
                    feed_id = self.config.master_specs['feed_id'], 
                    success=False,
                    skipped=True,
                    total_rows_inserted=0, 
                    total_rows_deleted=0, 
                    total_rows_updated=0
                )
            target_table = self._current_target_table_name
            primary_key = self.config.feed_specs["primary_key"]
            composite_keys = self.config.feed_specs["composite_key"]
            all_keys = [primary_key] if primary_key else []
            all_keys.extend([k for k in composite_keys if k not in all_keys])
            if not all_keys:
                self.logger.error(f"No keys defined for SCD2 load: {target_table}")
                return LoadResult(
                    feed_id = self.config.master_specs['feed_id'], 
                    success=False,
                    total_rows_inserted=0, 
                    total_rows_deleted=0, 
                    total_rows_updated=0
                )
            non_key_columns = [
                c for c in incr_df.columns
                if c not in all_keys and c not in ["_x_operation"]
            ]
            sort_cols = [F.col(k).asc() for k in all_keys]
            incr_df = incr_df.orderBy(*sort_cols)
            incr_df = incr_df.withColumn(
                "_x_row_hash",
                F.sha2(
                    F.concat_ws("||", *[F.col(c).cast("string") for c in non_key_columns]),
                    256
                )
            )
            incr_df = incr_df.withColumn("_x_date_from", F.current_timestamp())
            incr_df = incr_df.withColumn("_x_date_to", F.to_timestamp(F.lit("9999-12-31 23:59:59")))
            incr_df = incr_df.withColumn("_x_is_active", F.lit(1))
            incr_df = incr_df.withColumn("_x_last_modification_timestamp", F.current_timestamp())
            incr_df = incr_df.withColumn("_x_last_operation", F.col("_x_operation"))
            if not self.spark.catalog.tableExists(target_table):


                schema_cols = [
                    f"{field.name} {field.dataType.simpleString()}"
                    for field in incr_df.schema
                    if field.name not in ["_x_operation"]  # safety
                ]

                ddl = f"""
                CREATE TABLE {target_table} (
                    _x_surrogate_key BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                    {", ".join(schema_cols)}
                ) USING DELTA
                """

                self.spark.sql(ddl)

                (
                    incr_df
                        .drop("_x_operation")
                        .write
                        .format("delta")
                        .mode("append")
                        .saveAsTable(target_table)
                )

                self.logger.info(f"Created SCD-2 table {target_table} with IDENTITY surrogate key.")
                return LoadResult(
                    feed_id = self.config.master_specs['feed_id'], 
                    success=True,
                    total_rows_inserted=0, 
                    total_rows_deleted=0, 
                    total_rows_updated=0
                )
            target = DeltaTable.forName(self.spark, target_table)
            key_cond = " AND ".join([f"t.{k} = s.{k}" for k in all_keys])
            close_cond = (
                f"{key_cond} AND t._x_is_active = 1 AND ("
                f"t._x_row_hash <> s._x_row_hash OR s._x_operation = 'delete'"
                ")"
            )
            (
                target.alias("t")
                    .merge(incr_df.alias("s"), close_cond)
                    .whenMatchedUpdate(set={
                            "_x_is_active": "0",
                            "_x_date_to": "current_timestamp()",
                            "_x_last_modification_timestamp": "current_timestamp()",
                            "_x_last_operation": "s._x_operation"
                    })
                    .execute()
            )
            insert_on = f"{key_cond} AND t._x_is_active = 1 AND t._x_row_hash = s._x_row_hash"
            (
                target.alias("t")
                .merge(incr_df.alias("s"), insert_on)
                .whenNotMatchedInsert(
                    condition="s._x_operation != 'delete'",   # do not insert deletes
                    values={
                        c: f"s.{c}"
                        for c in incr_df.columns
                        if c not in ["_x_commit_version", "_x_commit_timestamp", "_x_operation"]  # drop ETL metadata
                    }
                )
                .execute()
            )
            self.spark.sql(
                f"ALTER TABLE {target_table} "
                f"SET TBLPROPERTIES ('data.load_type' = '{self.config.feed_specs["load_type"]}')"
            )
            self.logger.info(f"SCD-2 load completed: {target_table}")
            return LoadResult(
                feed_id = self.config.master_specs['feed_id'], 
                success=True,
                total_rows_inserted=0, 
                total_rows_deleted=0, 
                total_rows_updated=0
            )

        except Exception as e:
            raise DataLoadException(
                load_type=self.config.feed_specs["load_type"],
                original_exception=e,
                message=f"Error during APPEND_LOAD for {self._current_target_table_name}: {str(e)}"           
            )