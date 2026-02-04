# inbuilt
import re
import uuid
import logging
from abc import ABC, abstractmethod

# external
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, TimestampType, StructType
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

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
        if self.config.target_unity_catalog == "testing":
            self._current_target_table_name = (
                f"{self.config.target_schema_name}.{self.config.target_table_name}"
            )
            self._staging_schema = f"staging"
        else:
            self._current_target_table_name = f"{self.config.target_unity_catalog}.{self.config.target_schema_name}.{self.config.target_table_name}"
            self._staging_schema = f"{self.config.target_unity_catalog}.staging"

        self.logger.info(
            f"Current Table Name: {self._current_target_table_name}, Staging Schema: {self._staging_schema}"
        )

    def _normalize_column_names(self, df: DataFrame) -> DataFrame:
        for col in df.columns:
            clean = col.strip().lower()

            # replace anything not a–z, 0–9, _ with _
            clean = re.sub(r"[^a-z0-9_]", "_", clean)

            # collapse multiple underscores
            clean = re.sub(r"_+", "_", clean)

            # optional: remove leading/trailing underscores
            clean = clean.strip("_")

            if clean != col:
                df = df.withColumnRenamed(col, clean)

        return df

    def _enforce_schema(self, df: DataFrame, schema: StructType):
        return df.select(
            *[
                (
                    F.col(f.name).cast(f.dataType).alias(f.name)
                    if f.name in df.columns
                    else F.lit(None).cast(f.dataType).alias(f.name)
                )
                for f in schema.fields
            ]
        )

    def execute(self) -> LoadResult:
        """
        Orchestrates the load lifecycle:
        """
        try:
            result = self.load()
            return result
        except Exception as e:
            raise DataLoadException(
                message="Somethine went wrong while executing data load",
                original_exception=e,
            )

    @abstractmethod
    def load(self) -> LoadResult:
        """W
        Core load logic implemented by subclass.W
        Should return LoadResult on success.
        """

    def __get_max_table_version(self, table_path_or_name: str) -> int:
        """
        Returns the latest version number of a Delta table.

        Args:
            table_path_or_name (str): Path (e.g. '/mnt/data/mytable')
                                    or table name (e.g. 'db.mytable')
        Returns:
            int: Latest delta table version number
        """
        delta_tbl = (
            DeltaTable.forName(self.spark, table_path_or_name)
            if not table_path_or_name.startswith("/")
            else DeltaTable.forPath(self.spark, table_path_or_name)
        )

        history_df = delta_tbl.history(1)  # get only the latest record
        return history_df.collect()[0]["version"]

    def _create_staging_layer(self) -> bool:
        """
        Staging Layer (MERGE + CDC) with partitioning and schema alignment:
        - FULL table is updated using MERGE on _x_row_hash.
        - INCR table contains only true inserts/updates/deletes from CDF.
        - Partitioning handled via self.config.feed_specs['partition_keys'].
        """
        spark = self.spark
        staging_schema = self._staging_schema
        full_table = f"{staging_schema}.t_full_{self.config.target_table_name}"
        incr_table = f"{staging_schema}.t_incr_{self.config.target_table_name}"
        all_changes_table = (
            f"{staging_schema}.t_incr_cdf_changes_{self.config.target_table_name}"
        )

        self.logger.info(
            (
                "\n=== Staging Layer Creation ===\n"
                "Function: [_create_staging_layer]\n"
                "Operation: MERGE + CDC with partitioning and schema alignment\n"
                f"FULL Table: [{full_table}]\n"
                f"INCR Table: [{incr_table}]\n"
                f"CDF Table: [{all_changes_table}]\n"
                f"Current Partitioning Scheme: {self.config.feed_specs['partition_keys']}\n"
                "================================"
            )
        )

        self.logger.info(f"Creating Schema [{staging_schema}] if it doesn't exist.")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {staging_schema}")
        try:
            df = (
                spark.sql(self.config.feed_specs["selection_query"])
                if self.config.feed_specs["selection_query"]
                else spark.read.table(self.config.feed_specs["source_table_name"])
            )
            latest_source_version = -9999
            if self.config.feed_specs["source_table_name"]:
                src_history = spark.sql(
                    f"DESCRIBE HISTORY {self.config.feed_specs['source_table_name']}"
                )
                agg_result = src_history.agg({"version": "max"}).first()

                if agg_result and agg_result[0] is not None:
                    latest_source_version = int(agg_result[0])
                else:
                    latest_source_version = None
            else:
                self.logger.info("Source Data is in the form of a query:")
                self.logger.info(self.config.feed_specs["selection_query"])
            if df is None or len(df.columns) == 0:
                self.logger.info("Source empty. Skipping staging.")
                return False
            full_exists = spark.catalog.tableExists(full_table)
            partition_mismatch = False
            if full_exists:
                table_details = spark.sql(f"DESCRIBE DETAIL {full_table}").first()
                if table_details is not None:
                    existing_partition_cols = list(table_details["partitionColumns"])
                    partition_mismatch = set(existing_partition_cols) != set(
                        self.config.feed_specs["partition_keys"]
                    )
                    if partition_mismatch:
                        self.logger.info(
                            f"Partition mismatch detected, Rebuilding FULL and INCR table from [{existing_partition_cols}] to [{self.config.feed_specs['partition_keys']}]"
                        )

                    props = spark.sql(f"SHOW TBLPROPERTIES {full_table}")
                    row = (
                        props.filter("key = '_x_latest_source_version'")
                        .select("value")
                        .first()
                    )
                    if row is not None:
                        version_prop = int(row[0])
                    else:
                        version_prop = None

                    self.logger.info(
                        f"Latest source version: {latest_source_version}, Latest table version: {version_prop}"
                    )
                    if version_prop == latest_source_version:
                        self.logger.warning("No new data to load.")
                        return False

                else:
                    existing_partition_cols = None

            df_cols = df.columns
            load_id = str(uuid.uuid4())
            self.logger.info(f"Current _x_load_id: {load_id}")
            df = df.withColumn(
                "_x_row_hash",
                F.sha2(
                    F.concat_ws("||", *[F.col(c).cast("string") for c in df_cols]), 256
                ),
            ).withColumn("_x_load_id", F.lit(load_id))

            if len(self.config.feed_specs["partition_keys"]) != 0:
                null_rows = df.filter(
                    " OR ".join(
                        [
                            f"{c} IS NULL"
                            for c in self.config.feed_specs["partition_keys"]
                        ]
                    )
                )
                if null_rows.count() > 0:
                    self.logger.error(
                        f"There are null values in the selected partition columns => [{self.config.feed_specs['partition_keys']}]"
                    )
                    return False
            if full_exists == False or partition_mismatch == True:
                writer = df.write.format("delta").mode("overwrite")
                if partition_mismatch:
                    writer = writer.option("overwriteSchema", "true")
                writer.partitionBy(
                    *self.config.feed_specs["partition_keys"]
                ).saveAsTable(full_table)
                if (
                    self.config.feed_specs["selection_query"] == ""
                    or self.config.feed_specs["selection_query"] == None
                ):
                    self.logger.info(
                        f"Updating _x_latest_source_version in full table [{full_table}] with {latest_source_version}"
                    )
                    spark.sql(
                        f"""ALTER TABLE {full_table} SET TBLPROPERTIES (
                        '_x_latest_source_version' = '{latest_source_version}',
                        'delta.autoOptimize.autoCompact' = 'false',
                        'delta.autoOptimize.optimizeWrite' = 'false'
                    )"""
                    )
                spark.sql(
                    f"ALTER TABLE {full_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
                )
                incr_df = (
                    df.withColumn("_x_operation", F.lit("insert"))
                    .withColumn(
                        "_x_commit_version",
                        F.lit(self.__get_max_table_version(full_table)).cast(
                            LongType()
                        ),
                    )
                    .withColumn(
                        "_x_commit_timestamp",
                        F.lit(F.current_timestamp()).cast(TimestampType()),
                    )
                )
                incr_writer = incr_df.write.format("delta").mode("overwrite")
                if partition_mismatch:
                    incr_writer = incr_writer.option("overwriteSchema", "true")
                incr_writer.partitionBy(
                    *self.config.feed_specs["partition_keys"]
                ).saveAsTable(incr_table)
                self._current_staging_table_df = df
                self._current_staging_incremental_table_df = incr_df
                self.logger.info(
                    f"First load completed (FULL + INCR) | PARTION REBUILD: [{partition_mismatch}]"
                )
                return True
            current_version = self.__get_max_table_version(full_table)
            inc_data = f"incoming_data_{self.config.master_specs["feed_id"]}"
            df.createOrReplaceTempView(inc_data)
            primary_key = self.config.feed_specs.get("primary_key")
            composite_keys = self.config.feed_specs.get("composite_key", [])
            all_keys = [primary_key] if primary_key else []
            all_keys.extend([k for k in composite_keys if k not in all_keys])
            merge_condition = " AND ".join([f"tgt.{c} = src.{c}" for c in all_keys])
            data_cols = [c for c in df.columns if c not in composite_keys]
            data_cols_ins = [c for c in df.columns]
            set_clause = ", ".join([f"tgt.{c} = src.{c}" for c in data_cols])
            insert_cols = ", ".join(data_cols_ins)
            insert_vals = ", ".join([f"src.{c}" for c in data_cols_ins])
            self.logger.info(f"Target Staging Table: [{full_table}]")
            self.logger.info(f"Merge Condition: [{merge_condition}]")
            self.logger.info(f"Set Clause: [{set_clause}]")
            self.logger.info(f"Insert clause: [{data_cols_ins}]")
            merge_query = f"""

            MERGE INTO 
                {full_table} AS tgt
            USING 
                {inc_data} AS src
            ON 
                {merge_condition}
            WHEN MATCHED AND tgt._x_row_hash != src._x_row_hash THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
            WHEN NOT MATCHED BY SOURCE THEN
                DELETE

            """
            self.logger.info(merge_query)
            spark.sql(merge_query)
            new_version = self.__get_max_table_version(full_table)
            self.logger.info(
                f"Current Version: [{current_version}], New Version after Merge: [{new_version}]"
            )
            cdf_df = (
                spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", current_version + 1)
                .option("endingVersion", new_version)
                .table(full_table)
            )
            update_pre = cdf_df.filter("_change_type = 'update_preimage'")
            update_post = cdf_df.filter("_change_type = 'update_postimage'")
            true_updates = (
                (
                    update_post.alias("post")
                    .join(update_pre.alias("pre"), on=all_keys, how="left")
                    .filter("post._x_row_hash != pre._x_row_hash")
                )
                .select("post.*")
                .withColumn("_x_operation", F.lit("update"))
            )

            true_inserts = cdf_df.filter("_change_type = 'insert'").withColumn(
                "_x_operation", F.lit("insert")
            )
            true_deletes = cdf_df.filter("_change_type = 'delete'").withColumn(
                "_x_operation", F.lit("delete")
            )
            true_updates = true_updates.drop("_change_type").filter(
                f"_x_load_id = '{load_id}'"
            )
            true_inserts = true_inserts.drop("_change_type").filter(
                f"_x_load_id = '{load_id}'"
            )
            true_deletes = true_deletes.drop("_change_type")

            incr_df = (
                (true_inserts.unionByName(true_updates).unionByName(true_deletes))
                .withColumnRenamed("_commit_version", "_x_commit_version")
                .withColumnRenamed("_commit_timestamp", "_x_commit_timestamp")
            )
            incr_df.write.format("delta").mode("overwrite").partitionBy(
                *self.config.feed_specs["partition_keys"]
            ).saveAsTable(incr_table)
            cdf_df.write.format("delta").mode("overwrite").partitionBy(
                *self.config.feed_specs["partition_keys"]
            ).saveAsTable(all_changes_table)
            if self.config.feed_specs["selection_query"]:
                spark.sql(
                    f"""
                    ALTER TABLE {full_table}
                    SET TBLPROPERTIES ('_x_latest_source_version' = '{latest_source_version}')
                """
                )
            self.logger.info(
                f"INCR updated using MERGE + TRUE CDC logic (Δ {current_version} → {new_version}). "
                f"Affected Records: {incr_df.count()}"
            )
            if (
                self.config.feed_specs["selection_query"] == ""
                or self.config.feed_specs["selection_query"] is None
            ):
                self.logger.info(
                    f"Updating _x_latest_source_version in full table [{full_table}] with {latest_source_version}"
                )
                spark.sql(
                    f"""ALTER TABLE {full_table} SET TBLPROPERTIES ('_x_latest_source_version' = '{latest_source_version}')"""
                )
            self._current_staging_table_df = spark.read.table(full_table)
            self._current_staging_incremental_table_df = spark.read.table(incr_table)
            self.logger.info(
                f"INCR updated using MERGE + CDF (Δ {current_version} → {new_version}). Affected Records {incr_df.count()}"
            )
            return True
        except Exception as e:
            raise DataLoadException(
                message=f"Error in staging layer for {self.config.feed_specs['source_table_name']}",
                original_exception=e,
            )

    def _enforce_load_type_consistency(self) -> None:
        """
        Enforces that a target Delta table can only ever be loaded with a single load_type.
        Once a load_type has been applied, switching to another type is disallowed.
        This is enforced using Delta table properties.
        """
        target_table = self._current_target_table_name
        current_type = self.config.master_specs["load_type"]
        try:
            if self.spark.catalog.tableExists(target_table):
                props_df = self.spark.sql(f"SHOW TBLPROPERTIES {target_table}")
                existing_type_row = (
                    props_df.filter(F.col("key") == "data.load_type")
                    .select("value")
                    .collect()
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
                            original_exception=None,
                        )
                    else:
                        self.logger.info(
                            f"Verified consistent load_type '{existing_type}' for {target_table}."
                        )
                else:
                    self.spark.sql(
                        f"ALTER TABLE {target_table} SET TBLPROPERTIES ('data.load_type' = '{current_type}')"
                    )
                    self.logger.info(
                        f"Registered load_type '{current_type}' for existing table {target_table}."
                    )
            else:
                self.logger.info(
                    f"Target table {target_table} not found yet — will set load_type on creation."
                )

        except Exception as e:
            raise DataLoadException(
                message="Something went wrong while enforcing load type consistency",
                original_exception=e,
            )
