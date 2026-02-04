# inbuilt
import json
import time
import configparser

# external
from pyspark.sql import SparkSession

# internal
from sdmf.data_movement_framework.load_types.FullLoad import FullLoad
from sdmf.data_movement_framework.load_types.AppendLoad import AppendLoad
from sdmf.data_movement_framework.load_types.IncrementalCDC import IncrementalCDC
from sdmf.data_movement_framework.load_types.SCDType2 import SCDType2
from sdmf.data_movement_framework.load_types.APIExtractor import APIExtractor
from sdmf.data_movement_framework.load_types.StorageFetch import StorageFetch
from sdmf.data_movement_framework.data_class.LoadConfig import LoadConfig
from sdmf.data_movement_framework.data_class.LoadResult import LoadResult

class LoadDispatcher():
    def __init__(self, master_spec: dict, spark: SparkSession, config: configparser.ConfigParser) -> None:
        self.master_spec = master_spec
        self.spark = spark
        self.config = config

    def __format_duration(self, seconds: float) -> str:
        seconds = round(seconds, 2)
        hours, remainder = divmod(seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        parts = []
        if hours > 0:
            parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes > 0:
            parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        if secs > 0 or not parts:
            parts.append(f"{secs} second{'s' if secs != 1 else ''}")
        return ", ".join(parts[:-1]) + (" and " + parts[-1] if len(parts) > 1 else parts[-1])

    def dispatch(self) -> LoadResult:
        self.spark.sparkContext.setLocalProperty(
            "spark.scheduler.pool",
            f"group_{self.master_spec['parallelism_group_number']}"
        )
        start_time = time.time()
        # is_extraction = True if self.master_spec.get('data_flow_direction', "") == 'EXTRACTION' else False
        config = LoadConfig(
            config=self.config,
            master_specs=self.master_spec,
            feed_specs=json.loads(self.master_spec.get('feed_specs', '{}')),
            target_unity_catalog= self.master_spec.get('target_unity_catalog', ""),
            target_schema_name=self.master_spec.get('target_schema_name', ""),
            target_table_name=self.master_spec.get('target_table_name', "")
        )
        load_type_map = {
            'FULL_LOAD': FullLoad,
            "APPEND_LOAD": AppendLoad,
            "INCREMENTAL_CDC": IncrementalCDC,
            "SCD_TYPE_2": SCDType2,

            # extraction
            "API_EXTRACTOR": APIExtractor,
            "STORAGE_FETCH":StorageFetch
        }

        load_class = load_type_map.get(self.master_spec.get('load_type', ""))
        if not load_class:
            raise ValueError(f"Unsupported load type: {self.master_spec.get('load_type')}")
        loader = load_class(config=config, spark=self.spark)
        exception_if_any = None
        try:
            load_result = loader.load()
        except Exception as e:
            exception_if_any = e
            load_result = LoadResult(feed_id = self.master_spec['feed_id'], success=False, total_rows_inserted=0, total_rows_deleted=0, total_rows_updated=0)
        end_time = time.time()
        load_result.start_epoch = start_time
        load_result.end_epoch = end_time
        load_result.total_human_readable_time = self.__format_duration(end_time - start_time)
        load_result.exception_if_any = exception_if_any
        load_result.source_table_path = self.master_spec.get('source_unity_catalog', '')
        load_result.target_table_path = f"{self.master_spec.get('target_unity_catalog', '')}.{self.master_spec.get('target_schema_name', '')}.{self.master_spec.get('target_table_name', '')}"
        load_result.data_flow_direction = self.master_spec.get('data_flow_direction', '')
        return load_result


