import time
import logging
from sdmf.validation.old.FeedSpecValidation import FeedSpecValidation
from sdmf.data_quality.runner.FeedDataQualityRunner import FeedDataQualityRunner
from pyspark.sql import SparkSession
from datetime import datetime, timezone

from sdmf.validation.SystemLaunchValidator import SystemLaunchValidator


class Orchestrator():

    def  __init__(self, spark: SparkSession, file_hunt_path: str) -> None:
        self.logger = logging.getLogger(__name__)
        self.spark = spark
        self.file_hunt_path = file_hunt_path


    def setup(self):
        my_SystemLaunchValidator = SystemLaunchValidator(file_hunt_path=self.file_hunt_path, spark=self.spark)
        validation_result = my_SystemLaunchValidator.run()
        self.master_specs = my_SystemLaunchValidator.get_validated_master_specs()


        print(validation_result.results_df)


    # def run(self):
    #     obj = FeedSpecValidation("""
    #     {
    #         "primary_key": "alpha3-b",
    #         "composite_key": [],
    #         "partition_keys": [],
    #         "vacuum_hours": 168,
    #         "source_table_name":"demo.customers",
    #         "selection_schema": {
    #             "type": "struct",
    #             "fields": [
    #                 {
    #                     "name": "alpha3-b",
    #                     "type": "string",
    #                     "nullable": true
    #                 },
    #                 {
    #                     "name": "alpha3_t",
    #                     "type": "string",
    #                     "nullable": true
    #                 },
    #                 {
    #                     "name": "alpha2",
    #                     "type": "string",
    #                     "nullable": true
    #                 },
    #                 {
    #                     "name": "english",
    #                     "type": "string",
    #                     "nullable": true
    #                 }
    #             ]
    #         },
    #         "standard_checks": [
    #             {
    #                 "check_sequence": [
    #                     "_check_primary_key"
    #                 ],
    #                 "column_name": "alpha3-b",
    #                 "threshold": 0
    #             },
    #             {
    #                 "check_sequence": [
    #                     "_check_nulls"
    #                 ],
    #                 "column_name": "english",
    #                 "threshold": 0
    #             }
    #         ],
    #         "comprehensive_checks": [
    #             {
    #                 "check_name":"Some unique check name",
    #                 "query":"Select 1;",
    #                 "severity":"ERROR",
    #                 "threshold": 0,
    #                 "load_stage":"PRE_LOAD",
    #                 "dependency_dataset":[]
    #             },
    #             {
    #                 "check_name":"Some unique check name 1",
    #                 "query":"Select 1;",
    #                 "severity":"ERROR",
    #                 "threshold": 0,
    #                 "load_stage":"PRE_LOAD",
    #                 "dependency_dataset":[]
    #             },
    #             {
    #                 "check_name":"Some unique check name 2",
    #                 "query":"Select 1;",
    #                 "severity":"WARNING",
    #                 "threshold": 0,
    #                 "load_stage":"PRE_LOAD",
    #                 "dependency_dataset":[]
    #             },
    #             {
    #                 "check_name":"Some unique check name 3",
    #                 "query":"Select 1;",
    #                 "severity":"WARNING",
    #                 "threshold": 0,
    #                 "load_stage":"POST_LOAD",
    #                 "dependency_dataset":["demo.customers"]
    #             }
    #         ]
    #     }
    #     """ ,spark=self.spark)
    #     out = obj._validate()
    #     obj = FeedDataQualityRunner(self.spark, [out])
    #     obj._run()
    #     all_feed_manifest = obj._finalize()


    #     def human_readable_duration(seconds: float) -> str:
    #         seconds = int(round(seconds))
    #         hours, remainder = divmod(seconds, 3600)
    #         minutes, secs = divmod(remainder, 60)

    #         if hours:
    #             return f"{hours} hours, {minutes} minutes, {secs} seconds"
    #         if minutes:
    #             return f"{minutes} minutes, {secs} seconds"
    #         return f"{secs} seconds"


    #     for feed_manifest in all_feed_manifest:
    #         # Defensive copy (prevents accidental mutation)
    #         current_feed_manifest = dict(feed_manifest)

    #         can_ingest = current_feed_manifest.get("can_ingest", False)
    #         feed_name = current_feed_manifest.get("check_table_name", "unknown_feed")

    #         if not can_ingest:
    #             self.logger.info("Skipping feed %s: can_ingest is False or missing", feed_name)
    #             continue

    #         ingestion_start_time = datetime.now(timezone.utc)
    #         start_perf = time.perf_counter()

    #         status = "success"
    #         error_message = None

    #         try:
    #             # -------------------------
    #             # perform ingestion here
    #             # -------------------------

    #             time.sleep(5)

    #         except Exception as exc:
    #             status = "failed"
    #             error_message = str(exc)
    #             self.logger.exception("Ingestion failed for feed %s", feed_name)

    #         finally:
    #             ingestion_end_time = datetime.now(timezone.utc)
    #             total_time_taken = time.perf_counter() - start_perf

    #             # Example: attach metrics back to manifest or persist elsewhere
    #             feed_manifest.update({
    #                 "ingestion_start_time": ingestion_start_time.isoformat(),
    #                 "ingestion_end_time": ingestion_end_time.isoformat(),
    #                 "total_time_taken_seconds": round(total_time_taken, 3),
    #                 "total_time_taken_human": human_readable_duration(total_time_taken),
    #                 "status": status,
    #                 "error": error_message,
    #             })

    #             self.logger.info(
    #                 "Feed %s ingestion %s in %.3f seconds",
    #                 feed_name,
    #                 status,
    #                 total_time_taken,
    #             )
    #     import json

    #     print(json.dumps(all_feed_manifest))