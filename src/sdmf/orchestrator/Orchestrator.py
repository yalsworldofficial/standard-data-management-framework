# inbuilt
import logging
import uuid
import configparser

# external
from pyspark.sql import SparkSession

# internal
from sdmf.exception.SystemError import SystemError
from sdmf.result_generator.ResultGenerator import ResultGenerator
from sdmf.validation.SystemLaunchValidator import SystemLaunchValidator
from sdmf.data_quality.runner.FeedDataQualityRunner import FeedDataQualityRunner


class Orchestrator():

    def  __init__(self, spark: SparkSession, file_hunt_path: str, config: configparser.ConfigParser) -> None:
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.logger.info(
            f"""
            ============================================================
            Initializing System...
            {self.config['DEFAULT']['app_name']} ({self.config['DEFAULT']['version']})
            ============================================================

            Description:
            SDMF is a configurable, rule-driven data validation and ingestion
            framework designed to standardize, validate, and manage data
            pipelines with consistency, reliability, and scalability.

            Designed and Developed By:
            Harsh Handoo
            Data Engineer

            Supported Load Types:
            - FULL_LOAD          : Complete overwrite of target data
            - APPEND_LOAD        : Append-only ingestion
            - INCREMENTAL_CDC    : Change Data Capture (delta-based loads)
            - SCD_TYPE_2         : Slowly Changing Dimension Type 2

            Pre-run Checklist:
            - Ensure 'master_specs.xlsx' is present in the configured file_hunt_path
            - Validate all required feed specifications are defined
            - Confirm Spark session is initialized and accessible
            - Ensure source and target systems are reachable

            System Status:
            Framework initialization in progress...

            ============================================================
            """
        )
        self.spark = spark
        self.run_id = uuid.uuid4().hex
        self.file_hunt_path = file_hunt_path
        self.system_run_report = []

    def log_spark_cluster_info(self, spark: SparkSession, logger):
        conf = spark.conf


        logger.info(
            """Spark Runtime Information
            ============================================================
            Application Name : %s
            Spark Version    : %s
            Master URL       : %s
            Spark UI         : %s

            Driver Configuration
            ------------------------------------------------------------
            Driver Memory    : %s
            Driver Cores     : %s

            Executor Configuration
            ------------------------------------------------------------
            Executor Memory  : %s
            Executor Cores   : %s
            Executor Count   : %s

            SQL Configuration
            ------------------------------------------------------------
            Shuffle Partitions : %s

            """
            ,
            spark.sparkContext.appName if hasattr(spark, "sparkContext") else "UNKNOWN",
            spark.version,
            conf.get("spark.master", "NOT SET"),
            conf.get("spark.ui.enabled", "true"),
            conf.get("spark.driver.memory", "NOT SET"),
            conf.get("spark.driver.cores", "NOT SET"),
            conf.get("spark.executor.memory", "NOT SET"),
            conf.get("spark.executor.cores", "NOT SET"),
            conf.get("spark.executor.instances", "DYNAMIC / NOT SET"),
            conf.get("spark.sql.shuffle.partitions", "200")
        )


    def system_prerequisites(self):
        my_SystemLaunchValidator = SystemLaunchValidator(file_hunt_path=self.file_hunt_path, spark=self.spark, config = self.config)
        validation_result = my_SystemLaunchValidator.run()
        self.validated_master_specs_df = my_SystemLaunchValidator.get_validated_master_specs()
        self.system_run_report.append(validation_result.results_df)

    def run(self):
        # perform data quality
        self.logger.info('Ensuring system readiness...')
        self.system_prerequisites()
        self.log_spark_cluster_info(spark=self.spark, logger=self.logger)
        self.logger.info('System is up and ready.')
        self.logger.info('Starting Validate and Load....')
        self.validate_and_load()
        self.logger.info('System has finished processing data.')

    def validate_and_load(self):
        if self.validated_master_specs_df is None:
            raise SystemError(
                message = 'SDMF was not able to find validated specs.',
                details=None,
                original_exception=None
            )

        obj = FeedDataQualityRunner(self.spark, self.validated_master_specs_df.to_dict(orient="records"))
        obj.run()
        # ingest valid feeds
        obj.adhoc_post_load()
        all_feed_manifest = obj._finalize()
        my_ResultGenerator = ResultGenerator(all_feed_manifest, file_hunt_path=self.file_hunt_path, run_id=self.run_id, config=self.config)
        my_ResultGenerator.run()


    # def asd(self):

    #     pass
        
        
        # obj = FeedDataQualityRunner(self.spark, [out])
        # obj._run()
        # all_feed_manifest = obj._finalize()


        # def human_readable_duration(seconds: float) -> str:
        #     seconds = int(round(seconds))
        #     hours, remainder = divmod(seconds, 3600)
        #     minutes, secs = divmod(remainder, 60)

        #     if hours:
        #         return f"{hours} hours, {minutes} minutes, {secs} seconds"
        #     if minutes:
        #         return f"{minutes} minutes, {secs} seconds"
        #     return f"{secs} seconds"


        # for feed_manifest in all_feed_manifest:
        #     # Defensive copy (prevents accidental mutation)
        #     current_feed_manifest = dict(feed_manifest)

        #     can_ingest = current_feed_manifest.get("can_ingest", False)
        #     feed_name = current_feed_manifest.get("check_table_name", "unknown_feed")

        #     if not can_ingest:
        #         self.logger.info("Skipping feed %s: can_ingest is False or missing", feed_name)
        #         continue

        #     ingestion_start_time = datetime.now(timezone.utc)
        #     start_perf = time.perf_counter()

        #     status = "success"
        #     error_message = None

        #     try:
        #         # -------------------------
        #         # perform ingestion here
        #         # -------------------------

        #         time.sleep(5)

        #     except Exception as exc:
        #         status = "failed"
        #         error_message = str(exc)
        #         self.logger.exception("Ingestion failed for feed %s", feed_name)

        #     finally:
        #         ingestion_end_time = datetime.now(timezone.utc)
        #         total_time_taken = time.perf_counter() - start_perf

        #         # Example: attach metrics back to manifest or persist elsewhere
        #         feed_manifest.update({
        #             "ingestion_start_time": ingestion_start_time.isoformat(),
        #             "ingestion_end_time": ingestion_end_time.isoformat(),
        #             "total_time_taken_seconds": round(total_time_taken, 3),
        #             "total_time_taken_human": human_readable_duration(total_time_taken),
        #             "status": status,
        #             "error": error_message,
        #         })

        #         self.logger.info(
        #             "Feed %s ingestion %s in %.3f seconds",
        #             feed_name,
        #             status,
        #             total_time_taken,
        #         )
        # import json

        # print(json.dumps(all_feed_manifest))