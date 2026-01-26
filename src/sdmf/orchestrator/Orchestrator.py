# inbuilt
import logging
import uuid
import configparser

# external
import pandas as pd
from pyspark.sql import SparkSession

# internal
from sdmf.config.LoggingConfig import LoggingConfig
from sdmf.exception.SystemError import SystemError
from sdmf.result_generator.ResultGenerator import ResultGenerator
from sdmf.validation.SystemLaunchValidator import SystemLaunchValidator
from sdmf.data_quality.runner.FeedDataQualityRunner import FeedDataQualityRunner
from sdmf.data_movement_framework.DataLoadController import DataLoadController

class Orchestrator():

    def  __init__(self, spark: SparkSession, config: configparser.ConfigParser) -> None:
        self.config = config
        LoggingConfig().configure()
        self.logger = logging.getLogger(__name__)
        self.run_id = uuid.uuid4().hex
        self.logger.info(
            f"""
            Welcome to SDMF — Standard Data Management Framework
            Designed and Developed By: Harsh Handoo (Data Engineer)
            Supported Load Types: FULL_LOAD, APPEND_LOAD, INCREMENTAL_CDC, SCD_TYPE_2
            CURRENT RUN ID: {self.run_id}
            """
        )
        self.spark = spark
        self.file_hunt_path = config['DEFAULT']['file_hunt_path']
        self.system_run_report = pd.DataFrame()

    # def __log_spark_cluster_info(self, spark: SparkSession, logger):
    #     conf = spark.conf
    #     logger.info(
    #         """Spark Runtime Information
    #         ============================================================
    #         Application Name : %s
    #         Spark Version    : %s
    #         Master URL       : %s
    #         Spark UI         : %s

    #         Driver Configuration
    #         ------------------------------------------------------------
    #         Driver Memory    : %s
    #         Driver Cores     : %s

    #         Executor Configuration
    #         ------------------------------------------------------------
    #         Executor Memory  : %s
    #         Executor Cores   : %s
    #         Executor Count   : %s

    #         SQL Configuration
    #         ------------------------------------------------------------
    #         Shuffle Partitions : %s

    #         """
    #         ,
    #         spark.sparkContext.appName if hasattr(spark, "sparkContext") else "UNKNOWN",
    #         spark.version,
    #         conf.get("spark.master", "NOT SET"),
    #         conf.get("spark.ui.enabled", "true"),
    #         conf.get("spark.driver.memory", "NOT SET"),
    #         conf.get("spark.driver.cores", "NOT SET"),
    #         conf.get("spark.executor.memory", "NOT SET"),
    #         conf.get("spark.executor.cores", "NOT SET"),
    #         conf.get("spark.executor.instances", "DYNAMIC / NOT SET"),
    #         conf.get("spark.sql.shuffle.partitions", "200")
    #     )


    def __system_prerequisites(self):
        my_SystemLaunchValidator = SystemLaunchValidator(file_hunt_path=self.file_hunt_path, spark=self.spark, config = self.config)
        validation_result = my_SystemLaunchValidator.run()
        self.validated_master_specs_df = my_SystemLaunchValidator.get_validated_master_specs()
        self.system_run_report = validation_result.results_df

    def run(self):
        # perform data quality
        self.logger.info('Ensuring system readiness...')
        self.__system_prerequisites()
        # self.__log_spark_cluster_info(spark=self.spark, logger=self.logger)
        self.logger.info('System is up and ready.')
        self.logger.info('Starting Validate and Load....')
        self.__validate_and_load()
        self.logger.info('System has finished processing data.')

    def __validate_and_load(self):
        if self.validated_master_specs_df is None:
            raise SystemError(
                message = 'SDMF was not able to find validated specs.',
                details=None,
                original_exception=None
            )
        obj = FeedDataQualityRunner(self.spark, self.validated_master_specs_df.to_dict(orient="records"))
        obj.run()
        pre_load_mainifest = obj._finalize()
        can_ingest_feed_id = []
        for ready_for_ingest in pre_load_mainifest:
            if ready_for_ingest['can_ingest'] == True:
                can_ingest_feed_id.append(ready_for_ingest['feed_id'])
        self.logger.info(f"Valid Feed ID's: {can_ingest_feed_id}")
        load_results = []
        if len(can_ingest_feed_id) > 0:
            allowed_df = self.validated_master_specs_df[self.validated_master_specs_df["feed_id"].isin(can_ingest_feed_id)]
            my_DataLoadController = DataLoadController(allowed_df=allowed_df, spark=self.spark)
            my_DataLoadController.run()
            load_results = my_DataLoadController.get_load_results()
            obj.adhoc_post_load()
        else:
            self.logger.warning(f'No feeds passed their defined validation, Data transfer has been cancelled for this run. Please check the run report in SDMF outbound directory with run id: [{self.run_id}]')
        all_feed_manifest = obj._finalize()
        my_ResultGenerator = ResultGenerator(
            all_feed_manifest, 
            file_hunt_path=self.file_hunt_path, 
            run_id=self.run_id, 
            config=self.config,
            system_report = self.system_run_report,
            load_results = load_results
        )
        my_ResultGenerator.run()