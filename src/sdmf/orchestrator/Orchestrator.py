# inbuilt
import uuid
import logging
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
from sdmf.data_flow_diagram_generator.DataFlowDiagramGenerator import (
    DataFlowDiagramGenerator,
)


class Orchestrator:

    def __init__(self, spark: SparkSession, config: configparser.ConfigParser) -> None:
        self.config = config
        self.run_id = uuid.uuid4().hex
        self.my_LoggingConfig = LoggingConfig(run_id=self.run_id, config=config)
        self.my_LoggingConfig.configure()
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"""Welcome to SDMF — Standard Data Management Framework""")
        self.spark = spark
        self.file_hunt_path = config["DEFAULT"]["file_hunt_path"]
        self.system_run_report = pd.DataFrame()
        self.logger.info(f"Current Run Id: {self.run_id}")
        self.logger.info(
            f'Is FAIR: {spark.sparkContext.getConf().get("spark.scheduler.mode")}'
        )

    def __system_prerequisites(self) -> bool:
        my_SystemLaunchValidator = SystemLaunchValidator(
            file_hunt_path=self.file_hunt_path, spark=self.spark, config=self.config
        )
        validation_result = my_SystemLaunchValidator.run()
        self.validated_master_specs_df = (
            my_SystemLaunchValidator.get_validated_master_specs()
        )
        self.validated_master_specs_df2 = (
            my_SystemLaunchValidator.get_validated_master_specs()
        )
        self.system_run_report = validation_result.results_df
        return validation_result.passed
            

    def run(self):
        self.logger.info("Ensuring system readiness...")
        is_system_ready = self.__system_prerequisites()
        if is_system_ready == True:

            self.logger.info("System is up and ready.")
            self.logger.info("Validating and loading data...")
            self.__validate_and_load()
            self.logger.info("Generating lineage diagram...")
            self.__generate_lineage_diagram()
            self.logger.info("System has finished processing this batch.")
            self.logger.warning(
                "Saving logs to specified final log directory, no logs after this point will be retained in *.log file."
            )
            self.logger.info("==FINAL LOG==")
            self.my_LoggingConfig.move_logs_to_final_location()
            self.my_LoggingConfig.cleanup_final_logs()
            self.logger.info("System has finished processing data.")
            self.logger.info("Thanks for using SDMF.")
        else:
            self.logger.error("System is not ready, feeds will not be processed.")

    def __generate_lineage_diagram(self):
        try:
            my_DataFlowDiagramGenerator = DataFlowDiagramGenerator(
                validated_dataframe=self.validated_master_specs_df2,
                config=self.config,
                run_id=self.run_id,
            )
            my_DataFlowDiagramGenerator.run()
        except Exception as e:
            raise SystemError(
                "Something went wrong while generating Lineage diagram",
                details={},
                original_exception=e
            )

    def __validate_and_load(self):
        if self.validated_master_specs_df is None:
            raise SystemError(
                message="SDMF was not able to find validated specs.",
                details=None,
                original_exception=None,
            )
        extraction_df = self.validated_master_specs_df[
            self.validated_master_specs_df['data_flow_direction'] == 'SOURCE_TO_BRONZE'
        ]
        load_results = []
        if len(extraction_df) != 0:
            self.logger.info('Performing extraction as configured...')
            my_DataLoadController = DataLoadController(
                allowed_df=extraction_df, spark=self.spark, config=self.config
            )
            my_DataLoadController.run()
            res = my_DataLoadController.get_load_results()
            load_results.extend(res)
        self.validated_master_specs_df = self.validated_master_specs_df[
            self.validated_master_specs_df['data_flow_direction'] != 'SOURCE_TO_BRONZE'
        ]
        obj = FeedDataQualityRunner(
            self.spark, self.validated_master_specs_df.to_dict(orient="records")
        )
        obj.run()
        pre_load_mainifest = obj._finalize()
        can_ingest_feed_id = []
        for ready_for_ingest in pre_load_mainifest:
            if ready_for_ingest["can_ingest"] == True:
                can_ingest_feed_id.append(ready_for_ingest["feed_id"])
        self.logger.info(f"Valid Feed ID's: {can_ingest_feed_id}")
        if len(can_ingest_feed_id) > 0:
            allowed_df = self.validated_master_specs_df[
                self.validated_master_specs_df["feed_id"].isin(can_ingest_feed_id)
            ]
            my_DataLoadController = DataLoadController(
                allowed_df=allowed_df, spark=self.spark, config=self.config
            )
            my_DataLoadController.run()
            res2 = my_DataLoadController.get_load_results()
            load_results.extend(res2)
            obj.adhoc_post_load()
            all_feed_manifest = obj._finalize()
            my_ResultGenerator = ResultGenerator(
                all_feed_manifest,
                file_hunt_path=self.file_hunt_path,
                run_id=self.run_id,
                config=self.config,
                system_report=self.system_run_report,
                load_results=load_results,
            )
            my_ResultGenerator.run()
        else:
            self.logger.warning(
                f"No non-Extraction feeds passed their defined validation. Please check the run report in SDMF outbound directory with run id: [{self.run_id}]"
            )
            
