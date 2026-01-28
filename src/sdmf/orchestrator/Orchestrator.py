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
from sdmf.data_flow_diagram_generator.DataFlowDiagramGenerator import DataFlowDiagramGenerator

class Orchestrator():

    def  __init__(self, spark: SparkSession, config: configparser.ConfigParser) -> None:
        self.config = config
        self.run_id = uuid.uuid4().hex
        self.my_LoggingConfig = LoggingConfig(run_id=self.run_id, config=config)
        self.my_LoggingConfig.configure()
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"""Welcome to SDMF — Standard Data Management Framework""")
        self.spark = spark
        self.file_hunt_path = config['DEFAULT']['file_hunt_path']
        self.system_run_report = pd.DataFrame()
        self.logger.info(f"Current Run Id: {self.run_id}")
        self.logger.info(f'Is FAIR: {spark.sparkContext.getConf().get("spark.scheduler.mode")}')

    def __system_prerequisites(self):
        my_SystemLaunchValidator = SystemLaunchValidator(file_hunt_path=self.file_hunt_path, spark=self.spark, config = self.config)
        validation_result = my_SystemLaunchValidator.run()
        self.validated_master_specs_df = my_SystemLaunchValidator.get_validated_master_specs()
        self.system_run_report = validation_result.results_df

    def run(self):
        self.logger.info('Ensuring system readiness...')
        self.__system_prerequisites()
        self.logger.info('System is up and ready.')
        self.logger.info('Validating and loading data...')
        self.__validate_and_load()
        self.logger.info('Generating lineage diagram...')
        self.__generate_lineage_diagram()
        self.logger.info('System has finished processing this batch.')
        self.logger.warning('Saving logs to specified final log directory, no logs after this point will be retained in *.log file.')
        self.logger.info('==FINAL LOG==')
        self.my_LoggingConfig.move_logs_to_final_location()
        self.my_LoggingConfig.cleanup_final_logs()
        self.logger.info('System has finished processing data.')
        self.logger.info('Thanks for using SDMF.')

    def __generate_lineage_diagram(self):
        my_DataFlowDiagramGenerator = DataFlowDiagramGenerator(
            validated_dataframe=self.validated_master_specs_df,
            config=self.config,
            run_id=self.run_id
        )
        my_DataFlowDiagramGenerator.run()

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
            my_DataLoadController = DataLoadController(allowed_df=allowed_df, spark=self.spark, config = self.config)
            my_DataLoadController.run()
            load_results = my_DataLoadController.get_load_results()
            obj.adhoc_post_load()
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
        else:
            self.logger.warning(f'No feeds passed their defined validation, Data transfer has been cancelled for this run. Please check the run report in SDMF outbound directory with run id: [{self.run_id}]')
        