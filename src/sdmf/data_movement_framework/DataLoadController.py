# inbuilt
import os
import logging
import configparser
from concurrent.futures import ProcessPoolExecutor, as_completed

# external
import pandas as pd
from pyspark.sql import SparkSession

# internal
from sdmf.data_movement_framework.LoadDispatcher import LoadDispatcher
from sdmf.data_movement_framework.data_class.LoadResult import LoadResult


class DataLoadController():

    def __init__(self, spark: SparkSession, allowed_df: pd.DataFrame) -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.info('Data Load Controller has been initialized...')
        self.master_specs_df = allowed_df
        self.spark = spark
        self.load_results_list = []

    def run(self):
        self.__prepare()        
        self.load_results_list = self.__execute()
    
    def get_load_results(self) -> list[LoadResult]:
        return self.load_results_list

    def __execute(self) -> list[LoadResult]:
        results = []
        with ProcessPoolExecutor(max_workers=None) as executor:
            futures = []
            for batch in self.parallel_batch:
                self.logger.info(f'Processing parallel batch: {batch['parallelism_group_number']}...')
                for master_spec in batch['feeds_in_batch']:
                    futures.append(executor.submit(LoadDispatcher(master_spec=master_spec, spark=self.spark).dispatch))
        for batch in self.sequential_batch:
            self.logger.info(f'Sequential batch batch: {batch['parallelism_group_number']}...')
            for master_spec in batch['feeds_in_batch']:
                result = LoadDispatcher(master_spec=master_spec, spark=self.spark).dispatch()
                results.append(result)
        for future in as_completed(futures):
            results.append(future.result())
        return results

    def __prepare(self):
        self.logger.info('Loading validated master specs...')
        self.logger.info('Segregating parallel and sequential batches...')
        self.parallel_batch, self.sequential_batch = self.__segregate_feed_batches()

    def __segregate_feed_batches(self) -> tuple:
        grouped = self.master_specs_df.groupby('parallelism_group_number')
        all_parallel_batches = []
        all_sequential_batches = []
        for key, group in grouped:
            if len(group) > 1:
                self.logger.info(f"Parallel batch key: {key}, Total Feeds: {len(group)}")
                all_parallel_batches.append(
                    {
                        "parallelism_group_number":key,
                        "feeds_in_batch": group.to_dict(orient='records')
                    }
                )
            elif len(group) == 1:
                self.logger.info(f"Sequential batch key: {key}, Total Feeds: {len(group)}")
                all_sequential_batches.append(
                    {
                        "parallelism_group_number":key,
                        "feeds_in_batch": group.to_dict(orient='records')
                    }
                )
            self.logger.info(f"Total Parallel Batches: {len(all_parallel_batches)}, Total Sequential Batches: {len(all_sequential_batches)}")
        return all_parallel_batches, all_sequential_batches