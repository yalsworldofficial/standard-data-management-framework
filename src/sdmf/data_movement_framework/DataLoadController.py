# inbuilt
import os
import logging
import configparser
from concurrent.futures import ProcessPoolExecutor, as_completed

# external
import pandas as pd

# internal
from sdmf.data_movement_framework.load_types.FullLoad import FullLoad
from sdmf.exception.DataLoadException import DataLoadException


# Your Spark ingestion class
class SparkIngestor:
    def __init__(self, spec):
        self.spec = spec

    def load_data(self):
        # Spark ingestion logic here
        print(f"Loading data for {self.spec['feed_name']}")
        # Simulate work
        return f"Completed {self.spec['feed_name']}"


class DataLoadController():
    def __init__(self, config: configparser.ConfigParser) -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.info('Data Load Controller has been initialized...')
        self.master_specs_path = os.path.join(config['DEFAULT']['file_hunt_path'], config['FILES']['master_spec_name'])
        self.master_specs_df = pd.DataFrame()


    def run(self):
        
        results = []
        with ProcessPoolExecutor(max_workers=None) as executor:  # Use all cores dynamically
            futures = [executor.submit(SparkIngestor(spec).load_data) for spec in self.parallel_batch]

            # Process sequential feeds while parallel tasks run
            for seq_spec in self.sequential_batch:
                result = SparkIngestor(seq_spec).load_data()
                results.append(result)

            # Collect parallel results
            for future in as_completed(futures):
                results.append(future.result())

        print("All results:", results)


    def __prepare(self):
        self.logger.info('Loading validated master specs...')
        self.master_specs_df = self.__load_master_specs()
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

    def __load_master_specs(self) -> pd.DataFrame:
        try:
            return pd.read_excel(io=self.master_specs_path, sheet_name='master_specs')
        except Exception as e:
            raise DataLoadException(
                message="Something went wrong while loading master specs.",
                load_type="NA",
                original_exception=e
            )
