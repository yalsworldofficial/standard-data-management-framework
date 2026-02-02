# inbuilt
import os
import logging
import configparser
from concurrent.futures import ThreadPoolExecutor, as_completed


# external
import pandas as pd
from pyspark.sql import SparkSession

# internal
from sdmf.data_movement_framework.LoadDispatcher import LoadDispatcher
from sdmf.data_movement_framework.data_class.LoadResult import LoadResult


class DataLoadController:

    def __init__(
        self,
        spark: SparkSession,
        allowed_df: pd.DataFrame,
        config: configparser.ConfigParser,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.info("Data Load Controller has been initialized...")
        self.master_specs_df = allowed_df
        self.spark = spark
        self.load_results_list = []
        self.config = config

    def run(self):
        self.__prepare()
        self.load_results_list = self.__execute()

    def get_load_results(self) -> list[LoadResult]:
        return self.load_results_list

    def __run_group(self, group) -> list[LoadResult]:
        results = []

        feeds = group["feeds_in_group"]
        group_id = group["parallelism_group_number"]

        self.logger.info(f"Starting group {group_id} with {len(feeds)} feed(s)")

        if len(feeds) > 1:
            with ThreadPoolExecutor(max_workers=len(feeds)) as executor:

                futures = [
                    executor.submit(
                        LoadDispatcher(master_spec=feed, spark=self.spark, config=self.config).dispatch
                    )
                    for feed in feeds
                ]

                for future in as_completed(futures):
                    results.append(future.result())

        else:
            feed = feeds[0]
            results.append(
                LoadDispatcher(master_spec=feed, spark=self.spark, config=self.config).dispatch()
            )

        return results

    def __execute(self) -> list[LoadResult]:
        results = []

        for group in self.execution_groups:
            results.extend(self.__run_group(group))

        return results

    def __prepare(self):
        self.logger.info("Loading validated master specs...")
        self.logger.info("Building ordered execution groups...")

        # Sort first to guarantee execution order
        grouped = self.master_specs_df.sort_values("parallelism_group_number").groupby(
            "parallelism_group_number", sort=False
        )

        self.execution_groups = []

        for key, group in grouped:
            self.logger.info(f"Execution group {key}, Total Feeds: {len(group)}")

            self.execution_groups.append(
                {
                    "parallelism_group_number": key,
                    "feeds_in_group": group.to_dict(orient="records"),
                }
            )
