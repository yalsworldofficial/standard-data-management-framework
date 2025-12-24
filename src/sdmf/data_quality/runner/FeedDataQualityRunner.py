# sdmf/data_quality/runner/data_quality_runner.py

import logging
from pyspark.sql import SparkSession
from sdmf.data_quality.executors.StandardDQExecutor import StandardDQExecutor
from sdmf.data_quality.executors.ComprehensiveDQExecutor import ComprehensiveDQExecutor

logger = logging.getLogger(__name__)


class DataQualityRunner:
    """
    Orchestrates full data quality lifecycle.
    """

    def __init__(self, spark: SparkSession, feed_specs: list[dict]):
        self.spark = spark
        self.feed_specs = feed_specs

        self.standard_checker = StandardDQExecutor(spark)
        self.comprehensive_checker = ComprehensiveDQExecutor(spark)

        self.results = []

    # --------------------------------------------------
    # ENTRY POINT
    # --------------------------------------------------
    def run(self) -> dict:
        logger.info("Starting Data Quality Runner")

        passed_feeds = []

        # ---------------------------
        # PRE LOAD
        # ---------------------------
        for feed in self.feed_specs:
            result = self._run_pre_load(feed)
            self.results.append(result)

            if result["pre_load_passed"]:
                passed_feeds.append(feed["table"])

        can_ingest = len(passed_feeds) > 0

        if can_ingest:
            self._trigger_ingestion(passed_feeds)
            self._run_post_load(passed_feeds)

        return self._finalize(can_ingest)

    # --------------------------------------------------
    # PRE LOAD
    # --------------------------------------------------
    def _run_pre_load(self, feed: dict) -> dict:
        table = feed["table"]
        logger.info("PRE_LOAD checks for %s", table)

        df = self.spark.table(table)
        total_count = df.count()

        passed = True

        for check in feed.get("standard_checks", []):
            for method in check["check_sequence"]:
                ok = self.standard_checker.run_check(
                    method_name=method,
                    df=df,
                    column=check.get("column_name"),
                    total_count=total_count,
                    threshold=check.get("threshold", 0),
                )
                if not ok:
                    passed = False

        return {
            "table": table,
            "pre_load_passed": passed,
            "post_load_errors": 0,
            "post_load_warnings": 0,
        }

    # --------------------------------------------------
    # POST LOAD
    # --------------------------------------------------
    def _run_post_load(self, tables: list[str]):
        for feed in self.feed_specs:
            if feed["table"] not in tables:
                continue

            for check in feed.get("comprehensive_checks", []):
                if check.get("load_stage") != "POST_LOAD":
                    continue

                result = self.comprehensive_checker.run_check(check)
                self._apply_post_load_result(feed["table"], result)

    def _apply_post_load_result(self, table: str, result: dict):
        for r in self.results:
            if r["table"] == table:
                if not result["passed"]:
                    if result["severity"] == "ERROR":
                        r["post_load_errors"] += 1
                    elif result["severity"] == "WARNING":
                        r["post_load_warnings"] += 1

    # --------------------------------------------------
    # INGESTION
    # --------------------------------------------------
    def _trigger_ingestion(self, tables: list[str]):
        logger.info("Triggering ingestion for: %s", tables)
        # external ingestion system call

    # --------------------------------------------------
    # FINAL RESULT
    # --------------------------------------------------
    def _finalize(self, can_ingest: bool) -> dict:
        return {
            "can_ingest": can_ingest,
            "results": self.results,
        }
