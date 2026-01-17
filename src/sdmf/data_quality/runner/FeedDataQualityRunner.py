# inbuilt
import json
import logging

# external
from pyspark.sql import SparkSession

# internal
from sdmf.data_quality.executors.StandardDQExecutor import StandardDQExecutor
from sdmf.data_quality.executors.ComprehensiveDQExecutor import ComprehensiveDQExecutor




class FeedDataQualityRunner:
    """
    Orchestrates full data quality lifecycle.
    """

    def __init__(self, spark: SparkSession, master_specs: list[dict]):
        self.spark = spark
        self.master_specs = master_specs
        self.logger = logging.getLogger(__name__)
        self.standard_checker = StandardDQExecutor(spark)
        self.comprehensive_checker = ComprehensiveDQExecutor(spark)
        self.results = []
        self.passed_feeds = []

    # --------------------------------------------------
    # ENTRY POINT
    # --------------------------------------------------
    def run(self):
        self.logger.info("Starting Data Quality Runner")
        self.passed_feeds = []
        for feed1 in self.master_specs:
            feed = json.loads(feed1['feed_specs'])
            result = self._run_standard_checks(feed)
            cols = [
                "feed_id",
                "system_name",
                "subsystem_name",
                "category",
                "sub_category",
                "data_flow_direction",
                "residing_layer",
                "feed_name",
                "feed_type",
                "load_type",
                "target_table_name"
            ]
            for col in cols:
                result[col] = feed1[col]
            self.results.append(result)
            if result["standard_checks_passed"]:
                self.passed_feeds.append(feed["source_table_name"])
        standard_passed = len(self.passed_feeds) > 0
        if standard_passed:
            self._run_comprehensive_checks(self.passed_feeds)

    # --------------------------------------------------
    # PRE LOAD
    # --------------------------------------------------
    def _run_standard_checks(self, feed: dict) -> dict:
        table = feed["source_table_name"]
        self.logger.info("Standard checks for %s", table)
        df = self.spark.table(table)
        total_count = df.count()
        passed = True
        check_results = []
        for check in feed.get("standard_checks", []):
            for method in check["check_sequence"]:
                ok = self.standard_checker.run_check(
                    method_name=method,
                    df=df,
                    column=check.get("column_name"),
                    total_count=total_count,
                    threshold=check.get("threshold", 0),
                )
                passed = bool(ok['passed'])
                check_results.append(ok)
        return {
            "check_table_name": table,
            "standard_checks_passed": passed,
            "standard_checks_result": check_results,
            "comprehensive_pre_load_passed":None,
            "comprehensive_results": None
        }

    # --------------------------------------------------
    # PRE LOAD
    # --------------------------------------------------
    def _run_comprehensive_checks(self, tables: list[str]):
        for feed1 in self.master_specs:
            feed = json.loads(feed1['feed_specs'])
            if feed["source_table_name"] not in tables:
                continue
            has_errors, check_result = self.comprehensive_checker.run(feed)
            self._apply_pre_load_result(feed["source_table_name"], has_errors, check_result)

    def _apply_pre_load_result(self, table: str, has_errors:bool, result: list):
        for r in self.results:
            if r["check_table_name"] == table:
                r["comprehensive_results"] = result
                r["comprehensive_pre_load_passed"] = False if has_errors else True
                r["can_ingest"] = True if r["standard_checks_passed"] and r["comprehensive_pre_load_passed"] else False

    def adhoc_post_load(self):
        for r in self.results:
            feed_id = r['feed_id']

            for master_feed in self.master_specs:
                if master_feed['feed_id'] == feed_id:
                    feed = json.loads(master_feed['feed_specs'])
                    has_errors, check_result = self.comprehensive_checker.run(feed, is_post_load=True)
                    old = list(r["comprehensive_results"])
                    old.extend(check_result)
                    r["comprehensive_results"] = old
                    r['comprehensive_post_load_passed'] = False if has_errors else True

    def _finalize(self) -> list:
        return self.results