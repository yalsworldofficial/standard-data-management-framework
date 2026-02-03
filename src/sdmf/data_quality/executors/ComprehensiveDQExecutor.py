# inbuilt
import logging

# internal
from sdmf.exception.DataQualityException import DataQualityException


class ComprehensiveDQExecutor:
    """Executes SQL-based comprehensive checks"""

    def __init__(self, spark):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def run(self, feed_spec: dict, is_post_load: bool = False) -> tuple:
        has_errors = False
        check_result = []
        self.logger.info(f"Running {"POST_LOAD" if is_post_load else "PRE_LOAD"} comprehensive DQ checks")
        for check in feed_spec.get("comprehensive_checks", []):
            if is_post_load == True and check.get("load_stage") == "PRE_LOAD":
                continue
            if is_post_load == False and check.get("load_stage") == "POST_LOAD":
                continue
            dependency_ds = check.get("dependency_dataset", [])
            for dds in dependency_ds:
                if self.spark.catalog.tableExists(dds) == False:
                    raise DataQualityException
            query = check.get("query")
            severity = check.get("severity", "").upper()
            threshold = check.get("threshold", 0)
            self.logger.info(f"Executing {"POST_LOAD" if is_post_load else "PRE_LOAD"} check {check.get("check_name")} on {feed_spec.get("source_table_name")}")
            df = self.spark.sql(query)
            count = df.count()
            status = "PASSED"
            did_check_pass = True
            if count > threshold:
                status = "FAILED"
                if severity == "ERROR":
                    has_errors = True
                did_check_pass = False
            check_result.append(
                {
                    "table": feed_spec.get("source_table_name"),
                    "check_name": check.get("check_name"),
                    "load_stage": check.get("load_stage"),
                    "query": check.get("query"),
                    "failed_records": count,
                    "threshold": check.get("threshold", 0),
                    "status": status,
                    "severity":severity,
                    "did_check_pass":did_check_pass
                }
            )
        return has_errors, check_result
