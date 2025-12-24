import logging
from sdmf.data_quality.FeedSpec import FeedSpec
logger = logging.getLogger(__name__)


class ComprehensiveDQExecutor:
    """Executes SQL-based comprehensive checks"""

    def __init__(self, spark):
        self.spark = spark

    def run_pre_load(self, feed_spec: FeedSpec):
        warnings = []
        has_errors = False
        executed_checks = 0

        logger.info("Running PRE_LOAD comprehensive DQ checks")

        for check in feed_spec.comprehensive_checks:
            if check.get("load_stage") != "PRE_LOAD":
                logger.info(
                    "Skipping POST_LOAD check: %s", check.get("query")
                )
                continue

            executed_checks += 1
            query = check.get("query")
            severity = check.get("severity", "").upper()
            threshold = check.get("threshold", 0)

            logger.info("Executing PRE_LOAD check")

            df = self.spark.sql(query)
            count = df.count()

            if count > threshold:
                msg = f"{severity} | Records={count}"

                if severity == "ERROR":
                    logger.error(msg)
                    has_errors = True
                elif severity == "WARNING":
                    logger.warning(msg)
                    warnings.append(msg)

        return has_errors, warnings, executed_checks
