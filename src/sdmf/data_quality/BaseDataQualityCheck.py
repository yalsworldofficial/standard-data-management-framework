import logging
from pyspark.sql import DataFrame
from sdmf.exception.DataQualityError import DataQualityError

logger = logging.getLogger(__name__)

# ============================================================

class BaseDataQualityCheck:
    """
    Base class providing orchestration for running
    data quality checks. Subclasses only implement the check
    functions and default parameters.
    """

    DEFAULT_PARAMS = {}

    def __init__(self, df: DataFrame, feed_specs: dict, table_name: str = None):
        self.df = df
        self.feed_specs = feed_specs
        self.table_name = table_name or "UNKNOWN_TABLE"
        self.df_columns = df.columns
        self.results = []

    # ------------------------------------------------------------------
    # Top-level API
    # ------------------------------------------------------------------
    def run(self) -> bool:
        """Runs all checks defined in feed_specs."""
        try:
            checks = self.feed_specs.get("checks")
            if checks is None:
                raise ValueError("Missing required key `checks` in feed_specs")

            if not checks:
                logger.info(f"No checks defined for table [{self.table_name}]. Assuming pass.")
                return True

            total_count = self.df.count()

            for check_def in checks:
                self._run_single_check(check_def, total_count)

            passed = sum(r["passed"] for r in self.results)
            total = len(self.results)

            logger.info(
                f"DQ Summary for [{self.table_name}]: "
                f"{passed}/{total} passed ({(passed / total) * 100:.1f}% integrity)"
            )

            return all(r["passed"] for r in self.results)

        except Exception as e:
            raise DataQualityError(
                f"Something went wrong while enforcing data quality on [{self.table_name}]",
                original_exception = e
            )

    # ------------------------------------------------------------------
    # Runner for each check block
    # ------------------------------------------------------------------
    def _run_single_check(self, check: dict, total_count: int) -> None:
        column = check.get("column_name")
        sequence = check.get("check_sequence", [])
        params = {**self.DEFAULT_PARAMS, **check.get("parameters", {})}

        if column and column not in self.df_columns:
            raise ValueError(f"Column '{column}' not found in DataFrame.")

        self._validate_parameters(params)

        for method_name in sequence:
            if not hasattr(self, method_name):
                raise AttributeError(f"Unsupported check method: {method_name}")

            method = getattr(self)

            result = method_name
            result = getattr(self, method_name)(self.df, column, total_count, **params)

            context = self._format_context(method_name, params)

            if result:
                logger.info(f"✅ Check [{method_name}] on [{column}] ({context}) PASSED")
            else:
                logger.error(f"❌ Check [{method_name}] on [{column}] ({context}) FAILED")

            self.results.append({
                "column": column,
                "check": method_name,
                "passed": result,
                "threshold": params.get("threshold")
            })

    # ------------------------------------------------------------------
    # Shared logical helpers
    # ------------------------------------------------------------------
    def _validate_parameters(self, params: dict):
        """Basic validation to be inherited by subclasses."""
        pass  # subclasses override or extend

    @staticmethod
    def _format_context(method_name: str, params: dict) -> str:
        return f"params={params}"

    @staticmethod
    def _is_within_threshold(bad_count: int, total_count: int, threshold: float) -> bool:
        return total_count == 0 or (bad_count / total_count) <= threshold
