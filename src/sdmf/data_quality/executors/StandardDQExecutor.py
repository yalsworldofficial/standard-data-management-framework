import logging
from functools import reduce
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)


class StandardDQExecutor:
    """
    Standalone standard data quality checks.
    Each check returns True (pass) or False (fail).
    """


    DEFAULT_PARAMS = {
        "threshold": 0.0,
        "allowed_values": [],
        "range": {"min": float("-inf"), "max": float("inf")},
    }

    def __init__(self, spark):
        self.spark = spark


    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run_check(
        self,
        method_name: str,
        df,
        column=None,
        total_count: int = 0,
        **params,
    ) -> bool:
        """
        Executes a single standard DQ check.
        """

        if not hasattr(self, method_name):
            raise AttributeError(f"Unsupported check method: {method_name}")

        # Merge defaults
        final_params = {**self.DEFAULT_PARAMS, **params}

        # Validate parameters
        self._validate_parameters(final_params)

        # Resolve method
        method = getattr(self, method_name)

        logger.info(
            "Executing %s | %s",
            method_name,
            self._format_context(method_name, final_params),
        )

        result = method(
            df=df,
            column=column,
            total_count=total_count,
            **final_params,
        )

        if result:
            logger.info("✅ %s PASSED", method_name)
        else:
            logger.error("❌ %s FAILED", method_name)

        return bool(result)

    # ------------------------------------------------------------------
    # Parameter Validation
    # ------------------------------------------------------------------
    def _validate_parameters(self, params: dict):
        if not isinstance(params["threshold"], (int, float)):
            raise ValueError("Threshold must be numeric.")
        if not isinstance(params["allowed_values"], list):
            raise ValueError("Allowed values must be a list.")
        if not all(
            isinstance(params["range"][k], (int, float)) for k in ("min", "max")
        ):
            raise ValueError("Range bounds must be numeric.")

    # ------------------------------------------------------------------
    # Contextual Logging
    # ------------------------------------------------------------------
    @staticmethod
    def _format_context(method_name: str, params: dict) -> str:
        if method_name == "_check_value_range":
            r = params["range"]
            return f"range=[{r['min']}, {r['max']}]"
        if method_name == "_check_allowed_values":
            vals = params["allowed_values"]
            preview = vals[:5]
            more = "..." if len(vals) > 5 else ""
            return f"allowed_values={preview}{more}"
        return f"threshold={params.get('threshold')}"

    # ------------------------------------------------------------------
    # Shared Helper
    # ------------------------------------------------------------------
    @staticmethod
    def _is_within_threshold(
        bad_count: int, total_count: int, threshold: float
    ) -> bool:
        return total_count == 0 or (bad_count / total_count) <= threshold

    # ------------------------------------------------------------------
    # CHECK FUNCTIONS (Business Logic Only)
    # ------------------------------------------------------------------
    def _check_nulls(self, df, column, total_count, threshold, **_):
        bad = df.filter(F.col(column).isNull()).count()
        return self._is_within_threshold(bad, total_count, threshold)

    def _check_value_range(self, df, column, total_count, threshold, range, **_):
        bad = df.filter(
            (F.col(column) < range["min"]) | (F.col(column) > range["max"])
        ).count()
        return self._is_within_threshold(bad, total_count, threshold)

    def _check_allowed_values(
        self, df, column, total_count, threshold, allowed_values, **_
    ):
        bad = df.filter(~F.col(column).isin(allowed_values)).count()
        return self._is_within_threshold(bad, total_count, threshold)

    def _check_duplicates(self, df, column=None, total_count=0, threshold=0.0, **_):
        cols = [column] if column else df.columns
        dup = df.groupBy(cols).count().filter(F.col("count") > 1).count()
        return self._is_within_threshold(dup, total_count, threshold)

    def _check_composite_key(self, df, column, total_count, **_):
        cols = column if isinstance(column, list) else [column]

        nulls = df.filter(
            reduce(lambda x, y: x | y, [F.col(c).isNull() for c in cols])
        ).count()

        dups = df.groupBy(cols).count().filter(F.col("count") > 1).count()

        return nulls == 0 and dups == 0

    def _check_primary_key(self, df, column, total_count, **_):
        cols = [column] if isinstance(column, str) else column

        nulls = df.filter(
            reduce(lambda x, y: x | y, [F.col(c).isNull() for c in cols])
        ).count()

        dups = df.groupBy(cols).count().filter(F.col("count") > 1).count()

        return nulls == 0 and dups == 0
