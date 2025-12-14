import logging
from functools import reduce
import pyspark.sql.functions as F
from sdmf.data_quality.BaseDataQualityCheck import BaseDataQualityCheck
logger = logging.getLogger(__name__)


class StandardDataQualityChecks(BaseDataQualityCheck):

    DEFAULT_PARAMS = {
        "threshold": 0.0,
        "allowed_values": [],
        "range": {"min": float("-inf"), "max": float("inf")}
    }

    # ----------------------------------------------------------
    # Parameter Validation
    # ----------------------------------------------------------
    def _validate_parameters(self, params: dict):
        if not isinstance(params["threshold"], (int, float)):
            raise ValueError("Threshold must be numeric.")
        if not isinstance(params["allowed_values"], list):
            raise ValueError("Allowed values must be a list.")
        if not all(
            isinstance(params["range"][k], (int, float)) for k in ("min", "max")
        ):
            raise ValueError("Range bounds must be numeric.")

    # ----------------------------------------------------------
    # Contextual Logging Override
    # ----------------------------------------------------------
    @staticmethod
    def _format_context(method_name: str, params: dict) -> str:
        if method_name == "_check_value_range":
            r = params["range"]
            return f"range=[{r['min']}, {r['max']}]"
        if method_name == "_check_allowed_values":
            p = params["allowed_values"][:5]
            more = "..." if len(params["allowed_values"]) > 5 else ""
            return f"allowed_values={p}{more}"
        return f"threshold={params.get('threshold')}"

    # ----------------------------------------------------------
    # CHECK FUNCTIONS (Only business logic lives here)
    # ----------------------------------------------------------
    def _check_nulls(self, df, column, total_count, threshold, **_):
        nulls = df.filter(F.col(column).isNull()).count()
        return self._is_within_threshold(nulls, total_count, threshold)

    def _check_value_range(self, df, column, total_count, threshold, range, **_):
        bad = df.filter(
            (F.col(column) < range["min"]) | (F.col(column) > range["max"])
        ).count()
        return self._is_within_threshold(bad, total_count, threshold)

    def _check_allowed_values(self, df, column, total_count, threshold, allowed_values, **_):
        bad = df.filter(~F.col(column).isin(allowed_values)).count()
        return self._is_within_threshold(bad, total_count, threshold)

    def _check_duplicates(self, df, columns=None, total_count=0, threshold=0.0, **_):
        columns = columns or df.columns
        dup = df.groupBy(columns).count().filter(F.col("count") > 1).count()
        return self._is_within_threshold(dup, total_count, threshold)

    def _check_composite_key(self, df, columns, total_count, **_):
        if not columns:
            raise ValueError("Composite key requires a list of columns.")

        nulls = df.filter(reduce(lambda x, y: x | y, [F.col(c).isNull() for c in columns])).count()
        dups = df.groupBy(columns).count().filter(F.col("count") > 1).count()

        return nulls == 0 and dups == 0

    def _check_primary_key(self, df, column, total_count, **_):
        cols = [column] if isinstance(column, str) else column

        nulls = df.filter(
            reduce(lambda x, y: x | y, [F.col(c).isNull() for c in cols])
        ).count()

        dups = df.groupBy(cols).count().filter(F.col("count") > 1).count()

        return nulls == 0 and dups == 0