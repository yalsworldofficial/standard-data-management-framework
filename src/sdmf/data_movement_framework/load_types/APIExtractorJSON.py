# inbuilt
import time
import random
import logging
import requests
from requests.exceptions import RequestException
from typing import Any

# external
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType

# internal
from sdmf.data_movement_framework.BaseLoadStrategy import BaseLoadStrategy
from sdmf.data_movement_framework.data_class.LoadConfig import LoadConfig
from sdmf.data_movement_framework.data_class.LoadResult import LoadResult
from sdmf.exception.ExtractionException import ExtractionException


class APIExtractorJSON(BaseLoadStrategy):

    def __init__(self, config: LoadConfig, spark: SparkSession) -> None:
        super().__init__(config=config, spark=spark)
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.spark = spark
        if self.config.target_unity_catalog == "testing":
            self.__bronze_schema = f"bronze"
        else:
            self.__bronze_schema = f"{self.config.target_unity_catalog}.bronze"

    def load(self) -> LoadResult:
        result_df = self.__extract()
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.__bronze_schema}")
        feed_temp = f'{self.__bronze_schema}.t_{self.config.master_specs['feed_id']}_{self.config.master_specs['feed_name']}'
        self.logger.info(f"Creating bronze table: {feed_temp}")
        self.config.feed_specs["source_table_name"] = feed_temp
        self.config.feed_specs["selection_query"] = None
        result_df = self._enforce_schema(result_df, StructType.fromJson(self.config.feed_specs['selection_schema']))
        result_df.printSchema()

        (
            result_df.write.
            format("delta")
            .mode("overwrite")
            .saveAsTable(feed_temp)
        )
        return LoadResult(
            feed_id = self.config.master_specs['feed_id'],
            success=True, 
            total_rows_inserted=result_df.count(),
            total_rows_updated=0,
            total_rows_deleted=0
        )

    def __extract(self) -> DataFrame:
        """
        Core load logic implemented by subclass.
        Should return IngestionResult on success.
        ingestion_config = {
            # --------------------
            # Request
            # --------------------
            "url": "https://api.example.com/resource",
            "method": "GET",
            "headers": {
                "Authorization": "Bearer <token>"
            },
            "params": {
                "limit": 100
            },
            "body": None,
            "timeout": 30,

            # --------------------
            # Retry
            # --------------------
            "retry": {
                "max_attempts": 5,
                "backoff_factor": 2.0,
                "max_backoff": 60,
                "retry_statuses": [429, 500, 502, 503, 504]
            },

            # --------------------
            # Pagination (optional)
            # --------------------
            "pagination": {
                "type": "page",            # page | cursor

                # page-based
                "page_param": "page",
                "page_size_param": "limit",
                "page_size": 100,
                "start_page": 1,
                "max_pages": 10_000,

                # cursor-based
                "cursor_param": "cursor",
                "cursor_path": "next_cursor"
            },

            # --------------------
            # Enforced
            # --------------------
            "response_type": "json"
        }
        """
        try:
            records = self.__fetch_all_pages()
            schema = StructType.fromJson(self.config.feed_specs['selection_schema'])
            if not records:
                df = self.spark.createDataFrame([], schema)
            else:
                rows = [Row(**r) for r in records]
                df = self.spark.createDataFrame(rows, schema)
            return df
        except Exception as exc:
            raise ExtractionException(
                message="Something went wrong while running API Extractor",
                original_exception=exc
            )

    def __fetch_all_pages(self) -> list[dict[str, Any]]:
        cfg = self.config.feed_specs['ingestion_config']
        pagination = cfg.get("pagination", None)
        if not pagination:
            response = self.__fetch_response()
            payload = response.json()
            return self.__normalize_payload(payload)
        pagination_type = pagination.get("type")
        if pagination_type == "page":
            return self.__page_based_fetch(pagination)
        if pagination_type == "cursor":
            return self.__cursor_based_fetch(pagination)
        raise ValueError(f"Unsupported pagination type: {pagination_type}")

    def __page_based_fetch(self, pagination: dict) -> list[dict[str, Any]]:
        cfg = self.config.feed_specs['ingestion_config']
        results: list[dict] = []
        page = pagination.get("start_page", 1)
        max_pages = pagination.get("max_pages", 10000)
        while page <= max_pages:
            params = (cfg.get("params") or {}).copy()
            params[pagination["page_param"]] = page
            params[pagination["page_size_param"]] = pagination["page_size"]
            cfg["params"] = params
            self.logger.info("Fetching page %s", page)
            response = self.__fetch_response()
            payload = response.json()
            batch = self.__normalize_payload(payload)
            if not batch:
                break
            results.extend(batch)
            page += 1
        return results

    def __cursor_based_fetch(self, pagination: dict) -> list[dict[str, Any]]:
        cfg = self.config.feed_specs['ingestion_config']
        results: list[dict] = []
        cursor = None
        while True:
            params = (cfg.get("params") or {}).copy()
            if cursor:
                params[pagination["cursor_param"]] = cursor
            cfg["params"] = params
            response = self.__fetch_response()
            payload = response.json()
            batch = self.__normalize_payload(payload)
            results.extend(batch)
            cursor = payload.get(pagination["cursor_path"])
            if not cursor:
                break
        return results

    def __normalize_payload(self, payload) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            if "data" in payload and isinstance(payload["data"], list):
                return payload["data"]
            return [payload]
        raise ValueError(f"Unsupported JSON payload type: {type(payload)}")

    def __fetch_response(self) -> requests.Response:
        cfg = self.config.feed_specs['ingestion_config']
        retry_cfg = cfg.get("retry", {})

        max_attempts = retry_cfg.get("max_attempts", 3)
        backoff_factor = retry_cfg.get("backoff_factor", 2.0)
        max_backoff = retry_cfg.get("max_backoff", 60)
        retry_statuses = set(
            retry_cfg.get("retry_statuses", [429, 500, 502, 503, 504])
        )
        last_exception = None
        for attempt in range(1, max_attempts + 1):
            try:
                self.logger.info(
                    "API request attempt %s/%s: %s",
                    attempt, max_attempts, cfg["url"]
                )
                response = requests.request(
                    method=cfg.get("method", "GET"),
                    url=cfg["url"],
                    headers=cfg.get("headers"),
                    params=cfg.get("params"),
                    json=cfg.get("body"),
                    timeout=cfg.get("timeout", 30),
                )
                content_type = response.headers.get("Content-Type", "")
                if "application/json" not in content_type.lower():
                    raise ValueError(
                        f"Invalid response Content-Type: {content_type}"
                    )
                if response.status_code < 400:
                    return response
                if response.status_code in retry_statuses:
                    self.__sleep_before_retry(
                        attempt, backoff_factor, max_backoff, response
                    )
                    continue
                response.raise_for_status()
            except RequestException as exc:
                last_exception = exc
                self.logger.warning(
                    "Request error on attempt %s/%s: %s",
                    attempt, max_attempts, exc
                )
                if attempt >= max_attempts:
                    break
                self.__sleep_before_retry(
                    attempt, backoff_factor, max_backoff
                )
        self.logger.error(
            "API request failed after %s attempts: %s",
            max_attempts, cfg["url"]
        )
        if last_exception:
            raise last_exception
        raise RuntimeError("API request failed after retries")
    
    def __sleep_before_retry(
        self,
        attempt: int,
        backoff_factor: float,
        max_backoff: int,
        response: requests.Response | None = None
    ) -> None:
        retry_after = None
        if response is not None:
            retry_after = response.headers.get("Retry-After")
        if retry_after:
            sleep_time = min(int(retry_after), max_backoff)
        else:
            sleep_time = min(
                backoff_factor ** attempt + random.uniform(0, 1),
                max_backoff
            )
        self.logger.warning(
            "Retrying in %.2f seconds (attempt %s)",
            sleep_time, attempt
        )
        time.sleep(sleep_time)
