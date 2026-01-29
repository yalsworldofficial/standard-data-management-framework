# inbuilt
import time
import random
import logging
import requests
from requests.exceptions import RequestException

# external
from pyspark.sql import SparkSession

# internal
from sdmf.extraction_toolkit.BaseExtractor import BaseExtractor
from sdmf.extraction_toolkit.data_class.ExtractionConfig import ExtractionConfig
from sdmf.extraction_toolkit.data_class.ExtractionResult import ExtractionResult


class APIExtractor(BaseExtractor):

    def __init__(self, extraction_config: ExtractionConfig, spark: SparkSession) -> None:
        self.logger = logging.getLogger(__name__)
        self.extraction_config = extraction_config
        self.spark = spark

    def extract(self) -> ExtractionResult:
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
        return ExtractionResult(feed_id=1, success=False)
    
    def __fetch_response(self) -> requests.Response:
        cfg = self.extraction_config.config
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

                # Enforce JSON response
                content_type = response.headers.get("Content-Type", "")
                if "application/json" not in content_type.lower():
                    raise ValueError(
                        f"Invalid response Content-Type: {content_type}"
                    )

                # Success
                if response.status_code < 400:
                    return response

                # Retryable HTTP status
                if response.status_code in retry_statuses:
                    self.__sleep_before_retry(
                        attempt, backoff_factor, max_backoff, response
                    )
                    continue

                # Non-retryable HTTP error
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
