"""Dagster resources. Laptop and server differ only by environment variables."""

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

import httpx
import psycopg
from dagster import ConfigurableResource

from dagster_ingestion.common.errors import IngestionFailure

# httpx logs every request URL at INFO, and FRED wants the API key in the query string.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


class PostgresResource(ConfigurableResource):
    """Connection to the application database as the least-privileged app role."""

    host: str
    port: int = 5432
    dbname: str
    user: str
    password: str

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(
            host=self.host, port=self.port, dbname=self.dbname,
            user=self.user, password=self.password, connect_timeout=10,
        ) as conn:
            yield conn


@dataclass(frozen=True)
class FetchResult:
    data: dict
    http_status: int
    bytes_fetched: int
    latency_ms: int
    request_url: str  # safe to store: never contains the API key


class FredResource(ConfigurableResource):
    api_key: str
    base_url: str = "https://api.stlouisfed.org/fred/series/observations"
    timeout_seconds: float = 30.0
    user_agent: str = "data-ingestion-framework (portfolio project)"

    def fetch_observations(self, series_id: str, observation_start: str) -> FetchResult:
        params = {
            "series_id": series_id,
            "observation_start": observation_start,
            "file_type": "json",
        }
        safe_url = str(httpx.URL(self.base_url, params=params))  # no api_key in here
        started = time.monotonic()
        try:
            resp = httpx.get(
                self.base_url,
                params={**params, "api_key": self.api_key},
                headers={"User-Agent": self.user_agent},
                timeout=httpx.Timeout(self.timeout_seconds, connect=10.0),
            )
        except httpx.TimeoutException:
            raise IngestionFailure(
                f"FRED timed out for series {series_id}", category="timeout", retryable=True
            ) from None  # `from None`: the original error can embed the URL (and the key)
        except httpx.TransportError:
            raise IngestionFailure(
                f"Could not reach FRED for series {series_id}", category="timeout", retryable=True
            ) from None

        latency_ms = int((time.monotonic() - started) * 1000)
        status = resp.status_code
        if status == 429:
            raise IngestionFailure("FRED rate limit hit", category="rate_limited", retryable=True)
        if status >= 500:
            raise IngestionFailure(
                f"FRED returned HTTP {status} for series {series_id}",
                category="http_5xx", retryable=True,
            )
        if status >= 400:  # bad series id, bad key, ...: retrying cannot fix it
            raise IngestionFailure(
                f"FRED returned HTTP {status} for series {series_id}",
                category="http_4xx", retryable=False,
            )
        try:
            data = resp.json()
        except ValueError:
            raise IngestionFailure(
                f"FRED returned non-JSON for series {series_id}",
                category="parse_error", retryable=False,
            ) from None
        return FetchResult(data, status, len(resp.content), latency_ms, safe_url)
