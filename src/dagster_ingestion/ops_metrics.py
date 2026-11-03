"""Records how each run went into ops.run_metrics, success OR failure.

Why the asset writes this itself: if the process is killed (out of memory, container
restart) no code of ours runs, so a run-failure sensor (sensors.py) writes the row instead.
"""

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg
from dagster import AssetExecutionContext

from dagster_ingestion.common.errors import IngestionFailure
from dagster_ingestion.landing import upsert_run_metrics
from dagster_ingestion.resources import PostgresResource


@dataclass
class RunTally:
    rows_fetched: int = 0
    rows_inserted: int = 0
    bytes_fetched: int = 0
    latency_ms: int = 0
    http_status: int | None = None
    streams: dict[str, dict[str, int]] = field(default_factory=dict)

    def add(self, stream: str, *, fetched: int, inserted: int, bytes_fetched: int,
            latency_ms: int, http_status: int) -> None:
        self.rows_fetched += fetched
        self.rows_inserted += inserted
        self.bytes_fetched += bytes_fetched
        self.latency_ms += latency_ms
        self.http_status = http_status
        self.streams[stream] = {"fetched": fetched, "inserted": inserted}


def _category(exc: BaseException) -> str:
    if isinstance(exc, IngestionFailure):
        return exc.category
    if isinstance(exc, psycopg.Error):
        return "db_error"
    return "unknown"


@contextmanager
def track_run(
    pg: PostgresResource, context: AssetExecutionContext, *, source: str
) -> Iterator[RunTally]:
    tally = RunTally()
    started_at = datetime.now(timezone.utc)
    t0 = time.monotonic()
    status, error_class = "SUCCESS", None
    try:
        yield tally
    except BaseException as exc:
        status, error_class = "FAILURE", _category(exc)
        raise
    finally:
        try:
            with pg.connection() as conn:
                upsert_run_metrics(conn, {
                    "run_id": context.run.run_id,
                    "source": source,
                    "asset_key": context.asset_key.to_user_string(),
                    "status": status,
                    "started_at": started_at,
                    "finished_at": datetime.now(timezone.utc),
                    "latency_ms": tally.latency_ms or int((time.monotonic() - t0) * 1000),
                    "rows_fetched": tally.rows_fetched,
                    "rows_inserted": tally.rows_inserted,
                    "bytes_fetched": tally.bytes_fetched,
                    "http_status": tally.http_status,
                    "error_class": error_class,
                    "attempts": context.retry_number + 1,
                    "details": tally.streams,
                })
        except Exception as meta_exc:  # never mask the real error with a metrics error
            context.log.error(f"Could not record run metrics: {type(meta_exc).__name__}")
