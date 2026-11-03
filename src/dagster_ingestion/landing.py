"""Writes to the raw layer and the ops tables. Plain SQL, psycopg 3, no ORM."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class Record:
    key: str                      # natural key, unique per stream (e.g. 'DFF:2026-10-20')
    payload_hash: bytes           # sha256 of the record's canonical content
    observed_at: datetime | None  # period the record describes
    payload: dict[str, Any]       # the record as the source sent it


_LAND_SQL = """
insert into raw.records
    (_source, _stream, _run_id, _request_url, _record_key, _payload_hash, _observed_at, payload)
select %(source)s, %(stream)s, %(run_id)s, %(url)s,
       r.record_key, decode(r.payload_hash, 'hex'), r.observed_at, r.payload
from jsonb_to_recordset(%(batch)s) as r(
    record_key text, payload_hash text, observed_at timestamptz, payload jsonb)
on conflict (_source, _record_key, _payload_hash) do nothing
"""


def land_records(
    conn: psycopg.Connection,
    *,
    source: str,
    stream: str,
    run_id: str,
    request_url: str,
    records: list[Record],
) -> int:
    """Insert records, skipping any already stored. Returns the number of NEW rows.

    Running this twice with the same records returns 0 the second time.
    """
    if not records:
        return 0
    batch = [
        {
            "record_key": r.key,
            "payload_hash": r.payload_hash.hex(),
            "observed_at": r.observed_at.isoformat() if r.observed_at else None,
            "payload": r.payload,
        }
        for r in records
    ]
    with conn.cursor() as cur:
        cur.execute(
            _LAND_SQL,
            {"source": source, "stream": stream, "run_id": run_id,
             "url": request_url, "batch": Jsonb(batch)},
        )
        inserted = cur.rowcount
    conn.commit()
    return inserted


def latest_observed_at(conn: psycopg.Connection, source: str, stream: str) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute(
            "select max(_observed_at) from raw.records where _source = %s and _stream = %s",
            (source, stream),
        )
        row = cur.fetchone()
    conn.commit()
    return row[0] if row else None


_RUN_UPSERT = """
insert into ops.run_metrics
    (run_id, source, asset_key, status, started_at, finished_at, latency_ms, rows_fetched,
     rows_inserted, bytes_fetched, http_status, error_class, attempts, details)
values
    (%(run_id)s, %(source)s, %(asset_key)s, %(status)s, %(started_at)s, %(finished_at)s,
     %(latency_ms)s, %(rows_fetched)s, %(rows_inserted)s, %(bytes_fetched)s, %(http_status)s,
     %(error_class)s, %(attempts)s, %(details)s)
on conflict (run_id, asset_key) do update set
    status = excluded.status, finished_at = excluded.finished_at,
    latency_ms = excluded.latency_ms, rows_fetched = excluded.rows_fetched,
    rows_inserted = excluded.rows_inserted, bytes_fetched = excluded.bytes_fetched,
    http_status = excluded.http_status, error_class = excluded.error_class,
    attempts = excluded.attempts, details = excluded.details
"""


def upsert_run_metrics(conn: psycopg.Connection, row: dict[str, Any]) -> None:
    """Record how a run went. The last attempt of a run wins."""
    params = {**row, "details": Jsonb(row.get("details") or {})}
    with conn.cursor() as cur:
        cur.execute(_RUN_UPSERT, params)
    conn.commit()


_RUN_INSERT_IF_RUN_UNSEEN = """
insert into ops.run_metrics
    (run_id, source, asset_key, status, started_at, finished_at, error_class, attempts, details)
select %(run_id)s, %(source)s, %(asset_key)s, 'FAILURE', %(started_at)s, %(finished_at)s,
       %(error_class)s, 1, '{}'::jsonb
where not exists (select 1 from ops.run_metrics where run_id = %(run_id)s)
"""


def record_failure_if_run_unseen(conn: psycopg.Connection, row: dict[str, Any]) -> bool:
    """Backstop for crashed runs: add a FAILURE row only if the run has NO row at all.

    Matching on the whole run (not on asset_key) matters: a run launched from an asset job
    has no asset selection, so the sensor cannot know the asset key the asset itself used.
    """
    with conn.cursor() as cur:
        cur.execute(_RUN_INSERT_IF_RUN_UNSEEN, row)
        inserted = cur.rowcount == 1
    conn.commit()
    return inserted
