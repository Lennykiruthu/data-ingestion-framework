# D9: one raw table, list-partitioned by source

**Status:** accepted. **Supersedes:** the "one envelope table per source" part of D1 (storage stays Postgres JSONB).

## Decision
All sources land in `raw.records`, `PARTITION BY LIST (_source)`. Adding a source is
`select raw.register_source('<name>')`. Envelope columns: `_source, _stream, _run_id,
_ingested_at, _observed_at, _request_url, _record_key, _payload_hash, payload`.
Dedupe key: `(_source, _record_key, _payload_hash)`. Raw is append-only for the app role.

## Why
One insert path, one set of grants, one `ops` join, and per-source partitions keep per-source
retention (drop a partition) and index choices open.

## Constraint that shaped it
Postgres requires every unique index on a partitioned table to include all partition-key
columns. Partitioning by time (`_ingested_at`) would force the dedupe key to include it, so the
same record seen next month would no longer conflict and idempotency would silently break.
`_source` is already part of the dedupe key, so list-by-source is safe. No identity column:
PostgreSQL 16 does not support them on partitioned tables, so ids come from a plain sequence.

## Revisit when
A single source exceeds ~20 GB or needs time-based retention. Then move dedupe to a small
unpartitioned `raw.record_index (source, record_key, payload_hash)` and add time sub-partitions.

## Not decided here
Object storage for original response bytes (`_object_uri`). Deferred until a payload is not
JSON (the HTML scraper) or Postgres size becomes a problem.
