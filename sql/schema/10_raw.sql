-- Raw layer: ONE logical table, list-partitioned by source.
--
-- Why partition by source (and not by time): Postgres requires every unique index on a
-- partitioned table to include the partition key. Our dedupe key must NOT include
-- _ingested_at (the same record seen next month must still conflict), so time partitioning
-- would break idempotency. _source is already part of the dedupe key, so it is safe.
-- Revisit time sub-partitions only together with a separate dedupe index table.

create schema if not exists raw;

-- Standalone sequence instead of an identity column: identity columns on partitioned
-- tables are not supported on PostgreSQL 16.
create sequence if not exists raw.records_id_seq;

create table if not exists raw.records (
    _id            bigint      not null default nextval('raw.records_id_seq'),
    _source        text        not null,                 -- 'fred'
    _stream        text        not null,                 -- sub-feed inside a source (FRED series id)
    _run_id        text        not null,                 -- Dagster run id
    _ingested_at   timestamptz not null default now(),
    _observed_at   timestamptz,                          -- when the thing happened / the period it describes
    _request_url   text,                                 -- never contains credentials
    _record_key    text        not null,                 -- natural key, e.g. 'DFF:2026-10-20'
    _payload_hash  bytea       not null,                 -- sha256 of the canonical CONTENT of the record
    payload        jsonb       not null,
    primary key (_source, _id)
) partition by list (_source);

-- Idempotency: identical content for the same key is stored once; a revised value is a new row.
create unique index if not exists records_dedupe
    on raw.records (_source, _record_key, _payload_hash);
create index if not exists records_ingested
    on raw.records (_source, _ingested_at);
create index if not exists records_stream_observed
    on raw.records (_source, _stream, _observed_at desc);

-- Adding a source = one call:  select raw.register_source('usgs');
create or replace function raw.register_source(p_source text) returns void
language plpgsql as $$
begin
    execute format(
        'create table if not exists raw.%I partition of raw.records for values in (%L)',
        'records_' || p_source, p_source);
end $$;

select raw.register_source('fred') \g /dev/null
