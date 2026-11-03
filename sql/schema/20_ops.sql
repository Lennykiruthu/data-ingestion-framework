-- Operational metadata: what each source is, and how every run went.
create schema if not exists ops;

create table if not exists ops.source_config (
    source            text primary key,
    display_name      text     not null,        -- shown on the public dashboard
    description       text,
    expected_interval interval not null         -- cadence used for the "stale" rule (stale = 2x this)
);

insert into ops.source_config (source, display_name, description, expected_interval)
values ('fred',
        'Federal Reserve Economic Data (FRED)',
        'US interest rates, inflation and labour-market indicators from the St. Louis Fed',
        interval '1 day')
on conflict (source) do update
    set display_name = excluded.display_name,
        description = excluded.description,
        expected_interval = excluded.expected_interval;

create table if not exists ops.run_metrics (
    id             bigint generated always as identity primary key,
    run_id         text        not null,
    source         text        not null references ops.source_config (source),
    asset_key      text        not null,
    status         text        not null check (status in ('SUCCESS', 'FAILURE')),
    started_at     timestamptz not null,
    finished_at    timestamptz not null,
    latency_ms     integer,
    rows_fetched   integer,
    rows_inserted  integer,
    bytes_fetched  bigint,
    http_status    integer,
    schema_hash    text,                         -- reserved for the schema-drift check
    error_class    text,                         -- coarse category, see v_recent_failures
    attempts       integer     not null default 1,
    details        jsonb,                        -- per-stream counts
    unique (run_id, asset_key)
);
create index if not exists run_metrics_source_started on ops.run_metrics (source, started_at desc);

-- Human-readable names for streams (FRED series) so the public dashboard is readable.
create table if not exists ops.stream_config (
    source       text not null references ops.source_config (source),
    stream       text not null,
    display_name text not null,
    cadence      text not null,                -- how often the PROVIDER publishes new values
    primary key (source, stream)
);

insert into ops.stream_config (source, stream, display_name, cadence) values
    ('fred', 'DFF',      'Federal funds rate',            'Daily'),
    ('fred', 'GS10',     '10-year Treasury yield',        'Monthly'),
    ('fred', 'CPIAUCSL', 'Consumer price index (CPI)',    'Monthly'),
    ('fred', 'UNRATE',   'Unemployment rate',             'Monthly'),
    ('fred', 'USREC',    'US recession indicator',        'Monthly'),
    ('fred', 'T10YIE',   '10-year breakeven inflation',   'Daily')
on conflict (source, stream) do update
    set display_name = excluded.display_name, cadence = excluded.cadence;
