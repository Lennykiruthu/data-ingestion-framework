-- Views the dashboard reads. Grafana's role can SELECT these and nothing else.
-- Views run with their owner's rights, so v_stream_status can summarise raw.* without
-- the dashboard role ever being able to read raw payloads.

create or replace view ops.v_source_status as
select c.source,
       c.display_name,
       c.description,
       c.expected_interval,
       ls.last_success_at,
       now() - ls.last_success_at as staleness,
       lr.status                  as last_run_status,
       lr.finished_at             as last_run_at,
       case
         when lr.status is null then 'NO DATA'
         when lr.status = 'FAILURE' then 'FAILING'
         when ls.last_success_at is null
              or now() - ls.last_success_at > 2 * c.expected_interval then 'STALE'
         else 'HEALTHY'
       end as status
from ops.source_config c
left join lateral (
    select max(m.finished_at) as last_success_at
    from ops.run_metrics m where m.source = c.source and m.status = 'SUCCESS'
) ls on true
left join lateral (
    select m.status, m.finished_at
    from ops.run_metrics m where m.source = c.source
    order by m.finished_at desc limit 1
) lr on true;

-- Successful runs since the most recent failure.
create or replace view ops.v_source_streak as
with last_fail as (
    select source, max(started_at) as last_failure_at
    from ops.run_metrics where status = 'FAILURE' group by source
)
select c.source,
       lf.last_failure_at,
       count(m.id) filter (
           where m.status = 'SUCCESS'
             and m.started_at > coalesce(lf.last_failure_at, '-infinity')
       ) as streak_runs
from ops.source_config c
left join last_fail lf on lf.source = c.source
left join ops.run_metrics m on m.source = c.source
group by c.source, lf.last_failure_at;

create or replace view ops.v_success_rate_7d as
select c.source,
       count(m.id) as runs,
       case when count(m.id) = 0 then null
            else round(100.0 * count(m.id) filter (where m.status = 'SUCCESS') / count(m.id), 1)
       end as success_pct
from ops.source_config c
left join ops.run_metrics m
       on m.source = c.source and m.started_at > now() - interval '7 days'
group by c.source;

-- One row per source per day for the last 90 days.
-- 1 = ok, 0 = at least one failed run that day, 2 = no run that day.
create or replace view ops.v_daily_status as
select d.day::date as day,
       c.source,
       c.display_name,
       case
         when count(m.id) = 0 then 2
         when count(m.id) filter (where m.status = 'FAILURE') > 0 then 0
         else 1
       end as status_code
from ops.source_config c
cross join generate_series(
        ((now() at time zone 'utc')::date - 89)::timestamp,
        (now() at time zone 'utc')::date::timestamp,
        interval '1 day') as d(day)
left join ops.run_metrics m
       on m.source = c.source
      and (m.started_at at time zone 'utc')::date = d.day::date
group by d.day, c.source, c.display_name;

create or replace view ops.v_recent_failures as
select m.finished_at,
       c.display_name,
       m.source,
       m.asset_key,
       m.error_class,
       case m.error_class
         when 'timeout'      then 'The data provider did not respond in time'
         when 'rate_limited' then 'The data provider asked us to slow down'
         when 'http_5xx'     then 'The data provider had a server problem'
         when 'http_4xx'     then 'The data provider rejected our request'
         when 'parse_error'  then 'The data arrived in an unexpected format'
         when 'db_error'     then 'We could not save the data to our database'
         when 'run_crashed'  then 'The pipeline run stopped unexpectedly'
         else 'Unexpected error'
       end as reason,
       m.attempts
from ops.run_metrics m
join ops.source_config c on c.source = m.source
where m.status = 'FAILURE'
order by m.finished_at desc
limit 50;

-- Per stream (FRED series): how much we hold and how recent it is. No payloads exposed.
-- New columns are appended at the end so `create or replace` works on existing databases.
create or replace view ops.v_stream_status as
select r._source as source,
       r._stream as stream,
       count(*)           as rows,
       max(r._observed_at) as latest_observed_at,
       max(r._ingested_at) as last_ingested_at,
       coalesce(sc.display_name, r._stream) as display_name,
       sc.cadence
from raw.records r
left join ops.stream_config sc on sc.source = r._source and sc.stream = r._stream
group by r._source, r._stream, sc.display_name, sc.cadence;

create or replace view ops.v_daily_new_rows as
select (m.started_at at time zone 'utc')::date as day,
       sum(m.rows_inserted)::int               as new_rows
from ops.run_metrics m
where m.status = 'SUCCESS'
group by 1;
