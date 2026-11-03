-- Least privilege.
--   app user   : raw is append-only (INSERT, SELECT; no UPDATE/DELETE), ops is writable
--   grafana_ro : SELECT on the ops views only; no access to raw at all

grant usage on schema raw, ops to :"app_user";
grant insert, select on raw.records to :"app_user";
grant usage, select on sequence raw.records_id_seq to :"app_user";
grant select on ops.source_config to :"app_user";
grant select, insert, update on ops.run_metrics to :"app_user";

grant usage on schema ops to grafana_ro;
grant select on ops.v_source_status, ops.v_source_streak, ops.v_success_rate_7d,
                ops.v_daily_status, ops.v_recent_failures, ops.v_stream_status,
                ops.v_daily_new_rows
    to grafana_ro;
