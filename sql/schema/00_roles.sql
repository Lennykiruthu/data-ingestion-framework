-- Roles. Idempotent: safe to re-run on an existing volume.
-- Variables come from sql/002_schema.sh (psql -v ...).
--   app_user / app_password        the Dagster code's role (insert + select on raw, write on ops)
--   grafana_ro_password            read-only role used by Grafana (ops views only)

select format('create role %I login password %L', :'app_user', :'app_password')
where not exists (select 1 from pg_roles where rolname = :'app_user') \gexec

select format('create role grafana_ro login password %L', :'grafana_ro_password')
where not exists (select 1 from pg_roles where rolname = 'grafana_ro') \gexec

-- keep both passwords in sync with .env on every run (.env is the source of truth)
select format('alter role %I password %L', :'app_user', :'app_password') \gexec
select format('alter role grafana_ro password %L', :'grafana_ro_password') \gexec
