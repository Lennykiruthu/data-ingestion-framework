#!/bin/bash
set -e

# Runs once, on first start with an empty data volume.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
  create schema if not exists raw;
  create schema if not exists ops;

  create role ${APP_DB_USER} login password '${APP_DB_PASSWORD}';
  grant usage on schema raw, ops to ${APP_DB_USER};

  -- raw is append-only for the app: insert + select only
  alter default privileges in schema raw grant insert, select on tables to ${APP_DB_USER};
  -- ops needs upserts
  alter default privileges in schema ops grant insert, select, update on tables to ${APP_DB_USER};

  create role ${DAGSTER_PG_USER} login password '${DAGSTER_PG_PASSWORD}';
  create database ${DAGSTER_PG_DB} owner ${DAGSTER_PG_USER};
EOSQL