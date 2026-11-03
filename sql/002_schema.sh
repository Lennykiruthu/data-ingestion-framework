#!/usr/bin/env bash
# Applies sql/schema/*.sql in order. Every file is idempotent.
#
#  * Fresh volume: Postgres runs this automatically on first start.
#  * Existing volume (init scripts do NOT re-run):
#        docker compose exec db bash /docker-entrypoint-initdb.d/002_schema.sh
set -euo pipefail
export PGOPTIONS="-c client_min_messages=warning"

SCHEMA_DIR="${SCHEMA_DIR:-/docker-entrypoint-initdb.d/schema}"
: "${POSTGRES_USER:?}" "${POSTGRES_DB:?}" "${APP_DB_USER:?}" "${APP_DB_PASSWORD:?}" "${GRAFANA_RO_PASSWORD:?}"

for f in "$SCHEMA_DIR"/*.sql; do
  echo "schema: applying $(basename "$f")"
  psql -v ON_ERROR_STOP=1 -q \
       --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
       -v app_user="$APP_DB_USER" \
       -v app_password="$APP_DB_PASSWORD" \
       -v grafana_ro_password="$GRAFANA_RO_PASSWORD" \
       -f "$f"
done
