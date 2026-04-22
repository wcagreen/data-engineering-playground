#!/usr/bin/env bash
set -euo pipefail

# This script runs once on first PostgreSQL container start.
# It creates a playground database/user and the staging/production schemas.
PLAYGROUND_DB="${PLAYGROUND_DB:-playground}"
PLAYGROUND_USER="${PLAYGROUND_USER:-playground_user}"
PLAYGROUND_PASSWORD="${PLAYGROUND_PASSWORD:-playground_pass}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-airflow}"

if ! psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" -tAc \
  "SELECT 1 FROM pg_database WHERE datname = '${PLAYGROUND_DB}'" | grep -q 1; then
  psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" \
    -c "CREATE DATABASE \"${PLAYGROUND_DB}\";"
fi

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" \
  -v playground_db="${PLAYGROUND_DB}" \
  -v playground_user="${PLAYGROUND_USER}" \
  -v playground_password="${PLAYGROUND_PASSWORD}" <<'EOSQL'
SELECT format(
  'DO $$
   BEGIN
     IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %L) THEN
       EXECUTE format(''CREATE USER %%I WITH PASSWORD %%L'', %L, %L);
     ELSE
       EXECUTE format(''ALTER USER %%I WITH PASSWORD %%L'', %L, %L);
     END IF;
   END
   $$;',
  :'playground_user',
  :'playground_user',
  :'playground_password',
  :'playground_user',
  :'playground_password'
) \gexec

SELECT format('GRANT ALL PRIVILEGES ON DATABASE %I TO %I', :'playground_db', :'playground_user') \gexec
EOSQL

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${PLAYGROUND_DB}" \
  -v playground_user="${PLAYGROUND_USER}" <<'EOSQL'
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS production;

SELECT format('GRANT ALL ON SCHEMA staging TO %I', :'playground_user') \gexec
SELECT format('GRANT ALL ON SCHEMA production TO %I', :'playground_user') \gexec

SELECT format('ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO %I', :'playground_user') \gexec
SELECT format('ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON SEQUENCES TO %I', :'playground_user') \gexec
SELECT format('ALTER DEFAULT PRIVILEGES IN SCHEMA production GRANT ALL ON TABLES TO %I', :'playground_user') \gexec
SELECT format('ALTER DEFAULT PRIVILEGES IN SCHEMA production GRANT ALL ON SEQUENCES TO %I', :'playground_user') \gexec

SELECT format('ALTER SCHEMA staging OWNER TO %I', :'playground_user') \gexec
SELECT format('ALTER SCHEMA production OWNER TO %I', :'playground_user') \gexec
EOSQL
