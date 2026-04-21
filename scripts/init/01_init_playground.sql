-- scripts/init/01_init_playground.sql
-- -------------------------------------
-- This script runs ONCE when the PostgreSQL container first starts.
-- It is executed by psql as the postgres superuser, so \c works here.
--
-- Creates:
--   database  : playground
--   user      : playground_user  (password: playground_pass)
--   schemas   : staging, production  (inside playground)

-- Create the playground database and dedicated user
CREATE DATABASE playground;
CREATE USER playground_user WITH PASSWORD 'playground_pass';
GRANT ALL PRIVILEGES ON DATABASE playground TO playground_user;

-- Switch to the playground database and set up schemas
\c playground

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS production;

-- Grant the user full access to both schemas
GRANT ALL ON SCHEMA staging    TO playground_user;
GRANT ALL ON SCHEMA production TO playground_user;

-- Ensure future tables/sequences are also accessible
ALTER DEFAULT PRIVILEGES IN SCHEMA staging    GRANT ALL ON TABLES    TO playground_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging    GRANT ALL ON SEQUENCES TO playground_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA production GRANT ALL ON TABLES    TO playground_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA production GRANT ALL ON SEQUENCES TO playground_user;

-- Make playground_user the owner of both schemas so dbt can create tables
ALTER SCHEMA staging    OWNER TO playground_user;
ALTER SCHEMA production OWNER TO playground_user;
