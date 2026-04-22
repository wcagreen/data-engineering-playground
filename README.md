# data-engineering-playground
A local ELT / ETL practice environment built with Docker Compose.
Supports two Airflow versions with a **single compose file**: **v3** (default) and **v2**.

## Stack

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Airflow DAG Processor | internal service | parses/serializes DAGs |
| pgAdmin | http://localhost:5050 | admin@example.com / admin |
| PostgreSQL | localhost:5432 | postgres / postgres |
| Celery Flower *(optional)* | http://localhost:5555 | — |

**Databases**
- `airflow` — Airflow internal metadata (managed automatically)
- `playground` — your data; schemas `staging` and `production`

## Airflow version profiles

| Profile | One-command launcher | Airflow | Auth | Dockerfile |
|---|---|---|---|---|
| **v3** (default) | `./scripts/dev/up-v3.ps1` | 3.0.0 | SimpleAuthManager | `Dockerfile` |
| **v2** | `./scripts/dev/up-v2.ps1` | 2.10.3 | FAB (Flask-AppBuilder) | `Dockerfile.v2` |

Both profiles share the same `dags/`, `data/`, `dbt/`, and `soda/` folders and still use **separate Docker volumes** (via `AIRFLOW_STACK_VARIANT`) so v2/v3 metadata does not mix. Run only one profile at a time to avoid port conflicts on 8080 / 5432 / 5050.

By default, published ports are bound to **127.0.0.1** (localhost only), so the playground is not exposed to your LAN unless you explicitly change `HOST_BIND`.

## Quick start

```bash
# Optional: create a local env file for overrides
# cp .env.example .env

# Optional: set custom credentials in .env
# (Postgres / pgAdmin / Airflow 2.x login)

# 1. Copy your CSV files into data/raw/

# PowerShell one-command launchers
./scripts/dev/up-v3.ps1
# or
./scripts/dev/up-v2.ps1

# Optional: stop / reset
./scripts/dev/down-v3.ps1
./scripts/dev/reset-v3.ps1
```

## Advanced (raw compose)

```bash
# v3
docker compose --env-file airflow.v3.env up --build -d

# v2
docker compose --env-file airflow.v2.env up --build -d

# Flower (Celery monitoring) is opt-in for both profiles:
docker compose --env-file airflow.v3.env --profile flower up -d
docker compose --env-file airflow.v2.env --profile flower up -d
```

First startup takes a few minutes while the Airflow image builds and the
database migrates.

## Project layout

```
airflow-playground/
├── dags/
│   ├── elt_dag.py          ELT  — CSV → staging → dbt → production → Soda
│   └── etl_dag.py          ETL  — CSV → pandas transform → production → Soda
├── data/
│   ├── raw/                Drop your CSV files here
│   └── processed/          Optional output for intermediate files
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/             generate_schema_name override
│   └── models/
│       ├── staging/        Views over raw staging data
│       └── mart/           Production tables (business logic)
├── soda/
│   ├── configuration.yml   Data source connection
│   └── checks/
│       ├── staging_checks.yml
│       └── production_checks.yml
├── sql/
│   └── transformations/    Plain SQL snippets (no dbt)
├── scripts/
│   ├── init/               Runs once on first PostgreSQL start
│   ├── pgadmin/            Auto-registers the PG server in pgAdmin
│   ├── python/             Pure Python transforms + script-runner entrypoint
│   ├── airflow/            Airflow auth/password bootstrapping config
│   └── dev/                One-command local launchers (v2/v3 up/down/reset)
├── Dockerfile              Custom Airflow image (adds pandas, dbt, soda)
├── requirements.txt
├── airflow.v2.env          Airflow 2.x preset overrides (single-compose mode)
├── airflow.v3.env          Airflow 3.x preset overrides (single-compose mode)
├── docker-compose.yml
└── .env.example
```

## Using the DAGs

Both DAGs are set to **manual trigger** (`schedule_interval=None`).

### ELT pipeline (`elt_dag.py`)

1. Edit the `CONFIG` block at the top — set `CSV_FILE_PATH` and `TABLE_NAME`.
2. Rename/edit `dbt/models/staging/stg_example.sql` and `dbt/models/mart/mart_example.sql`.
3. Update `dbt/models/staging/sources.yml` with your table name.
4. Update `soda/checks/production_checks.yml` with your column names.
5. Trigger `elt_pipeline` from the Airflow UI.

### ETL pipeline (`etl_dag.py`)

1. Edit the `CONFIG` block at the top — set `CSV_FILE_PATH` and `TABLE_NAME`.
2. Add your transformation logic inside the `transform()` function.
3. Update `soda/checks/production_checks.yml` with your column names.
4. Trigger `etl_pipeline` from the Airflow UI.

## Stopping / resetting

```bash
# ── v3 ───────────────────────────────────────────────────────────────────────
./scripts/dev/down-v3.ps1
./scripts/dev/reset-v3.ps1

# ── v2 ───────────────────────────────────────────────────────────────────────
./scripts/dev/down-v2.ps1
./scripts/dev/reset-v2.ps1
```

## Optional credential customization

`docker compose` automatically reads `.env`. If you want custom local credentials:

1. Copy `.env.example` to `.env`
2. Change values like `POSTGRES_PASSWORD`, `PLAYGROUND_PASSWORD`, `PGADMIN_DEFAULT_PASSWORD`, `_AIRFLOW_WWW_USER_PASSWORD`
3. For Airflow 3.0 SimpleAuthManager, edit `scripts/airflow/passwords.json`
4. Rebuild and restart (`docker compose up --build -d`)

Note: the PostgreSQL init script runs only on first volume creation. If you change `PLAYGROUND_*` values later, run `docker compose down -v` first to reinitialize.

## Useful commands

**Stack management**
```powershell
# ── v3 (default) ─────────────────────────────────────────────────────────────
./scripts/dev/up-v3.ps1          # first time, or after Dockerfile/requirements.txt changes
./scripts/dev/down-v3.ps1        # stop, keep data
./scripts/dev/reset-v3.ps1       # full reset (wipes v3 Postgres volumes)
docker compose --env-file airflow.v3.env ps

# ── v2 ───────────────────────────────────────────────────────────────────────
./scripts/dev/up-v2.ps1          # first time, or after Dockerfile.v2/requirements.v2.txt changes
./scripts/dev/down-v2.ps1
./scripts/dev/reset-v2.ps1
docker compose --env-file airflow.v2.env ps
```

**Logs**
```powershell
docker compose logs airflow-webserver --tail 50
docker compose logs airflow-scheduler --tail 50
docker compose logs airflow-worker --tail 50
docker compose logs -f airflow-webserver   # follow live
```

**Restart a single service**
```powershell
docker compose restart airflow-webserver
docker compose up -d --force-recreate airflow-worker   # recreate with latest config
```

**Local dev / testing**
```powershell
uv sync --extra dev                                    # install/update local venv
.venv\Scripts\python.exe -m pytest tests/ -v           # run unit tests
```

**Airflow CLI inside the container**
```powershell
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags trigger elt_pipeline
docker compose exec airflow-scheduler airflow dags pause etl_pipeline
```

**Postgres direct access**
```powershell
docker compose exec postgres psql -U postgres -d playground
```
