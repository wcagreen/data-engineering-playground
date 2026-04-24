# airflow-playground

A local data-engineering practice environment built with Docker Compose. Run real Airflow DAGs against a PostgreSQL database, transform data with dbt, validate it with Soda, and author pure-Python transforms — all without a cloud account.

Supports **two Airflow versions** from a **single compose file**: **v3** (default, 3.0.0) and **v2** (2.10.3).

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with Compose v2)
- **GNU make** — for the platform-independent `make` targets (Windows: `choco install make`, Git Bash, or WSL; already available on macOS/Linux)
- PowerShell 5.1+ — only required for the `./scripts/dev/*.ps1` launchers (Windows)
- [uv](https://docs.astral.sh/uv/) — only required for local development / running tests outside Docker

## Stack

| Service | URL | Default credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | `admin` / `admin` |
| pgAdmin | http://localhost:5050 | `admin@example.com` / `admin` |
| Python Runner | http://localhost:8000 | — |
| PostgreSQL | `localhost:5432` | `postgres` / `postgres` |
| Celery Flower *(opt-in)* | http://localhost:5555 | — |

**Databases**
- `airflow` — Airflow internal metadata (managed automatically)
- `playground` — your data; schemas `staging` and `production`

## Airflow version profiles

| Profile | Launcher | Airflow | Auth | Dockerfile |
|---|---|---|---|---|
| **v3** (default) | `./scripts/dev/up-v3.ps1` | 3.0.0 | SimpleAuthManager | `Dockerfile` |
| **v2** | `./scripts/dev/up-v2.ps1` | 2.10.3 | FAB (Flask-AppBuilder) | `Dockerfile.v2` |

Both profiles share the same `dags/`, `data/`, `dbt/`, and `soda/` folders and use **separate Docker volumes** (via `AIRFLOW_STACK_VARIANT`) so v2/v3 metadata never mixes. Run only one profile at a time to avoid port conflicts on 8080 / 5432 / 5050.

Published ports are bound to **127.0.0.1** (localhost only) by default. Change `HOST_BIND` in your `.env` to expose them on the LAN.

## Quick start

```sh
# 1. Drop your CSV file(s) into data/raw/

# 2. Copy env files and edit credentials (optional)
make setup

# 3. Start the stack (first run builds the Docker image — takes a few minutes)
make             # defaults to up-v3
make up-v2       # or start the Airflow 2.x stack instead

# 4. Open the Airflow UI and trigger a DAG
#    http://localhost:8080  →  admin / admin
```

To stop or wipe the stack:

```sh
make down          # stop containers, keep data volumes (v3)
make reset         # stop + delete volumes, full reset (v3)
make down-v2       # v2 equivalents
make reset-v2
```

**Windows PowerShell alternative** (if you don't have `make`):

```powershell
./scripts/dev/up-v3.ps1
./scripts/dev/down-v3.ps1
./scripts/dev/reset-v3.ps1
```

## Advanced

**Extra args via make** (`ARGS` is forwarded to `docker compose`):

```sh
make up-v3 ARGS="--no-build"                          # skip image rebuild
make up-v3 ARGS="airflow-webserver --force-recreate"  # recreate one service
make down  ARGS="-v"                                  # also delete volumes
```

**Raw compose**:

```sh
# v3
docker compose --env-file airflow.v3.env up --build -d

# v2
docker compose --env-file airflow.v2.env up --build -d

# Enable optional Celery Flower monitoring dashboard
docker compose --env-file airflow.v3.env --profile flower up -d
```

## Project layout

```
airflow-playground/
├── dags/
│   ├── bank_customers_dag.py   Standalone ETL: CSV → staging → SQL validation
│   ├── elt_dag.py              ELT:  CSV → staging → dbt → production → Soda
│   ├── etl_dag.py              ETL:  CSV → pandas transform → production → Soda
│   └── script_runner_dag.py    Run any script/python/ file via python-runner service
├── data/
│   ├── raw/                    Drop your CSV files here
│   └── processed/              Optional output for intermediate files
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/                 generate_schema_name override
│   └── models/
│       ├── staging/            Views over raw staging data
│       └── mart/               Production tables (business logic)
├── soda/
│   ├── configuration.yml       Data source connection
│   └── checks/
│       ├── staging_checks.yml
│       └── production_checks.yml
├── sql/
│   └── transformations/        Plain SQL snippets (no dbt)
├── scripts/
│   ├── init/                   Runs once on first PostgreSQL start
│   ├── pgadmin/                Auto-registers the PG server in pgAdmin
│   ├── python/                 Pure Python transforms + script-runner entrypoint
│   ├── airflow/                Airflow auth/password bootstrapping config
│   └── dev/                    One-command local launchers (v2/v3 up/down/reset)
├── tests/
│   └── test_etl_transforms.py  Unit tests for scripts/python/etl_transforms.py
├── Dockerfile                  Custom Airflow image (v3) — adds pandas, dbt, soda
├── Dockerfile.v2               Custom Airflow image (v2)
├── Dockerfile.runner           python-runner service image
├── requirements.txt            Python deps installed in the v3 image
├── requirements.v2.txt         Python deps installed in the v2 image
├── airflow.v3.env              Airflow 3.x environment overrides
├── airflow.v2.env              Airflow 2.x environment overrides
├── docker-compose.yml
└── pyproject.toml              Local venv for IDE support and pytest
```

## Using the DAGs

All DAGs are set to **manual trigger** (`schedule=None`). Open the Airflow UI at http://localhost:8080, unpause the DAG, then click **Trigger DAG**.

### Bank customers (`bank_customers_dag.py`)

A self-contained ETL that loads `data/raw/BankChurners.csv` into the `staging.bank_customers` table and validates the row count.

1. Ensure `BankChurners.csv` is in `data/raw/` (included in the repo).
2. Trigger `bank_customers_dag` from the Airflow UI.

To adapt it for your own CSV: update `CSV_FILE_PATH`, `TARGET_SCHEMA`, and `TARGET_TABLE` at the top of the file.

### ELT pipeline (`elt_dag.py`)

**Extract → Load → Transform**: raw CSV lands in `staging`, dbt models promote it to `production`, Soda validates the result.

1. Drop your CSV into `data/raw/`.
2. Set `CSV_FILE_PATH` and `TABLE_NAME` in the `CONFIG` block.
3. Edit or rename `dbt/models/staging/stg_example.sql` and `dbt/models/mart/mart_example.sql`.
4. Update `dbt/models/staging/sources.yml` with your staging table name.
5. Update `soda/checks/production_checks.yml` with your column names.
6. Trigger `elt_pipeline` from the Airflow UI.

### ETL pipeline (`etl_dag.py`)

**Extract → Transform → Load**: data is transformed in Python (pandas) before it ever hits the database, then written straight to `production`.

1. Drop your CSV into `data/raw/`.
2. Set `CSV_FILE_PATH` and `TABLE_NAME` in the `CONFIG` block.
3. Add your business logic to `apply_transforms()` in `scripts/python/etl_transforms.py`.
4. Update `soda/checks/production_checks.yml` with your column names.
5. Trigger `etl_pipeline` from the Airflow UI.

### Script runner (`script_runner_dag.py`)

Invokes any Python script in `scripts/python/` via the **python-runner** sidecar service (a lightweight HTTP server). Mirrors the ECS / Lambda task pattern — useful for running heavier scripts without coupling them to the Airflow worker process.

1. Drop your script into `scripts/python/`.
2. Set `SCRIPT_NAME` and `SCRIPT_ARGS` at the top of `script_runner_dag.py`.
3. Trigger `script_runner_dag` from the Airflow UI, or pass `{"script": "my_script.py", "args": [...]}` as the trigger configuration at runtime.

## Local development

The `pyproject.toml` defines a local virtualenv so Pylance can resolve Airflow, dbt, and Soda types, and so you can run pytest without spinning up Docker.

```sh
# Create / update the local venv and run tests (requires uv + make)
make venv-v3     # creates .venv via uv sync
make test        # uv run pytest
make lint        # uv run ruff check
make lint-fix    # uv run ruff check --fix

# Regenerate dbt/models/staging/sources.yml from CSV headers in data/raw/
make sources
```

**Without make** (PowerShell):

```powershell
uv sync --extra dev
.venv\Scripts\Activate.ps1
python -m pytest tests/ -v
```

Transformation logic in `scripts/python/etl_transforms.py` has zero Airflow dependency and is fully testable locally. Add your own cases to `tests/test_etl_transforms.py`.

## Optional credential customization

`docker compose` reads variables from the env file passed with `--env-file`. To override defaults:

1. Copy `airflow.v3.env.example` → `airflow.v3.env` (or `airflow.v2.env.example` → `airflow.v2.env`).
2. Change `POSTGRES_PASSWORD`, `PLAYGROUND_PASSWORD`, `PGADMIN_DEFAULT_PASSWORD`, etc.
3. For the Airflow 3.x SimpleAuthManager, edit `scripts/airflow/passwords.json`.
4. Rebuild: `./scripts/dev/up-v3.ps1` (passes `--build` automatically).

> The PostgreSQL init script only runs on first volume creation. If you change `PLAYGROUND_*` credentials after the volume already exists, run `./scripts/dev/reset-v3.ps1` first.

## Useful commands

**Stack management**
```sh
# ── make (platform-independent) ──────────────────────────────────────────────
make              # up-v3 (default)
make up-v2
make down         # down-v3
make down-v2
make reset        # reset-v3
make reset-v2

# ── PowerShell launchers (Windows) ───────────────────────────────────────────
./scripts/dev/up-v3.ps1
./scripts/dev/down-v3.ps1
./scripts/dev/reset-v3.ps1
./scripts/dev/up-v2.ps1
./scripts/dev/down-v2.ps1
./scripts/dev/reset-v2.ps1

docker compose --env-file airflow.v3.env ps
docker compose --env-file airflow.v2.env ps
```

**Logs**
```powershell
docker compose logs airflow-webserver --tail 50
docker compose logs airflow-scheduler --tail 50
docker compose logs airflow-worker    --tail 50
docker compose logs -f airflow-webserver          # follow live
```

**Restart a single service**
```powershell
docker compose restart airflow-webserver
docker compose up -d --force-recreate airflow-worker
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
