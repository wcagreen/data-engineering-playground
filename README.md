# data-engineering-playground
A local ELT / ETL practice environment built with Docker Compose.

## Stack

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| pgAdmin | http://localhost:5050 | admin@example.com / admin |
| PostgreSQL | localhost:5432 | postgres / postgres |
| Celery Flower *(optional)* | http://localhost:5555 | — |

**Databases**
- `airflow` — Airflow internal metadata (managed automatically)
- `playground` — your data; schemas `staging` and `production`

## Quick start

```bash
# 1. Copy your CSV files into data/raw/
# 2. Build the custom Airflow image and start everything
docker compose up --build -d

# Flower (Celery monitoring) is opt-in:
docker compose --profile flower up -d
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
│   └── pgadmin/            Auto-registers the PG server in pgAdmin
├── notebooks/
│   └── exploration.ipynb   Prototype transforms, query PG
├── Dockerfile              Custom Airflow image (adds pandas, dbt, soda)
├── requirements.txt
├── docker-compose.yml
└── .env
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
# Stop all containers (data is preserved in Docker volumes)
docker compose down

# Full reset — removes all volumes including the PostgreSQL data
docker compose down -v
```

## Useful commands

**Stack management**
```powershell
docker compose up --build -d      # first time, or after Dockerfile/requirements.txt changes
docker compose up -d              # start (no rebuild)
docker compose down               # stop, keep data
docker compose down -v            # full reset (wipes Postgres volumes)
docker compose ps                 # check container status
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
