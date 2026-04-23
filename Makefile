# ─────────────────────────────────────────────────────────────────────────────
# Airflow Playground — Makefile
#
# Requires: GNU make  (Windows: choco install make  |  Git Bash  |  WSL)
#           PowerShell (pwsh / powershell)
#
# Usage:
#   make                        → up-v3 (default)
#   make up-v3
#   make up-v2
#   make down
#   make reset
#   make sources                → regenerate dbt/models/staging/sources.yml from data/raw/*.csv
#   make test                   → run pytest
#   make lint                   → run ruff
#
# Extra args are forwarded to the underlying docker compose command via ARGS:
#   make up-v3 ARGS="--no-build"
#   make up-v3 ARGS="airflow-webserver --force-recreate"
#   make down  ARGS="-v"
# ─────────────────────────────────────────────────────────────────────────────

ARGS ?=

.DEFAULT_GOAL := up-v3
.PHONY: up-v3 up-v2 down-v3 down-v2 down reset-v3 reset-v2 reset sources test lint setup

# ── First-time setup ──────────────────────────────────────────────────────────
# Copy .example env files to real env files if they don't exist yet.

setup:
	@if not exist airflow.v3.env copy airflow.v3.env.example airflow.v3.env
	@if not exist airflow.v2.env copy airflow.v2.env.example airflow.v2.env
	@echo Edit airflow.v3.env and airflow.v2.env with your passwords, then run: make up-v3

# ── Airflow stack ─────────────────────────────────────────────────────────────

up-v3:
	powershell -File scripts/dev/up-v3.ps1 $(ARGS)

up-v2:
	powershell -File scripts/dev/up-v2.ps1 $(ARGS)

down-v3:
	powershell -File scripts/dev/down-v3.ps1 $(ARGS)

down-v2:
	powershell -File scripts/dev/down-v2.ps1 $(ARGS)

## Aliases (defaults to v3)
down: down-v3

reset-v3:
	powershell -File scripts/dev/reset-v3.ps1 $(ARGS)

reset-v2:
	powershell -File scripts/dev/reset-v2.ps1 $(ARGS)

reset: reset-v3

# ── dbt sources ───────────────────────────────────────────────────────────────
# Regenerate dbt/models/staging/sources.yml from CSV headers in data/raw/.
# Run this whenever you add or replace a CSV.

sources:
	python scripts/dev/generate_sources.py

# ── Quality ───────────────────────────────────────────────────────────────────

test:
	uv run pytest

lint:
	uv run ruff check .

lint-fix:
	uv run ruff check --fix .
