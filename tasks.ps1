#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Task runner for Airflow Playground (Windows alternative to make).

.DESCRIPTION
    Usage:
      .\tasks.ps1              # up-v3 (default)
      .\tasks.ps1 up-v3
      .\tasks.ps1 up-v2
      .\tasks.ps1 down
      .\tasks.ps1 reset
      .\tasks.ps1 sources      # regenerate dbt/models/staging/sources.yml
      .\tasks.ps1 test         # run pytest
      .\tasks.ps1 lint         # run ruff
      .\tasks.ps1 lint-fix     # run ruff --fix
      .\tasks.ps1 setup        # first-time: copy .example env files
      .\tasks.ps1 help         # show this list
#>

param(
    [string]$Task = "up-v3",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Task([string]$Name) {
    switch ($Name) {

        "setup" {
            if (-not (Test-Path "airflow.v3.env")) {
                Copy-Item "airflow.v3.env.example" "airflow.v3.env"
                Write-Host "Created airflow.v3.env from example."
            }
            if (-not (Test-Path "airflow.v2.env")) {
                Copy-Item "airflow.v2.env.example" "airflow.v2.env"
                Write-Host "Created airflow.v2.env from example."
            }
            Write-Host "Edit airflow.v3.env and airflow.v2.env with your passwords, then run: .\tasks.ps1 up-v3"
        }

        "up-v3"    { & scripts\dev\up-v3.ps1 @ExtraArgs }
        "up-v2"    { & scripts\dev\up-v2.ps1 @ExtraArgs }

        "down-v3"  { & scripts\dev\down-v3.ps1 @ExtraArgs }
        "down-v2"  { & scripts\dev\down-v2.ps1 @ExtraArgs }
        "down"     { & scripts\dev\down-v3.ps1 @ExtraArgs }

        "reset-v3" { & scripts\dev\reset-v3.ps1 @ExtraArgs }
        "reset-v2" { & scripts\dev\reset-v2.ps1 @ExtraArgs }
        "reset"    { & scripts\dev\reset-v3.ps1 @ExtraArgs }

        "sources" {
            python scripts/dev/generate_sources.py
        }

        "test" {
            uv run pytest
        }

        "lint" {
            uv run ruff check .
        }

        "lint-fix" {
            uv run ruff check --fix .
        }

        "help" {
            Write-Host @"
Usage: .\tasks.ps1 [task]

  setup       First-time setup: copy .example env files to real env files
  up-v3       Start Airflow v3 stack (default)
  up-v2       Start Airflow v2 stack
  down        Stop v3 stack (keeps data volumes)
  down-v2     Stop v2 stack (keeps data volumes)
  reset       Stop v3 stack and wipe all volumes (fresh start)
  reset-v2    Stop v2 stack and wipe all volumes
  sources     Regenerate dbt/models/staging/sources.yml from data/raw/*.csv
  test        Run pytest
  lint        Run ruff linter
  lint-fix    Run ruff linter with auto-fix
  help        Show this help
"@
        }

        default {
            Write-Error "Unknown task: '$Name'. Run '.\tasks.ps1 help' for available tasks."
            exit 1
        }
    }
}

Invoke-Task $Task
