#!/usr/bin/env python3
"""
runner.py
---------
Lightweight HTTP server that executes a named Python script on request.

Mirrors the ECS / Lambda invoke pattern used in Airflow DAGs — the caller
sends a script name (and optional arguments), the runner executes it as a
subprocess, then returns the combined stdout/stderr and exit code.

Endpoints
---------
POST /run
    Body  : {"script": "script_name.py", "args": ["--flag", "value"], "timeout": 3600}
    Return: {"script": "script_name.py", "exit_code": 0, "output": "..."}

GET /health
    Return: {"status": "ok"}

Configuration (environment variables)
--------------------------------------
SCRIPTS_DIR   — directory that contains runnable scripts (default: /scripts)
RUNNER_PORT   — port to listen on (default: 8000)

Security
--------
Only scripts that resolve within SCRIPTS_DIR are allowed; attempts to
reference parent directories or absolute paths are rejected (path traversal
guard).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

SCRIPTS_DIR = Path(os.getenv("SCRIPTS_DIR", "/scripts")).resolve()
RUNNER_PORT = int(os.getenv("RUNNER_PORT", "8000"))

app = Flask(__name__)


def to_int(value: Any, default: int) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def execute_script(script_name: str, args: list[str], timeout_sec: int) -> dict[str, object]:
    try:
        script_path = (SCRIPTS_DIR / script_name).resolve()
        script_path.relative_to(SCRIPTS_DIR)
    except (ValueError, RuntimeError):
        return {"script": script_name, "exit_code": -1, "output": "Invalid script path"}

    if not script_path.is_file():
        return {"script": script_name, "exit_code": 2, "output": f"Script not found: {script_name}"}

    cmd = [sys.executable, str(script_path), *[str(a) for a in args]]
    print(f"[runner] Invoking : {' '.join(cmd)}", flush=True)
    print(f"[runner] Timeout  : {timeout_sec}s", flush=True)

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        output_lines: list[str] = []
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            output_lines.append(line)
        process.wait(timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        return {
            "script": script_name,
            "exit_code": -1,
            "output": f"Script timed out after {timeout_sec}s",
        }

    output = "".join(output_lines)
    print(f"[runner] Exit code: {process.returncode}", flush=True)
    return {
        "script": script_name,
        "exit_code": process.returncode,
        "output": output,
    }


@app.post("/run")
def run_script():
    body = request.get_json(force=True, silent=True) or {}
    script_name: str = body.get("script", "")
    args: list[str] = body.get("args", [])
    timeout_sec = to_int(body.get("timeout"), 3600)

    if not script_name:
        return jsonify({"error": "Missing required field: script"}), 400

    result = execute_script(script_name=script_name, args=args, timeout_sec=timeout_sec)
    return jsonify(result)


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--script", default=None)
    parser.add_argument("--timeout", type=int, default=3600)
    known, extra = parser.parse_known_args()

    # CLI mode (used by DockerOperator): execute one script and exit with script code.
    if known.script:
        result = execute_script(script_name=known.script, args=extra, timeout_sec=known.timeout)
        output = str(result.get("output", ""))
        if output:
            print(output, flush=True)
        sys.exit(to_int(result.get("exit_code"), 1))

    print(f"[runner] Scripts dir : {SCRIPTS_DIR}", flush=True)
    print(f"[runner] Listening on: 0.0.0.0:{RUNNER_PORT}", flush=True)
    app.run(host="0.0.0.0", port=RUNNER_PORT)
