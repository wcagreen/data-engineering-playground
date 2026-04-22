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

import os
import subprocess
import sys
from pathlib import Path

from flask import Flask, jsonify, request

SCRIPTS_DIR = Path(os.getenv("SCRIPTS_DIR", "/scripts")).resolve()
RUNNER_PORT = int(os.getenv("RUNNER_PORT", "8000"))

app = Flask(__name__)


@app.post("/run")
def run_script():
    body = request.get_json(force=True, silent=True) or {}
    script_name: str = body.get("script", "")
    args: list[str] = body.get("args", [])
    timeout_sec: int = int(body.get("timeout", 3600))

    if not script_name:
        return jsonify({"error": "Missing required field: script"}), 400

    # ── Path traversal guard ───────────────────────────────────────────────────
    # Resolve against SCRIPTS_DIR and verify the result is still inside it.
    # This rejects inputs like "../secrets.py" or "/etc/passwd".
    try:
        script_path = (SCRIPTS_DIR / script_name).resolve()
        script_path.relative_to(SCRIPTS_DIR)
    except (ValueError, RuntimeError):
        return jsonify({"error": "Invalid script path"}), 400

    if not script_path.is_file():
        return jsonify({"error": f"Script not found: {script_name}"}), 404

    cmd = [sys.executable, str(script_path), *[str(a) for a in args]]
    print(f"[runner] Invoking : {' '.join(cmd)}", flush=True)
    print(f"[runner] Timeout  : {timeout_sec}s", flush=True)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
    except subprocess.TimeoutExpired:
        return (
            jsonify(
                {
                    "script": script_name,
                    "exit_code": -1,
                    "error": f"Script timed out after {timeout_sec}s",
                    "output": "",
                }
            ),
            200,
        )

    # Merge stdout + stderr so callers get a single log stream (matches ECS task
    # log behaviour where all output lands in one CloudWatch log group).
    output = result.stdout + (("\n[stderr]\n" + result.stderr) if result.stderr else "")

    print(f"[runner] Exit code: {result.returncode}", flush=True)

    return jsonify(
        {
            "script": script_name,
            "exit_code": result.returncode,
            "output": output,
        }
    )


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    print(f"[runner] Scripts dir : {SCRIPTS_DIR}", flush=True)
    print(f"[runner] Listening on: 0.0.0.0:{RUNNER_PORT}", flush=True)
    app.run(host="0.0.0.0", port=RUNNER_PORT)
