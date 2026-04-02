from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]


def _normalize_meta(meta: dict | None = None) -> dict:
    payload = dict(meta or {})
    payload.setdefault("cwd", str(ROOT_DIR))
    return payload


def run_python_script(script_name: str, *args: str, timeout_s: int = 900, meta: dict | None = None) -> dict:
    script_path = ROOT_DIR / script_name
    cmd = [sys.executable, str(script_path), *args]
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
        timeout=timeout_s,
        check=False,
    )
    return {
        "ok": proc.returncode == 0,
        "runner": "python_script",
        "command": cmd,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "meta": {
            **_normalize_meta(meta),
            "script": script_name,
            "returncode": proc.returncode,
        },
    }


def run_python_commands(commands: list[dict], *, stop_on_error: bool = True) -> dict:
    outputs = []
    for item in commands:
        result = run_python_script(
            item["script"],
            *item.get("args", []),
            timeout_s=int(item.get("timeout_s", 900)),
            meta=item.get("meta"),
        )
        outputs.append(result)
        if stop_on_error and not result["ok"]:
            break
    ok = all(item["ok"] for item in outputs)
    return {
        "ok": ok,
        "runner": "python_pipeline",
        "command": [item["command"] for item in outputs],
        "stdout": "\n".join(item["stdout"] for item in outputs if item["stdout"]).strip(),
        "stderr": "\n".join(item["stderr"] for item in outputs if item["stderr"]).strip(),
        "meta": {
            "steps": [item["meta"] for item in outputs],
            "step_count": len(outputs),
        },
    }
