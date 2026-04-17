from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import db_compat as sqlite3
from check_gpt_provider_nodes import check_once
from services.agent_service.multi_role_v3 import reclaim_stale_multi_role_v3_jobs


ROOT_DIR = Path(__file__).resolve().parent.parent


def _list_multi_role_v3_worker_pids() -> list[int]:
    try:
        out = subprocess.check_output(["ps", "-ef"], text=True)
    except Exception:
        return []
    pids: list[int] = []
    for line in out.splitlines():
        text = line.strip()
        if not text:
            continue
        if "jobs/run_multi_role_v3_worker.py" not in text:
            continue
        if "grep " in text:
            continue
        cols = text.split()
        if len(cols) < 2:
            continue
        try:
            pids.append(int(cols[1]))
        except Exception:
            continue
    return sorted(set(pids))


def _list_chief_roundtable_worker_pids() -> list[int]:
    try:
        out = subprocess.check_output(["ps", "-ef"], text=True)
    except Exception:
        return []
    pids: list[int] = []
    for line in out.splitlines():
        text = line.strip()
        if not text:
            continue
        if "jobs/run_chief_roundtable_worker.py" not in text:
            continue
        if "grep " in text:
            continue
        cols = text.split()
        if len(cols) < 2:
            continue
        try:
            pids.append(int(cols[1]))
        except Exception:
            continue
    return sorted(set(pids))


def ensure_multi_role_v3_workers(*, desired_count: int, max_spawn_per_run: int = 3) -> dict:
    desired = max(1, int(desired_count or 1))
    max_spawn = max(1, int(max_spawn_per_run or 1))
    before = _list_multi_role_v3_worker_pids()
    need = max(0, desired - len(before))
    spawn_count = min(need, max_spawn)

    spawned: list[dict] = []
    for idx in range(spawn_count):
        log_path = f"/tmp/multi_role_v3_worker_guard_{int(time.time())}_{idx + 1}.log"
        cmd = (
            f"cd {ROOT_DIR} && . {ROOT_DIR}/runtime_env.sh && "
            f"python3 jobs/run_multi_role_v3_worker.py >> {log_path} 2>&1"
        )
        proc = subprocess.Popen(["/bin/bash", "-lc", cmd], cwd=str(ROOT_DIR))
        spawned.append({"pid": int(proc.pid), "log_path": log_path})

    # 等 0.3 秒再采样，减少刚拉起就查不到的误差
    time.sleep(0.3)
    after = _list_multi_role_v3_worker_pids()
    return {
        "ok": True,
        "desired_count": desired,
        "max_spawn_per_run": max_spawn,
        "before_count": len(before),
        "after_count": len(after),
        "before_pids": before,
        "after_pids": after,
        "spawned": spawned,
    }


def ensure_chief_roundtable_workers(*, desired_count: int, max_spawn_per_run: int = 3) -> dict:
    desired = max(1, int(desired_count or 1))
    max_spawn = max(1, int(max_spawn_per_run or 1))
    before = _list_chief_roundtable_worker_pids()
    need = max(0, desired - len(before))
    spawn_count = min(need, max_spawn)

    spawned: list[dict] = []
    for idx in range(spawn_count):
        log_path = f"/tmp/chief_roundtable_worker_guard_{int(time.time())}_{idx + 1}.log"
        cmd = (
            f"cd {ROOT_DIR} && . {ROOT_DIR}/runtime_env.sh && "
            f"python3 jobs/run_chief_roundtable_worker.py >> {log_path} 2>&1"
        )
        proc = subprocess.Popen(["/bin/bash", "-lc", cmd], cwd=str(ROOT_DIR))
        spawned.append({"pid": int(proc.pid), "log_path": log_path})

    time.sleep(0.3)
    after = _list_chief_roundtable_worker_pids()
    return {
        "ok": True,
        "desired_count": desired,
        "max_spawn_per_run": max_spawn,
        "before_count": len(before),
        "after_count": len(after),
        "before_pids": before,
        "after_pids": after,
        "spawned": spawned,
    }


def _list_quantaalpha_worker_pids() -> list[int]:
    try:
        out = subprocess.check_output(["ps", "-ef"], text=True)
    except Exception:
        return []
    pids: list[int] = []
    for line in out.splitlines():
        text = line.strip()
        if not text:
            continue
        if "jobs/run_quantaalpha_worker.py" not in text:
            continue
        if "grep " in text:
            continue
        cols = text.split()
        if len(cols) < 2:
            continue
        try:
            pids.append(int(cols[1]))
        except Exception:
            continue
    return sorted(set(pids))


def ensure_quantaalpha_workers(*, desired_count: int, max_spawn_per_run: int = 1) -> dict:
    """Guard for quantaalpha worker: only active when QUANTAALPHA_EXECUTION_MODE is hybrid or research."""
    execution_mode = str(os.getenv("QUANTAALPHA_EXECUTION_MODE", "hybrid") or "hybrid").strip().lower()
    if execution_mode not in ("hybrid", "research"):
        return {
            "ok": True,
            "skipped": True,
            "reason": f"execution_mode={execution_mode!r} — worker guard inactive outside hybrid/research mode",
            "before_count": 0,
            "after_count": 0,
            "spawned": [],
        }
    desired = max(1, int(desired_count or 1))
    max_spawn = max(1, int(max_spawn_per_run or 1))
    before = _list_quantaalpha_worker_pids()
    need = max(0, desired - len(before))
    spawn_count = min(need, max_spawn)

    spawned: list[dict] = []
    for idx in range(spawn_count):
        log_path = f"/tmp/quantaalpha_worker_guard_{int(time.time())}_{idx + 1}.log"
        cmd = (
            f"cd {ROOT_DIR} && . {ROOT_DIR}/runtime_env.sh && "
            f"python3 jobs/run_quantaalpha_worker.py >> {log_path} 2>&1"
        )
        proc = subprocess.Popen(["/bin/bash", "-lc", cmd], cwd=str(ROOT_DIR))
        spawned.append({"pid": int(proc.pid), "log_path": log_path})

    time.sleep(0.3)
    after = _list_quantaalpha_worker_pids()
    return {
        "ok": True,
        "desired_count": desired,
        "max_spawn_per_run": max_spawn,
        "before_count": len(before),
        "after_count": len(after),
        "before_pids": before,
        "after_pids": after,
        "spawned": spawned,
    }


def get_llm_job_target(job_key: str) -> dict:
    registry = {
        "llm_provider_nodes_probe": {
            "job_key": "llm_provider_nodes_probe",
            "category": "maintenance",
            "runner_type": "llm_probe",
            "target": "check_gpt_provider_nodes.check_once(all_providers=true)",
        },
        "multi_role_v3_stale_recovery": {
            "job_key": "multi_role_v3_stale_recovery",
            "category": "maintenance",
            "runner_type": "multi_role_maintenance",
            "target": "services.agent_service.multi_role_v3.reclaim_stale_multi_role_v3_jobs",
        },
        "multi_role_v3_worker_guard": {
            "job_key": "multi_role_v3_worker_guard",
            "category": "maintenance",
            "runner_type": "multi_role_maintenance",
            "target": "jobs.llm_jobs.ensure_multi_role_v3_workers",
        },
        "chief_roundtable_worker_guard": {
            "job_key": "chief_roundtable_worker_guard",
            "category": "maintenance",
            "runner_type": "roundtable_maintenance",
            "target": "jobs.llm_jobs.ensure_chief_roundtable_workers",
        },
        "quantaalpha_worker_guard": {
            "job_key": "quantaalpha_worker_guard",
            "category": "maintenance",
            "runner_type": "quantaalpha_maintenance",
            "target": "jobs.llm_jobs.ensure_quantaalpha_workers",
        },
    }
    if job_key not in registry:
        raise KeyError(job_key)
    return registry[job_key]


def run_llm_job(job_key: str) -> dict:
    if job_key == "llm_provider_nodes_probe":
        rc = check_once(
            config_path=ROOT_DIR / "config" / "llm_providers.json",
            provider_key="gpt-5.4",
            all_providers=True,
            probe_model="gpt-5.4",
            case_mode="auto",
            timeout=20.0,
            probe_retries=1,
            disable_fail_threshold=2,
            keep_unhealthy_enabled=False,
        )
        return {
            "ok": rc == 0,
            "meta": {"return_code": rc, "all_providers": True},
            "stdout": "",
            "stderr": "",
        }
    if job_key == "multi_role_v3_stale_recovery":
        payload = reclaim_stale_multi_role_v3_jobs(
            sqlite3_module=sqlite3,
            db_path=ROOT_DIR / "stock_codes.db",
            stale_after_seconds=60,
        )
        return {
            "ok": bool(payload.get("ok", False)),
            "meta": payload,
            "stdout": "",
            "stderr": "",
        }
    if job_key == "multi_role_v3_worker_guard":
        desired_count = int(os.getenv("MULTI_ROLE_V3_WORKER_GUARD_DESIRED", "2") or 2)
        max_spawn = int(os.getenv("MULTI_ROLE_V3_WORKER_GUARD_MAX_SPAWN", "3") or 3)
        payload = ensure_multi_role_v3_workers(
            desired_count=desired_count,
            max_spawn_per_run=max_spawn,
        )
        return {
            "ok": bool(payload.get("ok", False)),
            "meta": payload,
            "stdout": "",
            "stderr": "",
        }
    if job_key == "chief_roundtable_worker_guard":
        desired_count = int(os.getenv("CHIEF_ROUNDTABLE_WORKER_GUARD_DESIRED", "1") or 1)
        max_spawn = int(os.getenv("CHIEF_ROUNDTABLE_WORKER_GUARD_MAX_SPAWN", "2") or 2)
        payload = ensure_chief_roundtable_workers(
            desired_count=desired_count,
            max_spawn_per_run=max_spawn,
        )
        return {
            "ok": bool(payload.get("ok", False)),
            "meta": payload,
            "stdout": "",
            "stderr": "",
        }
    if job_key == "quantaalpha_worker_guard":
        desired_count = int(os.getenv("QUANTAALPHA_WORKER_GUARD_DESIRED", "1") or 1)
        max_spawn = int(os.getenv("QUANTAALPHA_WORKER_GUARD_MAX_SPAWN", "1") or 1)
        payload = ensure_quantaalpha_workers(
            desired_count=desired_count,
            max_spawn_per_run=max_spawn,
        )
        return {
            "ok": bool(payload.get("ok", False)),
            "meta": payload,
            "stdout": "",
            "stderr": "",
        }
    raise KeyError(job_key)
