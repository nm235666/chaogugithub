from __future__ import annotations

import json
import os
import re
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from llm_provider_config import get_provider_candidates

ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = ROOT_DIR / "stock_codes.db"
DEFAULT_QUANTAALPHA_ROOT = ROOT_DIR / "external" / "quantaalpha"
DEFAULT_QUANTAALPHA_DATA = ROOT_DIR / "runtime" / "quantaalpha_data"
DEFAULT_QUANTAALPHA_RESULTS = ROOT_DIR / "runtime" / "quantaalpha_results"
DEFAULT_QUANTAALPHA_ENV = ROOT_DIR / "runtime" / "quantaalpha_runtime.env"

_TASK_LOCK = threading.RLock()
_TASK_THREADS: dict[str, threading.Thread] = {}

ERR_DATA_NOT_READY = "DATA_NOT_READY"
ERR_LLM_PROVIDER_UNAVAILABLE = "LLM_PROVIDER_UNAVAILABLE"
ERR_PROCESS_TIMEOUT = "PROCESS_TIMEOUT"
ERR_RUNNER_CONFIG_INVALID = "RUNNER_CONFIG_INVALID"
ERR_UNKNOWN = "UNKNOWN_ERROR"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _ensure_tables(conn, sqlite3_module) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quantaalpha_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT UNIQUE,
            job_key TEXT,
            task_type TEXT NOT NULL,
            status TEXT NOT NULL,
            error_code TEXT,
            error_message TEXT,
            input_json TEXT,
            output_json TEXT,
            artifacts_json TEXT,
            metrics_json TEXT,
            started_at TEXT,
            finished_at TEXT,
            duration_seconds REAL,
            created_at TEXT NOT NULL,
            update_time TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quantaalpha_factor_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            factor_name TEXT,
            ic REAL,
            rank_ic REAL,
            effective_window TEXT,
            source_version TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quantaalpha_backtest_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            strategy_name TEXT,
            arr REAL,
            mdd REAL,
            calmar REAL,
            params_json TEXT,
            artifact_path TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS research_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_date TEXT NOT NULL,
            report_type TEXT NOT NULL,
            subject_key TEXT NOT NULL,
            subject_name TEXT,
            model TEXT,
            markdown_content TEXT,
            context_json TEXT,
            created_at TEXT,
            update_time TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_quantaalpha_runs_task ON quantaalpha_runs(task_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_quantaalpha_runs_status ON quantaalpha_runs(status, created_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_quantaalpha_factor_task ON quantaalpha_factor_results(task_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_quantaalpha_backtest_task ON quantaalpha_backtest_results(task_id)")
    conn.commit()


def _pick_active_llm(llm_profile: str) -> dict[str, str] | None:
    requested = str(llm_profile or "").strip()
    model_name = requested if requested and requested.lower() != "auto" else "gpt-5.4"
    candidates = get_provider_candidates(model_name)
    if not candidates and model_name != "gpt-5.4":
        candidates = get_provider_candidates("gpt-5.4")
    if not candidates:
        return None
    chosen = candidates[0]
    return {
        "model": str(chosen.model or "gpt-5.4"),
        "base_url": str(chosen.base_url or ""),
        "api_key": str(chosen.api_key or ""),
    }


def _write_quantaalpha_env(path: Path, payload: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{k}={v}" for k, v in payload.items() if str(v or "").strip()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _parse_metrics(stdout: str) -> dict[str, float]:
    text = str(stdout or "")
    patterns = {
        "ic": r"\bIC\b[^0-9\-]*(-?\d+(?:\.\d+)?)",
        "rank_ic": r"(?:RankIC|Rank IC)[^0-9\-]*(-?\d+(?:\.\d+)?)",
        "arr": r"\bARR\b[^0-9\-]*(-?\d+(?:\.\d+)?)",
        "mdd": r"\bMDD\b[^0-9\-]*(-?\d+(?:\.\d+)?)",
        "calmar": r"\bCalmar\b[^0-9\-]*(-?\d+(?:\.\d+)?)",
    }
    out: dict[str, float] = {}
    for key, pattern in patterns.items():
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            try:
                out[key] = float(m.group(1))
            except Exception:
                continue
    return out


def _check_python_dependency(python_bin: str, module_name: str) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            [python_bin, "-c", f"import {module_name}"],
            capture_output=True,
            text=True,
            timeout=8,
            check=False,
        )
    except Exception as exc:
        return False, f"依赖预检失败: {exc}"
    if proc.returncode == 0:
        return True, ""
    detail = (proc.stderr or proc.stdout or "").strip()
    return False, detail


def _classify_cli_failure(return_code: int, stdout: str, stderr: str) -> tuple[str, str]:
    merged = f"{stdout}\n{stderr}"
    missing_mod = re.search(r"ModuleNotFoundError:\s+No module named ['\"]([^'\"]+)['\"]", merged)
    if missing_mod:
        mod = str(missing_mod.group(1) or "").strip() or "unknown"
        if mod == "dotenv":
            return (
                ERR_RUNNER_CONFIG_INVALID,
                "QuantaAlpha 运行依赖缺失：python-dotenv 未安装（模块 dotenv）。请在任务运行环境安装后重试。",
            )
        return (
            ERR_RUNNER_CONFIG_INVALID,
            f"QuantaAlpha 运行依赖缺失：模块 {mod} 未安装。请在任务运行环境安装后重试。",
        )
    if ".env file not found" in merged:
        return (
            ERR_RUNNER_CONFIG_INVALID,
            "QuantaAlpha 配置缺失：external/quantaalpha/.env 不存在，请先完成配置再重试。",
        )
    if "QLIB_DATA_DIR" in merged and ("not set" in merged or "does not exist" in merged):
        return (
            ERR_RUNNER_CONFIG_INVALID,
            "QuantaAlpha 数据目录未就绪：请配置有效 QLIB_DATA_DIR（需包含 calendars/features/instruments）。",
        )
    return ERR_UNKNOWN, f"exit_code={return_code}"


def _resolve_python_bin() -> str:
    explicit = str(os.getenv("QUANTAALPHA_PYTHON_BIN", "") or "").strip()
    if explicit:
        return explicit
    fallback = str(os.getenv("PYTHON_BIN", "") or "").strip()
    if fallback:
        return fallback
    return "python3"


def _ensure_quantaalpha_dotenv(
    *,
    qa_root: Path,
    llm: dict[str, str],
    data_dir: Path,
    out_dir: Path,
) -> Path:
    dotenv_path = qa_root / ".env"
    if dotenv_path.exists():
        return dotenv_path
    qlib_dir = str(os.getenv("QLIB_DATA_DIR", "") or "").strip() or str(data_dir)
    lines = [
        f"QLIB_DATA_DIR={qlib_dir}",
        f"QLIB_PROVIDER_URI={qlib_dir}",
        f"DATA_RESULTS_DIR={out_dir}",
        f"OPENAI_API_KEY={llm.get('api_key') or ''}",
        f"OPENAI_BASE_URL={llm.get('base_url') or ''}",
        f"CHAT_MODEL={llm.get('model') or 'gpt-5.4'}",
        f"REASONING_MODEL={llm.get('model') or 'gpt-5.4'}",
        "USE_LOCAL=True",
    ]
    dotenv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return dotenv_path


def _run_cli(task_type: str, payload: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    qa_root = Path(str(payload.get("qa_root") or DEFAULT_QUANTAALPHA_ROOT)).resolve()
    launcher = qa_root / "launcher.py"
    if not launcher.exists():
        return False, {"error_code": ERR_DATA_NOT_READY, "error_message": f"QuantaAlpha launcher 不存在: {launcher}"}
    llm = _pick_active_llm(str(payload.get("llm_profile") or "auto"))
    if llm is None:
        return False, {"error_code": ERR_LLM_PROVIDER_UNAVAILABLE, "error_message": "未找到 active LLM provider"}

    data_dir = Path(str(payload.get("data_dir") or DEFAULT_QUANTAALPHA_DATA)).resolve()
    out_dir = Path(str(payload.get("results_dir") or DEFAULT_QUANTAALPHA_RESULTS)).resolve()
    env_file = Path(str(payload.get("env_file") or DEFAULT_QUANTAALPHA_ENV)).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    env_payload = {
        "QUANTAALPHA_DATA_DIR": str(data_dir),
        "QUANTAALPHA_RESULTS_DIR": str(out_dir),
        "QUANTAALPHA_LLM_MODEL": llm["model"],
        "QUANTAALPHA_LLM_BASE_URL": llm["base_url"],
        "QUANTAALPHA_LLM_API_KEY": llm["api_key"],
    }
    _write_quantaalpha_env(env_file, env_payload)
    dotenv_path = _ensure_quantaalpha_dotenv(
        qa_root=qa_root,
        llm=llm,
        data_dir=data_dir,
        out_dir=out_dir,
    )
    python_bin = _resolve_python_bin()
    dep_ok, dep_detail = _check_python_dependency(python_bin, "dotenv")
    if not dep_ok:
        print(f"[quantaalpha] dependency_check_failed python_bin={python_bin}")
        return False, {
            "error_code": ERR_RUNNER_CONFIG_INVALID,
            "error_message": "QuantaAlpha 运行依赖缺失：python-dotenv 未安装（模块 dotenv）。请在任务运行环境安装后重试。",
            "stdout": "",
            "stderr": dep_detail[-12000:],
            "artifacts": {"results_dir": str(out_dir), "env_file": str(env_file), "qa_root": str(qa_root), "python_bin": python_bin, "dotenv_file": str(dotenv_path)},
        }

    command = [python_bin, str(launcher)]
    if task_type == "mine":
        command.append("mine")
    elif task_type == "backtest":
        command.append("backtest")
    elif task_type == "health_check":
        command.append("health_check")
    else:
        return False, {"error_code": ERR_RUNNER_CONFIG_INVALID, "error_message": f"未知 task_type: {task_type}"}
    for arg in payload.get("extra_args") or []:
        command.append(str(arg))
    timeout_s = int(payload.get("timeout_s") or 1800)

    env = os.environ.copy()
    env.update(env_payload)
    started = time.time()
    print(f"[quantaalpha] run task_type={task_type} python_bin={python_bin} cwd={qa_root}")
    try:
        proc = subprocess.run(
            command,
            cwd=str(qa_root),
            capture_output=True,
            text=True,
            timeout=max(60, timeout_s),
            check=False,
            env=env,
        )
    except subprocess.TimeoutExpired as exc:
        return False, {
            "error_code": ERR_PROCESS_TIMEOUT,
            "error_message": f"命令超时: {exc}",
            "stdout": str(getattr(exc, "stdout", "") or ""),
            "stderr": str(getattr(exc, "stderr", "") or ""),
            "duration_seconds": round(time.time() - started, 3),
        }
    except Exception as exc:
        return False, {"error_code": ERR_UNKNOWN, "error_message": str(exc)}

    stdout = str(proc.stdout or "")
    stderr = str(proc.stderr or "")
    metrics = _parse_metrics(stdout)
    ok = proc.returncode == 0
    if not ok:
        print(f"[quantaalpha] run_failed task_type={task_type} python_bin={python_bin} exit_code={proc.returncode}")
        error_code, error_message = _classify_cli_failure(proc.returncode, stdout, stderr)
        return False, {
            "error_code": error_code,
            "error_message": error_message,
            "stdout": stdout[-12000:],
            "stderr": stderr[-12000:],
            "metrics": metrics,
            "artifacts": {"results_dir": str(out_dir), "env_file": str(env_file), "python_bin": python_bin, "dotenv_file": str(dotenv_path)},
            "duration_seconds": round(time.time() - started, 3),
        }
    return True, {
        "stdout": stdout[-12000:],
        "stderr": stderr[-12000:],
        "metrics": metrics,
        "artifacts": {"results_dir": str(out_dir), "env_file": str(env_file), "qa_root": str(qa_root), "python_bin": python_bin, "dotenv_file": str(dotenv_path)},
        "duration_seconds": round(time.time() - started, 3),
    }


def _update_task(conn, task_id: str, *, status: str, error_code: str = "", error_message: str = "", output: dict | None = None, artifacts: dict | None = None, metrics: dict | None = None):
    now = _utc_now()
    started_row = conn.execute("SELECT started_at FROM quantaalpha_runs WHERE task_id = ? LIMIT 1", (task_id,)).fetchone()
    started_at = str((started_row[0] if started_row else "") or "")
    duration = None
    try:
        if started_at:
            duration = (datetime.strptime(now, "%Y-%m-%dT%H:%M:%SZ") - datetime.strptime(started_at, "%Y-%m-%dT%H:%M:%SZ")).total_seconds()
    except Exception:
        duration = None
    conn.execute(
        """
        UPDATE quantaalpha_runs
        SET status = ?, error_code = ?, error_message = ?, output_json = ?, artifacts_json = ?, metrics_json = ?,
            finished_at = ?, duration_seconds = ?, update_time = ?
        WHERE task_id = ?
        """,
        (
            status,
            error_code,
            error_message,
            _safe_json(output or {}),
            _safe_json(artifacts or {}),
            _safe_json(metrics or {}),
            now,
            duration,
            now,
            task_id,
        ),
    )
    conn.commit()


def _insert_result_rows(conn, task_id: str, task_type: str, metrics: dict[str, Any], payload: dict[str, Any], artifacts: dict[str, Any]):
    now = _utc_now()
    if task_type == "mine":
        conn.execute(
            """
            INSERT INTO quantaalpha_factor_results (task_id, factor_name, ic, rank_ic, effective_window, source_version, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                str(payload.get("direction") or "quantaalpha_factor"),
                metrics.get("ic"),
                metrics.get("rank_ic"),
                f"{int(payload.get('lookback') or 0)}d",
                str(payload.get("config_profile") or "default"),
                now,
            ),
        )
    if task_type == "backtest":
        conn.execute(
            """
            INSERT INTO quantaalpha_backtest_results (task_id, strategy_name, arr, mdd, calmar, params_json, artifact_path, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                str(payload.get("direction") or "quantaalpha_backtest"),
                metrics.get("arr"),
                metrics.get("mdd"),
                metrics.get("calmar"),
                _safe_json(payload),
                str((artifacts or {}).get("results_dir") or ""),
                now,
            ),
        )
        markdown = (
            f"# QuantaAlpha 回测报告\n\n"
            f"- 方向: {payload.get('direction') or '-'}\n"
            f"- 市场: {payload.get('market_scope') or 'A_share'}\n"
            f"- ARR: {metrics.get('arr', '-')}\n"
            f"- MDD: {metrics.get('mdd', '-')}\n"
            f"- Calmar: {metrics.get('calmar', '-')}\n"
            f"- 产物目录: {(artifacts or {}).get('results_dir') or '-'}\n"
        )
        report_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        conn.execute(
            """
            INSERT INTO research_reports (
                report_date, report_type, subject_key, subject_name, model, markdown_content, context_json, created_at, update_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                report_date,
                "quant_backtest",
                "quantaalpha_a_share",
                "QuantaAlpha A股回测",
                str(payload.get("llm_profile") or "quantaalpha"),
                markdown,
                _safe_json({"task_id": task_id, "metrics": metrics, "artifacts": artifacts}),
                now,
                now,
            ),
        )
    conn.commit()


def _run_task_thread(*, sqlite3_module, db_path: str, task_id: str):
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        row = conn.execute(
            "SELECT task_type, input_json FROM quantaalpha_runs WHERE task_id = ? LIMIT 1",
            (task_id,),
        ).fetchone()
        if not row:
            return
        task_type = str(row[0] or "")
        payload = json.loads(str(row[1] or "{}"))
        conn.execute("UPDATE quantaalpha_runs SET status = 'running', update_time = ? WHERE task_id = ?", (_utc_now(), task_id))
        conn.commit()
        ok, out = _run_cli(task_type, payload)
        if not ok:
            _update_task(
                conn,
                task_id,
                status="error",
                error_code=str(out.get("error_code") or ERR_UNKNOWN),
                error_message=str(out.get("error_message") or "执行失败"),
                output={"stdout": out.get("stdout") or "", "stderr": out.get("stderr") or ""},
                artifacts=out.get("artifacts") or {},
                metrics=out.get("metrics") or {},
            )
            return
        metrics = out.get("metrics") or {}
        artifacts = out.get("artifacts") or {}
        _insert_result_rows(conn, task_id, task_type, metrics, payload, artifacts)
        _update_task(
            conn,
            task_id,
            status="done",
            output={"stdout": out.get("stdout") or "", "stderr": out.get("stderr") or ""},
            artifacts=artifacts,
            metrics=metrics,
        )
    finally:
        conn.close()
        with _TASK_LOCK:
            _TASK_THREADS.pop(task_id, None)


def _start_task(*, sqlite3_module, db_path: str, task_type: str, payload: dict[str, Any], job_key: str) -> dict[str, Any]:
    now = _utc_now()
    task_id = f"qa_{task_type}_{uuid.uuid4().hex[:12]}"
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn, sqlite3_module)
        conn.execute(
            """
            INSERT INTO quantaalpha_runs (
                task_id, job_key, task_type, status, error_code, error_message,
                input_json, output_json, artifacts_json, metrics_json,
                started_at, finished_at, duration_seconds, created_at, update_time
            ) VALUES (?, ?, ?, 'pending', '', '', ?, '{}', '{}', '{}', ?, '', NULL, ?, ?)
            """,
            (task_id, job_key, task_type, _safe_json(payload), now, now, now),
        )
        conn.commit()
    finally:
        conn.close()
    thread = threading.Thread(
        target=_run_task_thread,
        kwargs={"sqlite3_module": sqlite3_module, "db_path": db_path, "task_id": task_id},
        daemon=True,
    )
    with _TASK_LOCK:
        _TASK_THREADS[task_id] = thread
    thread.start()
    return {"ok": True, "task_id": task_id, "status": "pending", "job_key": job_key, "task_type": task_type}


def start_quantaalpha_mine_task(*, sqlite3_module, db_path: str, direction: str, market_scope: str, lookback: int, config_profile: str, llm_profile: str, extra_args: list[str] | None = None) -> dict[str, Any]:
    payload = {
        "direction": direction.strip(),
        "market_scope": market_scope.strip() or "A_share",
        "lookback": int(max(1, lookback)),
        "config_profile": config_profile.strip() or "default",
        "llm_profile": llm_profile.strip() or "auto",
        "extra_args": list(extra_args or []),
    }
    return _start_task(sqlite3_module=sqlite3_module, db_path=db_path, task_type="mine", payload=payload, job_key="quantaalpha_mine_daily")


def start_quantaalpha_backtest_task(*, sqlite3_module, db_path: str, direction: str, market_scope: str, lookback: int, config_profile: str, llm_profile: str, extra_args: list[str] | None = None) -> dict[str, Any]:
    payload = {
        "direction": direction.strip(),
        "market_scope": market_scope.strip() or "A_share",
        "lookback": int(max(1, lookback)),
        "config_profile": config_profile.strip() or "default",
        "llm_profile": llm_profile.strip() or "auto",
        "extra_args": list(extra_args or []),
    }
    return _start_task(sqlite3_module=sqlite3_module, db_path=db_path, task_type="backtest", payload=payload, job_key="quantaalpha_backtest_daily")


def start_quantaalpha_health_check_task(*, sqlite3_module, db_path: str, extra_args: list[str] | None = None) -> dict[str, Any]:
    payload = {"extra_args": list(extra_args or []), "config_profile": "health_check"}
    return _start_task(sqlite3_module=sqlite3_module, db_path=db_path, task_type="health_check", payload=payload, job_key="quantaalpha_health_check")


def get_quantaalpha_task(*, sqlite3_module, db_path: str, task_id: str) -> dict[str, Any] | None:
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn, sqlite3_module)
        row = conn.execute(
            """
            SELECT task_id, job_key, task_type, status, error_code, error_message,
                   input_json, output_json, artifacts_json, metrics_json,
                   started_at, finished_at, duration_seconds, created_at, update_time
            FROM quantaalpha_runs
            WHERE task_id = ?
            LIMIT 1
            """,
            (task_id,),
        ).fetchone()
        if not row:
            return None
        item = dict(row)
        for field in ("input_json", "output_json", "artifacts_json", "metrics_json"):
            try:
                item[field.replace("_json", "")] = json.loads(str(item.get(field) or "{}"))
            except Exception:
                item[field.replace("_json", "")] = {}
            item.pop(field, None)
        return item
    finally:
        conn.close()


def query_quantaalpha_results(*, sqlite3_module, db_path: str, task_type: str, status: str, page: int, page_size: int) -> dict[str, Any]:
    page = max(1, int(page or 1))
    page_size = max(1, min(200, int(page_size or 20)))
    offset = (page - 1) * page_size
    task_type = str(task_type or "").strip()
    status = str(status or "").strip()
    where = []
    params: list[Any] = []
    if task_type:
        where.append("task_type = ?")
        params.append(task_type)
    if status:
        where.append("status = ?")
        params.append(status)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn, sqlite3_module)
        total = int(conn.execute(f"SELECT COUNT(*) FROM quantaalpha_runs {where_sql}", tuple(params)).fetchone()[0] or 0)
        rows = conn.execute(
            f"""
            SELECT task_id, job_key, task_type, status, error_code, error_message,
                   metrics_json, artifacts_json, created_at, started_at, finished_at, duration_seconds
            FROM quantaalpha_runs
            {where_sql}
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            tuple([*params, page_size, offset]),
        ).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            for field in ("metrics_json", "artifacts_json"):
                try:
                    item[field.replace("_json", "")] = json.loads(str(item.get(field) or "{}"))
                except Exception:
                    item[field.replace("_json", "")] = {}
                item.pop(field, None)
            items.append(item)
        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": (total + page_size - 1) // page_size if total else 0,
            "items": items,
        }
    finally:
        conn.close()


def run_quantaalpha_scheduled_job(*, sqlite3_module, db_path: str, job_key: str) -> dict[str, Any]:
    if job_key == "quantaalpha_health_check":
        task = start_quantaalpha_health_check_task(sqlite3_module=sqlite3_module, db_path=db_path)
    elif job_key == "quantaalpha_mine_daily":
        task = start_quantaalpha_mine_task(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            direction=os.getenv("QUANTAALPHA_DEFAULT_DIRECTION", "A股多因子挖掘"),
            market_scope="A_share",
            lookback=int(os.getenv("QUANTAALPHA_DEFAULT_LOOKBACK", "120")),
            config_profile="default",
            llm_profile="auto",
        )
    elif job_key == "quantaalpha_backtest_daily":
        task = start_quantaalpha_backtest_task(
            sqlite3_module=sqlite3_module,
            db_path=db_path,
            direction=os.getenv("QUANTAALPHA_DEFAULT_DIRECTION", "A股多因子回测"),
            market_scope="A_share",
            lookback=int(os.getenv("QUANTAALPHA_DEFAULT_LOOKBACK", "120")),
            config_profile="default",
            llm_profile="auto",
        )
    else:
        raise KeyError(job_key)

    # wait briefly in scheduler path to provide deterministic status
    task_id = str(task.get("task_id") or "")
    deadline = time.time() + 5
    latest = task
    while task_id and time.time() < deadline:
        item = get_quantaalpha_task(sqlite3_module=sqlite3_module, db_path=db_path, task_id=task_id)
        if not item:
            break
        latest = {"ok": item.get("status") != "error", "task_id": task_id, "status": item.get("status"), "job_key": job_key}
        if item.get("status") in {"done", "error"}:
            break
        time.sleep(0.5)
    return latest


def build_quantaalpha_service_runtime_deps(*, sqlite3_module, db_path: str) -> dict[str, Any]:
    return {
        "start_quantaalpha_mine_task": lambda **kwargs: start_quantaalpha_mine_task(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "start_quantaalpha_backtest_task": lambda **kwargs: start_quantaalpha_backtest_task(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "start_quantaalpha_health_check_task": lambda **kwargs: start_quantaalpha_health_check_task(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "get_quantaalpha_task": lambda task_id: get_quantaalpha_task(sqlite3_module=sqlite3_module, db_path=db_path, task_id=task_id),
        "query_quantaalpha_results": lambda **kwargs: query_quantaalpha_results(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "run_quantaalpha_scheduled_job": lambda job_key: run_quantaalpha_scheduled_job(sqlite3_module=sqlite3_module, db_path=db_path, job_key=job_key),
    }
