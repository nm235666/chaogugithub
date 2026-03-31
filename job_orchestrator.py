#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import db_compat as sqlite3
from db_compat import get_redis_client
from job_registry import ROOT_DIR, JobDefinition, get_default_jobs
from realtime_streams import publish_app_event


JOB_DEFINITION_TABLE = "job_definitions"
JOB_RUN_TABLE = "job_runs"
JOB_ALERT_TABLE = "job_alerts"
LOCK_PREFIX = "job:lock:"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def cn_today() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=8)).strftime("%Y-%m-%d")


def resolve_recent_trade_dates() -> list[str]:
    proc = subprocess.run(
        [sys.executable, str(ROOT_DIR / "market_calendar.py"), "--count", "2"],
        capture_output=True,
        text=True,
        cwd=str(ROOT_DIR),
        timeout=60,
        check=False,
    )
    dates = [line.strip() for line in (proc.stdout or "").splitlines() if line.strip()]
    return dates[:2]


def expand_command(cmd: tuple[str, ...]) -> list[str]:
    trade_dates = resolve_recent_trade_dates()
    trade_1 = trade_dates[0] if len(trade_dates) >= 1 else ""
    trade_2 = trade_dates[1] if len(trade_dates) >= 2 else trade_1
    out: list[str] = []
    for part in cmd:
        out.append(
            str(part)
            .replace("__CN_DATE__", cn_today())
            .replace("__TRADE_DATE_1__", trade_1)
            .replace("__TRADE_DATE_2__", trade_2)
        )
    return out


def ensure_tables(conn: sqlite3.Connection) -> None:
    def _exists(name: str) -> bool:
        return conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        ).fetchone()[0] > 0

    if not _exists(JOB_DEFINITION_TABLE):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {JOB_DEFINITION_TABLE} (
                job_key TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT,
                owner TEXT,
                schedule_expr TEXT,
                description TEXT,
                enabled INTEGER DEFAULT 1,
                commands_json TEXT,
                created_at TEXT,
                update_time TEXT
            )
            """
        )
    if not _exists(JOB_RUN_TABLE):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {JOB_RUN_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_key TEXT NOT NULL,
                trigger_mode TEXT,
                status TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                duration_seconds REAL,
                host_name TEXT,
                process_id INTEGER,
                exit_code INTEGER,
                command_json TEXT,
                stdout_text TEXT,
                stderr_text TEXT,
                error_text TEXT,
                metadata_json TEXT,
                created_at TEXT NOT NULL,
                update_time TEXT NOT NULL
            )
            """
        )
    if not _exists(JOB_ALERT_TABLE):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {JOB_ALERT_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_key TEXT NOT NULL,
                run_id INTEGER,
                severity TEXT NOT NULL DEFAULT 'error',
                message TEXT NOT NULL,
                detail_text TEXT,
                acknowledged INTEGER NOT NULL DEFAULT 0,
                acknowledged_at TEXT,
                created_at TEXT NOT NULL,
                update_time TEXT NOT NULL
            )
            """
        )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{JOB_RUN_TABLE}_job_time ON {JOB_RUN_TABLE}(job_key, created_at DESC)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{JOB_RUN_TABLE}_status ON {JOB_RUN_TABLE}(status, created_at DESC)")
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{JOB_ALERT_TABLE}_job_time ON {JOB_ALERT_TABLE}(job_key, created_at DESC)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{JOB_ALERT_TABLE}_ack ON {JOB_ALERT_TABLE}(acknowledged, created_at DESC)"
    )
    conn.commit()


def sync_job_definitions(conn: sqlite3.Connection) -> None:
    now = utc_now()
    for job in get_default_jobs():
        conn.execute(
            f"""
            INSERT INTO {JOB_DEFINITION_TABLE} (
                job_key, name, category, owner, schedule_expr, description, enabled, commands_json, created_at, update_time
            ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
            ON CONFLICT(job_key) DO UPDATE SET
                name=excluded.name,
                category=excluded.category,
                owner=excluded.owner,
                schedule_expr=excluded.schedule_expr,
                description=excluded.description,
                commands_json=excluded.commands_json,
                update_time=excluded.update_time
            """,
            (
                job.job_key,
                job.name,
                job.category,
                job.owner,
                job.schedule_expr,
                job.description,
                json.dumps([list(x) for x in job.commands], ensure_ascii=False),
                now,
                now,
            ),
        )
    conn.commit()


def find_job(job_key: str) -> JobDefinition:
    for job in get_default_jobs():
        if job.job_key == job_key:
            return job
    raise KeyError(job_key)


def acquire_lock(job_key: str, ttl_seconds: int = 7200):
    client = get_redis_client()
    if client is None:
        return None
    token = f"{socket.gethostname()}:{os.getpid()}:{time.time()}"
    key = f"{LOCK_PREFIX}{job_key}"
    ok = client.set(key, token, nx=True, ex=max(ttl_seconds, 60))
    if not ok:
        return None
    return (client, key, token)


def release_lock(lock_info) -> None:
    if not lock_info:
        return
    client, key, token = lock_info
    try:
        current = client.get(key)
        if isinstance(current, bytes):
            current = current.decode("utf-8", errors="ignore")
        if current and current == token:
            client.delete(key)
    except Exception:
        pass


def insert_job_run(conn: sqlite3.Connection, job_key: str, trigger_mode: str, command_json: str, metadata_json: str = "") -> int:
    now = utc_now()
    conn.execute(
        f"""
        INSERT INTO {JOB_RUN_TABLE} (
            job_key, trigger_mode, status, started_at, host_name, process_id, command_json,
            metadata_json, created_at, update_time
        ) VALUES (?, ?, 'running', ?, ?, ?, ?, ?, ?, ?)
        """,
        (job_key, trigger_mode, now, socket.gethostname(), os.getpid(), command_json, metadata_json, now, now),
    )
    conn.commit()
    row = conn.execute(f"SELECT MAX(id) FROM {JOB_RUN_TABLE} WHERE job_key = ?", (job_key,)).fetchone()
    return int(row[0] or 0)


def update_job_run(conn: sqlite3.Connection, run_id: int, *, status: str, exit_code: int, stdout_text: str, stderr_text: str, error_text: str) -> None:
    finished_at = utc_now()
    row = conn.execute(f"SELECT started_at FROM {JOB_RUN_TABLE} WHERE id = ?", (run_id,)).fetchone()
    started_at = str(row[0] or "") if row else ""
    duration = None
    try:
        if started_at:
            duration = (datetime.strptime(finished_at, "%Y-%m-%dT%H:%M:%SZ") - datetime.strptime(started_at, "%Y-%m-%dT%H:%M:%SZ")).total_seconds()
    except Exception:
        duration = None
    conn.execute(
        f"""
        UPDATE {JOB_RUN_TABLE}
        SET status = ?, finished_at = ?, duration_seconds = ?, exit_code = ?,
            stdout_text = ?, stderr_text = ?, error_text = ?, update_time = ?
        WHERE id = ?
        """,
        (status, finished_at, duration, exit_code, stdout_text, stderr_text, error_text, finished_at, run_id),
    )
    conn.commit()


def insert_job_alert(
    conn: sqlite3.Connection,
    *,
    job_key: str,
    run_id: int,
    severity: str,
    message: str,
    detail_text: str = "",
) -> int:
    now = utc_now()
    conn.execute(
        f"""
        INSERT INTO {JOB_ALERT_TABLE} (
            job_key, run_id, severity, message, detail_text, acknowledged,
            acknowledged_at, created_at, update_time
        ) VALUES (?, ?, ?, ?, ?, 0, '', ?, ?)
        """,
        (job_key, run_id, severity, message, detail_text, now, now),
    )
    conn.commit()
    row = conn.execute(f"SELECT MAX(id) FROM {JOB_ALERT_TABLE}").fetchone()
    return int(row[0] or 0)


def dry_run_job(job_key: str, trigger_mode: str = "dry-run") -> dict:
    conn = sqlite3.connect(str(ROOT_DIR / "stock_codes.db"))
    conn.row_factory = sqlite3.Row
    try:
        ensure_tables(conn)
        sync_job_definitions(conn)
        job = find_job(job_key)
        expanded_commands = [expand_command(cmd) for cmd in job.commands]
        return {
            "ok": True,
            "job_key": job.job_key,
            "name": job.name,
            "trigger_mode": trigger_mode,
            "commands": expanded_commands,
        }
    finally:
        conn.close()


def run_job(job_key: str, trigger_mode: str = "manual", max_retries_per_command: int = 1) -> dict:
    conn = sqlite3.connect(str(ROOT_DIR / "stock_codes.db"))
    conn.row_factory = sqlite3.Row
    ensure_tables(conn)
    sync_job_definitions(conn)
    job = find_job(job_key)
    lock_info = acquire_lock(job_key)
    if lock_info is None:
        raise RuntimeError(f"任务正在运行中: {job_key}")
    expanded_commands = [expand_command(cmd) for cmd in job.commands]
    run_id = insert_job_run(
        conn,
        job_key=job.job_key,
        trigger_mode=trigger_mode,
        command_json=json.dumps(expanded_commands, ensure_ascii=False),
        metadata_json=json.dumps({"name": job.name, "category": job.category}, ensure_ascii=False),
    )
    publish_app_event(event="job_run_update", payload={"job_key": job.job_key, "run_id": run_id, "status": "running"}, producer="job_orchestrator.py")
    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    exit_code = 0
    error_text = ""
    try:
        for cmd in expanded_commands:
            cmd_text = " ".join(cmd)
            attempt = 0
            while True:
                proc = subprocess.run(
                    cmd,
                    cwd=str(ROOT_DIR),
                    capture_output=True,
                    text=True,
                    timeout=7200,
                    check=False,
                )
                stdout_parts.append(f"$ {cmd_text}\n[attempt={attempt}]\n{proc.stdout}")
                stderr_parts.append(f"$ {cmd_text}\n[attempt={attempt}]\n{proc.stderr}")
                if proc.returncode == 0:
                    break
                if attempt >= max(max_retries_per_command, 0):
                    exit_code = proc.returncode
                    error_text = f"command failed after retry: {cmd_text}"
                    break
                attempt += 1
                time.sleep(min(10, 2 * attempt))
            if exit_code != 0:
                break
        status = "success" if exit_code == 0 else "error"
        update_job_run(
            conn,
            run_id,
            status=status,
            exit_code=exit_code,
            stdout_text="\n\n".join(stdout_parts)[-20000:],
            stderr_text="\n\n".join(stderr_parts)[-20000:],
            error_text=error_text,
        )
        alert_id = 0
        if status != "success":
            alert_id = insert_job_alert(
                conn,
                job_key=job.job_key,
                run_id=run_id,
                severity="error",
                message=f"任务失败: {job.job_key}",
                detail_text=error_text or "\n\n".join(stderr_parts)[-8000:],
            )
            publish_app_event(
                event="job_alert",
                payload={"job_key": job.job_key, "run_id": run_id, "alert_id": alert_id, "status": status},
                producer="job_orchestrator.py",
            )
        publish_app_event(event="job_run_update", payload={"job_key": job.job_key, "run_id": run_id, "status": status, "exit_code": exit_code}, producer="job_orchestrator.py")
        return {
            "ok": exit_code == 0,
            "run_id": run_id,
            "job_key": job.job_key,
            "status": status,
            "exit_code": exit_code,
            "alert_id": alert_id,
        }
    finally:
        release_lock(lock_info)
        conn.close()


def query_job_definitions() -> dict:
    conn = sqlite3.connect(str(ROOT_DIR / "stock_codes.db"))
    conn.row_factory = sqlite3.Row
    try:
        ensure_tables(conn)
        sync_job_definitions(conn)
        rows = conn.execute(
            f"""
            SELECT job_key, name, category, owner, schedule_expr, description, enabled, update_time
            FROM {JOB_DEFINITION_TABLE}
            ORDER BY category, job_key
            """
        ).fetchall()
        items = [dict(r) for r in rows]
        return {"items": items, "total": len(items)}
    finally:
        conn.close()


def query_job_runs(job_key: str = "", status: str = "", limit: int = 50) -> dict:
    conn = sqlite3.connect(str(ROOT_DIR / "stock_codes.db"))
    conn.row_factory = sqlite3.Row
    try:
        ensure_tables(conn)
        where = []
        params: list[object] = []
        if job_key.strip():
            where.append("job_key = ?")
            params.append(job_key.strip())
        if status.strip():
            where.append("status = ?")
            params.append(status.strip())
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"""
            SELECT id, job_key, trigger_mode, status, started_at, finished_at, duration_seconds,
                   host_name, process_id, exit_code, error_text, created_at, update_time
            FROM {JOB_RUN_TABLE}
            {where_sql}
            ORDER BY id DESC
            LIMIT ?
            """,
            (*params, max(limit, 1)),
        ).fetchall()
        items = [dict(r) for r in rows]
        return {"items": items, "total": len(items)}
    finally:
        conn.close()


def query_job_alerts(job_key: str = "", unresolved_only: bool = True, limit: int = 50) -> dict:
    conn = sqlite3.connect(str(ROOT_DIR / "stock_codes.db"))
    conn.row_factory = sqlite3.Row
    try:
        ensure_tables(conn)
        where = []
        params: list[object] = []
        if job_key.strip():
            where.append("job_key = ?")
            params.append(job_key.strip())
        if unresolved_only:
            where.append("COALESCE(acknowledged, 0) = 0")
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"""
            SELECT id, job_key, run_id, severity, message, detail_text,
                   acknowledged, acknowledged_at, created_at, update_time
            FROM {JOB_ALERT_TABLE}
            {where_sql}
            ORDER BY id DESC
            LIMIT ?
            """,
            (*params, max(limit, 1)),
        ).fetchall()
        items = [dict(r) for r in rows]
        return {"items": items, "total": len(items)}
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="统一任务编排器")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("sync", help="同步任务定义到数据库")
    run_p = sub.add_parser("run", help="执行指定任务")
    run_p.add_argument("job_key", help="任务标识")
    run_p.add_argument("--trigger-mode", default="manual", help="manual / cron / api")
    sub.add_parser("list", help="列出任务定义")
    dry_p = sub.add_parser("dry-run", help="展开任务命令但不执行")
    dry_p.add_argument("job_key", help="任务标识")
    runs_p = sub.add_parser("runs", help="列出任务运行记录")
    runs_p.add_argument("--job-key", default="", help="按 job_key 过滤")
    runs_p.add_argument("--status", default="", help="按状态过滤")
    runs_p.add_argument("--limit", type=int, default=20, help="返回条数")
    alerts_p = sub.add_parser("alerts", help="列出任务告警")
    alerts_p.add_argument("--job-key", default="", help="按 job_key 过滤")
    alerts_p.add_argument("--all", action="store_true", help="包含已确认告警")
    alerts_p.add_argument("--limit", type=int, default=20, help="返回条数")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.cmd == "sync":
        conn = sqlite3.connect(str(ROOT_DIR / "stock_codes.db"))
        try:
            ensure_tables(conn)
            sync_job_definitions(conn)
            print("synced")
        finally:
            conn.close()
        return 0
    if args.cmd == "run":
        result = run_job(args.job_key, trigger_mode=args.trigger_mode)
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result.get("ok") else 1
    if args.cmd == "list":
        print(json.dumps(query_job_definitions(), ensure_ascii=False))
        return 0
    if args.cmd == "dry-run":
        print(json.dumps(dry_run_job(args.job_key), ensure_ascii=False))
        return 0
    if args.cmd == "runs":
        print(json.dumps(query_job_runs(job_key=args.job_key, status=args.status, limit=args.limit), ensure_ascii=False))
        return 0
    if args.cmd == "alerts":
        print(
            json.dumps(
                query_job_alerts(job_key=args.job_key, unresolved_only=not args.all, limit=args.limit),
                ensure_ascii=False,
            )
        )
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
