from __future__ import annotations

import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


def query_news_daily_summaries(
    *,
    sqlite3_module,
    db_path,
    get_or_build_cached_logic_view,
    extract_logic_view_from_markdown,
    summary_date: str,
    source_filter: str,
    model: str,
    page: int,
    page_size: int,
):
    summary_date = summary_date.strip()
    source_filter = source_filter.strip()
    model = model.strip()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    where_clauses = []
    params: list[object] = []
    if summary_date:
        where_clauses.append("summary_date = ?")
        params.append(summary_date)
    if source_filter:
        where_clauses.append("source_filter LIKE ?")
        params.append(f"%{source_filter}%")
    if model:
        where_clauses.append("model = ?")
        params.append(model)
    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='news_daily_summaries'"
        ).fetchone()[0]
        if not table_exists:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}
        count_sql = f"SELECT COUNT(*) FROM news_daily_summaries{where_sql}"
        data_sql = f"""
        SELECT id, summary_date, filter_importance, source_filter, news_count, model, prompt_version, summary_markdown, created_at
        FROM news_daily_summaries
        {where_sql}
        ORDER BY summary_date DESC, id DESC
        LIMIT ? OFFSET ?
        """
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = conn.execute(data_sql, [*params, page_size, offset]).fetchall()
        data = []
        for row in rows:
            item = dict(row)
            try:
                item["logic_view"] = get_or_build_cached_logic_view(
                    conn,
                    entity_type="news_daily_summary",
                    entity_key=str(item.get("id") or ""),
                    source_payload=item.get("summary_markdown", ""),
                    builder=lambda text=item.get("summary_markdown", ""): extract_logic_view_from_markdown(text),
                )
            except Exception as exc:
                # 列表接口优先可用性：单条逻辑视图异常不应导致整页 500。
                item["logic_view"] = {
                    "summary": {"conclusion": "", "focus": "", "risk": ""},
                    "chains": [],
                    "has_logic": False,
                    "error": f"logic_view_build_failed: {exc}",
                }
            data.append(item)
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "items": data,
    }


def get_daily_summary_by_date(*, sqlite3_module, db_path, get_or_build_cached_logic_view, extract_logic_view_from_markdown, summary_date: str):
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='news_daily_summaries'"
        ).fetchone()[0]
        if not table_exists:
            return None
        row = conn.execute(
            """
            SELECT id, summary_date, filter_importance, source_filter, news_count, model, prompt_version, summary_markdown, created_at
            FROM news_daily_summaries
            WHERE summary_date = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (summary_date,),
        ).fetchone()
        if not row:
            return None
        item = dict(row)
        try:
            item["logic_view"] = get_or_build_cached_logic_view(
                conn,
                entity_type="news_daily_summary",
                entity_key=str(item.get("id") or ""),
                source_payload=item.get("summary_markdown", ""),
                builder=lambda text=item.get("summary_markdown", ""): extract_logic_view_from_markdown(text),
            )
        except Exception as exc:
            item["logic_view"] = {
                "summary": {"conclusion": "", "focus": "", "risk": ""},
                "chains": [],
                "has_logic": False,
                "error": f"logic_view_build_failed: {exc}",
            }
        return item
    finally:
        conn.close()


def generate_daily_summary(*, root_dir: Path, extract_llm_result_marker, model: str, summary_date: str):
    script_path = root_dir / "llm_summarize_daily_important_news.py"
    cmd = [
        "python3",
        str(script_path),
        "--date",
        summary_date,
        "--model",
        model,
        "--max-news",
        "12",
        "--min-news",
        "6",
        "--max-prompt-chars",
        "9000",
        "--title-max-len",
        "80",
        "--summary-max-len",
        "100",
        "--request-timeout",
        "90",
        "--max-retries",
        "1",
        "--use-retrieval",
        "1",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired as exc:
        stdout_text = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr_text = exc.stderr if isinstance(exc.stderr, str) else ""
        meta = extract_llm_result_marker(stdout_text)
        timeout_meta = {
            **(meta or {}),
            "final_error_code": "timeout",
            "attempts": list((meta or {}).get("attempts") or []),
        }
        timeout_message = (
            "日报总结生成失败: 子进程超时（600秒），"
            "请检查模型响应速度或尝试缩短输出内容。"
        )
        detail = (stderr_text or stdout_text or "").strip()
        if detail:
            timeout_message = f"{timeout_message} 详情: {detail[:400]}"
        error = RuntimeError(timeout_message)
        setattr(error, "meta", timeout_meta)
        raise error from exc
    meta = extract_llm_result_marker(proc.stdout)
    if proc.returncode != 0:
        error = RuntimeError(f"日报总结生成失败: {proc.stderr.strip() or proc.stdout.strip()}")
        setattr(error, "meta", meta)
        raise error
    return {"stdout": proc.stdout, "stderr": proc.stderr, "meta": meta}


def cleanup_async_jobs(*, jobs: dict[str, dict], lock: threading.Lock, ttl_seconds: int):
    cutoff = time.time() - ttl_seconds
    with lock:
        expired = [job_id for job_id, job in jobs.items() if float(job.get("updated_at_ts", 0)) < cutoff]
        for job_id in expired:
            jobs.pop(job_id, None)


def serialize_async_daily_summary_job(job: dict):
    return {
        "job_id": job.get("job_id"),
        "status": job.get("status"),
        "progress": job.get("progress"),
        "stage": job.get("stage"),
        "message": job.get("message"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "finished_at": job.get("finished_at"),
        "summary_date": job.get("summary_date"),
        "model": job.get("model"),
        "requested_model": job.get("requested_model"),
        "used_model": job.get("used_model"),
        "attempts": job.get("attempts") or [],
        "item": job.get("item"),
        "run_stdout": job.get("run_stdout"),
        "notification": job.get("notification"),
        "error": job.get("error"),
        "fallback_used": bool(job.get("fallback_used")),
        "final_error_code": job.get("final_error_code") or "",
    }


def create_async_daily_summary_job(*, jobs: dict[str, dict], lock: threading.Lock, publish_app_event, model: str, summary_date: str):
    now = datetime.now(timezone.utc).isoformat()
    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "status": "queued",
        "progress": 5,
        "stage": "queued",
        "message": "任务已创建，等待后台生成日报总结",
        "created_at": now,
        "updated_at": now,
        "finished_at": "",
        "updated_at_ts": time.time(),
        "summary_date": summary_date,
        "model": model,
        "requested_model": model,
        "used_model": "",
        "attempts": [],
        "item": None,
        "run_stdout": "",
        "notification": None,
        "error": "",
        "fallback_used": False,
        "final_error_code": "",
    }
    with lock:
        jobs[job_id] = job
    publish_app_event(
        event="daily_summary_job_update",
        payload={"job_id": job_id, "status": "queued", "progress": 5, "stage": "queued", "summary_date": summary_date, "model": model},
        producer="backend.server",
    )
    return job


def run_async_daily_summary_job(
    *,
    jobs: dict[str, dict],
    lock: threading.Lock,
    publish_app_event,
    generate_daily_summary_fn,
    get_daily_summary_by_date_fn,
    notify_fn=None,
    job_id: str,
):
    with lock:
        job = jobs.get(job_id)
        if not job:
            return
        job["status"] = "running"
        job["progress"] = 15
        job["stage"] = "llm"
        job["message"] = "正在生成日报总结，请稍候"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        job["updated_at_ts"] = time.time()
        model = str(job["model"])
        summary_date = str(job["summary_date"])
    publish_app_event(
        event="daily_summary_job_update",
        payload={"job_id": job_id, "status": "running", "progress": 15, "stage": "llm", "summary_date": summary_date, "model": model},
        producer="backend.server",
    )

    try:
        run_info = generate_daily_summary_fn(model=model, summary_date=summary_date)
        item = get_daily_summary_by_date_fn(summary_date)
        now = datetime.now(timezone.utc).isoformat()
        with lock:
            job = jobs.get(job_id)
            if not job:
                return
            job["status"] = "done"
            job["progress"] = 100
            job["stage"] = "done"
            job["message"] = "日报总结生成完成"
            job["item"] = item
            job["used_model"] = str((item or {}).get("model") or "")
            job["attempts"] = list((run_info.get("meta") or {}).get("attempts") or [])
            job["fallback_used"] = bool((run_info.get("meta") or {}).get("fallback_used") or (job.get("used_model") and job.get("used_model") != job.get("requested_model")))
            job["final_error_code"] = str((run_info.get("meta") or {}).get("final_error_code") or "")
            job["run_stdout"] = run_info.get("stdout", "")
            job["finished_at"] = now
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
        publish_app_event(
            event="daily_summary_job_update",
            payload={"job_id": job_id, "status": "done", "progress": 100, "stage": "done", "summary_date": summary_date, "model": str((item or {}).get("model") or model)},
            producer="backend.server",
        )
        if callable(notify_fn):
            try:
                markdown = str((item or {}).get("summary_markdown") or "")
                notify_result = notify_fn(
                    title=f"新闻日报已生成 {summary_date}",
                    summary=f"模型：{str((item or {}).get('model') or model)}",
                    markdown=markdown,
                    subject_key=f"daily_news_summary:{summary_date}",
                    link="",
                )
                with lock:
                    job = jobs.get(job_id)
                    if job is not None:
                        job["notification"] = notify_result
            except Exception as notify_exc:
                with lock:
                    job = jobs.get(job_id)
                    if job is not None:
                        job["notification"] = {"ok": False, "error": str(notify_exc)}
    except Exception as e:
        now = datetime.now(timezone.utc).isoformat()
        meta = getattr(e, "meta", {}) or {}
        with lock:
            job = jobs.get(job_id)
            if not job:
                return
            job["status"] = "error"
            job["progress"] = 100
            job["stage"] = "error"
            job["message"] = "日报总结生成失败"
            job["error"] = str(e)
            job["attempts"] = list(meta.get("attempts") or job.get("attempts") or [])
            job["fallback_used"] = bool(meta.get("fallback_used") or job.get("fallback_used"))
            job["final_error_code"] = str(meta.get("final_error_code") or ("rate_limited" if "rate_limited" in str(e) else ""))
            job["finished_at"] = now
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
        publish_app_event(
            event="daily_summary_job_update",
            payload={"job_id": job_id, "status": "error", "progress": 100, "stage": "error", "summary_date": summary_date, "model": model, "error": str(e)},
            producer="backend.server",
        )
        if callable(notify_fn):
            try:
                notify_result = notify_fn(
                    title=f"新闻日报生成失败 {summary_date}",
                    summary=f"模型：{model}",
                    markdown=f"日报任务失败：{str(e)}",
                    subject_key=f"daily_news_summary:{summary_date}",
                    link="",
                )
                with lock:
                    job = jobs.get(job_id)
                    if job is not None:
                        job["notification"] = notify_result
            except Exception as notify_exc:
                with lock:
                    job = jobs.get(job_id)
                    if job is not None:
                        job["notification"] = {"ok": False, "error": str(notify_exc)}


def start_async_daily_summary_job(*, jobs: dict[str, dict], lock: threading.Lock, cleanup_async_jobs_fn, create_async_daily_summary_job_fn, serialize_async_daily_summary_job_fn, run_async_daily_summary_job_fn, model: str, summary_date: str):
    cleanup_async_jobs_fn()
    job = create_async_daily_summary_job_fn(model=model, summary_date=summary_date)
    worker = threading.Thread(
        target=run_async_daily_summary_job_fn,
        args=(job["job_id"],),
        daemon=True,
        name=f"daily_summary_{job['job_id'][:8]}",
    )
    worker.start()
    return serialize_async_daily_summary_job_fn(job)


def get_async_daily_summary_job(*, jobs: dict[str, dict], lock: threading.Lock, cleanup_async_jobs_fn, serialize_async_daily_summary_job_fn, job_id: str):
    cleanup_async_jobs_fn()
    with lock:
        job = jobs.get(job_id)
        if not job:
            return None
        return serialize_async_daily_summary_job_fn(job)
