from __future__ import annotations

import subprocess
from pathlib import Path

from services.news_priority_guard import ensure_news_scored_before_stock_news


def query_stock_news(
    *,
    sqlite3_module,
    db_path,
    ts_code: str,
    company_name: str,
    keyword: str,
    source: str,
    finance_levels: str,
    date_from: str,
    date_to: str,
    scored: str,
    page: int,
    page_size: int,
):
    ts_code = ts_code.strip().upper()
    company_name = company_name.strip()
    keyword = keyword.strip()
    source = source.strip()
    finance_levels = finance_levels.strip()
    date_from = date_from.strip()
    date_to = date_to.strip()
    scored = scored.strip().lower()
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)
    offset = (page - 1) * page_size

    where_clauses = []
    params: list[object] = []
    if ts_code:
        where_clauses.append("ts_code = ?")
        params.append(ts_code)
    if company_name:
        where_clauses.append("company_name LIKE ?")
        params.append(f"%{company_name}%")
    if keyword:
        where_clauses.append("(title LIKE ? OR summary LIKE ?)")
        kw = f"%{keyword}%"
        params.extend([kw, kw])
    if source:
        where_clauses.append("source = ?")
        params.append(source)
    if date_from:
        where_clauses.append("pub_time >= ?")
        params.append(date_from)
    if date_to:
        where_clauses.append("pub_time <= ?")
        params.append(date_to)

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_news_items'"
        ).fetchone()[0]
        if not table_exists:
            return {"page": page, "page_size": page_size, "total": 0, "total_pages": 0, "items": []}

        cols = {r[1] for r in conn.execute("PRAGMA table_info(stock_news_items)").fetchall()}
        valid_levels = []
        if finance_levels:
            levels = [x.strip() for x in finance_levels.split(",") if x.strip()]
            valid_levels = [x for x in levels if x in {"极高", "高", "中", "低", "极低"}]
        if valid_levels and "llm_finance_importance" in cols:
            placeholders = ",".join(["?"] * len(valid_levels))
            where_clauses.append(f"llm_finance_importance IN ({placeholders})")
            params.extend(valid_levels)
        if scored in {"scored", "unscored"}:
            scored_sql = (
                "(llm_system_score IS NOT NULL AND llm_finance_impact_score IS NOT NULL "
                "AND llm_finance_importance IS NOT NULL AND llm_summary IS NOT NULL)"
            )
            where_clauses.append(scored_sql if scored == "scored" else f"NOT {scored_sql}")
        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        count_sql = f"SELECT COUNT(*) FROM stock_news_items{where_sql}"
        select_sentiment = ", ".join(
            [
                "llm_sentiment_score" if "llm_sentiment_score" in cols else "NULL AS llm_sentiment_score",
                "llm_sentiment_label" if "llm_sentiment_label" in cols else "NULL AS llm_sentiment_label",
                "llm_sentiment_reason" if "llm_sentiment_reason" in cols else "NULL AS llm_sentiment_reason",
                "llm_sentiment_confidence" if "llm_sentiment_confidence" in cols else "NULL AS llm_sentiment_confidence",
                "llm_sentiment_model" if "llm_sentiment_model" in cols else "NULL AS llm_sentiment_model",
                "llm_sentiment_scored_at" if "llm_sentiment_scored_at" in cols else "NULL AS llm_sentiment_scored_at",
            ]
        )
        data_sql = f"""
        SELECT
            id, ts_code, company_name, source, news_code, title, summary, link, pub_time,
            comment_num, relation_stock_tags_json,
            llm_system_score, llm_finance_impact_score, llm_finance_importance, llm_impacts_json,
            llm_summary, llm_model, llm_scored_at,
            {select_sentiment},
            fetched_at, update_time
        FROM stock_news_items
        {where_sql}
        ORDER BY pub_time DESC, id DESC
        LIMIT ? OFFSET ?
        """
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = conn.execute(data_sql, [*params, page_size, offset]).fetchall()
        data = [dict(r) for r in rows]
    finally:
        conn.close()

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "items": data,
    }


def query_stock_news_sources(*, sqlite3_module, db_path):
    conn = sqlite3_module.connect(db_path)
    try:
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='stock_news_items'"
        ).fetchone()[0]
        if not table_exists:
            return []
        rows = conn.execute(
            "SELECT DISTINCT source FROM stock_news_items WHERE source IS NOT NULL AND source <> '' ORDER BY source"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def fetch_stock_news_now(
    *,
    root_dir: Path,
    db_path,
    publish_app_event,
    ts_code: str,
    company_name: str,
    page_size: int,
    timeout_s: int = 180,
):
    script_path = root_dir / "fetch_stock_news_eastmoney_to_db.py"
    cmd = [
        "python3",
        str(script_path),
        "--db-path",
        str(db_path),
        "--page-size",
        str(page_size),
    ]
    if ts_code.strip():
        cmd.extend(["--ts-code", ts_code.strip().upper()])
    if company_name.strip():
        cmd.extend(["--name", company_name.strip()])
    publish_app_event(
        event="stock_news_fetch_update",
        payload={
            "ts_code": ts_code.strip().upper(),
            "company_name": company_name.strip(),
            "status": "running",
            "page_size": page_size,
        },
        producer="services.stock_news_service",
    )
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s, check=False)
    if proc.returncode != 0:
        publish_app_event(
            event="stock_news_fetch_update",
            payload={
                "ts_code": ts_code.strip().upper(),
                "company_name": company_name.strip(),
                "status": "error",
                "error": proc.stderr.strip() or proc.stdout.strip() or "采集失败",
            },
            producer="services.stock_news_service",
        )
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "采集失败")
    publish_app_event(
        event="stock_news_fetch_update",
        payload={
            "ts_code": ts_code.strip().upper(),
            "company_name": company_name.strip(),
            "status": "done",
        },
        producer="services.stock_news_service",
    )
    return {"stdout": proc.stdout, "stderr": proc.stderr}


def score_stock_news_now(
    *,
    sqlite3_module,
    root_dir: Path,
    db_path,
    publish_app_event,
    extract_llm_result_marker,
    ts_code: str,
    limit: int,
    model: str,
    timeout_s: int = 300,
    row_id: int = 0,
    force: bool = False,
):
    priority_guard = ensure_news_scored_before_stock_news(
        sqlite3_module=sqlite3_module,
        db_path=db_path,
        root_dir=root_dir,
        batch_limit=40,
        retry=0,
        sleep_s=0.05,
        timeout_s=300,
        max_rounds=6,
    )
    if not priority_guard.get("ok"):
        publish_app_event(
            event="stock_news_score_update",
            payload={
                "ts_code": ts_code.strip().upper(),
                "row_id": int(row_id or 0),
                "status": "blocked",
                "reason": "news_unscored_pending",
                "pending_before": priority_guard.get("pending_before"),
                "pending_after": priority_guard.get("pending_after"),
            },
            producer="services.stock_news_service",
        )
        raise RuntimeError(
            f"国际/国内新闻存在未评分（before={priority_guard.get('pending_before')}, after={priority_guard.get('pending_after')}），已阻止个股评分。"
        )

    script_path = root_dir / "llm_score_stock_news.py"
    used_model = "GPT-5.4"
    cmd = [
        "python3",
        str(script_path),
        "--db-path",
        str(db_path),
        "--limit",
        str(limit),
        "--workers",
        "4",
        "--batch-size",
        "6",
        "--retry",
        "1",
        "--sleep",
        "0.02",
        "--model",
        used_model,
    ]
    if ts_code.strip():
        cmd.extend(["--ts-code", ts_code.strip().upper()])
    if int(row_id or 0) > 0:
        cmd.extend(["--row-id", str(int(row_id))])
    if force:
        cmd.append("--force")
    publish_app_event(
        event="stock_news_score_update",
        payload={
            "ts_code": ts_code.strip().upper(),
            "row_id": int(row_id or 0),
            "status": "running",
            "limit": int(limit),
            "model": used_model,
            "requested_model": model,
        },
        producer="services.stock_news_service",
    )
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s, check=False)
    if proc.returncode != 0:
        publish_app_event(
            event="stock_news_score_update",
            payload={
                "ts_code": ts_code.strip().upper(),
                "row_id": int(row_id or 0),
                "status": "error",
                "model": used_model,
                "requested_model": model,
                "error": proc.stderr.strip() or proc.stdout.strip() or "评分失败",
            },
            producer="services.stock_news_service",
        )
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "评分失败")
    publish_app_event(
        event="stock_news_score_update",
        payload={
            "ts_code": ts_code.strip().upper(),
            "row_id": int(row_id or 0),
            "status": "done",
            "model": used_model,
            "requested_model": model,
        },
        producer="services.stock_news_service",
    )
    meta = extract_llm_result_marker(proc.stdout)
    meta = dict(meta or {})
    meta["news_priority_guard"] = priority_guard
    return {"stdout": proc.stdout, "stderr": proc.stderr, "meta": meta}


def build_fetch_response(deps: dict, *, ts_code: str, company_name: str, page_size: int, model: str, score: bool) -> dict:
    fetch_info = deps["fetch_stock_news_now"](
        ts_code=ts_code,
        company_name=company_name,
        page_size=page_size,
    )
    score_info = None
    if score and ts_code:
        score_info = deps["score_stock_news_now"](
            ts_code=ts_code,
            limit=min(page_size, 10),
            model=model,
        )
    payload = deps["query_stock_news_feed"](
        ts_code=ts_code,
        company_name=company_name,
        keyword="",
        source="",
        finance_levels="",
        date_from="",
        date_to="",
        scored="",
        page=1,
        page_size=min(page_size, 20),
    )
    return {
        "ok": True,
        "ts_code": ts_code,
        "company_name": company_name,
        "model": model,
        "requested_model": model,
        "used_model": ((score_info or {}).get("meta") or {}).get("used_models", [None])[0] if score_info else "",
        "attempts": ((score_info or {}).get("meta") or {}).get("items", []),
        "fetch_stdout": fetch_info.get("stdout", ""),
        "score_stdout": score_info.get("stdout", "") if score_info else "",
        "items": payload.get("items", []),
        "total": payload.get("total", 0),
    }


def build_score_response(deps: dict, *, ts_code: str, model: str, row_id: int, limit: int, force: bool) -> dict:
    score_info = deps["score_stock_news_now"](
        ts_code=ts_code,
        limit=max(limit, 1),
        model=model,
        row_id=row_id,
        force=force,
    )
    return {
        "ok": True,
        "ts_code": ts_code,
        "row_id": row_id,
        "requested_model": model,
        "used_model": ((score_info or {}).get("meta") or {}).get("used_models", [None])[0] if score_info else "",
        "attempts": ((score_info or {}).get("meta") or {}).get("items", []),
        "stdout": (score_info or {}).get("stdout", ""),
        "meta": (score_info or {}).get("meta", {}),
    }


def build_stock_news_service_deps(
    *,
    root_dir: Path,
    db_path,
    sqlite3_module,
    publish_app_event,
    extract_llm_result_marker,
) -> dict:
    return {
        "query_stock_news_feed": lambda **kwargs: query_stock_news(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "query_stock_news_sources_feed": lambda: query_stock_news_sources(sqlite3_module=sqlite3_module, db_path=db_path),
        "fetch_stock_news_now": lambda **kwargs: fetch_stock_news_now(
            root_dir=root_dir,
            db_path=db_path,
            publish_app_event=publish_app_event,
            **kwargs,
        ),
        "score_stock_news_now": lambda **kwargs: score_stock_news_now(
            sqlite3_module=sqlite3_module,
            root_dir=root_dir,
            db_path=db_path,
            publish_app_event=publish_app_event,
            extract_llm_result_marker=extract_llm_result_marker,
            **kwargs,
        ),
        "fetch_stock_news_bundle": lambda deps, **kwargs: build_fetch_response(deps, **kwargs),
        "score_stock_news_bundle": lambda deps, **kwargs: build_score_response(deps, **kwargs),
    }
