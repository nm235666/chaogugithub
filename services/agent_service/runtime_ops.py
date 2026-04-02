from __future__ import annotations

import json
import math
import statistics
import threading
import time
import uuid
from datetime import datetime, timezone


def build_trend_features(*, sqlite3_module, db_path, safe_float, calc_ma, ts_code: str, lookback: int):
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        rows = conn.execute(
            """
            SELECT p.trade_date, p.open, p.high, p.low, p.close, p.pct_chg, p.vol, p.amount, s.name
            FROM stock_daily_prices p
            LEFT JOIN stock_codes s ON s.ts_code = p.ts_code
            WHERE p.ts_code = ?
            ORDER BY p.trade_date DESC
            LIMIT ?
            """,
            (ts_code, lookback),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        raise ValueError(f"未找到 {ts_code} 的日线数据")

    data = [dict(r) for r in rows]
    data.reverse()

    closes = [safe_float(r["close"]) for r in data if safe_float(r["close"]) is not None]
    pct_chg = [safe_float(r["pct_chg"]) for r in data if safe_float(r["pct_chg"]) is not None]
    vols = [safe_float(r["vol"]) for r in data if safe_float(r["vol"]) is not None]
    if len(closes) < 2:
        raise ValueError("数据不足，至少需要2条日线")

    first_close = closes[0]
    last_close = closes[-1]
    total_return = (last_close - first_close) / first_close * 100 if first_close else None

    daily_returns = []
    for i in range(1, len(closes)):
        prev, curr = closes[i - 1], closes[i]
        if prev:
            daily_returns.append((curr - prev) / prev)
    vol_annualized = statistics.pstdev(daily_returns) * math.sqrt(252) * 100 if len(daily_returns) >= 2 else None

    latest = data[-1]
    latest_close = safe_float(latest["close"])
    ma20 = calc_ma(closes, 20)

    return {
        "name": latest.get("name") or "",
        "samples": len(data),
        "date_range": {"start": data[0]["trade_date"], "end": data[-1]["trade_date"]},
        "latest": {
            "trade_date": latest["trade_date"],
            "close": latest_close,
            "pct_chg": safe_float(latest["pct_chg"]),
            "vol": safe_float(latest["vol"]),
        },
        "trend_metrics": {
            "total_return_pct": total_return,
            "ma5": calc_ma(closes, 5),
            "ma10": calc_ma(closes, 10),
            "ma20": ma20,
            "ma60": calc_ma(closes, 60),
            "distance_to_ma20_pct": ((latest_close - ma20) / ma20 * 100) if (latest_close and ma20) else None,
            "annualized_volatility_pct": vol_annualized,
            "avg_daily_pct_chg": (sum(pct_chg) / len(pct_chg)) if pct_chg else None,
            "avg_volume": (sum(vols) / len(vols)) if vols else None,
        },
        "recent_bars": data[-20:],
    }


def call_llm_trend(
    *,
    normalize_model_name,
    normalize_temperature_for_model,
    chat_completion_with_fallback,
    default_llm_model,
    sanitize_json_value,
    trend_template_text: str = "",
    ts_code: str,
    features: dict,
    model: str,
    temperature: float = 0.2,
):
    features = sanitize_json_value(features)
    model = normalize_model_name(model)
    temperature = normalize_temperature_for_model(model, temperature)
    system_prompt = (
        "你是专业的A股量化研究助手。请基于给定特征做趋势分析，"
        "输出客观、结构化结论，并明确不确定性。"
    )
    user_prompt = (
        f"请分析股票 {ts_code} 的走势。\n"
        "请按以下结构输出：\n"
        "1) 趋势判断（上涨/震荡/下跌）\n"
        "2) 置信度（0-100）\n"
        "3) 依据（3-5条）\n"
        "4) 风险点（2-4条）\n"
        "5) 未来5-20个交易日观察要点\n"
        "6) 免责声明（非投资建议）\n\n"
        f"输入特征JSON：\n{json.dumps(features, ensure_ascii=False, allow_nan=False)}"
    )
    if trend_template_text:
        user_prompt += f"\n\n输出模板参考（可补充但不要删减结构化要求）：\n{trend_template_text}"
    try:
        result = chat_completion_with_fallback(
            model=model or default_llm_model,
            temperature=temperature,
            timeout_s=120,
            max_retries=3,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
    except Exception as e:
        raise RuntimeError(f"LLM接口错误: {e}") from e
    return {
        "analysis": result.text,
        "requested_model": result.requested_model,
        "used_model": result.used_model,
        "used_base_url": result.used_base_url,
        "attempts": [{"model": item.model, "base_url": item.base_url, "error": item.error} for item in result.attempts],
    }


def build_multi_role_context(
    *,
    sqlite3_module,
    db_path,
    default_llm_model,
    build_trend_features_fn,
    ensure_stock_news_fresh,
    build_stock_news_summary,
    build_financial_summary,
    build_valuation_summary,
    build_capital_flow_summary,
    build_event_summary,
    build_macro_context,
    build_fx_context,
    build_rate_spread_context,
    build_governance_summary,
    build_risk_summary,
    ts_code: str,
    lookback: int,
):
    ts_code = ts_code.strip().upper()
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        profile = conn.execute(
            """
            SELECT ts_code, symbol, name, area, industry, market, list_date, delist_date, list_status
            FROM stock_codes
            WHERE ts_code = ?
            """,
            (ts_code,),
        ).fetchone()
        company_name = profile["name"] if profile else ""
    finally:
        conn.close()

    stock_news_freshness = {"fetched": False, "scored": False, "latest_pub": ""}
    if profile:
        stock_news_freshness = ensure_stock_news_fresh(
            ts_code=ts_code,
            company_name=company_name,
            page_size=10,
            score_model=default_llm_model,
            score_limit=2,
            score_timeout_s=60,
        )

    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        stock_news_summary = build_stock_news_summary(conn, ts_code)
        financial_summary = build_financial_summary(conn, ts_code)
        valuation_summary = build_valuation_summary(conn, ts_code)
        capital_flow_summary = build_capital_flow_summary(conn, ts_code)
        event_summary = build_event_summary(conn, ts_code)
        macro_context = build_macro_context(conn)
        fx_context = build_fx_context(conn)
        rate_spread_context = build_rate_spread_context(conn)
        governance_summary = build_governance_summary(conn, ts_code)
        risk_summary = build_risk_summary(conn, ts_code)
    finally:
        conn.close()

    if not profile:
        raise ValueError(f"未找到股票基础信息: {ts_code}")

    features = build_trend_features_fn(ts_code, lookback)

    return {
        "company_profile": dict(profile),
        "price_summary": {
            "samples": features["samples"],
            "date_range": features["date_range"],
            "latest": features["latest"],
            "metrics": features["trend_metrics"],
            "recent_20_bars": features["recent_bars"],
        },
        "financial_summary": financial_summary,
        "valuation_summary": valuation_summary,
        "capital_flow_summary": capital_flow_summary,
        "event_summary": event_summary,
        "macro_context": macro_context,
        "fx_context": fx_context,
        "rate_spread_context": rate_spread_context,
        "governance_summary": governance_summary,
        "risk_summary": risk_summary,
        "stock_news_summary": stock_news_summary,
        "stock_news_freshness": stock_news_freshness,
    }


def build_multi_role_prompt(
    *,
    sanitize_json_value,
    role_profiles: dict,
    context: dict,
    roles: list[str],
    multi_role_template_text: str = "",
) -> str:
    context = sanitize_json_value(context)
    role_lines = "\n".join([f"- {r}" for r in roles])
    role_specs = []
    for role in roles:
        spec = role_profiles.get(role, {})
        role_specs.append(
            {
                "role": role,
                "focus": spec.get("focus", "围绕该角色的标准职责进行分析"),
                "framework": spec.get("framework", "使用该角色常用分析框架"),
                "indicators": spec.get("indicators", []),
                "risk_bias": spec.get("risk_bias", "识别与该角色相关的核心风险"),
            }
        )
    role_specs = sanitize_json_value(role_specs)
    prompt = (
        "请你以“投研委员会会议纪要”的形式，基于下面数据进行多角色分析。\n\n"
        f"参与角色：\n{role_lines}\n\n"
        "每个角色必须严格按其角色设定发言，不得混淆口径。\n"
        "请综合使用公司画像、价格行为、财务、估值、个股/市场资金流、公司事件、宏观流动性、汇率、利率利差、公司治理、风险情景等信息。\n"
        "如提供了股票相关新闻，请优先识别其中的事件催化、风险暴露、情绪扰动和与公司基本面的冲突或共振。\n"
        "如果某部分数据为空或不足，请明确指出“该维度数据暂缺/不足”，不要假装已看到数据。\n"
        "分析时优先引用数据里的最新日期、最新值、变化方向和分位水平，避免空泛表述。\n"
        "输出要求：\n"
        "1) 每个角色单独一节，给出观点、核心依据、主要风险、关注指标。\n"
        "2) 角色观点可以有分歧，但要明确分歧点。\n"
        "3) 最后给出“综合结论”：短期(5-20交易日)、中期(1-3个月)两个层面的概率判断。\n"
        "4) 给出“行动清单”：继续观察/择时/风控阈值（价格、波动、量能、估值、资金流、汇率或利率信号）。\n"
        "5) 若发现基本面、估值、资金流、宏观或治理之间存在冲突，请单列“关键分歧”。\n"
        "6) 必须附上非投资建议免责声明。\n"
        "7) 输出格式必须严格使用 Markdown 二级标题；每个角色都必须以“## 角色名”单独起一节，标题文字必须与角色名完全一致，不要改写，不要加编号。\n"
        "8) 公共部分必须使用以下固定二级标题：## 综合结论、## 行动清单、## 关键分歧、## 非投资建议免责声明。\n\n"
        "请严格按如下骨架输出，不要遗漏任何标题：\n"
        + "".join([f"## {role}\n" for role in roles])
        + "## 综合结论\n## 行动清单\n## 关键分歧\n## 非投资建议免责声明\n\n"
        f"角色设定(JSON)：\n{json.dumps(role_specs, ensure_ascii=False, allow_nan=False)}\n\n"
        f"输入数据(JSON)：\n{json.dumps(context, ensure_ascii=False, allow_nan=False)}"
    )
    if multi_role_template_text:
        prompt += f"\n\n输出模板参考（可补充但不要删减结构化要求）：\n{multi_role_template_text}"
    return prompt


def call_llm_multi_role(
    *,
    normalize_model_name,
    normalize_temperature_for_model,
    chat_completion_with_fallback,
    default_llm_model,
    build_multi_role_prompt_fn,
    context: dict,
    roles: list[str],
    model: str,
    temperature: float = 0.2,
):
    model = normalize_model_name(model)
    temperature = normalize_temperature_for_model(model, temperature)
    prompt = build_multi_role_prompt_fn(context, roles)
    try:
        result = chat_completion_with_fallback(
            model=model or default_llm_model,
            temperature=temperature,
            timeout_s=180,
            max_retries=3,
            messages=[
                {
                    "role": "system",
                    "content": "你是专业投研团队的总协调人，擅长多角色观点整合，表达清晰、客观、可执行。",
                },
                {"role": "user", "content": prompt},
            ],
        )
    except Exception as e:
        raise RuntimeError(f"LLM接口错误: {e}") from e
    return {
        "analysis": result.text,
        "requested_model": result.requested_model,
        "used_model": result.used_model,
        "used_base_url": result.used_base_url,
        "attempts": [{"model": item.model, "base_url": item.base_url, "error": item.error} for item in result.attempts],
    }


def split_multi_role_analysis(
    *,
    extract_logic_view_from_markdown,
    normalize_markdown_lines,
    build_common_sections_markdown,
    find_section_start,
    markdown: str,
    roles: list[str],
) -> dict:
    text = normalize_markdown_lines(markdown).strip()
    normalized_roles = [str(role).strip() for role in (roles or []) if str(role).strip()]
    common_markdown = build_common_sections_markdown(text)
    common_names = ["综合结论", "行动清单", "关键分歧", "非投资建议免责声明"]
    role_sections = []
    if not text:
        return {"role_sections": role_sections, "common_sections_markdown": common_markdown}
    for role in normalized_roles:
        start = find_section_start(text, role)
        if start == -1:
            fallback = (
                f"## {role}\n\n未从大模型原始输出中稳定切分出该角色的独立段落，以下附上公共结论部分供参考。\n\n{common_markdown}"
                if common_markdown
                else f"## {role}\n\n未从大模型原始输出中稳定切分出该角色的独立段落，以下附上完整分析原文供参考。\n\n{text}"
            )
            role_sections.append(
                {
                    "role": role,
                    "content": fallback,
                    "matched": False,
                    "logic_view": extract_logic_view_from_markdown(fallback),
                }
            )
            continue
        end = len(text)
        for name in [x for x in normalized_roles if x != role] + common_names:
            pos = find_section_start(text[start + 1 :], name)
            if pos != -1:
                end = min(end, start + 1 + pos)
        content = text[start:end].strip()
        compact_content = "".join(content.split())
        compact_common = "".join(common_markdown.split())
        if common_markdown and compact_common and compact_common not in compact_content:
            content = f"{content}\n\n{common_markdown}"
        role_sections.append(
            {
                "role": role,
                "content": content,
                "matched": True,
                "logic_view": extract_logic_view_from_markdown(content),
            }
        )
    return {
        "role_sections": role_sections,
        "common_sections_markdown": common_markdown,
        "logic_view": extract_logic_view_from_markdown(text),
    }


def cleanup_async_jobs(*, jobs: dict[str, dict], lock: threading.Lock, ttl_seconds: int):
    cutoff = time.time() - ttl_seconds
    with lock:
        expired = [job_id for job_id, job in jobs.items() if float(job.get("updated_at_ts", 0)) < cutoff]
        for job_id in expired:
            jobs.pop(job_id, None)


def serialize_async_job(job: dict):
    return {
        "job_id": job.get("job_id"),
        "status": job.get("status"),
        "progress": job.get("progress"),
        "stage": job.get("stage"),
        "message": job.get("message"),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "finished_at": job.get("finished_at"),
        "ts_code": job.get("ts_code"),
        "name": job.get("name"),
        "lookback": job.get("lookback"),
        "model": job.get("model"),
        "requested_model": job.get("requested_model"),
        "used_model": job.get("used_model"),
        "attempts": job.get("attempts") or [],
        "roles": job.get("roles"),
        "context": job.get("context"),
        "analysis": job.get("analysis"),
        "analysis_markdown": job.get("analysis_markdown"),
        "logic_view": job.get("logic_view"),
        "role_sections": job.get("role_sections"),
        "role_outputs": job.get("role_outputs"),
        "decision_confidence": job.get("decision_confidence"),
        "risk_review": job.get("risk_review"),
        "portfolio_view": job.get("portfolio_view"),
        "used_context_dims": job.get("used_context_dims"),
        "pre_trade_check": job.get("pre_trade_check"),
        "notification": job.get("notification"),
        "common_sections_markdown": job.get("common_sections_markdown"),
        "error": job.get("error"),
    }


def create_async_multi_role_job(
    *,
    jobs: dict[str, dict],
    lock: threading.Lock,
    publish_app_event,
    ts_code: str,
    lookback: int,
    model: str,
    roles: list[str],
    context: dict | None = None,
):
    now = datetime.now(timezone.utc).isoformat()
    job_id = uuid.uuid4().hex
    context = context or {}
    job = {
        "job_id": job_id,
        "status": "queued",
        "progress": 5,
        "stage": "queued",
        "message": "任务已创建，等待后台分析",
        "created_at": now,
        "updated_at": now,
        "finished_at": "",
        "updated_at_ts": time.time(),
        "ts_code": ts_code,
        "name": context.get("company_profile", {}).get("name", ""),
        "lookback": lookback,
        "model": model,
        "requested_model": model,
        "used_model": "",
        "attempts": [],
        "roles": roles,
        "context": context,
        "analysis": "",
        "analysis_markdown": "",
        "logic_view": {"summary": {}, "chains": [], "has_logic": False},
        "role_sections": [],
        "role_outputs": [],
        "decision_confidence": {},
        "risk_review": {},
        "portfolio_view": {},
        "used_context_dims": [],
        "pre_trade_check": {},
        "notification": {},
        "common_sections_markdown": "",
        "error": "",
    }
    with lock:
        jobs[job_id] = job
    publish_app_event(
        event="multi_role_job_update",
        payload={"job_id": job_id, "status": "queued", "progress": 5, "stage": "queued", "ts_code": ts_code, "model": model},
        producer="backend.server",
    )
    return job


def run_async_multi_role_job(
    *,
    jobs: dict[str, dict],
    lock: threading.Lock,
    publish_app_event,
    build_multi_role_context_fn,
    agent_deps_builder,
    job_id: str,
):
    with lock:
        job = jobs.get(job_id)
        if not job:
            return
        job["status"] = "running"
        job["progress"] = 10
        job["stage"] = "context"
        job["message"] = "分析上下文构建中，请稍候"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        job["updated_at_ts"] = time.time()
        roles = list(job["roles"])
        model = str(job["model"])
        ts_code = str(job.get("ts_code") or "")
        lookback = int(job.get("lookback") or 120)
    publish_app_event(
        event="multi_role_job_update",
        payload={"job_id": job_id, "status": "running", "progress": 10, "stage": "context", "ts_code": ts_code, "model": model},
        producer="backend.server",
    )
    try:
        context = build_multi_role_context_fn(ts_code, lookback)
        with lock:
            job = jobs.get(job_id)
            if not job:
                return
            job["context"] = context
            if not job.get("name"):
                job["name"] = context.get("company_profile", {}).get("name", "")
            job["progress"] = 35
            job["stage"] = "llm"
            job["message"] = "大模型分析中，请稍候"
            job["updated_at"] = datetime.now(timezone.utc).isoformat()
            job["updated_at_ts"] = time.time()
        publish_app_event(
            event="multi_role_job_update",
            payload={"job_id": job_id, "status": "running", "progress": 35, "stage": "llm", "ts_code": ts_code, "model": model},
            producer="backend.server",
        )
        agent_deps = agent_deps_builder()
        payload = agent_deps["run_multi_role_analysis"](
            agent_deps,
            ts_code=ts_code,
            lookback=lookback,
            roles=roles,
            model=model,
            temperature=0.2,
            context=context,
        )
        now = datetime.now(timezone.utc).isoformat()
        with lock:
            job = jobs.get(job_id)
            if not job:
                return
            job["status"] = "done"
            job["progress"] = 100
            job["stage"] = "done"
            job["message"] = "分析完成"
            job.update(payload)
            job["analysis"] = payload.get("analysis_markdown") or payload.get("analysis") or ""
            job["finished_at"] = now
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
        publish_app_event(
            event="multi_role_job_update",
            payload={"job_id": job_id, "status": "done", "progress": 100, "stage": "done", "ts_code": ts_code, "model": payload.get("used_model") or model},
            producer="backend.server",
        )
    except Exception as e:
        now = datetime.now(timezone.utc).isoformat()
        with lock:
            job = jobs.get(job_id)
            if not job:
                return
            job["status"] = "error"
            job["progress"] = 100
            job["stage"] = "error"
            job["message"] = "分析失败"
            job["error"] = str(e)
            job["finished_at"] = now
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
        publish_app_event(
            event="multi_role_job_update",
            payload={"job_id": job_id, "status": "error", "progress": 100, "stage": "error", "ts_code": ts_code, "model": model, "error": str(e)},
            producer="backend.server",
        )


def start_async_multi_role_job(
    *,
    cleanup_async_jobs_fn,
    create_async_multi_role_job_fn,
    serialize_async_job_fn,
    run_async_multi_role_job_fn,
    ts_code: str,
    lookback: int,
    model: str,
    roles: list[str],
):
    cleanup_async_jobs_fn()
    job = create_async_multi_role_job_fn(ts_code=ts_code, lookback=lookback, model=model, roles=roles, context=None)
    worker = threading.Thread(
        target=run_async_multi_role_job_fn,
        args=(job["job_id"],),
        daemon=True,
        name=f"multi_role_{job['job_id'][:8]}",
    )
    worker.start()
    return serialize_async_job_fn(job)


def get_async_multi_role_job(*, jobs: dict[str, dict], lock: threading.Lock, cleanup_async_jobs_fn, serialize_async_job_fn, job_id: str):
    cleanup_async_jobs_fn()
    with lock:
        job = jobs.get(job_id)
        if not job:
            return None
        return serialize_async_job_fn(job)
