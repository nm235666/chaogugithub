#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import threading
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path

import db_compat as sqlite3
from llm_gateway import (
    DEFAULT_LLM_MODEL,
    chat_completion_with_fallback,
    normalize_model_name,
)
from llm_provider_config import get_provider_candidates
from realtime_streams import publish_app_event
from services.agent_service import (
build_backend_runtime_deps,
build_multi_role_context as agent_build_multi_role_context,
build_multi_role_prompt as agent_build_multi_role_prompt,
call_llm_multi_role as agent_call_llm_multi_role,
cleanup_async_jobs as agent_cleanup_async_jobs,
create_async_multi_role_job as agent_create_async_multi_role_job,
get_async_multi_role_job as agent_get_async_multi_role_job,
run_async_multi_role_job as agent_run_async_multi_role_job,
serialize_async_job as agent_serialize_async_job,
start_async_multi_role_job as agent_start_async_multi_role_job,
)
from services.agent_service.multi_role_v3 import (
control_multi_role_v3_job,
create_multi_role_v3_job,
get_multi_role_v3_job,
run_multi_role_v3_worker_loop,
)
from services.reporting import build_reporting_runtime_deps
from services.reporting import (
cleanup_async_jobs as reporting_cleanup_async_jobs,
create_async_daily_summary_job as reporting_create_async_daily_summary_job,
get_async_daily_summary_job as reporting_get_async_daily_summary_job,
run_async_daily_summary_job as reporting_run_async_daily_summary_job,
serialize_async_daily_summary_job as reporting_serialize_async_daily_summary_job,
start_async_daily_summary_job as reporting_start_async_daily_summary_job,
)
from services.chatrooms_service import build_chatrooms_service_deps
from services.signals_service import build_signals_runtime_deps
from services.quantaalpha_service import build_quantaalpha_service_runtime_deps
from services.decision_service import build_decision_runtime_deps as build_decision_service_runtime_deps
from services.stock_news_service import build_stock_news_service_deps
from services.stock_detail_service import build_stock_detail_runtime_deps
from services.system.llm_providers_admin import (
get_multi_role_v2_policies,
get_multi_role_v3_policies,
update_multi_role_v2_policies,
)

from backend.http_server import config
from backend.http_server.llm_workbench import build_multi_role_context

def _cleanup_async_multi_role_jobs():
    agent_cleanup_async_jobs(
        jobs=config.ASYNC_MULTI_ROLE_JOBS,
        lock=config.ASYNC_MULTI_ROLE_LOCK,
        ttl_seconds=config.ASYNC_JOB_TTL_SECONDS,
    )


def _serialize_async_job(job: dict):
    return agent_serialize_async_job(job)


def _create_async_multi_role_job(ts_code: str, lookback: int, model: str, roles: list[str], context: dict | None = None):
    _cleanup_async_multi_role_jobs()
    return agent_create_async_multi_role_job(
        jobs=config.ASYNC_MULTI_ROLE_JOBS,
        lock=config.ASYNC_MULTI_ROLE_LOCK,
        publish_app_event=publish_app_event,
        ts_code=ts_code,
        lookback=lookback,
        model=model,
        roles=roles,
        context=context,
    )


def _run_async_multi_role_job(job_id: str):
    agent_run_async_multi_role_job(
        jobs=config.ASYNC_MULTI_ROLE_JOBS,
        lock=config.ASYNC_MULTI_ROLE_LOCK,
        publish_app_event=publish_app_event,
        build_multi_role_context_fn=build_multi_role_context,
        agent_deps_builder=build_agent_service_deps,
        job_id=job_id,
    )


def start_async_multi_role_job(ts_code: str, lookback: int, model: str, roles: list[str]):
    return agent_start_async_multi_role_job(
        cleanup_async_jobs_fn=_cleanup_async_multi_role_jobs,
        create_async_multi_role_job_fn=_create_async_multi_role_job,
        serialize_async_job_fn=_serialize_async_job,
        run_async_multi_role_job_fn=_run_async_multi_role_job,
        ts_code=ts_code,
        lookback=lookback,
        model=model,
        roles=roles,
    )


def get_async_multi_role_job(job_id: str):
    return agent_get_async_multi_role_job(
        jobs=config.ASYNC_MULTI_ROLE_JOBS,
        lock=config.ASYNC_MULTI_ROLE_LOCK,
        cleanup_async_jobs_fn=_cleanup_async_multi_role_jobs,
        serialize_async_job_fn=_serialize_async_job,
        job_id=job_id,
    )


def _cleanup_async_multi_role_v2_jobs():
    agent_cleanup_async_jobs(
        jobs=config.ASYNC_MULTI_ROLE_V2_JOBS,
        lock=config.ASYNC_MULTI_ROLE_V2_LOCK,
        ttl_seconds=config.ASYNC_JOB_TTL_SECONDS,
    )
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        live_ids = set(config.ASYNC_MULTI_ROLE_V2_JOBS.keys())
        config.ASYNC_MULTI_ROLE_V2_ACTIVE.intersection_update(live_ids)
        old_queue = list(config.ASYNC_MULTI_ROLE_V2_QUEUE)
        rebuilt_queue = [jid for jid in old_queue if jid in live_ids]
        if rebuilt_queue != old_queue:
            config.ASYNC_MULTI_ROLE_V2_QUEUE.clear()
            for jid in rebuilt_queue:
                config.ASYNC_MULTI_ROLE_V2_QUEUE.append(jid)
        _refresh_multi_role_v2_runtime_meta_locked()


def _queue_position_locked(job_id: str) -> int:
    for idx, queued_job_id in enumerate(list(config.ASYNC_MULTI_ROLE_V2_QUEUE), start=1):
        if queued_job_id == job_id:
            return idx
    return 0


def _refresh_multi_role_v2_runtime_meta_locked() -> None:
    active_count = len(config.ASYNC_MULTI_ROLE_V2_ACTIVE)
    queue_length = len(config.ASYNC_MULTI_ROLE_V2_QUEUE)
    queue_pos_map = {jid: idx for idx, jid in enumerate(list(config.ASYNC_MULTI_ROLE_V2_QUEUE), start=1)}
    for jid, job in config.ASYNC_MULTI_ROLE_V2_JOBS.items():
        status = str((job or {}).get("status") or "")
        job["current_concurrent_jobs"] = active_count
        job["queue_length"] = queue_length
        job["queue_total"] = queue_length
        job["max_concurrent_jobs"] = config.MULTI_ROLE_V2_MAX_CONCURRENT_JOBS
        job["queue_position"] = int(queue_pos_map.get(jid, 0)) if status == "queued" else 0


def _dispatch_multi_role_v2_queue():
    launch_ids: list[str] = []
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        while config.ASYNC_MULTI_ROLE_V2_QUEUE and len(config.ASYNC_MULTI_ROLE_V2_ACTIVE) < config.MULTI_ROLE_V2_MAX_CONCURRENT_JOBS:
            job_id = str(config.ASYNC_MULTI_ROLE_V2_QUEUE.popleft() or "")
            if not job_id:
                continue
            job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
            if not job:
                continue
            if str(job.get("status") or "") != "queued":
                continue
            if job_id in config.ASYNC_MULTI_ROLE_V2_ACTIVE:
                continue
            config.ASYNC_MULTI_ROLE_V2_ACTIVE.add(job_id)
            now = datetime.now(timezone.utc).isoformat()
            job["message"] = "排队结束，任务开始执行"
            job["queue_position"] = 0
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
            launch_ids.append(job_id)
            try:
                _persist_multi_role_analysis_v2_job(job)
            except Exception:
                pass
        _refresh_multi_role_v2_runtime_meta_locked()
    for job_id in launch_ids:
        worker = threading.Thread(
            target=_run_async_multi_role_v2_job,
            args=(job_id,),
            daemon=True,
            name=f"multi_role_v2_{job_id[:8]}",
        )
        worker.start()


def _release_multi_role_v2_slot(job_id: str):
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        config.ASYNC_MULTI_ROLE_V2_ACTIVE.discard(str(job_id or ""))
        _refresh_multi_role_v2_runtime_meta_locked()
    _dispatch_multi_role_v2_queue()


def _context_cache_cn_date() -> str:
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")


def _context_cache_key(ts_code: str, lookback: int, cn_date: str) -> str:
    return f"{str(ts_code or '').strip().upper()}|{int(lookback or 120)}|{str(cn_date or '')}"


def _get_cached_multi_role_v2_context(ts_code: str, lookback: int):
    key = _context_cache_key(ts_code, lookback, _context_cache_cn_date())
    with config.MULTI_ROLE_V2_CONTEXT_CACHE_LOCK:
        cached = config.MULTI_ROLE_V2_CONTEXT_CACHE.get(key)
        if not isinstance(cached, dict):
            return None
        context = cached.get("context")
        if not isinstance(context, dict):
            return None
        # Context is treated as read-only in the v2 pipeline; return directly to
        # avoid deep-copy overhead on hot path.
        return context


def _set_cached_multi_role_v2_context(ts_code: str, lookback: int, context: dict):
    if not isinstance(context, dict):
        return
    today = _context_cache_cn_date()
    key = _context_cache_key(ts_code, lookback, today)
    now = datetime.now(timezone.utc).isoformat()
    with config.MULTI_ROLE_V2_CONTEXT_CACHE_LOCK:
        # 跨天自动清理旧 key，避免常驻进程内存膨胀。
        stale = [k for k in config.MULTI_ROLE_V2_CONTEXT_CACHE.keys() if str(k).split("|")[-1] != today]
        for stale_key in stale:
            config.MULTI_ROLE_V2_CONTEXT_CACHE.pop(stale_key, None)
        config.MULTI_ROLE_V2_CONTEXT_CACHE[key] = {"context": context, "updated_at": now}


def _build_role_specific_context(role: str, full_context: dict) -> dict:
    source = dict(full_context or {})
    price = source.get("price_summary") or {}
    stock_news = source.get("stock_news_summary") or {}
    cap_flow = source.get("capital_flow_summary") or {}

    scoped = {
        "company_profile": source.get("company_profile") or {},
        "price_summary": {
            "latest": (price.get("latest") or {}),
            "metrics": (price.get("metrics") or {}),
        },
        "stock_news_summary": {
            "latest_pub_time": stock_news.get("latest_pub_time"),
            "high_importance_count_recent_8": stock_news.get("high_importance_count_recent_8"),
        },
    }

    role_name = str(role or "").strip()
    if role_name == "宏观经济分析师":
        scoped["macro_context"] = source.get("macro_context") or {}
        scoped["rate_spread_context"] = source.get("rate_spread_context") or {}
    elif role_name == "股票分析师":
        scoped["price_summary"]["recent_20_bars"] = price.get("recent_20_bars") or []
        scoped["valuation_summary"] = source.get("valuation_summary") or {}
        scoped["capital_flow_summary"] = {"stock_flow": (cap_flow.get("stock_flow") or {})}
    elif role_name == "国际资本分析师":
        scoped["capital_flow_summary"] = {"market_flow": (cap_flow.get("market_flow") or {})}
        scoped["macro_context"] = source.get("macro_context") or {}
        scoped["fx_context"] = source.get("fx_context") or {}
    elif role_name == "汇率分析师":
        scoped["fx_context"] = source.get("fx_context") or {}
        scoped["rate_spread_context"] = source.get("rate_spread_context") or {}
    elif role_name == "风险控制官":
        scoped["risk_summary"] = source.get("risk_summary") or {}
    elif role_name == "行业分析师":
        scoped["event_summary"] = source.get("event_summary") or {}
        news_items = list((stock_news.get("recent_items") or []))[:3]
        scoped["stock_news_summary"]["recent_items"] = news_items
    else:
        # 默认给一个轻量可用集合，避免未知角色直接空数据。
        scoped["macro_context"] = source.get("macro_context") or {}
        scoped["valuation_summary"] = source.get("valuation_summary") or {}

    return _sanitize_json_value(scoped)


def _default_multi_role_v2_policies() -> dict:
    out = {"__aggregator__": {"primary_model": "gpt-5.4-multi-role", "fallback_models": ["kimi-k2.5", "deepseek-chat"]}}
    for role in config.ROLE_PROFILES.keys():
        out[str(role)] = {"primary_model": "gpt-5.4-multi-role", "fallback_models": ["kimi-k2.5", "deepseek-chat"]}
    return out


def _load_multi_role_v2_policies() -> dict:
    defaults = _default_multi_role_v2_policies()
    try:
        payload = get_multi_role_v2_policies()
        raw = payload.get("multi_role_v2_policies") or {}
        if isinstance(raw, dict):
            for key, cfg in raw.items():
                role = str(key or "").strip()
                if not role or not isinstance(cfg, dict):
                    continue
                primary = str(cfg.get("primary_model") or "").strip() or defaults.get(role, {}).get("primary_model", DEFAULT_LLM_MODEL)
                fallback_raw = cfg.get("fallback_models") or []
                if isinstance(fallback_raw, str):
                    fallback = [x.strip() for x in fallback_raw.split(",") if x.strip()]
                elif isinstance(fallback_raw, list):
                    fallback = [str(x).strip() for x in fallback_raw if str(x).strip()]
                else:
                    fallback = []
                defaults[role] = {"primary_model": primary, "fallback_models": fallback}
        config.LAST_MULTI_ROLE_V2_POLICY_LOAD_ERROR = ""
    except Exception as exc:
        config.LAST_MULTI_ROLE_V2_POLICY_LOAD_ERROR = f"{type(exc).__name__}: {exc}"
        print(
            f"[multi-role-v2] policy load failed; fallback to defaults. error={config.LAST_MULTI_ROLE_V2_POLICY_LOAD_ERROR}",
            flush=True,
        )
    return defaults


def _default_multi_role_v3_policies() -> dict:
    return {
        "quick_think_llm": "deepseek-chat",
        "deep_think_llm": "gpt-5.4-multi-role",
        "fallback_models": ["deepseek-chat"],
        "stage_models": {
            "analyst": {"mode": "quick"},
            "research_debate": {"mode": "deep"},
            "research_manager": {"mode": "deep"},
            "trader": {"mode": "deep"},
            "risk_debate": {"mode": "deep"},
            "portfolio_manager": {"mode": "deep"},
        },
        "role_models": {
            "analyst:news": {"primary_model": "zhipu-news", "fallback_models": ["deepseek-chat"]},
            "analyst:sentiment": {"primary_model": "zhipu-news", "fallback_models": ["deepseek-chat"]},
        },
    }


def _normalize_multi_role_v3_policy_entry(
    raw: dict,
    *,
    fallback_profile: str,
    quick_model: str,
    deep_model: str,
    global_fallback: list[str],
) -> dict:
    mode = str(raw.get("mode") or "").strip().lower()
    if mode not in {"quick", "deep"}:
        mode = fallback_profile
    primary = str(raw.get("primary_model") or "").strip()
    if not primary:
        primary = quick_model if mode == "quick" else deep_model
    fallback_raw = raw.get("fallback_models") or []
    if isinstance(fallback_raw, str):
        fallback = [x.strip() for x in fallback_raw.split(",") if x.strip()]
    elif isinstance(fallback_raw, list):
        fallback = [str(x).strip() for x in fallback_raw if str(x).strip()]
    else:
        fallback = []
    if not fallback:
        fallback = list(global_fallback)
    return {
        "mode": mode,
        "primary_model": primary,
        "fallback_models": fallback,
    }


def _load_multi_role_v3_policies() -> dict:
    defaults = _default_multi_role_v3_policies()
    try:
        payload = get_multi_role_v3_policies()
        raw = payload.get("multi_role_v3_policies") or {}
        if isinstance(raw, dict):
            quick = str(raw.get("quick_think_llm") or defaults.get("quick_think_llm") or "").strip() or defaults["quick_think_llm"]
            deep = str(raw.get("deep_think_llm") or defaults.get("deep_think_llm") or "").strip() or defaults["deep_think_llm"]
            fallback_raw = raw.get("fallback_models") or defaults.get("fallback_models") or []
            if isinstance(fallback_raw, str):
                fallback = [x.strip() for x in fallback_raw.split(",") if x.strip()]
            elif isinstance(fallback_raw, list):
                fallback = [str(x).strip() for x in fallback_raw if str(x).strip()]
            else:
                fallback = list(defaults.get("fallback_models") or [])
            defaults["quick_think_llm"] = quick
            defaults["deep_think_llm"] = deep
            defaults["fallback_models"] = fallback

            stage_models = dict(defaults.get("stage_models") or {})
            role_models = dict(defaults.get("role_models") or {})
            raw_stages = raw.get("stage_models") or {}
            if isinstance(raw_stages, dict):
                for stage_key, cfg in raw_stages.items():
                    key = str(stage_key or "").strip()
                    if not key or not isinstance(cfg, dict):
                        continue
                    stage_models[key] = _normalize_multi_role_v3_policy_entry(
                        cfg,
                        fallback_profile="deep",
                        quick_model=quick,
                        deep_model=deep,
                        global_fallback=fallback,
                    )
            raw_roles = raw.get("role_models") or {}
            if isinstance(raw_roles, dict):
                for role_key, cfg in raw_roles.items():
                    key = str(role_key or "").strip().lower()
                    if not key or not isinstance(cfg, dict):
                        continue
                    role_models[key] = _normalize_multi_role_v3_policy_entry(
                        cfg,
                        fallback_profile="quick",
                        quick_model=quick,
                        deep_model=deep,
                        global_fallback=fallback,
                    )
            defaults["stage_models"] = stage_models
            defaults["role_models"] = role_models
        config.LAST_MULTI_ROLE_V3_POLICY_LOAD_ERROR = ""
    except Exception as exc:
        config.LAST_MULTI_ROLE_V3_POLICY_LOAD_ERROR = f"{type(exc).__name__}: {exc}"
        print(
            f"[multi-role-v3] policy load failed; fallback to defaults. error={config.LAST_MULTI_ROLE_V3_POLICY_LOAD_ERROR}",
            flush=True,
        )
    return defaults


def _policy_model_chain(policy_map: dict, role: str) -> list[str]:
    cfg = policy_map.get(role) or {}
    primary = normalize_model_name(str(cfg.get("primary_model") or DEFAULT_LLM_MODEL))
    fallback = [normalize_model_name(str(x or "")) for x in list(cfg.get("fallback_models") or []) if str(x or "").strip()]
    chain: list[str] = []
    for item in [primary, *fallback]:
        if item and item not in chain:
            chain.append(item)
    return chain or [normalize_model_name(DEFAULT_LLM_MODEL)]


def _is_kimi_model(model: str) -> bool:
    return "kimi-k2.5" in normalize_model_name(str(model or "")).lower()


def _is_gpt54_model(model: str) -> bool:
    return normalize_model_name(str(model or "")).lower() == "gpt-5.4"


def _is_gpt54_family_model(model: str) -> bool:
    normalized = normalize_model_name(str(model or "")).lower()
    return normalized == "gpt-5.4" or "gpt-5.4" in normalized


def _candidate_route_chain(model_chain: list[str], *, per_model_limit: int) -> list[dict]:
    routes: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for model in model_chain:
        normalized = normalize_model_name(str(model or ""))
        if not normalized:
            continue
        candidates = list(get_provider_candidates(normalized))
        if not candidates:
            key = (normalized, "")
            if key in seen:
                continue
            seen.add(key)
            routes.append({"model": normalized, "base_url": "", "api_key": ""})
            continue
        for item in candidates[: max(1, int(per_model_limit or 1))]:
            route_model = normalize_model_name(str(item.model or normalized))
            route_base = str(item.base_url or "").strip()
            route_key = str(item.api_key or "").strip()
            key = (route_model, route_base.rstrip("/"))
            if key in seen:
                continue
            seen.add(key)
            routes.append({"model": route_model, "base_url": route_base, "api_key": route_key})
    return routes


def _build_multi_role_v2_single_prompt(context: dict, role: str) -> str:
    profile = config.ROLE_PROFILES.get(role, {})
    role_spec = {
        "role": role,
        "focus": profile.get("focus", "围绕该角色职责进行分析"),
        "framework": profile.get("framework", "使用该角色常用框架"),
        "indicators": profile.get("indicators", []),
        "risk_bias": profile.get("risk_bias", "识别该角色关注的核心风险"),
    }
    return (
        "你将以单一角色完成独立研究任务。\n"
        f"你的角色是：{role}\n"
        "请使用该角色视角给出结构化结论，避免泛泛而谈。\n"
        "输出要求（必须严格使用 Markdown 二级标题）：\n"
        f"## {role}\n"
        "内容必须包含：\n"
        "1) 核心观点（结论先行）\n"
        "2) 关键依据（3-5条，尽量引用给定数据中的最近日期/数值）\n"
        "3) 主要风险（2-4条）\n"
        "4) 后续跟踪指标（3-5条，尽量量化）\n\n"
        f"角色设定(JSON)：\n{json.dumps(_sanitize_json_value(role_spec), ensure_ascii=False, allow_nan=False)}\n\n"
        f"输入数据(JSON)：\n{json.dumps(_sanitize_json_value(context), ensure_ascii=False, allow_nan=False)}"
    )


def _run_multi_role_v2_single_role(*, role: str, context: dict, policy_map: dict, attempt_budget: int) -> dict:
    model_chain = _policy_model_chain(policy_map, role)
    route_chain = _candidate_route_chain(model_chain, per_model_limit=2)
    attempts: list[dict] = []
    started = time.time()
    prompt = _build_multi_role_v2_single_prompt(context, role)
    system_prompt = f"你是{role}，请保持角色口径稳定、可执行、可审计。"
    for idx in range(max(1, int(attempt_budget))):
        route = route_chain[idx % len(route_chain)] if route_chain else {"model": model_chain[idx % len(model_chain)], "base_url": "", "api_key": ""}
        request_model = str(route.get("model") or model_chain[idx % len(model_chain)])
        request_base_url = str(route.get("base_url") or "")
        request_api_key = str(route.get("api_key") or "")
        role_timeout_s = 90 if _is_gpt54_family_model(request_model) else 60
        try:
            result = chat_completion_with_fallback(
                model=request_model,
                base_url=request_base_url,
                api_key=request_api_key,
                temperature=0.2,
                timeout_s=role_timeout_s,
                max_retries=0,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt},
                ],
            )
            for item in result.attempts:
                attempts.append({"model": item.model, "base_url": item.base_url, "error": item.error})
            return {
                "ok": True,
                "role": role,
                "output": str(result.text or ""),
                "used_model": str(result.used_model or request_model),
                "requested_model": str(result.requested_model or request_model),
                "attempts": attempts,
                "error": "",
                "duration_ms": int((time.time() - started) * 1000),
            }
        except Exception as exc:
            attempts.append({"model": request_model, "base_url": "", "error": str(exc)})
    return {
        "ok": False,
        "role": role,
        "output": "",
        "used_model": "",
        "requested_model": "",
        "attempts": attempts,
        "error": str(attempts[-1].get("error") if attempts else "unknown error"),
        "duration_ms": int((time.time() - started) * 1000),
    }


def _trim_role_output_for_aggregate(role: str, content: str, per_role_limit: int = 1800) -> str:
    text = str(content or "").strip()
    if not text:
        return ""
    lines = [ln.rstrip() for ln in text.splitlines()]
    while lines and not lines[0].strip():
        lines.pop(0)
    if lines and lines[0].lstrip().startswith("##"):
        heading = lines[0].replace(" ", "")
        if role and role.replace(" ", "") in heading:
            lines = lines[1:]
    text = "\n".join(lines).strip()
    if len(text) <= per_role_limit:
        return text
    keep = max(500, per_role_limit)
    return text[:keep]


def _trim_aggregator_inputs(inputs: list[str], max_total_chars: int = 12000) -> list[str]:
    if not inputs:
        return []
    total = sum(len(x) for x in inputs)
    if total <= max_total_chars:
        return inputs
    scale = float(max_total_chars) / float(max(total, 1))
    trimmed: list[str] = []
    for item in inputs:
        limit = max(400, int(len(item) * scale))
        trimmed.append(item[:limit])
    return trimmed


def _run_multi_role_v2_aggregator(*, role_runs: list[dict], ts_code: str, lookback: int, policy_map: dict) -> dict:
    started = time.time()
    inputs = []
    role_order = []
    for item in role_runs:
        role = str(item.get("role") or "").strip()
        content = str(item.get("output") or "").strip()
        if not role or not content:
            continue
        compact_content = _trim_role_output_for_aggregate(role, content)
        if not compact_content:
            continue
        role_order.append(role)
        inputs.append(f"## {role}\n{compact_content}")
    if not inputs:
        raise RuntimeError("没有可用的角色输出，无法汇总")
    inputs = _trim_aggregator_inputs(inputs)

    chain = _policy_model_chain(policy_map, "__aggregator__")
    route_chain = _candidate_route_chain(chain, per_model_limit=2)
    prompt = (
        "你是投研委员会秘书，请将多个角色的独立结论串行汇总，输出最终会议纪要。\n"
        "输出要求：\n"
        "1) 保留所有角色的独立观点，不得混合改写为单一口径。\n"
        "2) 必须包含公共段落：综合结论、行动清单、关键分歧、非投资建议免责声明。\n"
        "3) 结构必须使用 Markdown 二级标题，角色标题必须与输入角色名一致。\n"
        "4) 若角色间冲突，务必在“关键分歧”明确记录。\n"
        "5) 行动清单优先给出可执行和可验证项。\n\n"
        "请严格按如下标题骨架输出：\n"
        + "".join([f"## {role}\n" for role in role_order])
        + "## 综合结论\n## 行动清单\n## 关键分歧\n## 非投资建议免责声明\n\n"
        f"股票：{ts_code}，观察窗口：{lookback}日\n\n"
        "角色输入如下：\n\n"
        + "\n\n".join(inputs)
    )
    attempts: list[dict] = []
    agg_attempt_budget = max(2, len(route_chain) if route_chain else len(chain))
    for idx in range(agg_attempt_budget):
        route = route_chain[idx % len(route_chain)] if route_chain else {"model": chain[idx % len(chain)], "base_url": "", "api_key": ""}
        request_model = str(route.get("model") or chain[idx % len(chain)])
        request_base_url = str(route.get("base_url") or "")
        request_api_key = str(route.get("api_key") or "")
        aggregate_timeout_s = 120 if _is_gpt54_family_model(request_model) else 75
        try:
            result = chat_completion_with_fallback(
                model=request_model,
                base_url=request_base_url,
                api_key=request_api_key,
                temperature=0.2,
                timeout_s=aggregate_timeout_s,
                max_retries=0,
                messages=[
                    {"role": "system", "content": "你是严谨的投研纪要整合器，只输出结构化 Markdown。"},
                    {"role": "user", "content": prompt},
                ],
            )
            for item in result.attempts:
                attempts.append({"model": item.model, "base_url": item.base_url, "error": item.error})
            return {
                "ok": True,
                "analysis_markdown": str(result.text or ""),
                "used_model": str(result.used_model or request_model),
                "requested_model": str(result.requested_model or request_model),
                "attempts": attempts,
                "error": "",
                "duration_ms": int((time.time() - started) * 1000),
            }
        except Exception as exc:
            attempts.append({"model": request_model, "base_url": "", "error": str(exc)})

    fallback = "\n\n".join(inputs)
    fallback += (
        "\n\n## 综合结论\n聚合模型暂不可用，本次先保留角色原文，请结合各角色观点自行判读。\n"
        "\n## 行动清单\n1. 等待聚合模型恢复后重试汇总。\n2. 对冲突观点优先做数据复核。\n"
        "\n## 关键分歧\n角色间存在潜在冲突，需人工复核。\n"
        "\n## 非投资建议免责声明\n以上内容仅供研究参考，不构成任何投资建议。\n"
    )
    return {
        "ok": False,
        "analysis_markdown": fallback,
        "used_model": "",
        "requested_model": "",
        "attempts": attempts,
        "error": str(attempts[-1].get("error") if attempts else "aggregator failed"),
        "duration_ms": int((time.time() - started) * 1000),
    }


def _serialize_async_multi_role_v2_job(job: dict) -> dict:
    aggregate_ms = int(((job.get("aggregator_run") or {}).get("duration_ms") or 0))
    debug = {}
    if str(config.LAST_MULTI_ROLE_V2_POLICY_LOAD_ERROR or "").strip():
        debug["policy_warning"] = config.LAST_MULTI_ROLE_V2_POLICY_LOAD_ERROR
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
        "roles": job.get("roles"),
        "accept_auto_degrade": bool(job.get("accept_auto_degrade", True)),
        "decision_timeout_seconds": int(job.get("decision_timeout_seconds") or 0),
        "decision_state": job.get("decision_state") or {},
        "role_runs": list(job.get("role_runs") or []),
        "aggregator_run": job.get("aggregator_run") or {},
        "analysis": job.get("analysis") or "",
        "analysis_markdown": job.get("analysis_markdown") or "",
        "role_outputs": job.get("role_outputs") or [],
        "role_sections": job.get("role_sections") or [],
        "common_sections_markdown": job.get("common_sections_markdown") or "",
        "used_model": job.get("used_model") or "",
        "requested_model": job.get("requested_model") or "",
        "attempts": job.get("attempts") or [],
        "decision_confidence": job.get("decision_confidence") or {},
        "risk_review": job.get("risk_review") or {},
        "portfolio_view": job.get("portfolio_view") or {},
        "used_context_dims": job.get("used_context_dims") or [],
        "context": job.get("context") or {},
        "context_build_ms": int(job.get("context_build_ms") or 0),
        "role_parallel_ms": int(job.get("role_parallel_ms") or 0),
        "aggregate_ms": aggregate_ms,
        "total_ms": int(job.get("total_ms") or 0),
        "warnings": list(job.get("warnings") or []),
        "error": job.get("error") or "",
        "queue_position": int(job.get("queue_position") or 0),
        "queue_total": int(job.get("queue_total") or 0),
        "max_concurrent_jobs": int(job.get("max_concurrent_jobs") or config.MULTI_ROLE_V2_MAX_CONCURRENT_JOBS),
        "current_concurrent_jobs": int(job.get("current_concurrent_jobs") or 0),
        "queue_length": int(job.get("queue_length") or 0),
        "debug": debug,
    }


def _create_async_multi_role_v2_job(
    *,
    ts_code: str,
    lookback: int,
    roles: list[str],
    accept_auto_degrade: bool,
    decision_timeout_seconds: int,
) -> dict:
    _cleanup_async_multi_role_v2_jobs()
    now = datetime.now(timezone.utc).isoformat()
    job_id = uuid.uuid4().hex
    policy_map = _load_multi_role_v2_policies()
    role_runs = []
    for role in roles:
        role_policy = policy_map.get(role, {})
        role_runs.append(
            {
                "role": role,
                "status": "queued",
                "requested_model": str(role_policy.get("primary_model") or DEFAULT_LLM_MODEL),
                "used_model": "",
                "attempts": [],
                "retry_count": 0,
                "duration_ms": 0,
                "error": "",
                "output": "",
            }
        )
    job = {
        "job_id": job_id,
        "status": "queued",
        "progress": 5,
        "stage": "queued",
        "message": "V2 任务已创建，等待后台执行",
        "created_at": now,
        "updated_at": now,
        "finished_at": "",
        "updated_at_ts": time.time(),
        "ts_code": ts_code,
        "name": "",
        "lookback": lookback,
        "roles": roles,
        "requested_model": DEFAULT_LLM_MODEL,
        "used_model": "",
        "accept_auto_degrade": bool(accept_auto_degrade),
        "decision_timeout_seconds": max(60, int(decision_timeout_seconds or 600)),
        "decision_state": {"pending_user_decision": False, "round": 0, "last_action": "", "pending_roles": []},
        "role_runs": role_runs,
        "aggregator_run": {"status": "queued", "used_model": "", "attempts": [], "error": "", "duration_ms": 0},
        "context": {},
        "analysis": "",
        "analysis_markdown": "",
        "role_outputs": [],
        "role_sections": [],
        "common_sections_markdown": "",
        "decision_confidence": {},
        "risk_review": {},
        "portfolio_view": {},
        "used_context_dims": [],
        "context_build_ms": 0,
        "role_parallel_ms": 0,
        "total_ms": 0,
        "attempts": [],
        "warnings": [],
        "error": "",
        "queue_position": 0,
        "queue_total": 0,
        "max_concurrent_jobs": config.MULTI_ROLE_V2_MAX_CONCURRENT_JOBS,
        "current_concurrent_jobs": 0,
        "queue_length": 0,
    }
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        config.ASYNC_MULTI_ROLE_V2_JOBS[job_id] = job
    try:
        _persist_multi_role_analysis_v2_job(job)
    except Exception:
        pass
    publish_app_event(
        event="multi_role_job_update",
        payload={"job_id": job_id, "status": "queued", "progress": 5, "stage": "queued", "ts_code": ts_code, "mode": "v2"},
        producer="backend.server",
    )
    return job


def _run_multi_role_v2_role_batch(job_id: str, role_names: list[str], attempt_budget: int, stage: str):
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            return []
        context = dict(job.get("context") or {})
        ts_code = str(job.get("ts_code") or "")
        for item in job.get("role_runs", []):
            if item.get("role") in role_names:
                item["status"] = "retrying" if stage == "role_retry" else "running"
                item["error"] = ""
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        job["updated_at_ts"] = time.time()
        job["stage"] = stage
        job["message"] = "角色任务并行执行中"
    policy_map = _load_multi_role_v2_policies()
    results: list[dict] = []
    workers = max(1, min(len(role_names), 6))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="multi_role_v2_role") as pool:
        futures = [
            pool.submit(
                _run_multi_role_v2_single_role,
                role=role,
                context=_build_role_specific_context(role, context),
                policy_map=policy_map,
                attempt_budget=attempt_budget,
            )
            for role in role_names
        ]
        for fut in concurrent.futures.as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as exc:
                results.append({"ok": False, "role": "", "attempts": [], "output": "", "used_model": "", "error": str(exc), "duration_ms": 0})

    by_role = {str(x.get("role") or ""): x for x in results}
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            return results
        for item in job.get("role_runs", []):
            role = str(item.get("role") or "")
            if role not in by_role:
                continue
            result = by_role[role]
            item_attempts = list(item.get("attempts") or [])
            item_attempts.extend(list(result.get("attempts") or []))
            item["attempts"] = item_attempts
            item["retry_count"] = max(0, len(item_attempts) - 1)
            item["duration_ms"] = int((item.get("duration_ms") or 0) + int(result.get("duration_ms") or 0))
            item["used_model"] = str(result.get("used_model") or item.get("used_model") or "")
            item["requested_model"] = str(result.get("requested_model") or item.get("requested_model") or "")
            item["output"] = str(result.get("output") or item.get("output") or "")
            item["error"] = str(result.get("error") or "")
            item["status"] = "done" if result.get("ok") else "error"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        job["updated_at_ts"] = time.time()
        current_role_runs = [
            item
            for item in list(job.get("role_runs") or [])
            if str(item.get("role") or "") in set(role_names)
        ]
    role_done = [x for x in current_role_runs if str(x.get("status") or "") == "done"]
    kimi_hits = sum(1 for x in role_done if _is_kimi_model(str(x.get("used_model") or "")))
    gpt_fallback_hits = sum(
        1
        for x in role_done
        if _is_gpt54_model(str(x.get("used_model") or "")) and _is_kimi_model(str(x.get("requested_model") or ""))
    )
    print(
        f"[multi-role-v2] role_model_usage stage={stage} ts_code={ts_code} "
        f"done={len(role_done)} kimi_hits={kimi_hits} gpt_fallback_hits={gpt_fallback_hits}",
        flush=True,
    )
    return results


def _finalize_multi_role_v2_job(job_id: str, *, final_status: str):
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            return
        done_roles = [x for x in list(job.get("role_runs") or []) if x.get("status") == "done" and str(x.get("output") or "").strip()]
        failed_roles = [x for x in list(job.get("role_runs") or []) if x.get("status") != "done"]
        if failed_roles:
            job["warnings"] = [f"{x.get('role')}: {x.get('error') or 'failed'}" for x in failed_roles]
        ts_code = str(job.get("ts_code") or "")
        lookback = int(job.get("lookback") or 120)
        roles = [str(x.get("role") or "") for x in list(job.get("role_runs") or []) if str(x.get("role") or "").strip()]
        job["stage"] = "aggregating"
        job["progress"] = 85
        job["message"] = "角色阶段完成，正在汇总"
        job["aggregator_run"] = {"status": "running", "used_model": "", "attempts": [], "error": "", "duration_ms": 0}
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        job["updated_at_ts"] = time.time()

    publish_app_event(
        event="multi_role_job_update",
        payload={"job_id": job_id, "status": "running", "progress": 85, "stage": "aggregating", "ts_code": ts_code, "mode": "v2"},
        producer="backend.server",
    )

    aggregator = _run_multi_role_v2_aggregator(
        role_runs=done_roles,
        ts_code=ts_code,
        lookback=lookback,
        policy_map=_load_multi_role_v2_policies(),
    )
    agg_attempts = list(aggregator.get("attempts") or [])
    agg_kimi_hits = sum(1 for x in agg_attempts if _is_kimi_model(str(x.get("model") or "")) and not str(x.get("error") or "").strip())
    agg_gpt_fallback_hits = sum(1 for x in agg_attempts if _is_gpt54_model(str(x.get("model") or "")) and not str(x.get("error") or "").strip())
    agg_used_model = str(aggregator.get("used_model") or "")

    analysis_markdown = str(aggregator.get("analysis_markdown") or "")
    split_payload = split_multi_role_analysis(analysis_markdown, roles)
    role_outputs = split_payload.get("role_sections") or [
        {"role": x.get("role"), "content": x.get("output"), "matched": True, "logic_view": {}}
        for x in done_roles
    ]
    confidence = infer_decision_confidence(analysis_markdown).to_dict()
    risk_review = build_risk_review(analysis_markdown).to_dict()
    portfolio = build_portfolio_view(analysis_markdown).to_dict()

    now = datetime.now(timezone.utc).isoformat()
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            return
        context_build_ms = int(job.get("context_build_ms") or 0)
        role_parallel_ms = int(job.get("role_parallel_ms") or 0)
        aggregate_ms = int(aggregator.get("duration_ms") or 0)
        total_ms = context_build_ms + role_parallel_ms + aggregate_ms
        if total_ms <= 0:
            created_dt = _parse_iso_dt(str(job.get("created_at") or ""))
            if created_dt is not None:
                if created_dt.tzinfo is None:
                    created_dt = created_dt.replace(tzinfo=timezone.utc)
                total_ms = int((datetime.now(timezone.utc) - created_dt.astimezone(timezone.utc)).total_seconds() * 1000)
        total_ms = max(total_ms, 0)
        job["aggregator_run"] = {
            "status": "done" if aggregator.get("ok") else "error",
            "used_model": aggregator.get("used_model") or "",
            "requested_model": aggregator.get("requested_model") or "",
            "attempts": aggregator.get("attempts") or [],
            "error": aggregator.get("error") or "",
            "duration_ms": aggregate_ms,
        }
        job["analysis_markdown"] = analysis_markdown
        job["analysis"] = analysis_markdown
        job["used_model"] = str(aggregator.get("used_model") or "")
        job["attempts"] = aggregator.get("attempts") or []
        job["role_outputs"] = role_outputs
        job["role_sections"] = role_outputs
        job["common_sections_markdown"] = split_payload.get("common_sections_markdown") or ""
        job["decision_confidence"] = confidence
        job["risk_review"] = risk_review
        job["portfolio_view"] = portfolio
        context = job.get("context") or {}
        job["used_context_dims"] = [k for k, v in context.items() if v not in (None, "", [], {})]
        job["status"] = final_status
        job["stage"] = "done"
        job["progress"] = 100
        base_msg = "分析完成（含降级告警）" if final_status == "done_with_warnings" else "分析完成"
        job["total_ms"] = total_ms
        job["message"] = (
            f"{base_msg} · total {total_ms}ms "
            f"(context {context_build_ms}ms / roles {role_parallel_ms}ms / aggregate {aggregate_ms}ms)"
        )
        job["finished_at"] = now
        job["updated_at"] = now
        job["updated_at_ts"] = time.time()
        job["decision_state"] = {
            **(job.get("decision_state") or {}),
            "pending_user_decision": False,
            "pending_roles": [],
            "updated_at": now,
        }
        ts_code = str(job.get("ts_code") or "")
        try:
            _persist_multi_role_analysis_v2_job(job)
        except Exception:
            pass
    print(
        f"[multi-role-v2] total_ms={total_ms} context_build_ms={context_build_ms} "
        f"role_parallel_ms={role_parallel_ms} aggregate_ms={aggregate_ms} ts_code={ts_code} status={final_status} "
        f"aggregator_used_model={agg_used_model or '-'} aggregator_kimi_hits={agg_kimi_hits} "
        f"aggregator_gpt_fallback_hits={agg_gpt_fallback_hits}",
        flush=True,
    )
    publish_app_event(
        event="multi_role_job_update",
        payload={"job_id": job_id, "status": final_status, "progress": 100, "stage": "done", "ts_code": ts_code, "mode": "v2"},
        producer="backend.server",
    )


def _run_async_multi_role_v2_job(job_id: str):
    try:
        with config.ASYNC_MULTI_ROLE_V2_LOCK:
            job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
            if not job:
                return
            ts_code = str(job.get("ts_code") or "")
            lookback = int(job.get("lookback") or 120)
            roles = [str(x) for x in list(job.get("roles") or []) if str(x).strip()]
            job["status"] = "running"
            job["progress"] = 12
            job["stage"] = "context"
            job["message"] = "正在构建分析上下文"
            job["updated_at"] = datetime.now(timezone.utc).isoformat()
            job["updated_at_ts"] = time.time()
        publish_app_event(
            event="multi_role_job_update",
            payload={"job_id": job_id, "status": "running", "progress": 12, "stage": "context", "ts_code": ts_code, "mode": "v2"},
            producer="backend.server",
        )
        context_started = time.time()
        cached_context = _get_cached_multi_role_v2_context(ts_code, lookback)
        context_cache_hit = isinstance(cached_context, dict)
        if context_cache_hit:
            context = cached_context
        else:
            context = build_multi_role_context(ts_code, lookback)
            _set_cached_multi_role_v2_context(ts_code, lookback, context)
        context_build_ms = int((time.time() - context_started) * 1000)
        print(
            f"[multi-role-v2] context_build_ms={context_build_ms} ts_code={ts_code} lookback={lookback} cache_hit={context_cache_hit}",
            flush=True,
        )
        with config.ASYNC_MULTI_ROLE_V2_LOCK:
            job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
            if not job:
                return
            job["context"] = context
            job["context_build_ms"] = context_build_ms
            job["name"] = context.get("company_profile", {}).get("name", "")
            job["progress"] = 30
            job["stage"] = "role_parallel"
            job["message"] = "角色任务并行执行中"
            job["updated_at"] = datetime.now(timezone.utc).isoformat()
            job["updated_at_ts"] = time.time()
            roles = [str(x) for x in list(job.get("roles") or []) if str(x).strip()]
        publish_app_event(
            event="multi_role_job_update",
            payload={"job_id": job_id, "status": "running", "progress": 30, "stage": "role_parallel", "ts_code": ts_code, "mode": "v2"},
            producer="backend.server",
        )
        role_started = time.time()
        _run_multi_role_v2_role_batch(job_id, roles, 2, "role_parallel")
        role_parallel_ms = int((time.time() - role_started) * 1000)
        print(
            f"[multi-role-v2] role_parallel_ms={role_parallel_ms} ts_code={ts_code} roles={len(roles)}",
            flush=True,
        )
        with config.ASYNC_MULTI_ROLE_V2_LOCK:
            job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
            if not job:
                return
            job["role_parallel_ms"] = int(job.get("role_parallel_ms") or 0) + role_parallel_ms
            failed_roles = [x.get("role") for x in list(job.get("role_runs") or []) if x.get("status") != "done"]
            accept_auto_degrade = bool(job.get("accept_auto_degrade", True))
            if failed_roles and not accept_auto_degrade:
                now = datetime.now(timezone.utc).isoformat()
                job["status"] = "pending_user_decision"
                job["progress"] = 70
                job["stage"] = "pending_user_decision"
                job["message"] = (
                    f"部分角色失败，等待用户决策（context {int(job.get('context_build_ms') or 0)}ms / "
                    f"roles {int(job.get('role_parallel_ms') or 0)}ms）"
                )
                job["decision_state"] = {
                    "pending_user_decision": True,
                    "pending_roles": failed_roles,
                    "round": int((job.get("decision_state") or {}).get("round") or 0) + 1,
                    "last_action": "awaiting",
                    "updated_at": now,
                }
                job["updated_at"] = now
                job["updated_at_ts"] = time.time()
                try:
                    _persist_multi_role_analysis_v2_job(job)
                except Exception:
                    pass
                publish_app_event(
                    event="multi_role_job_update",
                    payload={
                        "job_id": job_id,
                        "status": "pending_user_decision",
                        "progress": 70,
                        "stage": "pending_user_decision",
                        "ts_code": ts_code,
                        "mode": "v2",
                    },
                    producer="backend.server",
                )
                return
            final_status = "done_with_warnings" if failed_roles else "done"
        _finalize_multi_role_v2_job(job_id, final_status=final_status)
    except Exception as exc:
        now = datetime.now(timezone.utc).isoformat()
        with config.ASYNC_MULTI_ROLE_V2_LOCK:
            job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
            if not job:
                return
            job["status"] = "error"
            job["progress"] = 100
            job["stage"] = "error"
            job["message"] = "分析失败"
            job["error"] = str(exc)
            job["finished_at"] = now
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
            ts_code = str(job.get("ts_code") or "")
            try:
                _persist_multi_role_analysis_v2_job(job)
            except Exception:
                pass
        publish_app_event(
            event="multi_role_job_update",
            payload={"job_id": job_id, "status": "error", "progress": 100, "stage": "error", "ts_code": ts_code, "mode": "v2", "error": str(exc)},
            producer="backend.server",
        )
    finally:
        _release_multi_role_v2_slot(job_id)


def _retry_async_multi_role_v2_job(job_id: str):
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            return
        if str(job.get("status")) != "running":
            return
        ts_code = str(job.get("ts_code") or "")
        pending = list((job.get("decision_state") or {}).get("pending_roles") or [])
        if not pending:
            return
        job["progress"] = 72
        job["stage"] = "role_retry"
        job["message"] = "按用户指令重试失败角色中"
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        job["updated_at_ts"] = time.time()
    publish_app_event(
        event="multi_role_job_update",
        payload={"job_id": job_id, "status": "running", "progress": 72, "stage": "role_retry", "ts_code": ts_code, "mode": "v2"},
        producer="backend.server",
    )
    role_started = time.time()
    _run_multi_role_v2_role_batch(job_id, pending, 2, "role_retry")
    role_retry_ms = int((time.time() - role_started) * 1000)
    print(
        f"[multi-role-v2] role_retry_ms={role_retry_ms} ts_code={ts_code} retry_roles={len(pending)}",
        flush=True,
    )
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            return
        job["role_parallel_ms"] = int(job.get("role_parallel_ms") or 0) + role_retry_ms
        failed_roles = [x.get("role") for x in list(job.get("role_runs") or []) if x.get("status") != "done"]
        if failed_roles:
            now = datetime.now(timezone.utc).isoformat()
            job["status"] = "pending_user_decision"
            job["progress"] = 78
            job["stage"] = "pending_user_decision"
            job["message"] = (
                f"重试后仍有失败角色，等待用户决策（roles {int(job.get('role_parallel_ms') or 0)}ms）"
            )
            job["decision_state"] = {
                "pending_user_decision": True,
                "pending_roles": failed_roles,
                "round": int((job.get("decision_state") or {}).get("round") or 0) + 1,
                "last_action": "retry_failed",
                "updated_at": now,
            }
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
            try:
                _persist_multi_role_analysis_v2_job(job)
            except Exception:
                pass
            publish_app_event(
                event="multi_role_job_update",
                payload={"job_id": job_id, "status": "pending_user_decision", "progress": 78, "stage": "pending_user_decision", "ts_code": ts_code, "mode": "v2"},
                producer="backend.server",
            )
            return
    _finalize_multi_role_v2_job(job_id, final_status="done")


def start_async_multi_role_v2_job(*, ts_code: str, lookback: int, roles: list[str], accept_auto_degrade: bool, decision_timeout_seconds: int):
    job = _create_async_multi_role_v2_job(
        ts_code=ts_code,
        lookback=lookback,
        roles=roles,
        accept_auto_degrade=accept_auto_degrade,
        decision_timeout_seconds=decision_timeout_seconds,
    )
    should_start = False
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        job_id = str(job.get("job_id") or "")
        if len(config.ASYNC_MULTI_ROLE_V2_ACTIVE) < config.MULTI_ROLE_V2_MAX_CONCURRENT_JOBS:
            config.ASYNC_MULTI_ROLE_V2_ACTIVE.add(job_id)
            job["queue_position"] = 0
            should_start = True
        else:
            if job_id not in config.ASYNC_MULTI_ROLE_V2_QUEUE:
                config.ASYNC_MULTI_ROLE_V2_QUEUE.append(job_id)
            queue_pos = _queue_position_locked(job_id)
            now = datetime.now(timezone.utc).isoformat()
            job["status"] = "queued"
            job["stage"] = "queued"
            job["progress"] = 5
            job["message"] = (
                f"任务排队中，前方 {max(0, queue_pos - 1)} 个任务，"
                f"并发上限 {config.MULTI_ROLE_V2_MAX_CONCURRENT_JOBS}"
            )
            job["queue_position"] = int(queue_pos)
            job["updated_at"] = now
            job["updated_at_ts"] = time.time()
            try:
                _persist_multi_role_analysis_v2_job(job)
            except Exception:
                pass
        _refresh_multi_role_v2_runtime_meta_locked()
    if should_start:
        worker = threading.Thread(
            target=_run_async_multi_role_v2_job,
            args=(job["job_id"],),
            daemon=True,
            name=f"multi_role_v2_{job['job_id'][:8]}",
        )
        worker.start()
    return _serialize_async_multi_role_v2_job(job)


def get_async_multi_role_v2_job(job_id: str):
    _cleanup_async_multi_role_v2_jobs()
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        _refresh_multi_role_v2_runtime_meta_locked()
        job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            persisted = _load_persisted_multi_role_v2_job(job_id)
            if not persisted:
                return None
            status = str(persisted.get("status") or "")
            # 进程内任务字典不包含该 job，但持久化状态仍是 queued/running，
            # 通常意味着服务重启后执行线程已丢失，避免前端长期“假运行”。
            if status in {"queued", "running"}:
                updated_dt = _parse_iso_dt(str(persisted.get("updated_at") or ""))
                stale_seconds = None
                if updated_dt:
                    if updated_dt.tzinfo is None:
                        updated_dt = updated_dt.replace(tzinfo=timezone.utc)
                    stale_seconds = (datetime.now(timezone.utc) - updated_dt.astimezone(timezone.utc)).total_seconds()
                if stale_seconds is None or stale_seconds >= 90:
                    now = datetime.now(timezone.utc).isoformat()
                    persisted["status"] = "error"
                    persisted["stage"] = "error"
                    persisted["progress"] = 100
                    persisted["error"] = "任务执行上下文已丢失（可能服务重启），请重新发起分析"
                    persisted["message"] = "任务未实际执行，已自动转为失败，请重试"
                    persisted["updated_at"] = now
                    persisted["finished_at"] = now
                    persisted["updated_at_ts"] = time.time()
                    try:
                        _persist_multi_role_analysis_v2_job(persisted)
                    except Exception:
                        pass
            persisted["current_concurrent_jobs"] = len(config.ASYNC_MULTI_ROLE_V2_ACTIVE)
            persisted["queue_length"] = len(config.ASYNC_MULTI_ROLE_V2_QUEUE)
            persisted["queue_total"] = len(config.ASYNC_MULTI_ROLE_V2_QUEUE)
            persisted["queue_position"] = 0
            persisted["max_concurrent_jobs"] = config.MULTI_ROLE_V2_MAX_CONCURRENT_JOBS
            return persisted
        if str(job.get("status") or "") == "queued":
            queue_pos = _queue_position_locked(str(job_id or ""))
            if queue_pos > 0:
                job["message"] = (
                    f"任务排队中，前方 {max(0, queue_pos - 1)} 个任务，"
                    f"并发上限 {config.MULTI_ROLE_V2_MAX_CONCURRENT_JOBS}"
                )
            job["queue_position"] = int(queue_pos)
        return _serialize_async_multi_role_v2_job(job)


def decide_async_multi_role_v2_job(*, job_id: str, action: str):
    action = str(action or "").strip().lower()
    if action not in {"retry", "degrade", "abort"}:
        raise ValueError("action 必须是 retry|degrade|abort")
    _cleanup_async_multi_role_v2_jobs()
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(job_id)
        if not job:
            raise RuntimeError(f"任务不存在或已过期: {job_id}")
        if str(job.get("status")) != "pending_user_decision":
            raise RuntimeError(f"当前任务状态不允许决策: {job.get('status')}")
        ts_code = str(job.get("ts_code") or "")
        now = datetime.now(timezone.utc).isoformat()
        state = dict(job.get("decision_state") or {})
        state["last_action"] = action
        state["updated_at"] = now
        state["pending_user_decision"] = False
        job["decision_state"] = state
        job["updated_at"] = now
        job["updated_at_ts"] = time.time()

        if action == "abort":
            job["status"] = "error"
            job["progress"] = 100
            job["stage"] = "error"
            job["message"] = "用户终止任务"
            job["error"] = "用户选择 abort"
            job["finished_at"] = now
            try:
                _persist_multi_role_analysis_v2_job(job)
            except Exception:
                pass
            publish_app_event(
                event="multi_role_job_update",
                payload={"job_id": job_id, "status": "error", "progress": 100, "stage": "error", "ts_code": ts_code, "mode": "v2"},
                producer="backend.server",
            )
            return _serialize_async_multi_role_v2_job(job)

        if action == "degrade":
            job["status"] = "running"
            job["progress"] = 82
            job["stage"] = "aggregating"
            job["message"] = "按用户决策执行降级汇总"
        else:
            job["status"] = "running"
            job["progress"] = 72
            job["stage"] = "role_retry"
            job["message"] = "按用户决策执行补重试"
        try:
            _persist_multi_role_analysis_v2_job(job)
        except Exception:
            pass
    if action == "degrade":
        _finalize_multi_role_v2_job(job_id, final_status="done_with_warnings")
    else:
        worker = threading.Thread(
            target=_retry_async_multi_role_v2_job,
            args=(job_id,),
            daemon=True,
            name=f"multi_role_v2_retry_{job_id[:8]}",
        )
        worker.start()
    return get_async_multi_role_v2_job(job_id) or {"job_id": job_id, "status": "error", "error": "任务不存在"}


def _retry_async_multi_role_v2_aggregate_worker(job_id: str):
    target_job_id = str(job_id or "").strip()
    in_memory = False
    ts_code = ""
    try:
        _cleanup_async_multi_role_v2_jobs()
        with config.ASYNC_MULTI_ROLE_V2_LOCK:
            live_job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(target_job_id)
        if live_job:
            job = live_job
            in_memory = True
        else:
            persisted = _load_persisted_multi_role_v2_job(target_job_id)
            if not persisted:
                return
            job = persisted
            in_memory = False

        ts_code = str(job.get("ts_code") or "")
        lookback = int(job.get("lookback") or 120)
        role_runs = list(job.get("role_runs") or [])
        done_roles = [x for x in role_runs if str(x.get("status") or "") == "done" and str(x.get("output") or "").strip()]
        if not done_roles:
            raise RuntimeError("没有可用的角色输出，无法重试汇总")

        aggregator = _run_multi_role_v2_aggregator(
            role_runs=done_roles,
            ts_code=ts_code,
            lookback=lookback,
            policy_map=_load_multi_role_v2_policies(),
        )
        analysis_markdown = str(aggregator.get("analysis_markdown") or "")
        split_payload = split_multi_role_analysis(analysis_markdown, list(job.get("roles") or []))
        role_outputs = split_payload.get("role_sections") or [
            {"role": x.get("role"), "content": x.get("output"), "matched": True, "logic_view": {}}
            for x in done_roles
        ]
        confidence = infer_decision_confidence(analysis_markdown).to_dict()
        risk_review = build_risk_review(analysis_markdown).to_dict()
        portfolio = build_portfolio_view(analysis_markdown).to_dict()

        final_status = "done" if aggregator.get("ok") else "done_with_warnings"
        complete_now = datetime.now(timezone.utc).isoformat()

        if in_memory:
            with config.ASYNC_MULTI_ROLE_V2_LOCK:
                live = config.ASYNC_MULTI_ROLE_V2_JOBS.get(target_job_id)
                if not live:
                    return
                live["aggregator_run"] = {
                    "status": "done" if aggregator.get("ok") else "error",
                    "used_model": aggregator.get("used_model") or "",
                    "requested_model": aggregator.get("requested_model") or "",
                    "attempts": aggregator.get("attempts") or [],
                    "error": aggregator.get("error") or "",
                    "duration_ms": int(aggregator.get("duration_ms") or 0),
                }
                live["analysis_markdown"] = analysis_markdown
                live["analysis"] = analysis_markdown
                live["used_model"] = str(aggregator.get("used_model") or "")
                live["attempts"] = aggregator.get("attempts") or []
                live["role_outputs"] = role_outputs
                live["role_sections"] = role_outputs
                live["common_sections_markdown"] = split_payload.get("common_sections_markdown") or ""
                live["decision_confidence"] = confidence
                live["risk_review"] = risk_review
                live["portfolio_view"] = portfolio
                live["status"] = final_status
                live["stage"] = "done"
                live["progress"] = 100
                live["message"] = "汇总重试完成" if aggregator.get("ok") else "汇总重试失败，已保留角色原文"
                live["error"] = ""
                live["finished_at"] = complete_now
                live["updated_at"] = complete_now
                live["updated_at_ts"] = time.time()
                try:
                    _persist_multi_role_analysis_v2_job(live)
                except Exception:
                    pass
            publish_app_event(
                event="multi_role_job_update",
                payload={"job_id": target_job_id, "status": final_status, "progress": 100, "stage": "done", "ts_code": ts_code, "mode": "v2"},
                producer="backend.server",
            )
            return

        job["aggregator_run"] = {
            "status": "done" if aggregator.get("ok") else "error",
            "used_model": aggregator.get("used_model") or "",
            "requested_model": aggregator.get("requested_model") or "",
            "attempts": aggregator.get("attempts") or [],
            "error": aggregator.get("error") or "",
            "duration_ms": int(aggregator.get("duration_ms") or 0),
        }
        job["analysis_markdown"] = analysis_markdown
        job["analysis"] = analysis_markdown
        job["used_model"] = str(aggregator.get("used_model") or "")
        job["attempts"] = aggregator.get("attempts") or []
        job["role_outputs"] = role_outputs
        job["role_sections"] = role_outputs
        job["common_sections_markdown"] = split_payload.get("common_sections_markdown") or ""
        job["decision_confidence"] = confidence
        job["risk_review"] = risk_review
        job["portfolio_view"] = portfolio
        job["status"] = final_status
        job["stage"] = "done"
        job["progress"] = 100
        job["message"] = "汇总重试完成" if aggregator.get("ok") else "汇总重试失败，已保留角色原文"
        job["error"] = ""
        job["finished_at"] = complete_now
        job["updated_at"] = complete_now
        job["updated_at_ts"] = time.time()
        try:
            _persist_multi_role_analysis_v2_job(job)
        except Exception:
            pass
    except Exception as exc:
        fail_now = datetime.now(timezone.utc).isoformat()
        if in_memory:
            with config.ASYNC_MULTI_ROLE_V2_LOCK:
                live = config.ASYNC_MULTI_ROLE_V2_JOBS.get(target_job_id)
                if live:
                    live["status"] = "done_with_warnings"
                    live["stage"] = "done"
                    live["progress"] = 100
                    live["message"] = f"汇总重试失败：{exc}"
                    live["error"] = str(exc)
                    live["finished_at"] = fail_now
                    live["updated_at"] = fail_now
                    live["updated_at_ts"] = time.time()
                    try:
                        _persist_multi_role_analysis_v2_job(live)
                    except Exception:
                        pass
            if ts_code:
                publish_app_event(
                    event="multi_role_job_update",
                    payload={"job_id": target_job_id, "status": "done_with_warnings", "progress": 100, "stage": "done", "ts_code": ts_code, "mode": "v2"},
                    producer="backend.server",
                )
            return
        persisted = _load_persisted_multi_role_v2_job(target_job_id)
        if persisted:
            persisted["status"] = "done_with_warnings"
            persisted["stage"] = "done"
            persisted["progress"] = 100
            persisted["message"] = f"汇总重试失败：{exc}"
            persisted["error"] = str(exc)
            persisted["finished_at"] = fail_now
            persisted["updated_at"] = fail_now
            persisted["updated_at_ts"] = time.time()
            try:
                _persist_multi_role_analysis_v2_job(persisted)
            except Exception:
                pass


def retry_async_multi_role_v2_aggregate(*, job_id: str):
    target_job_id = str(job_id or "").strip()
    if not target_job_id:
        raise ValueError("job_id 不能为空")
    _cleanup_async_multi_role_v2_jobs()
    with config.ASYNC_MULTI_ROLE_V2_LOCK:
        live_job = config.ASYNC_MULTI_ROLE_V2_JOBS.get(target_job_id)
    if live_job:
        job = live_job
        in_memory = True
    else:
        persisted = _load_persisted_multi_role_v2_job(target_job_id)
        if not persisted:
            raise RuntimeError(f"任务不存在或已过期: {target_job_id}")
        job = persisted
        in_memory = False

    status = str(job.get("status") or "")
    if status in {"queued", "running"}:
        raise RuntimeError(f"任务仍在执行中，当前状态={status}，暂不支持重试汇总")

    role_runs = list(job.get("role_runs") or [])
    done_roles = [x for x in role_runs if str(x.get("status") or "") == "done" and str(x.get("output") or "").strip()]
    if not done_roles:
        raise RuntimeError("没有可用的角色输出，无法重试汇总")

    now = datetime.now(timezone.utc).isoformat()
    ts_code = str(job.get("ts_code") or "")
    if in_memory:
        with config.ASYNC_MULTI_ROLE_V2_LOCK:
            live = config.ASYNC_MULTI_ROLE_V2_JOBS.get(target_job_id)
            if not live:
                raise RuntimeError(f"任务不存在或已过期: {target_job_id}")
            live["status"] = "running"
            live["progress"] = 85
            live["stage"] = "aggregating"
            live["message"] = "正在重试汇总器（后台执行）"
            live["updated_at"] = now
            live["updated_at_ts"] = time.time()
            try:
                _persist_multi_role_analysis_v2_job(live)
            except Exception:
                pass
            payload_job = _serialize_async_multi_role_v2_job(live)
    else:
        job["status"] = "running"
        job["progress"] = 85
        job["stage"] = "aggregating"
        job["message"] = "正在重试汇总器（后台执行）"
        job["updated_at"] = now
        job["updated_at_ts"] = time.time()
        try:
            _persist_multi_role_analysis_v2_job(job)
        except Exception:
            pass
        payload_job = _serialize_async_multi_role_v2_job(job)

    publish_app_event(
        event="multi_role_job_update",
        payload={"job_id": target_job_id, "status": "running", "progress": 85, "stage": "aggregating", "ts_code": ts_code, "mode": "v2"},
        producer="backend.server",
    )
    worker = threading.Thread(
        target=_retry_async_multi_role_v2_aggregate_worker,
        args=(target_job_id,),
        daemon=True,
        name=f"multi_role_v2_reagg_{target_job_id[:8]}",
    )
    worker.start()
    return payload_job


def build_agent_service_deps() -> dict:
    return build_backend_runtime_deps(
        sqlite3_module=sqlite3,
        db_path=config.DB_PATH,
        build_trend_features=build_trend_features,
        call_llm_trend=call_llm_trend,
        extract_logic_view_from_markdown=extract_logic_view_from_markdown,
        get_or_build_cached_logic_view=get_or_build_cached_logic_view,
        build_multi_role_context=build_multi_role_context,
        call_llm_multi_role=call_llm_multi_role,
        split_multi_role_analysis=split_multi_role_analysis,
        enable_risk_precheck=config.ENABLE_AGENT_RISK_PRECHECK,
        pre_trade_check_fn=pre_trade_check,
        enable_notifications=config.ENABLE_AGENT_NOTIFICATIONS,
        notify_result_fn=_notify_result,
    )


def build_reporting_service_deps() -> dict:
    return build_reporting_runtime_deps(
        query_news_daily_summaries=query_news_daily_summaries,
        start_async_daily_summary_job=start_async_daily_summary_job,
        get_async_daily_summary_job=get_async_daily_summary_job,
    )


def build_stock_news_service_runtime_deps() -> dict:
    return build_stock_news_service_deps(
        root_dir=ROOT_DIR,
        db_path=config.DB_PATH,
        sqlite3_module=sqlite3,
        publish_app_event=publish_app_event,
        extract_llm_result_marker=_extract_llm_result_marker,
    )


def build_chatrooms_service_runtime_deps() -> dict:
    return build_chatrooms_service_deps(
        sqlite3_module=sqlite3,
        db_path=config.DB_PATH,
        root_dir=ROOT_DIR,
        publish_app_event=publish_app_event,
    )


def build_stock_detail_service_runtime_deps() -> dict:
    return build_stock_detail_runtime_deps(
        sqlite3_module=sqlite3,
        db_path=config.DB_PATH,
    )


def build_signals_service_runtime_deps() -> dict:
    return build_signals_runtime_deps(
        sqlite3_module=sqlite3,
        db_path=config.DB_PATH,
        resolve_signal_table_fn=resolve_signal_table,
        cache_get_json_fn=cache_get_json,
        cache_set_json_fn=cache_set_json,
        redis_cache_ttl_signals=REDIS_CACHE_TTL_SIGNALS,
        redis_cache_ttl_themes=REDIS_CACHE_TTL_THEMES,
        get_or_build_cached_logic_view_fn=get_or_build_cached_logic_view,
        build_signal_logic_view_fn=build_signal_logic_view,
        build_signal_event_logic_view_fn=build_signal_event_logic_view,
        query_research_reports_fn=query_research_reports,
        query_macro_indicators_fn=query_macro_indicators,
        query_macro_series_fn=query_macro_series,
        query_signal_chain_graph_fn=query_signal_chain_graph,
    )


def build_quantaalpha_runtime_deps() -> dict:
    return build_quantaalpha_service_runtime_deps(
        sqlite3_module=sqlite3,
        db_path=str(config.DB_PATH),
    )


def build_decision_runtime_deps() -> dict:
    return build_decision_service_runtime_deps(
        sqlite3_module=sqlite3,
        db_path=str(config.DB_PATH),
    )


def _cleanup_async_daily_summary_jobs():
    reporting_cleanup_async_jobs(
        jobs=config.ASYNC_DAILY_SUMMARY_JOBS,
        lock=config.ASYNC_DAILY_SUMMARY_LOCK,
        ttl_seconds=config.ASYNC_JOB_TTL_SECONDS,
    )


def _serialize_async_daily_summary_job(job: dict):
    return reporting_serialize_async_daily_summary_job(job)


def _create_async_daily_summary_job(model: str, summary_date: str):
    _cleanup_async_daily_summary_jobs()
    return reporting_create_async_daily_summary_job(
        jobs=config.ASYNC_DAILY_SUMMARY_JOBS,
        lock=config.ASYNC_DAILY_SUMMARY_LOCK,
        publish_app_event=publish_app_event,
        model=model,
        summary_date=summary_date,
    )


def _run_async_daily_summary_job(job_id: str):
    reporting_run_async_daily_summary_job(
        jobs=config.ASYNC_DAILY_SUMMARY_JOBS,
        lock=config.ASYNC_DAILY_SUMMARY_LOCK,
        publish_app_event=publish_app_event,
        generate_daily_summary_fn=generate_daily_summary,
        get_daily_summary_by_date_fn=get_daily_summary_by_date,
        notify_fn=_notify_result if config.ENABLE_REPORTING_NOTIFICATIONS else None,
        job_id=job_id,
    )


def start_async_daily_summary_job(model: str, summary_date: str):
    return reporting_start_async_daily_summary_job(
        jobs=config.ASYNC_DAILY_SUMMARY_JOBS,
        lock=config.ASYNC_DAILY_SUMMARY_LOCK,
        cleanup_async_jobs_fn=_cleanup_async_daily_summary_jobs,
        create_async_daily_summary_job_fn=_create_async_daily_summary_job,
        serialize_async_daily_summary_job_fn=_serialize_async_daily_summary_job,
        run_async_daily_summary_job_fn=_run_async_daily_summary_job,
        model=model,
        summary_date=summary_date,
    )


def get_async_daily_summary_job(job_id: str):
    return reporting_get_async_daily_summary_job(
        jobs=config.ASYNC_DAILY_SUMMARY_JOBS,
        lock=config.ASYNC_DAILY_SUMMARY_LOCK,
        cleanup_async_jobs_fn=_cleanup_async_daily_summary_jobs,
        serialize_async_daily_summary_job_fn=_serialize_async_daily_summary_job,
        job_id=job_id,
    )


def _multi_role_v3_resolve_stage_role(node_key: str, strong: bool) -> tuple[str, str]:
    key = str(node_key or "").strip().lower()
    if key.startswith("analyst:"):
        return "analyst", key
    if key in {"research:bull", "research:bear", "research:manager"}:
        return "research_debate", key
    if key.startswith("risk:"):
        return "risk_debate", key
    if key == "decision:research_manager":
        return "research_manager", key
    if key == "decision:trader":
        return "trader", key
    if key == "decision:portfolio_manager":
        return "portfolio_manager", key
    # 未识别节点回退到阶段强弱模型。
    return ("default_strong" if strong else "default_quick"), key


def _multi_role_v3_entry_chain(entry: dict | None, *, quick_model: str, deep_model: str, global_fallback: list[str], default_mode: str) -> list[str]:
    cfg = dict(entry or {})
    mode = str(cfg.get("mode") or "").strip().lower()
    if mode not in {"quick", "deep"}:
        mode = default_mode
    base = str(cfg.get("primary_model") or "").strip() or (quick_model if mode == "quick" else deep_model)
    fallback_raw = cfg.get("fallback_models") or []
    if isinstance(fallback_raw, str):
        fallback = [x.strip() for x in fallback_raw.split(",") if x.strip()]
    elif isinstance(fallback_raw, list):
        fallback = [str(x).strip() for x in fallback_raw if str(x).strip()]
    else:
        fallback = []
    if not fallback:
        fallback = list(global_fallback or [])
    out: list[str] = []
    for item in [base, *fallback]:
        normalized = normalize_model_name(str(item or ""))
        if normalized and normalized not in out:
            out.append(normalized)
    return out


def _multi_role_v3_model_chain(*, node_key: str, strong: bool) -> list[str]:
    policies = _load_multi_role_v3_policies()
    stage, role_key = _multi_role_v3_resolve_stage_role(node_key, strong)
    quick_model = normalize_model_name(str(policies.get("quick_think_llm") or "kimi-k2.5"))
    deep_model = normalize_model_name(str(policies.get("deep_think_llm") or "gpt-5.4-multi-role"))
    global_fallback = [normalize_model_name(str(x or "")) for x in list(policies.get("fallback_models") or []) if str(x or "").strip()]

    default_mode = "deep" if strong else "quick"
    base_chain = _multi_role_v3_entry_chain(
        None,
        quick_model=quick_model,
        deep_model=deep_model,
        global_fallback=global_fallback,
        default_mode=default_mode,
    )
    stage_chain = _multi_role_v3_entry_chain(
        dict((policies.get("stage_models") or {}).get(stage) or {}),
        quick_model=quick_model,
        deep_model=deep_model,
        global_fallback=global_fallback,
        default_mode=default_mode,
    )
    role_chain = _multi_role_v3_entry_chain(
        dict((policies.get("role_models") or {}).get(role_key.lower()) or {}),
        quick_model=quick_model,
        deep_model=deep_model,
        global_fallback=global_fallback,
        default_mode=default_mode,
    )

    chain: list[str] = []
    for bucket in (role_chain, stage_chain, base_chain):
        for item in bucket:
            if item and item not in chain:
                chain.append(item)
    return chain or [normalize_model_name(DEFAULT_LLM_MODEL)]


def _multi_role_v3_call_llm(*, node_key: str, prompt: str, strong: bool, timeout_s: int = 60) -> dict:
    chain = _multi_role_v3_model_chain(node_key=node_key, strong=strong)
    attempts: list[dict] = []
    last_err = ""
    for model in chain:
        try:
            result = chat_completion_with_fallback(
                model=model,
                temperature=0.2 if strong else 0.1,
                timeout_s=max(20, int(timeout_s or 60)),
                max_retries=0,
                messages=[
                    {
                        "role": "system",
                        "content": "你是结构化投研节点执行器。只输出 JSON，不要额外解释。",
                    },
                    {"role": "user", "content": str(prompt or "")},
                ],
            )
            for item in list(result.attempts or []):
                attempts.append({"model": item.model, "base_url": item.base_url, "error": item.error})
            return {
                "text": str(result.text or ""),
                "used_model": str(result.used_model or model),
                "requested_model": str(result.requested_model or model),
                "attempts": attempts,
            }
        except Exception as exc:
            last_err = str(exc)
            attempts.append({"model": model, "base_url": "", "error": last_err})
            continue
    raise RuntimeError(f"{node_key} 调用失败: {last_err or 'unknown error'}")


def _multi_role_v3_build_context(*, ts_code: str, lookback: int) -> dict:
    return build_multi_role_context(ts_code=ts_code, lookback=lookback)


def start_multi_role_v3_job(payload: dict) -> dict:
    return create_multi_role_v3_job(
        sqlite3_module=sqlite3,
        db_path=config.DB_PATH,
        payload=payload,
    )


def get_multi_role_v3_job_by_id(job_id: str):
    return get_multi_role_v3_job(sqlite3_module=sqlite3, db_path=config.DB_PATH, job_id=job_id)


def decide_multi_role_v3_job(*, job_id: str, action: str):
    return control_multi_role_v3_job(
        sqlite3_module=sqlite3,
        db_path=config.DB_PATH,
        job_id=job_id,
        action=action,
    )


def action_multi_role_v3_job(*, job_id: str, action: str, stage: str = ""):
    return control_multi_role_v3_job(
        sqlite3_module=sqlite3,
        db_path=config.DB_PATH,
        job_id=job_id,
        action=action,
        stage=stage,
    )


def run_multi_role_v3_worker_once() -> None:
    run_multi_role_v3_worker_loop(
        sqlite3_module=sqlite3,
        db_path=config.DB_PATH,
        runtime={
            "build_context": _multi_role_v3_build_context,
            "llm_call": _multi_role_v3_call_llm,
        },
        once=True,
    )

__all__ = ['_cleanup_async_multi_role_jobs', '_serialize_async_job', '_create_async_multi_role_job', '_run_async_multi_role_job', 'start_async_multi_role_job', 'get_async_multi_role_job', '_cleanup_async_multi_role_v2_jobs', '_queue_position_locked', '_refresh_multi_role_v2_runtime_meta_locked', '_dispatch_multi_role_v2_queue', '_release_multi_role_v2_slot', '_context_cache_cn_date', '_context_cache_key', '_get_cached_multi_role_v2_context', '_set_cached_multi_role_v2_context', '_build_role_specific_context', '_default_multi_role_v2_policies', '_load_multi_role_v2_policies', '_default_multi_role_v3_policies', '_normalize_multi_role_v3_policy_entry', '_load_multi_role_v3_policies', '_policy_model_chain', '_is_kimi_model', '_is_gpt54_model', '_is_gpt54_family_model', '_candidate_route_chain', '_build_multi_role_v2_single_prompt', '_run_multi_role_v2_single_role', '_trim_role_output_for_aggregate', '_trim_aggregator_inputs', '_run_multi_role_v2_aggregator', '_serialize_async_multi_role_v2_job', '_create_async_multi_role_v2_job', '_run_multi_role_v2_role_batch', '_finalize_multi_role_v2_job', '_run_async_multi_role_v2_job', '_retry_async_multi_role_v2_job', 'start_async_multi_role_v2_job', 'get_async_multi_role_v2_job', 'decide_async_multi_role_v2_job', '_retry_async_multi_role_v2_aggregate_worker', 'retry_async_multi_role_v2_aggregate', 'build_agent_service_deps', 'build_reporting_service_deps', 'build_stock_news_service_runtime_deps', 'build_chatrooms_service_runtime_deps', 'build_stock_detail_service_runtime_deps', 'build_signals_service_runtime_deps', 'build_quantaalpha_runtime_deps', 'build_decision_runtime_deps', '_cleanup_async_daily_summary_jobs', '_serialize_async_daily_summary_job', '_create_async_daily_summary_job', '_run_async_daily_summary_job', 'start_async_daily_summary_job', 'get_async_daily_summary_job', '_multi_role_v3_resolve_stage_role', '_multi_role_v3_entry_chain', '_multi_role_v3_model_chain', '_multi_role_v3_call_llm', '_multi_role_v3_build_context', 'start_multi_role_v3_job', 'get_multi_role_v3_job_by_id', 'decide_multi_role_v3_job', 'action_multi_role_v3_job', 'run_multi_role_v3_worker_once']
