#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import db_compat as sqlite3
from llm_gateway import (
    DEFAULT_LLM_MODEL,
    chat_completion_with_fallback,
    normalize_model_name,
    normalize_temperature_for_model,
)
from llm_provider_config import get_provider_candidates
from realtime_streams import publish_app_event
from services.notifications import build_notification_payload, notify_with_wecom
from services.agent_service import (
build_trend_features as agent_build_trend_features,
call_llm_multi_role as agent_call_llm_multi_role,
call_llm_trend as agent_call_llm_trend,
split_multi_role_analysis as agent_split_multi_role_analysis,
)
from skills.strategies import load_strategy_template_text

from backend.http_server import config

def _extract_llm_result_marker(text: str) -> dict:
    raw = str(text or "")
    for line in reversed(raw.splitlines()):
        if not line.startswith("__LLM_RESULT__="):
            continue
        try:
            obj = json.loads(line.split("=", 1)[1])
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def build_trend_features(ts_code: str, lookback: int):
    return agent_build_trend_features(
        sqlite3_module=sqlite3,
        db_path=config.DB_PATH,
        safe_float=_safe_float,
        calc_ma=_calc_ma,
        ts_code=ts_code,
        lookback=lookback,
    )


def _parse_json_text(raw: str):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _round_or_none(value, digits: int = 4):
    num = _safe_float(value)
    if num is None:
        return None
    return round(num, digits)


def _build_price_rollups_summary(conn: sqlite3.Connection, ts_code: str):
    try:
        rows = conn.execute(
            """
            SELECT
                ts_code, window_days, start_date, end_date, rows_count,
                close_first, close_last, close_change_pct, high_max, low_min, vol_avg, amount_avg, update_time
            FROM stock_daily_price_rollups
            WHERE ts_code = ?
            ORDER BY window_days ASC, end_date DESC
            """,
            (ts_code,),
        ).fetchall()
    except Exception:
        return {"items": [], "by_window": {}}

    latest_by_window: dict[str, dict] = {}
    for row in rows:
        d = dict(row)
        key = str(d.get("window_days") or "")
        if not key or key in latest_by_window:
            continue
        latest_by_window[key] = {
            "window_days": d.get("window_days"),
            "start_date": d.get("start_date"),
            "end_date": d.get("end_date"),
            "rows_count": d.get("rows_count"),
            "close_first": _round_or_none(d.get("close_first"), 3),
            "close_last": _round_or_none(d.get("close_last"), 3),
            "close_change_pct": _round_or_none(d.get("close_change_pct"), 2),
            "high_max": _round_or_none(d.get("high_max"), 3),
            "low_min": _round_or_none(d.get("low_min"), 3),
            "vol_avg": _round_or_none(d.get("vol_avg"), 2),
            "amount_avg": _round_or_none(d.get("amount_avg"), 2),
            "update_time": d.get("update_time"),
        }
    ordered_items = [latest_by_window[k] for k in sorted(latest_by_window.keys(), key=lambda x: int(x))]
    return {"items": ordered_items, "by_window": latest_by_window}


def _build_event_summary(conn: sqlite3.Connection, ts_code: str):
    rows = conn.execute(
        """
        SELECT event_type, event_date, ann_date, title, detail_json, source
        FROM stock_events
        WHERE ts_code = ?
        ORDER BY COALESCE(event_date, ann_date) DESC, ann_date DESC, id DESC
        LIMIT 8
        """,
        (ts_code,),
    ).fetchall()
    if not rows:
        return {}
    items = []
    type_count: dict[str, int] = {}
    for row in rows:
        d = dict(row)
        type_count[d["event_type"]] = type_count.get(d["event_type"], 0) + 1
        items.append(
            {
                "event_type": d["event_type"],
                "event_date": d["event_date"],
                "ann_date": d["ann_date"],
                "title": d["title"],
                "detail": _parse_json_text(d["detail_json"]),
            }
        )
    return {"recent_events": items, "event_type_count": type_count}


def _stock_news_latest_pub(conn: sqlite3.Connection, ts_code: str):
    row = conn.execute(
        "SELECT MAX(pub_time) FROM stock_news_items WHERE ts_code = ?",
        (ts_code,),
    ).fetchone()
    return row[0] if row and row[0] else ""


def _stock_news_is_fresh(conn: sqlite3.Connection, ts_code: str):
    latest_pub = _stock_news_latest_pub(conn, ts_code)
    if not latest_pub:
        return False, ""
    latest_date = str(latest_pub).strip()[:10]
    today_cn = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
    return latest_date == today_cn, latest_pub


def _ensure_stock_news_fresh(
    ts_code: str,
    company_name: str,
    page_size: int = 20,
    score_model: str = DEFAULT_LLM_MODEL,
    score_limit: int = 3,
    score_timeout_s: int = 90,
    non_blocking: bool = False,
):
    conn = sqlite3.connect(config.DB_PATH)
    try:
        fresh, latest_pub = _stock_news_is_fresh(conn, ts_code)
    finally:
        conn.close()
    if fresh:
        return {"fetched": False, "scored": False, "latest_pub": latest_pub}
    if non_blocking:
        # 页面主链路优先：非阻塞模式下不在请求链路同步做采集/评分，避免多角色分析长时间卡住。
        return {
            "fetched": False,
            "scored": False,
            "latest_pub": latest_pub,
            "skipped": True,
            "reason": "non_blocking_mode",
        }
    out = {"fetched": False, "scored": False, "latest_pub": latest_pub, "fetch_error": "", "score_error": ""}
    fetch_info = {"stdout": "", "stderr": ""}
    score_info = {"stdout": "", "stderr": ""}
    try:
        fetch_info = fetch_stock_news_now(ts_code=ts_code, company_name=company_name, page_size=page_size)
        out["fetched"] = True
    except Exception as exc:
        out["fetch_error"] = str(exc)
    try:
        safe_limit = max(1, min(int(score_limit or 1), min(page_size, 10)))
        score_info = score_stock_news_now(
            ts_code=ts_code,
            limit=safe_limit,
            model=score_model,
            timeout_s=max(30, int(score_timeout_s or 90)),
        )
        out["scored"] = True
    except Exception as exc:
        out["score_error"] = str(exc)
    conn = sqlite3.connect(config.DB_PATH)
    try:
        latest_pub = _stock_news_latest_pub(conn, ts_code)
    finally:
        conn.close()
    out["latest_pub"] = latest_pub
    out["fetch_stdout"] = fetch_info.get("stdout", "")
    out["score_stdout"] = score_info.get("stdout", "")
    return out


def _load_strategy_template_for(name: str) -> str:
    if not ENABLE_SKILLS_TEMPLATE_PROMPTS:
        return ""
    return load_strategy_template_text(name)


def _notify_result(title: str, summary: str, markdown: str, subject_key: str = "", link: str = "") -> dict:
    payload = build_notification_payload(
        title=title,
        summary=summary,
        markdown=markdown,
        subject_key=subject_key,
        link=link,
    )
    if not WECOM_WEBHOOK_URL:
        return {"ok": False, "skipped": True, "reason": "missing_webhook"}
    try:
        result = notify_with_wecom(payload, webhook_url=WECOM_WEBHOOK_URL)
        return {"ok": bool(result.get("ok")), "result": result}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def call_llm_trend(ts_code: str, features: dict, model: str, temperature: float = 0.2):
    return agent_call_llm_trend(
        normalize_model_name=normalize_model_name,
        normalize_temperature_for_model=normalize_temperature_for_model,
        chat_completion_with_fallback=chat_completion_with_fallback,
        default_llm_model=DEFAULT_LLM_MODEL,
        sanitize_json_value=_sanitize_json_value,
        trend_template_text=_load_strategy_template_for("trend_analysis_template.md"),
        ts_code=ts_code,
        features=features,
        model=model,
        temperature=temperature,
    )


def _resolve_roles(raw: str) -> list[str]:
    roles = [x.strip() for x in (raw or "").split(",") if x.strip()]
    return roles or list(DEFAULT_MULTI_ROLES)


def build_multi_role_context(ts_code: str, lookback: int):
    context = agent_build_multi_role_context(
        sqlite3_module=sqlite3,
        db_path=config.DB_PATH,
        default_llm_model=DEFAULT_LLM_MODEL,
        build_trend_features_fn=build_trend_features,
        ensure_stock_news_fresh=_ensure_stock_news_fresh,
        build_stock_news_summary=stock_detail_build_stock_news_summary,
        build_financial_summary=stock_detail_build_financial_summary,
        build_valuation_summary=stock_detail_build_valuation_summary,
        build_capital_flow_summary=stock_detail_build_capital_flow_summary,
        build_event_summary=_build_event_summary,
        build_macro_context=stock_detail_build_macro_context,
        build_fx_context=stock_detail_build_fx_context,
        build_rate_spread_context=stock_detail_build_rate_spread_context,
        build_governance_summary=stock_detail_build_governance_summary,
        build_risk_summary=stock_detail_build_risk_summary,
        ts_code=ts_code,
        lookback=lookback,
    )
    if AI_RETRIEVAL_ENABLED and not AI_RETRIEVAL_SHADOW_MODE:
        try:
            company_name = str((context.get("company_profile") or {}).get("name") or "").strip()
            retrieval_query = f"{ts_code} {company_name}".strip()
            if retrieval_query:
                report_ctx = ai_retrieval_context(query=retrieval_query, scene="report", top_k=4, max_chars=1200)
                news_ctx = ai_retrieval_context(query=retrieval_query, scene="news", top_k=4, max_chars=1200)
                context["retrieval_context"] = {
                    "query": retrieval_query,
                    "report_context": report_ctx.get("context") or {},
                    "news_context": news_ctx.get("context") or {},
                    "trace": {
                        "report": report_ctx.get("trace") or {},
                        "news": news_ctx.get("trace") or {},
                    },
                }
        except Exception as exc:
            context["retrieval_context"] = {"error": str(exc), "query": ts_code}
    return context


def build_multi_role_prompt(context: dict, roles: list[str]) -> str:
    return agent_build_multi_role_prompt(
        sanitize_json_value=_sanitize_json_value,
        role_profiles=ROLE_PROFILES,
        context=context,
        roles=roles,
        multi_role_template_text=_load_strategy_template_for("multi_role_research_template.md"),
    )


def call_llm_multi_role(context: dict, roles: list[str], model: str, temperature: float = 0.2):
    return agent_call_llm_multi_role(
        normalize_model_name=normalize_model_name,
        normalize_temperature_for_model=normalize_temperature_for_model,
        chat_completion_with_fallback=chat_completion_with_fallback,
        default_llm_model=DEFAULT_LLM_MODEL,
        build_multi_role_prompt_fn=build_multi_role_prompt,
        context=context,
        roles=roles,
        model=model,
        temperature=temperature,
    )


def _normalize_markdown_lines(text: str) -> str:
    return str(text or "").replace("\r\n", "\n")


def _normalize_section_heading(text: str) -> str:
    line = str(text or "").strip()
    line = re.sub(r"^#{1,6}\s*", "", line)
    line = re.sub(r"^\*+\s*", "", line)
    line = re.sub(r"\*+\s*$", "", line)
    line = re.sub(r"^[>\-\*\s]+", "", line)
    line = re.sub(r"^[\(（]?[0-9一二三四五六七八九十]+[\)）\.\、:\-]\s*", "", line)
    line = re.sub(r"^(第[0-9一二三四五六七八九十]+部分[:：]?)\s*", "", line)
    return line.strip()


def _find_section_start(text: str, name: str) -> int:
    normalized_name = _normalize_section_heading(name)
    cursor = 0
    for raw_line in str(text or "").splitlines(True):
        line = raw_line.rstrip("\n")
        heading = _normalize_section_heading(line)
        if heading == normalized_name or normalized_name in heading:
            return cursor
        if heading.endswith("：") or heading.endswith(":"):
            heading2 = heading[:-1].strip()
            if heading2 == normalized_name or normalized_name in heading2:
                return cursor
        cursor += len(raw_line)
    return -1


def _build_common_sections_markdown(text: str) -> str:
    normalized = _normalize_markdown_lines(text)
    common_names = ["综合结论", "行动清单", "关键分歧", "非投资建议免责声明"]
    start = -1
    for name in common_names:
        pos = _find_section_start(normalized, name)
        if pos != -1 and (start == -1 or pos < start):
            start = pos
    if start == -1:
        return ""
    return normalized[start:].strip()


def ensure_logic_view_cache_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS logic_view_cache (
            entity_type TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            logic_view_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            update_time TEXT NOT NULL,
            PRIMARY KEY (entity_type, entity_key, content_hash)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_logic_view_cache_update_time ON logic_view_cache(update_time)"
    )
    conn.commit()


def ensure_multi_role_analysis_history_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS multi_role_analysis_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT NOT NULL,
            version TEXT NOT NULL,
            status TEXT NOT NULL,
            ts_code TEXT NOT NULL,
            name TEXT,
            lookback INTEGER NOT NULL DEFAULT 120,
            roles_json TEXT NOT NULL DEFAULT '[]',
            accept_auto_degrade INTEGER NOT NULL DEFAULT 1,
            requested_model TEXT,
            used_model TEXT,
            attempts_json TEXT NOT NULL DEFAULT '[]',
            role_runs_json TEXT NOT NULL DEFAULT '[]',
            aggregator_run_json TEXT NOT NULL DEFAULT '{}',
            decision_state_json TEXT NOT NULL DEFAULT '{}',
            warnings_json TEXT NOT NULL DEFAULT '[]',
            error TEXT,
            analysis_markdown TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            finished_at TEXT
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_multi_role_analysis_history_job_id ON multi_role_analysis_history(job_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_multi_role_analysis_history_ts_code ON multi_role_analysis_history(ts_code, created_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_multi_role_analysis_history_status ON multi_role_analysis_history(status, created_at)"
    )
    conn.commit()


def _persist_multi_role_analysis_v2_job(job: dict):
    if not isinstance(job, dict):
        return
    conn = sqlite3.connect(config.DB_PATH)
    try:
        ensure_multi_role_analysis_history_table(conn)
        conn.execute(
            """
            INSERT INTO multi_role_analysis_history (
                job_id, version, status, ts_code, name, lookback, roles_json, accept_auto_degrade,
                requested_model, used_model, attempts_json, role_runs_json, aggregator_run_json,
                decision_state_json, warnings_json, error, analysis_markdown, created_at, updated_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                status = excluded.status,
                name = excluded.name,
                lookback = excluded.lookback,
                roles_json = excluded.roles_json,
                accept_auto_degrade = excluded.accept_auto_degrade,
                requested_model = excluded.requested_model,
                used_model = excluded.used_model,
                attempts_json = excluded.attempts_json,
                role_runs_json = excluded.role_runs_json,
                aggregator_run_json = excluded.aggregator_run_json,
                decision_state_json = excluded.decision_state_json,
                warnings_json = excluded.warnings_json,
                error = excluded.error,
                analysis_markdown = excluded.analysis_markdown,
                updated_at = excluded.updated_at,
                finished_at = excluded.finished_at
            """,
            (
                str(job.get("job_id") or ""),
                "v2",
                str(job.get("status") or ""),
                str(job.get("ts_code") or ""),
                str(job.get("name") or ""),
                int(job.get("lookback") or 120),
                json.dumps(_sanitize_json_value(job.get("roles") or []), ensure_ascii=False, allow_nan=False),
                1 if bool(job.get("accept_auto_degrade", True)) else 0,
                str(job.get("requested_model") or ""),
                str(job.get("used_model") or ""),
                json.dumps(_sanitize_json_value(job.get("attempts") or []), ensure_ascii=False, allow_nan=False),
                json.dumps(_sanitize_json_value(job.get("role_runs") or []), ensure_ascii=False, allow_nan=False),
                json.dumps(_sanitize_json_value(job.get("aggregator_run") or {}), ensure_ascii=False, allow_nan=False),
                json.dumps(_sanitize_json_value(job.get("decision_state") or {}), ensure_ascii=False, allow_nan=False),
                json.dumps(_sanitize_json_value(job.get("warnings") or []), ensure_ascii=False, allow_nan=False),
                str(job.get("error") or ""),
                str(job.get("analysis_markdown") or ""),
                str(job.get("created_at") or datetime.now(timezone.utc).isoformat()),
                str(job.get("updated_at") or datetime.now(timezone.utc).isoformat()),
                str(job.get("finished_at") or ""),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _parse_iso_dt(value: str):
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _cn_day_utc_range(now_utc: datetime | None = None) -> tuple[datetime, datetime]:
    now_utc = now_utc or datetime.now(timezone.utc)
    cn_tz = timezone(timedelta(hours=8))
    cn_now = now_utc.astimezone(cn_tz)
    start_cn = cn_now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_cn = start_cn + timedelta(days=1)
    return start_cn.astimezone(timezone.utc), end_cn.astimezone(timezone.utc)


def _hydrate_persisted_multi_role_v2_row(row: dict) -> dict:
    def _loads(value, default):
        try:
            parsed = json.loads(value or "")
            return parsed if isinstance(parsed, type(default)) else default
        except Exception:
            return default

    return {
        "job_id": str(row.get("job_id") or ""),
        "status": str(row.get("status") or ""),
        "progress": 100 if str(row.get("status") or "") in {"done", "done_with_warnings", "error"} else 0,
        "stage": "done" if str(row.get("status") or "") in {"done", "done_with_warnings"} else str(row.get("status") or ""),
        "message": "复用当日已完成分析结果" if str(row.get("status") or "") in {"done", "done_with_warnings"} else str(row.get("status") or ""),
        "created_at": str(row.get("created_at") or ""),
        "updated_at": str(row.get("updated_at") or ""),
        "finished_at": str(row.get("finished_at") or ""),
        "ts_code": str(row.get("ts_code") or ""),
        "name": str(row.get("name") or ""),
        "lookback": int(row.get("lookback") or 120),
        "roles": _loads(row.get("roles_json"), []),
        "accept_auto_degrade": bool(row.get("accept_auto_degrade")),
        "requested_model": str(row.get("requested_model") or ""),
        "used_model": str(row.get("used_model") or ""),
        "attempts": _loads(row.get("attempts_json"), []),
        "role_runs": _loads(row.get("role_runs_json"), []),
        "aggregator_run": _loads(row.get("aggregator_run_json"), {}),
        "decision_state": _loads(row.get("decision_state_json"), {}),
        "warnings": _loads(row.get("warnings_json"), []),
        "error": str(row.get("error") or ""),
        "analysis": str(row.get("analysis_markdown") or ""),
        "analysis_markdown": str(row.get("analysis_markdown") or ""),
        "role_outputs": [],
        "role_sections": [],
        "common_sections_markdown": "",
        "decision_confidence": infer_decision_confidence(str(row.get("analysis_markdown") or "")).to_dict(),
        "risk_review": build_risk_review(str(row.get("analysis_markdown") or "")).to_dict(),
        "portfolio_view": build_portfolio_view(str(row.get("analysis_markdown") or "")).to_dict(),
        "used_context_dims": [],
        "context_build_ms": 0,
        "role_parallel_ms": 0,
        "total_ms": 0,
        "queue_position": 0,
        "queue_total": 0,
        "max_concurrent_jobs": MULTI_ROLE_V2_MAX_CONCURRENT_JOBS,
        "current_concurrent_jobs": 0,
        "queue_length": 0,
        "context": {},
    }


def _load_persisted_multi_role_v2_job(job_id: str):
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        ensure_multi_role_analysis_history_table(conn)
        row = conn.execute(
            """
            SELECT
              job_id, status, ts_code, name, lookback, roles_json, accept_auto_degrade,
              requested_model, used_model, attempts_json, role_runs_json, aggregator_run_json,
              decision_state_json, warnings_json, error, analysis_markdown, created_at, updated_at, finished_at
            FROM multi_role_analysis_history
            WHERE job_id = ? AND version = 'v2'
            LIMIT 1
            """,
            (str(job_id or "").strip(),),
        ).fetchone()
        if not row:
            return None
        return _hydrate_persisted_multi_role_v2_row(dict(row))
    finally:
        conn.close()


def find_today_reusable_multi_role_v2_job(*, ts_code: str, lookback: int, roles: list[str]):
    ts_code = str(ts_code or "").strip().upper()
    target_roles = [str(x).strip() for x in list(roles or []) if str(x).strip()]
    target_roles_sorted = sorted(target_roles)
    start_utc, end_utc = _cn_day_utc_range()
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        ensure_multi_role_analysis_history_table(conn)
        rows = conn.execute(
            """
            SELECT
              job_id, status, ts_code, name, lookback, roles_json, accept_auto_degrade,
              requested_model, used_model, attempts_json, role_runs_json, aggregator_run_json,
              decision_state_json, warnings_json, error, analysis_markdown, created_at, updated_at, finished_at
            FROM multi_role_analysis_history
            WHERE version = 'v2'
              AND ts_code = ?
              AND lookback = ?
              AND status IN ('done', 'done_with_warnings')
            ORDER BY id DESC
            LIMIT 100
            """,
            (ts_code, int(lookback or 120)),
        ).fetchall()
        for row_obj in rows:
            row = dict(row_obj)
            # 复用口径优先使用完成时间，避免“前一日创建、当日完成”的任务漏命中。
            anchor_dt = (
                _parse_iso_dt(str(row.get("finished_at") or ""))
                or _parse_iso_dt(str(row.get("updated_at") or ""))
                or _parse_iso_dt(str(row.get("created_at") or ""))
            )
            if not anchor_dt:
                continue
            if anchor_dt.tzinfo is None:
                anchor_dt = anchor_dt.replace(tzinfo=timezone.utc)
            anchor_utc = anchor_dt.astimezone(timezone.utc)
            if anchor_utc < start_utc or anchor_utc >= end_utc:
                continue
            try:
                row_roles = json.loads(row.get("roles_json") or "[]")
                if not isinstance(row_roles, list):
                    row_roles = []
            except Exception:
                row_roles = []
            row_roles_sorted = sorted([str(x).strip() for x in row_roles if str(x).strip()])
            if row_roles_sorted != target_roles_sorted:
                continue
            analysis_markdown = str(row.get("analysis_markdown") or "").strip()
            if not analysis_markdown:
                continue
            return _hydrate_persisted_multi_role_v2_row(row)
        return None
    finally:
        conn.close()


def _logic_view_content_hash(source_payload) -> str:
    if isinstance(source_payload, str):
        raw = source_payload
    else:
        raw = json.dumps(
            _sanitize_json_value(source_payload),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
        )
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


def _load_cached_logic_view(conn, entity_type: str, entity_key: str, content_hash: str):
    ensure_logic_view_cache_table(conn)
    row = conn.execute(
        """
        SELECT logic_view_json
        FROM logic_view_cache
        WHERE entity_type = ? AND entity_key = ? AND content_hash = ?
        LIMIT 1
        """,
        (entity_type, entity_key, content_hash),
    ).fetchone()
    if not row:
        return None
    try:
        obj = json.loads(row[0] if not isinstance(row, dict) else row.get("logic_view_json", "{}"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _save_cached_logic_view(conn, entity_type: str, entity_key: str, content_hash: str, logic_view: dict):
    ensure_logic_view_cache_table(conn)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = json.dumps(_sanitize_json_value(logic_view), ensure_ascii=False, allow_nan=False)
    updated = conn.execute(
        """
        UPDATE logic_view_cache
        SET logic_view_json = ?, update_time = ?
        WHERE entity_type = ? AND entity_key = ? AND content_hash = ?
        """,
        (payload, now, entity_type, entity_key, content_hash),
    ).rowcount
    if not updated:
        conn.execute(
            """
            INSERT INTO logic_view_cache (
                entity_type, entity_key, content_hash, logic_view_json, created_at, update_time
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (entity_type, entity_key, content_hash, payload, now, now),
        )
    conn.commit()


def get_or_build_cached_logic_view(conn, entity_type: str, entity_key: str, source_payload, builder):
    content_hash = _logic_view_content_hash(source_payload)
    cached = _load_cached_logic_view(conn, entity_type, entity_key, content_hash)
    if cached is not None:
        return cached
    logic_view = builder()
    _save_cached_logic_view(conn, entity_type, entity_key, content_hash, logic_view)
    return logic_view


def _clean_logic_line(line: str) -> str:
    text = str(line or "")
    text = re.sub(r"^\s*[-*]\s*", "", text)
    text = re.sub(r"^\s*\d+\.\s*", "", text)
    text = re.sub(r"^\s*>\s*", "", text)
    return text.strip()


def _strip_markdown_emphasis(text: str) -> str:
    return str(text or "").replace("**", "").strip()


def _normalize_logic_chain_text(text: str) -> str:
    cleaned = _clean_logic_line(text)
    cleaned = re.sub(r'[“”"]', "", cleaned)
    cleaned = re.sub(r"\s*[-=]*>\s*", " -> ", cleaned)
    cleaned = re.sub(r"\s*→\s*", " -> ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def _split_logic_nodes(text: str) -> list[str]:
    return [
        _strip_markdown_emphasis(x).replace("事件发生", "", 1).strip()
        for x in re.split(r"\s*->\s*", _normalize_logic_chain_text(text))
        if _strip_markdown_emphasis(x).strip()
    ]


def _parse_markdown_headline(line: str) -> str:
    m = re.match(r"^\d+\.\s*\*\*(.+?)\*\*", str(line or "").strip())
    return str(m.group(1)).strip() if m else ""


def extract_logic_view_from_markdown(markdown: str) -> dict:
    text = _normalize_markdown_lines(markdown).strip()
    lines = text.splitlines()
    summary = {"conclusion": "", "focus": "", "risk": ""}
    chains: list[dict] = []
    current_title = ""
    capture_next_chain = False

    for raw in lines:
        line = str(raw or "").strip()
        if not line:
            if capture_next_chain:
                capture_next_chain = False
            continue

        maybe_title = _parse_markdown_headline(line)
        if maybe_title:
            current_title = maybe_title

        plain = _strip_markdown_emphasis(_clean_logic_line(line))
        if re.match(r"^(核心结论|结论)[:：]", plain):
            summary["conclusion"] = re.sub(r"^(核心结论|结论)[:：]\s*", "", plain).strip()
        elif re.match(r"^(最值得关注的方向|关注方向)[:：]", plain):
            summary["focus"] = re.sub(r"^(最值得关注的方向|关注方向)[:：]\s*", "", plain).strip()
        elif re.match(r"^(当前最需要警惕的风险|风险提示)[:：]", plain):
            summary["risk"] = re.sub(r"^(当前最需要警惕的风险|风险提示)[:：]\s*", "", plain).strip()

        if re.search(r"(影响传导路径|传导路径|逻辑链条)[:：]", plain):
            inline = re.sub(r"^(影响传导路径|传导路径|逻辑链条)[:：]\s*", "", plain).strip()
            if inline:
                chains.append(
                    {
                        "title": current_title or f"链路 {len(chains) + 1}",
                        "raw": inline,
                        "nodes": _split_logic_nodes(inline),
                    }
                )
                capture_next_chain = False
            else:
                capture_next_chain = True
            continue

        if capture_next_chain:
            candidate = _clean_logic_line(line)
            plain_candidate = _strip_markdown_emphasis(candidate)
            if (
                not candidate
                or re.match(r"^(确定性判断|可能受影响的市场)[:：]", plain_candidate)
            ):
                capture_next_chain = False
                continue
            chains.append(
                {
                    "title": current_title or f"链路 {len(chains) + 1}",
                    "raw": candidate,
                    "nodes": _split_logic_nodes(candidate),
                }
            )
            capture_next_chain = False

    normalized_chains = [
        {
            "title": str(item.get("title") or f"链路 {idx + 1}").strip(),
            "raw": str(item.get("raw") or "").strip(),
            "nodes": [str(x).strip() for x in item.get("nodes", []) if str(x).strip()],
        }
        for idx, item in enumerate(chains)
        if len(item.get("nodes", []) if isinstance(item, dict) else []) >= 2
    ][:8]
    return {
        "summary": summary,
        "chains": normalized_chains,
        "has_logic": bool(
            normalized_chains
            or summary.get("conclusion")
            or summary.get("focus")
            or summary.get("risk")
        ),
    }


def build_signal_logic_view(signal_row: dict | None) -> dict:
    if not signal_row:
        return {"summary": {}, "chains": [], "has_logic": False, "evidence_chain": []}
    source_summary = {}
    try:
        obj = json.loads(signal_row.get("source_summary_json") or "{}")
        source_summary = obj if isinstance(obj, dict) else {}
    except Exception:
        source_summary = {}
    evidence_items = []
    try:
        obj = json.loads(signal_row.get("evidence_json") or "[]")
        evidence_items = obj if isinstance(obj, list) else []
    except Exception:
        evidence_items = []

    source_nodes = []
    label_map = {
        "intl_news": "国际新闻",
        "domestic_news": "国内新闻",
        "stock_news": "个股新闻",
        "chatroom": "群聊",
        "theme_mapping": "主题映射",
    }
    for key, label in label_map.items():
        count = int(source_summary.get(key, 0) or 0)
        if count > 0:
            source_nodes.append(f"{label}({count})")

    evidence_titles = []
    evidence_chain = []
    for item in evidence_items[:5]:
        if not isinstance(item, dict):
            continue
        label = str(
            item.get("title")
            or item.get("theme_name")
            or item.get("group")
            or item.get("source")
            or ""
        ).strip()
        if label:
            evidence_titles.append(label)
            evidence_chain.append(
                {
                    "label": label,
                    "source": str(item.get("source") or item.get("driver_type") or "").strip(),
                    "direction": str(item.get("direction") or "").strip(),
                    "date": str(item.get("date") or item.get("pub_date") or "").strip(),
                }
            )

    summary = {
        "conclusion": f"{signal_row.get('subject_name') or ''} 当前方向为 {signal_row.get('direction') or '-'}，状态 {signal_row.get('signal_status') or '-'}",
        "focus": f"强度 {signal_row.get('signal_strength') or 0} / 置信度 {signal_row.get('confidence') or 0}%",
        "risk": "若证据源减少、方向反转或置信度显著回落，需要重新评估信号有效性",
    }
    chain_nodes = [
        str(signal_row.get("subject_name") or signal_row.get("signal_key") or "").strip(),
        *source_nodes[:3],
        *(evidence_titles[:2] if evidence_titles else []),
        f"{signal_row.get('direction') or '-'} / {signal_row.get('signal_status') or '-'}",
    ]
    chain_nodes = [x for x in chain_nodes if x]
    chains = []
    if len(chain_nodes) >= 2:
        chains.append(
            {
                "title": "当前信号形成链路",
                "raw": " -> ".join(chain_nodes),
                "nodes": chain_nodes,
            }
        )
    return {
        "summary": summary,
        "chains": chains,
        "has_logic": bool(chains),
        "evidence_chain": evidence_chain,
    }


def build_signal_event_logic_view(event_row: dict) -> dict:
    event = dict(event_row or {})
    try:
        evidence_items = json.loads(event.get("evidence_json") or "[]")
        if not isinstance(evidence_items, list):
            evidence_items = []
    except Exception:
        evidence_items = []
    driver_type_map = {
        "intl_news": "国际新闻",
        "domestic_news": "国内新闻",
        "news": "新闻",
        "stock_news": "个股新闻",
        "chatroom": "群聊",
        "price": "价格",
        "mixed": "混合驱动",
    }
    driver_label = driver_type_map.get(str(event.get("driver_type") or "").strip(), str(event.get("driver_type") or "").strip() or "未知驱动")
    evidence_chain = []
    evidence_titles = []
    for item in evidence_items[:5]:
        if not isinstance(item, dict):
            continue
        label = str(
            item.get("title")
            or item.get("theme_name")
            or item.get("group")
            or item.get("source")
            or ""
        ).strip()
        if label:
            evidence_titles.append(label)
            evidence_chain.append(
                {
                    "label": label,
                    "source": str(item.get("source") or "").strip(),
                    "direction": str(item.get("direction") or "").strip(),
                    "date": str(item.get("date") or "").strip(),
                }
            )
    summary = {
        "conclusion": str(event.get("event_summary") or display_name_from_event_type(event.get("event_type"))).strip(),
        "focus": f"{event.get('old_direction') or '-'} -> {event.get('new_direction') or '-'} | 强度 Δ {event.get('delta_strength') or 0}",
        "risk": f"事件后状态 {event.get('status_after_event') or '-'}，需持续跟踪后续证据是否延续",
    }
    chain_nodes = [
        driver_label,
        str(event.get("driver_source") or "").strip(),
        *(evidence_titles[:2] if evidence_titles else []),
        str(event.get("event_summary") or "").strip(),
        f"{event.get('new_direction') or '-'} / {event.get('status_after_event') or '-'}",
    ]
    chain_nodes = [x for x in chain_nodes if x]
    chains = []
    if len(chain_nodes) >= 2:
        chains.append(
            {
                "title": "事件驱动链路",
                "raw": " -> ".join(chain_nodes),
                "nodes": chain_nodes,
            }
        )
    return {
        "summary": summary,
        "chains": chains,
        "has_logic": bool(chains),
        "evidence_chain": evidence_chain,
    }


def display_name_from_event_type(event_type: str) -> str:
    mapping = {
        "new_signal": "新信号",
        "strengthen": "信号增强",
        "weaken": "信号减弱",
        "flip": "方向反转",
        "falsify": "原判断被证伪",
        "revive": "信号恢复",
        "expire": "信号失效",
        "status_change": "状态变化",
    }
    return mapping.get(str(event_type or "").strip(), str(event_type or "").strip())


def split_multi_role_analysis(markdown: str, roles: list[str]) -> dict:
    return agent_split_multi_role_analysis(
        extract_logic_view_from_markdown=extract_logic_view_from_markdown,
        normalize_markdown_lines=_normalize_markdown_lines,
        build_common_sections_markdown=_build_common_sections_markdown,
        find_section_start=_find_section_start,
        markdown=markdown,
        roles=roles,
    )

__all__ = ['_extract_llm_result_marker', 'build_trend_features', '_parse_json_text', '_round_or_none', '_build_price_rollups_summary', '_build_event_summary', '_stock_news_latest_pub', '_stock_news_is_fresh', '_ensure_stock_news_fresh', '_load_strategy_template_for', '_notify_result', 'call_llm_trend', '_resolve_roles', 'build_multi_role_context', 'build_multi_role_prompt', 'call_llm_multi_role', '_normalize_markdown_lines', '_normalize_section_heading', '_find_section_start', '_build_common_sections_markdown', 'ensure_logic_view_cache_table', 'ensure_multi_role_analysis_history_table', '_persist_multi_role_analysis_v2_job', '_parse_iso_dt', '_cn_day_utc_range', '_hydrate_persisted_multi_role_v2_row', '_load_persisted_multi_role_v2_job', 'find_today_reusable_multi_role_v2_job', '_logic_view_content_hash', '_load_cached_logic_view', '_save_cached_logic_view', 'get_or_build_cached_logic_view', '_clean_logic_line', '_strip_markdown_emphasis', '_normalize_logic_chain_text', '_split_logic_nodes', '_parse_markdown_headline', 'extract_logic_view_from_markdown', 'build_signal_logic_view', 'build_signal_event_logic_view', 'display_name_from_event_type', 'split_multi_role_analysis']
