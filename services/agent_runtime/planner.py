"""Optional tool-sequence planner for Agent staging (stub by default; LLM gated)."""

from __future__ import annotations

import json
import os
from typing import Any


def planner_mode() -> str:
    return str(os.getenv("AGENT_PLANNER_MODE", "stub") or "stub").strip().lower()


def build_stub_plan(*, goal: dict[str, Any], closure_summary: dict[str, Any] | None) -> dict[str, Any]:
    gaps = list((closure_summary or {}).get("gaps") or [])
    steps: list[dict[str, Any]] = [
        {"tool_name": "system.health_snapshot", "arguments": {}, "rationale": "baseline_health"},
        {"tool_name": "business.closure_gap_scan", "arguments": {}, "rationale": "closure_gaps"},
    ]
    if "funnel_ingested_backlog" in gaps or bool(goal.get("force_score_align")):
        steps.append(
            {
                "tool_name": "business.repair_funnel_score_align",
                "arguments": {
                    "score_date": str(goal.get("score_date") or ""),
                    "max_candidates": int(goal.get("max_candidates") or 10000),
                },
                "rationale": "funnel_ingested_backlog_or_goal",
            }
        )
    if bool(goal.get("force_review_refresh")) or any("review" in str(g) for g in gaps):
        steps.append(
            {
                "tool_name": "business.repair_funnel_review_refresh",
                "arguments": {"horizon_days": int(goal.get("horizon_days") or 5), "limit": int(goal.get("review_limit") or 200)},
                "rationale": "review_gap_or_goal",
            }
        )
    return {"mode": "stub", "ok": True, "steps": steps, "gap_count": len(gaps)}


def _plan_via_llm(*, goal: dict[str, Any], closure_summary: dict[str, Any] | None) -> dict[str, Any]:
    try:
        from llm_gateway import chat_completion_with_fallback
    except Exception as exc:  # pragma: no cover
        return {"mode": "llm", "ok": False, "error": f"llm_import_failed:{exc}", "steps": []}

    allowed = {
        "system.health_snapshot",
        "business.closure_gap_scan",
        "jobs.list_alerts",
        "jobs.list_runs",
    }
    prompt = (
        "You output JSON only: {\"steps\":[{\"tool_name\":...,\"arguments\":{}}]} "
        "where tool_name must be one of: "
        + ", ".join(sorted(allowed))
        + ". Max 5 steps. Goal="
        + json.dumps(goal, ensure_ascii=False)
        + " closure="
        + json.dumps(closure_summary or {}, ensure_ascii=False)
    )
    raw = chat_completion_with_fallback(
        model="auto",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
        timeout_s=60,
    )
    text = str(getattr(raw, "text", "") or "").strip()
    try:
        obj = json.loads(text)
    except Exception:
        m = __import__("re").search(r"(\{[\s\S]*\})", text)
        if not m:
            return {"mode": "llm", "ok": False, "error": "planner_json_parse_failed", "steps": []}
        obj = json.loads(m.group(1))
    steps_in = obj.get("steps") if isinstance(obj.get("steps"), list) else []
    steps: list[dict[str, Any]] = []
    for item in steps_in[:5]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("tool_name") or "").strip()
        if name not in allowed:
            continue
        args = item.get("arguments") if isinstance(item.get("arguments"), dict) else {}
        steps.append({"tool_name": name, "arguments": args, "rationale": str(item.get("rationale") or "llm")})
    return {"mode": "llm", "ok": True, "steps": steps, "raw_model": getattr(raw, "used_model", "")}


def build_planner_preview(*, goal: dict[str, Any], closure_summary: dict[str, Any] | None) -> dict[str, Any]:
    mode = planner_mode()
    if mode == "llm":
        out = _plan_via_llm(goal=goal, closure_summary=closure_summary)
        out.setdefault("staging_metric", "llm_plan_len")
        out["len"] = len(out.get("steps") or [])
        return out
    out = build_stub_plan(goal=goal, closure_summary=closure_summary)
    out["staging_metric"] = "stub_plan_len"
    out["len"] = len(out.get("steps") or [])
    return out
