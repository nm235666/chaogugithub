"""
首席圆桌 v1 — Chief Roundtable

Three chief personas (成长派 / 价值派 / 宏观派) debate a stock in sequence,
then a moderator synthesises a consensus verdict.

Stages: queued → context → chiefs → synthesis → done | error

Usage (from server.py):
    from services.agent_service.chief_roundtable_v1 import (
        ensure_roundtable_tables,
        create_roundtable_job,
        get_roundtable_job,
        list_roundtable_jobs,
        process_one_roundtable_job,
        run_roundtable_worker_loop,
    )
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import db_compat as sqlite3
from llm_gateway import chat_completion_text

_TABLE = "chief_roundtable_jobs"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _loads(text: str, default: Any = None) -> Any:
    try:
        return json.loads(text or "") if text else default
    except Exception:
        return default


def _dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def _extract_json(text: str) -> dict[str, Any]:
    """Extract first JSON object from LLM response text."""
    start = text.find("{")
    end = text.rfind("}") + 1
    if start < 0 or end <= start:
        return {}
    try:
        return json.loads(text[start:end])
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# DB schema
# ---------------------------------------------------------------------------

def ensure_roundtable_tables(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_TABLE} (
            id BIGSERIAL PRIMARY KEY,
            job_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'queued',
            stage TEXT NOT NULL DEFAULT '',
            ts_code TEXT NOT NULL,
            trigger TEXT NOT NULL DEFAULT 'manual',
            source_job_id TEXT NOT NULL DEFAULT '',
            context_json TEXT NOT NULL DEFAULT '{{}}',
            positions_json TEXT NOT NULL DEFAULT '{{}}',
            synthesis_json TEXT NOT NULL DEFAULT '{{}}',
            error TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            finished_at TEXT NOT NULL DEFAULT '',
            worker_id TEXT NOT NULL DEFAULT '',
            owner TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_roundtable_jobs_status ON {_TABLE}(status, updated_at)"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_roundtable_jobs_ts_code ON {_TABLE}(ts_code, created_at)"
    )
    # Compat: add owner column to tables created before this column existed
    has_owner = conn.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = 'owner'",
        (_TABLE,),
    ).fetchone()
    if not has_owner:
        conn.execute(f"ALTER TABLE {_TABLE} ADD COLUMN owner TEXT NOT NULL DEFAULT ''")
    # Compat: add lease_until column to tables created before this column existed
    has_lease_until = conn.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = 'lease_until'",
        (_TABLE,),
    ).fetchone()
    if not has_lease_until:
        conn.execute(f"ALTER TABLE {_TABLE} ADD COLUMN lease_until TEXT NOT NULL DEFAULT ''")
    conn.commit()


def _connect(sqlite3_module=None):
    module = sqlite3_module or sqlite3
    return module.connect()


def reset_stale_roundtable_jobs(conn) -> None:
    """Reset running jobs whose lease has expired back to queued."""
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        f"""
        UPDATE {_TABLE}
           SET status = 'queued', worker_id = '', lease_until = ''
         WHERE status = 'running'
           AND lease_until != ''
           AND lease_until < ?
        """,
        (now_iso,),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Job CRUD
# ---------------------------------------------------------------------------

def create_roundtable_job(
    *,
    sqlite3_module=sqlite3,
    ts_code: str,
    trigger: str = "manual",
    source_job_id: str = "",
    owner: str = "",
) -> dict[str, Any]:
    ts_code = str(ts_code or "").strip().upper()
    if not ts_code:
        raise ValueError("ts_code 不能为空")
    conn = _connect(sqlite3_module)
    conn.row_factory = sqlite3_module.Row
    try:
        ensure_roundtable_tables(conn)
        job_id = str(uuid.uuid4())
        now = _now()
        conn.execute(
            f"""
            INSERT INTO {_TABLE}
                (job_id, status, stage, ts_code, trigger, source_job_id,
                 context_json, positions_json, synthesis_json, created_at, updated_at, owner)
            VALUES (?, 'queued', '', ?, ?, ?, '{{}}', '{{}}', '{{}}', ?, ?, ?)
            """,
            (job_id, ts_code, trigger or "manual", source_job_id or "", now, now, owner or ""),
        )
        conn.commit()
        return {"job_id": job_id, "status": "queued", "ts_code": ts_code, "trigger": trigger}
    finally:
        conn.close()


def _hydrate(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "job_id": str(row.get("job_id") or ""),
        "status": str(row.get("status") or ""),
        "stage": str(row.get("stage") or ""),
        "ts_code": str(row.get("ts_code") or ""),
        "trigger": str(row.get("trigger") or "manual"),
        "source_job_id": str(row.get("source_job_id") or ""),
        "context": _loads(str(row.get("context_json") or ""), {}),
        "positions": _loads(str(row.get("positions_json") or ""), {}),
        "synthesis": _loads(str(row.get("synthesis_json") or ""), {}),
        "error": str(row.get("error") or ""),
        "created_at": str(row.get("created_at") or ""),
        "updated_at": str(row.get("updated_at") or ""),
        "finished_at": str(row.get("finished_at") or ""),
        "owner": str(row.get("owner") or ""),
    }


def get_roundtable_job(*, sqlite3_module=sqlite3, job_id: str) -> dict[str, Any] | None:
    conn = _connect(sqlite3_module)
    conn.row_factory = sqlite3_module.Row
    try:
        ensure_roundtable_tables(conn)
        row = conn.execute(f"SELECT * FROM {_TABLE} WHERE job_id = ?", (job_id,)).fetchone()
        return _hydrate(dict(row)) if row else None
    finally:
        conn.close()


def list_roundtable_jobs(
    *, sqlite3_module=sqlite3, ts_code: str = "", owner: str = "", page: int = 1, page_size: int = 20
) -> dict[str, Any]:
    conn = _connect(sqlite3_module)
    conn.row_factory = sqlite3_module.Row
    try:
        ensure_roundtable_tables(conn)
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        offset = (page - 1) * page_size
        where = []
        params: list[Any] = []
        if ts_code:
            where.append("ts_code = ?")
            params.append(str(ts_code).strip().upper())
        if owner:
            where.append("owner = ?")
            params.append(str(owner))
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        total = int(
            conn.execute(f"SELECT COUNT(*) FROM {_TABLE} {where_sql}", tuple(params)).fetchone()[0] or 0
        )
        rows = conn.execute(
            f"SELECT * FROM {_TABLE} {where_sql} ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?",
            (*params, page_size, offset),
        ).fetchall()
        return {
            "page": page, "page_size": page_size, "total": total,
            "total_pages": (total + page_size - 1) // page_size if total else 0,
            "items": [_hydrate(dict(r)) for r in rows],
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# LLM helpers
# ---------------------------------------------------------------------------

_VALID_POSITIONS = {"bullish", "bearish", "neutral"}

def _chief_prompt(
    *,
    persona: str,
    philosophy: str,
    ts_code: str,
    stock_name: str,
    context_brief: str,
    prior_positions: list[dict[str, Any]],
) -> str:
    prior_text = ""
    if prior_positions:
        lines = []
        for p in prior_positions:
            lines.append(f"  · {p['name']}（{p['philosophy_short']}）：{p['position']} — {p['argument']}")
        prior_text = "\n\n其他首席已给出的立场：\n" + "\n".join(lines)

    return f"""你是{persona}，是一位专注于{philosophy}的顶级首席分析师。
{context_brief}{prior_text}

请对股票 {stock_name}（{ts_code}）给出你的投资立场。
你的立场必须明确，不允许"两方面都有道理"式的中庸表述。
如果数据不足，基于行业通用规律做出判断，并在 concern 中说明。

请严格返回以下 JSON 格式，不要有任何额外说明：
{{
  "position": "bullish|bearish|neutral（必须三选一，neutral 仅在真正无法判断时使用）",
  "confidence": 整数0-100,
  "argument": "一句话的核心押注，不超过80字",
  "concern": "最主要的风险或反驳观点，不超过60字"
}}"""


def _synthesis_prompt(
    *,
    ts_code: str,
    stock_name: str,
    positions: dict[str, Any],
) -> str:
    lines = []
    for key, p in positions.items():
        lines.append(
            f"  · {p.get('name', key)}（{p.get('philosophy_short', '')}）："
            f"立场={p.get('position','?')} 置信={p.get('confidence','?')} "
            f"核心论点={p.get('argument','?')} 顾虑={p.get('concern','?')}"
        )
    positions_text = "\n".join(lines)

    return f"""你是首席圆桌主持人，负责综合三位首席的立场，形成最终裁决。
你的职责是裁决，不是平衡。如果有明确多数观点，直接采纳；如果存在根本分歧，明确标注。

三位首席关于 {stock_name}（{ts_code}）的立场：
{positions_text}

请严格返回以下 JSON 格式，不要有任何额外说明：
{{
  "consensus": "agree（一致）|split（分歧）|oppose（对立）",
  "direction": "bullish|bearish|neutral",
  "verdict": "一句话最终裁决，不超过80字，禁止"一方面…另一方面"句式",
  "majority_argument": "主流观点的核心逻辑，不超过80字",
  "dissent": "少数/反方观点，不超过60字（若一致则填 '无明显分歧'）"
}}"""


_CHIEF_PERSONAS = [
    {
        "key": "growth",
        "name": "成长派首席",
        "philosophy": "盈利增长趋势、商业模式护城河与行业扩张空间",
        "philosophy_short": "成长/动量",
    },
    {
        "key": "value",
        "name": "价值派首席",
        "philosophy": "估值安全边际、分红能力与资产质量",
        "philosophy_short": "价值/防御",
    },
    {
        "key": "macro",
        "name": "宏观策略首席",
        "philosophy": "政策环境、行业周期位置与资金流向",
        "philosophy_short": "宏观/周期",
    },
]


def _call_llm(model: str, messages: list[dict]) -> str:
    """Call LLM with the configured model."""
    return chat_completion_text(
        model=model,
        messages=messages,
        temperature=0.3,
        timeout_s=60,
        max_retries=2,
    )


def _default_llm_model() -> str:
    """Pick default model from env, falling back to deepseek-chat."""
    return os.getenv("ROUNDTABLE_MODEL", os.getenv("DEFAULT_LLM_MODEL", "deepseek-chat"))


# ---------------------------------------------------------------------------
# Worker: process one job
# ---------------------------------------------------------------------------

def _save_job(conn, *, job_id: str, status: str, stage: str, **fields) -> None:
    sets = ["status = ?", "stage = ?", "updated_at = ?"]
    vals: list[Any] = [status, stage, _now()]
    if "context_json" in fields:
        sets.append("context_json = ?")
        vals.append(fields["context_json"])
    if "positions_json" in fields:
        sets.append("positions_json = ?")
        vals.append(fields["positions_json"])
    if "synthesis_json" in fields:
        sets.append("synthesis_json = ?")
        vals.append(fields["synthesis_json"])
    if "error" in fields:
        sets.append("error = ?")
        vals.append(fields["error"])
    if "finished_at" in fields:
        sets.append("finished_at = ?")
        vals.append(fields["finished_at"])
    if "worker_id" in fields:
        sets.append("worker_id = ?")
        vals.append(fields["worker_id"])
    vals.append(job_id)
    conn.execute(f"UPDATE {_TABLE} SET {', '.join(sets)} WHERE job_id = ?", vals)
    conn.commit()


def process_one_roundtable_job(
    *, sqlite3_module=sqlite3, model: str = ""
) -> bool:
    """Claim and process the oldest queued roundtable job. Returns True if a job was handled."""
    worker_id = f"rtw-{os.getpid()}-{uuid.uuid4().hex[:6]}"
    model = model or _default_llm_model()

    conn = _connect(sqlite3_module)
    conn.row_factory = sqlite3_module.Row
    try:
        ensure_roundtable_tables(conn)
        reset_stale_roundtable_jobs(conn)
        # Atomic claim: UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED)
        # prevents multiple workers from picking the same job.
        lease_until = (datetime.now(timezone.utc) + timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
        row = conn.execute(
            f"""
            UPDATE {_TABLE}
               SET status = 'running', stage = 'context', worker_id = ?, lease_until = ?
             WHERE job_id = (
                   SELECT job_id FROM {_TABLE}
                    WHERE status = 'queued'
                    ORDER BY id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
             )
            RETURNING job_id, ts_code, source_job_id
            """,
            (worker_id, lease_until),
        ).fetchone()
        conn.commit()
        if not row:
            return False

        job_id = str(row["job_id"])
        ts_code = str(row["ts_code"])
        source_job_id = str(row["source_job_id"] or "")
    finally:
        conn.close()

    # --- outside the initial connection to avoid long-held locks ---
    try:
        _execute_job(
            sqlite3_module=sqlite3_module,
            job_id=job_id,
            ts_code=ts_code,
            source_job_id=source_job_id,
            model=model,
        )
    except Exception as exc:
        conn2 = _connect(sqlite3_module)
        try:
            _save_job(conn2, job_id=job_id, status="error", stage="error",
                      error=str(exc), finished_at=_now())
        finally:
            conn2.close()
    return True


def _get_stock_name(sqlite3_module, ts_code: str) -> str:
    """Fetch stock display name from stock_codes table."""
    try:
        conn = _connect(sqlite3_module)
        try:
            row = conn.execute(
                "SELECT name FROM stock_codes WHERE ts_code = ? LIMIT 1", (ts_code,)
            ).fetchone()
            return str(row[0]) if row and row[0] else ts_code
        finally:
            conn.close()
    except Exception:
        return ts_code


def _get_source_analysis(sqlite3_module, source_job_id: str) -> str:
    """Fetch summary text from a completed multi_role_v3 job if available."""
    if not source_job_id:
        return ""
    try:
        conn = _connect(sqlite3_module)
        conn.row_factory = sqlite3_module.Row
        try:
            row = conn.execute(
                "SELECT result_json FROM multi_role_v3_jobs WHERE job_id = ? LIMIT 1",
                (source_job_id,),
            ).fetchone()
            if not row:
                return ""
            result = _loads(str(row["result_json"] or ""), {})
            markdown = str(result.get("common_sections_markdown") or "").strip()
            if markdown:
                # Truncate to ~2000 chars to fit context window
                return markdown[:2000] + ("..." if len(markdown) > 2000 else "")
        finally:
            conn.close()
    except Exception:
        pass
    return ""


def _execute_job(
    *, sqlite3_module, job_id: str, ts_code: str,
    source_job_id: str, model: str
) -> None:
    stock_name = _get_stock_name(sqlite3_module, ts_code)
    source_analysis = _get_source_analysis(sqlite3_module, source_job_id)

    context_brief = f"当前研究标的：{stock_name}（{ts_code}）。"
    if source_analysis:
        context_brief += f"\n\n以下是来自多角色分析系统的研究摘要（可作参考，但你应基于自己的投资框架独立判断）：\n{source_analysis}"

    context = {"stock_name": stock_name, "ts_code": ts_code, "has_source_analysis": bool(source_analysis)}

    # Persist context
    conn = _connect(sqlite3_module)
    try:
        _save_job(conn, job_id=job_id, status="running", stage="chiefs",
                  context_json=_dumps(context))
    finally:
        conn.close()

    # --- Stage: chiefs ---
    positions: dict[str, Any] = {}
    prior: list[dict[str, Any]] = []

    for persona in _CHIEF_PERSONAS:
        prompt_text = _chief_prompt(
            persona=persona["name"],
            philosophy=persona["philosophy"],
            ts_code=ts_code,
            stock_name=stock_name,
            context_brief=context_brief,
            prior_positions=prior,
        )
        raw = _call_llm(model, [{"role": "user", "content": prompt_text}])
        parsed = _extract_json(raw)

        position_val = str(parsed.get("position") or "neutral").lower()
        if position_val not in _VALID_POSITIONS:
            position_val = "neutral"

        pos = {
            "key": persona["key"],
            "name": persona["name"],
            "philosophy_short": persona["philosophy_short"],
            "position": position_val,
            "confidence": max(0, min(100, int(parsed.get("confidence") or 50))),
            "argument": str(parsed.get("argument") or "").strip()[:120],
            "concern": str(parsed.get("concern") or "").strip()[:80],
            "raw_text": raw[:500],
        }
        positions[persona["key"]] = pos
        prior.append(pos)

    # Persist positions
    conn = _connect(sqlite3_module)
    try:
        _save_job(conn, job_id=job_id, status="running", stage="synthesis",
                  positions_json=_dumps(positions))
    finally:
        conn.close()

    # --- Stage: synthesis ---
    synthesis_prompt = _synthesis_prompt(
        ts_code=ts_code,
        stock_name=stock_name,
        positions=positions,
    )
    raw_synth = _call_llm(model, [{"role": "user", "content": synthesis_prompt}])
    parsed_synth = _extract_json(raw_synth)

    direction = str(parsed_synth.get("direction") or "neutral").lower()
    if direction not in _VALID_POSITIONS:
        direction = "neutral"
    consensus = str(parsed_synth.get("consensus") or "split").lower()
    if consensus not in {"agree", "split", "oppose"}:
        consensus = "split"

    synthesis = {
        "consensus": consensus,
        "direction": direction,
        "verdict": str(parsed_synth.get("verdict") or "").strip()[:120],
        "majority_argument": str(parsed_synth.get("majority_argument") or "").strip()[:120],
        "dissent": str(parsed_synth.get("dissent") or "").strip()[:80],
        "raw_text": raw_synth[:500],
    }

    # Persist final result
    conn = _connect(sqlite3_module)
    try:
        _save_job(conn, job_id=job_id, status="done", stage="done",
                  synthesis_json=_dumps(synthesis), finished_at=_now())
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Worker loop
# ---------------------------------------------------------------------------

def run_roundtable_worker_loop(
    *, sqlite3_module=sqlite3, model: str = "",
    once: bool = False, poll_seconds: float = 2.0
) -> None:
    while True:
        handled = False
        try:
            handled = process_one_roundtable_job(
                sqlite3_module=sqlite3_module,
                model=model or _default_llm_model(),
            )
        except Exception:
            handled = False
        if once:
            return
        if not handled:
            time.sleep(max(0.5, float(poll_seconds or 2.0)))
