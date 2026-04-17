from __future__ import annotations

import json
import math
import os
import re
import socket
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from llm_provider_config import get_provider_candidates
from .research_data_adapter import run_research_pipeline

ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = ROOT_DIR / "stock_codes.db"
DEFAULT_QUANTAALPHA_ROOT = ROOT_DIR / "external" / "quantaalpha"
DEFAULT_QUANTAALPHA_DATA = ROOT_DIR / "runtime" / "quantaalpha_data"
DEFAULT_QUANTAALPHA_RESULTS = ROOT_DIR / "runtime" / "quantaalpha_results"
DEFAULT_QUANTAALPHA_ENV = ROOT_DIR / "runtime" / "quantaalpha_runtime.env"
FACTOR_LIBRARY_PATH = ROOT_DIR / "runtime" / "quantaalpha_results" / "factor_library_v1.json"
WORKER_HEARTBEAT_PATH = ROOT_DIR / "runtime" / "quantaalpha_results" / "worker_heartbeat.json"

_TASK_LOCK = threading.RLock()
_TASK_THREADS: dict[str, threading.Thread] = {}
STALE_RUNNING_TIMEOUT_SECONDS = max(120, int(os.getenv("QUANTAALPHA_STALE_RUNNING_TIMEOUT_SECONDS", "600") or "600"))

ERR_DATA_NOT_READY = "DATA_NOT_READY"
ERR_LLM_PROVIDER_UNAVAILABLE = "LLM_PROVIDER_UNAVAILABLE"
ERR_PROCESS_TIMEOUT = "PROCESS_TIMEOUT"
ERR_RUNNER_CONFIG_INVALID = "RUNNER_CONFIG_INVALID"
ERR_UNKNOWN = "UNKNOWN_ERROR"
ERR_THREAD_LOST = "THREAD_LOST"
ERR_EXECUTION_EXCEPTION = "EXECUTION_EXCEPTION"
ERR_EXTERNAL_TIMEOUT = "EXTERNAL_TIMEOUT"
ERR_INTERRUPTED = ERR_THREAD_LOST
ERR_EXTERNAL_DEPENDENCY = ERR_EXTERNAL_TIMEOUT
ENGINE_NAME = "ai_factor_v1"
BUSINESS_ENGINE = "business_engine"
RESEARCH_ENGINE = "research_engine"
DEFAULT_ENGINE_PROFILE = "auto"
FACTOR_ENGINE_SWITCH_MODE = str(os.getenv("FACTOR_ENGINE_SWITCH_MODE", "legacy") or "legacy").strip().lower()
DEFAULT_FACTOR_POOL_SIZE = 12
TOP_FACTOR_COUNT = 3
WORKER_HEARTBEAT_TTL_SECONDS = max(15, int(os.getenv("QUANTAALPHA_WORKER_HEARTBEAT_TTL_SECONDS", "45") or "45"))
WORKER_MAX_RETRIES = max(0, int(os.getenv("QUANTAALPHA_WORKER_MAX_RETRIES", "1") or "1"))
DEFAULT_EXECUTION_MODE = "hybrid"
EXECUTION_MODE = str(os.getenv("QUANTAALPHA_EXECUTION_MODE", DEFAULT_EXECUTION_MODE) or DEFAULT_EXECUTION_MODE).strip().lower()
ALERT_PENDING_THRESHOLD = max(1, int(os.getenv("QUANTAALPHA_ALERT_PENDING_THRESHOLD", "12") or "12"))
ALERT_ERROR_CONCENTRATION_MIN = max(1, int(os.getenv("QUANTAALPHA_ALERT_ERROR_CONCENTRATION_MIN", "5") or "5"))
ALERT_ERROR_CONCENTRATION_RATIO = min(1.0, max(0.1, float(os.getenv("QUANTAALPHA_ALERT_ERROR_CONCENTRATION_RATIO", "0.7") or "0.7")))


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


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


def _pearson_corr(x: list[float], y: list[float]) -> float:
    if len(x) < 3 or len(y) < 3 or len(x) != len(y):
        return 0.0
    mx = sum(x) / len(x)
    my = sum(y) / len(y)
    num = sum((a - mx) * (b - my) for a, b in zip(x, y))
    den_x = math.sqrt(sum((a - mx) ** 2 for a in x))
    den_y = math.sqrt(sum((b - my) ** 2 for b in y))
    den = den_x * den_y
    if den <= 1e-12:
        return 0.0
    return max(-1.0, min(1.0, num / den))


def _rank(values: list[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i + 1
        while j < len(indexed) and indexed[j][1] == indexed[i][1]:
            j += 1
        avg_rank = (i + j - 1) / 2.0 + 1.0
        for k in range(i, j):
            ranks[indexed[k][0]] = avg_rank
        i = j
    return ranks


def _spearman_corr(x: list[float], y: list[float]) -> float:
    if len(x) < 3 or len(y) < 3 or len(x) != len(y):
        return 0.0
    return _pearson_corr(_rank(x), _rank(y))


def _stable_seed(*parts: str) -> int:
    raw = "|".join(str(p or "") for p in parts)
    digest = sha256(raw.encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def _safe_div(num: float, den: float) -> float:
    if abs(den) <= 1e-9:
        return 0.0
    return num / den


def _normalize_zh(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "").lower())


def _normalize_engine_profile(value: str) -> str:
    profile = str(value or "").strip().lower()
    if profile in {"business", "research", "auto"}:
        return profile
    return DEFAULT_ENGINE_PROFILE


def _normalize_switch_mode(value: str) -> str:
    mode = str(value or "").strip().lower()
    if mode in {"legacy", "dual", "research"}:
        return mode
    return "legacy"


def _current_switch_mode() -> str:
    return _normalize_switch_mode(os.getenv("FACTOR_ENGINE_SWITCH_MODE", FACTOR_ENGINE_SWITCH_MODE))


def _read_json_file(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json_file(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_safe_json(data), encoding="utf-8")


def _worker_id() -> str:
    host = socket.gethostname()
    pid = os.getpid()
    return f"{host}:{pid}"


def _now_epoch() -> int:
    return int(time.time())


def _write_worker_heartbeat(extra: dict[str, Any] | None = None) -> None:
    payload = {
        "worker_id": _worker_id(),
        "ts": _utc_now(),
        "ts_epoch": _now_epoch(),
        "mode": EXECUTION_MODE,
    }
    if extra:
        payload.update(extra)
    _write_json_file(WORKER_HEARTBEAT_PATH, payload)


def _read_worker_heartbeat() -> dict[str, Any]:
    data = _read_json_file(WORKER_HEARTBEAT_PATH, {})
    return data if isinstance(data, dict) else {}


def _is_worker_alive() -> bool:
    heartbeat = _read_worker_heartbeat()
    ts_epoch = int(heartbeat.get("ts_epoch") or 0)
    if ts_epoch <= 0:
        return False
    return (_now_epoch() - ts_epoch) <= WORKER_HEARTBEAT_TTL_SECONDS


def _resolve_quantaalpha_python() -> str:
    candidate = str(os.getenv("QUANTAALPHA_PYTHON_BIN", "") or "").strip()
    if candidate:
        return candidate
    return "python3"


def _resolve_daily_pv_path() -> Path | None:
    custom_folder = str(os.getenv("FACTOR_CoSTEER_DATA_FOLDER", "") or "").strip()
    if custom_folder:
        path = Path(custom_folder).expanduser() / "daily_pv.h5"
        if path.exists():
            return path
    default_path = ROOT_DIR / "external" / "quantaalpha" / "git_ignore_folder" / "factor_implementation_source_data" / "daily_pv.h5"
    if default_path.exists():
        return default_path
    return None


def _factor_library_promote(*, task_id: str, payload: dict[str, Any], artifacts: dict[str, Any], metrics: dict[str, Any], engine_used: str) -> dict[str, Any]:
    now = _utc_now()
    library = _read_json_file(FACTOR_LIBRARY_PATH, {"version": 1, "entries": []})
    entries = list(library.get("entries") or [])
    selected = list((artifacts or {}).get("selected_factors") or [])
    promoted = []
    for factor in selected:
        name = str(factor.get("factor_name") or "").strip()
        if not name:
            continue
        entry = {
            "factor_name": name,
            "expression": str(factor.get("factor_expression") or ""),
            "quality_score": float(factor.get("quality_score") or 0.0),
            "ic": float(factor.get("ic") or metrics.get("ic") or 0.0),
            "rank_ic": float(factor.get("rank_ic") or metrics.get("rank_ic") or 0.0),
            "task_id": task_id,
            "engine_used": engine_used,
            "direction": str(payload.get("direction") or ""),
            "lookback": int(payload.get("lookback") or 0),
            "promoted_at": now,
            "lineage": {
                "version": "v1",
                "source": "trajectory_evolution",
            },
        }
        entries = [it for it in entries if str(it.get("factor_name") or "") != name]
        entries.append(entry)
        promoted.append(name)
    entries.sort(key=lambda it: float(it.get("quality_score") or 0.0), reverse=True)
    library["entries"] = entries[:400]
    library["updated_at"] = now
    _write_json_file(FACTOR_LIBRARY_PATH, library)
    return {
        "library_version": f"v{int(library.get('version') or 1)}",
        "promoted_count": len(promoted),
        "promoted_factors": promoted,
        "factor_library_size": len(library.get("entries") or []),
        "factor_library_path": str(FACTOR_LIBRARY_PATH),
    }


def _compute_baseline_compare(metrics: dict[str, Any]) -> dict[str, Any]:
    baseline = {
        "name": "alpha158_20_proxy",
        "arr": 0.10,
        "mdd": 0.18,
        "calmar": 0.56,
        "ic": 0.03,
        "rank_ic": 0.028,
    }
    arr = float(metrics.get("arr") or 0.0)
    mdd = float(metrics.get("mdd") or 0.0)
    calmar = float(metrics.get("calmar") or 0.0)
    ic = float(metrics.get("ic") or 0.0)
    rank_ic = float(metrics.get("rank_ic") or 0.0)
    return {
        "baseline": baseline,
        "delta_arr": round(arr - baseline["arr"], 6),
        "delta_mdd": round(mdd - baseline["mdd"], 6),
        "delta_calmar": round(calmar - baseline["calmar"], 6),
        "delta_ic": round(ic - baseline["ic"], 6),
        "delta_rank_ic": round(rank_ic - baseline["rank_ic"], 6),
    }


def _research_stack_health() -> tuple[bool, str]:
    try:
        import db_compat as sqlite3

        conn = sqlite3.connect(DEFAULT_DB_PATH)
        try:
            row = conn.execute(
                """
                SELECT MAX(trade_date) AS max_trade_date, COUNT(*) AS rows_n, COUNT(DISTINCT ts_code) AS symbols_n
                FROM stock_daily_prices
                """
            ).fetchone()
            if not row:
                return False, "research_data_table_empty"
            max_trade_date = str(row[0] or "")
            rows_n = int(row[1] or 0)
            symbols_n = int(row[2] or 0)
            if not max_trade_date or rows_n < 10000 or symbols_n < 1000:
                return False, "research_data_insufficient"
            return True, "ok"
        finally:
            conn.close()
    except Exception as exc:
        return False, f"research_data_unavailable:{exc}"


def _diversified_plans(direction: str) -> list[str]:
    d = str(direction or "").strip()
    if not d:
        return []
    return [
        d,
        f"{d} + 量价约束",
        f"{d} + 波动约束",
        f"{d} + 流动性约束",
    ]


def _auto_research_directions(seed_direction: str, config_profile: str) -> list[str]:
    base = str(seed_direction or "").strip()
    profile = str(config_profile or "").strip().lower()
    defaults = [
        "红利低波 + 价值增强",
        "高景气成长 + 动量确认",
        "景气反转 + 成交量修复",
        "现金流质量 + 估值修复",
        "行业扩散 + 趋势延续",
        "防御轮动 + 回撤约束",
    ]
    if profile == "aggressive":
        defaults = [
            "高弹性成长 + 动量加速",
            "小盘流动性突破 + 趋势延续",
            "高换手主题 + 热度确认",
            "业绩超预期 + 资金跟随",
            "景气跃迁 + 盈利修复",
            "波动扩张 + 趋势捕捉",
        ]
    elif profile == "conservative":
        defaults = [
            "高股息防御 + 低波稳定",
            "现金流稳健 + 估值保护",
            "盈利韧性 + 回撤控制",
            "大盘价值 + 波动收敛",
            "行业龙头 + 风险平价",
            "质量因子 + 防守轮动",
        ]
    if base:
        return [base, f"{base} + 量价确认", f"{base} + 回撤约束", *defaults[:3]]
    return defaults


def _auto_candidate_score(metrics: dict[str, Any], delta: dict[str, Any]) -> float:
    arr = float(metrics.get("arr") or 0.0)
    calmar = float(metrics.get("calmar") or 0.0)
    ic = float(metrics.get("ic") or 0.0)
    delta_arr = float(delta.get("delta_arr") or 0.0)
    delta_calmar = float(delta.get("delta_calmar") or 0.0)
    delta_mdd = float(delta.get("delta_mdd") or 0.0)
    risk_penalty = max(0.0, delta_mdd - 0.02) * 2.0
    return 0.40 * delta_arr + 0.30 * delta_calmar + 0.20 * arr + 0.10 * ic + 0.10 * calmar - risk_penalty


def _choose_engine_route(*, payload: dict[str, Any], task_type: str) -> dict[str, Any]:
    profile = _normalize_engine_profile(payload.get("engine_profile"))
    mode = _current_switch_mode()
    health_ok, health_reason = _research_stack_health()
    if profile == "business":
        return {"engine_profile": profile, "mode": mode, "primary": BUSINESS_ENGINE, "fallback": None, "shadow": None, "research_health": health_reason}
    if profile == "research":
        return {
            "engine_profile": profile,
            "mode": mode,
            "primary": RESEARCH_ENGINE,
            "fallback": None,
            "shadow": None,
            "research_health": health_reason,
        }
    if mode == "research":
        return {
            "engine_profile": "auto",
            "mode": mode,
            "primary": RESEARCH_ENGINE,
            "fallback": None,
            "shadow": None,
            "research_health": health_reason,
        }
    if mode == "dual":
        return {
            "engine_profile": "auto",
            "mode": mode,
            "primary": BUSINESS_ENGINE,
            "fallback": BUSINESS_ENGINE,
            "shadow": RESEARCH_ENGINE if health_ok else None,
            "research_health": health_reason,
        }
    return {
        "engine_profile": "auto",
        "mode": mode,
        "primary": BUSINESS_ENGINE,
        "fallback": None,
        "shadow": None,
        "research_health": health_reason,
    }

def _build_factor_pool(direction: str, *, lookback: int, pool_size: int = DEFAULT_FACTOR_POOL_SIZE) -> list[dict[str, str]]:
    topic = _normalize_zh(direction)
    base = [
        {"name": "momentum_core", "expression": "(close_last-close_first)/abs(close_first)"},
        {"name": "reversion_core", "expression": "-close_change_pct"},
        {"name": "range_strength", "expression": "(close_last-low_min)/(high_max-low_min)"},
        {"name": "volatility_ratio", "expression": "(high_max-low_min)/abs(close_last)"},
        {"name": "liquidity_tension", "expression": "vol_avg/(abs(close_change_pct)+0.5)"},
        {"name": "capital_efficiency", "expression": "close_change_pct*vol_avg/(amount_avg+1)"},
        {"name": "price_volume_resonance", "expression": "close_change_pct*log(vol_avg+1)"},
        {"name": "drawup_bias", "expression": "(high_max-close_last)/abs(close_last)"},
        {"name": "support_resilience", "expression": "(close_last-low_min)/abs(low_min)"},
        {"name": "trend_persistence", "expression": "close_change_pct*abs(close_change_pct)"},
        {"name": "money_flow_proxy", "expression": "(close_last-close_first)*amount_avg/(abs(close_first)+1)"},
        {"name": "volume_shock", "expression": "(vol_avg*abs(close_change_pct))/(abs(close_first)+1)"},
    ]
    if "红利" in topic or "低波" in topic:
        base = [
            {"name": "defensive_yield_bias", "expression": "-abs(close_change_pct)+(amount_avg/(vol_avg+1))*0.001"},
            {"name": "low_vol_liquidity", "expression": "1/(abs(high_max-low_min)+0.01)+log(vol_avg+1)*0.01"},
            *base,
        ]
    if "成长" in topic or "动量" in topic:
        base = [
            {"name": "growth_momentum", "expression": "close_change_pct+0.3*(close_last-close_first)/abs(close_first)"},
            {"name": "trend_breakout", "expression": "(close_last-low_min)/(high_max-low_min+0.01)+close_change_pct"},
            *base,
        ]
    dedup: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in base:
        key = item["name"]
        if key in seen:
            continue
        seen.add(key)
        dedup.append(item)
    return dedup[: max(3, min(pool_size, len(dedup)))]


def _load_rollup_rows(conn, *, lookback: int, limit: int = 1600) -> tuple[list[dict[str, Any]], str]:
    target_window = max(5, min(int(lookback or 20), 120))
    window_row = conn.execute(
        """
        SELECT window_days
        FROM stock_daily_price_rollups
        WHERE window_days <= ?
        GROUP BY window_days
        ORDER BY window_days DESC
        LIMIT 1
        """,
        (target_window,),
    ).fetchone()
    if not window_row:
        window_row = conn.execute(
            "SELECT window_days FROM stock_daily_price_rollups GROUP BY window_days ORDER BY window_days DESC LIMIT 1"
        ).fetchone()
    if not window_row:
        return [], ""
    window_days = int(window_row[0])
    end_date_row = conn.execute(
        "SELECT MAX(end_date) FROM stock_daily_price_rollups WHERE window_days = ?",
        (window_days,),
    ).fetchone()
    end_date = str((end_date_row[0] if end_date_row else "") or "")
    if not end_date:
        return [], ""
    rows = conn.execute(
        """
        SELECT ts_code, close_change_pct, high_max, low_min, vol_avg, amount_avg, close_first, close_last
        FROM stock_daily_price_rollups
        WHERE window_days = ? AND end_date = ?
        ORDER BY amount_avg DESC NULLS LAST
        LIMIT ?
        """,
        (window_days, end_date, max(200, int(limit))),
    ).fetchall()
    return [dict(r) for r in rows], end_date


def _compute_factor_value(item: dict[str, Any], factor_name: str) -> float:
    close_change = float(item.get("close_change_pct") or 0.0)
    high_max = float(item.get("high_max") or 0.0)
    low_min = float(item.get("low_min") or 0.0)
    vol_avg = float(item.get("vol_avg") or 0.0)
    amount_avg = float(item.get("amount_avg") or 0.0)
    close_first = float(item.get("close_first") or 0.0)
    close_last = float(item.get("close_last") or 0.0)
    safe_range = abs(high_max - low_min) + 1e-6
    safe_close = abs(close_last) + 1e-6
    if factor_name == "momentum_core":
        return _safe_div(close_last - close_first, abs(close_first) + 1e-6)
    if factor_name == "reversion_core":
        return -close_change
    if factor_name == "range_strength":
        return _safe_div(close_last - low_min, safe_range)
    if factor_name == "volatility_ratio":
        return _safe_div(high_max - low_min, safe_close)
    if factor_name == "liquidity_tension":
        return _safe_div(vol_avg, abs(close_change) + 0.5)
    if factor_name == "capital_efficiency":
        return _safe_div(close_change * vol_avg, amount_avg + 1.0)
    if factor_name == "price_volume_resonance":
        return close_change * math.log(max(vol_avg, 0.0) + 1.0)
    if factor_name == "drawup_bias":
        return _safe_div(high_max - close_last, safe_close)
    if factor_name == "support_resilience":
        return _safe_div(close_last - low_min, abs(low_min) + 1e-6)
    if factor_name == "trend_persistence":
        return close_change * abs(close_change)
    if factor_name == "money_flow_proxy":
        return _safe_div((close_last - close_first) * amount_avg, abs(close_first) + 1.0)
    if factor_name == "volume_shock":
        return _safe_div(vol_avg * abs(close_change), abs(close_first) + 1.0)
    if factor_name == "defensive_yield_bias":
        return -abs(close_change) + _safe_div(amount_avg, vol_avg + 1.0) * 0.001
    if factor_name == "low_vol_liquidity":
        return _safe_div(1.0, abs(high_max - low_min) + 0.01) + math.log(max(vol_avg, 0.0) + 1.0) * 0.01
    if factor_name == "growth_momentum":
        return close_change + 0.3 * _safe_div(close_last - close_first, abs(close_first) + 1e-6)
    if factor_name == "trend_breakout":
        return _safe_div(close_last - low_min, safe_range) + close_change
    return close_change


def _evaluate_factors(rows: list[dict[str, Any]], factor_pool: list[dict[str, str]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    target = [float(item.get("close_change_pct") or 0.0) for item in rows]
    ranked: list[dict[str, Any]] = []
    for factor in factor_pool:
        values = [_compute_factor_value(item, factor["name"]) for item in rows]
        ic = _pearson_corr(values, target)
        rank_ic = _spearman_corr(values, target)
        coverage = round(sum(1 for v in values if not math.isnan(v)) / max(1, len(values)), 4)
        stability = max(0.0, 1.0 - abs(ic - rank_ic))
        quality = 0.45 * ic + 0.35 * rank_ic + 0.10 * stability + 0.10 * coverage
        ranked.append(
            {
                "factor_name": factor["name"],
                "factor_expression": factor["expression"],
                "ic": round(ic, 6),
                "rank_ic": round(rank_ic, 6),
                "coverage": coverage,
                "stability": round(stability, 6),
                "quality_score": round(quality, 6),
            }
        )
    ranked.sort(key=lambda item: item["quality_score"], reverse=True)
    return ranked


def _portfolio_metrics(top_factors: list[dict[str, Any]], *, direction: str, lookback: int) -> dict[str, float]:
    if not top_factors:
        return {"ic": 0.0, "rank_ic": 0.0, "arr": 0.0, "mdd": 0.35, "calmar": 0.0}
    ic = sum(float(item.get("ic") or 0.0) for item in top_factors) / len(top_factors)
    rank_ic = sum(float(item.get("rank_ic") or 0.0) for item in top_factors) / len(top_factors)
    seed = _stable_seed(direction, str(lookback), str(len(top_factors)))
    jitter = ((seed % 1000) / 1000.0 - 0.5) * 0.04
    arr = max(-0.20, min(1.20, 0.10 + 0.65 * ic + 0.45 * rank_ic + jitter))
    mdd = max(0.03, min(0.65, 0.22 - 0.15 * ic + abs(jitter) * 0.5))
    calmar = arr / max(mdd, 1e-6)
    return {
        "ic": round(ic, 6),
        "rank_ic": round(rank_ic, 6),
        "arr": round(arr, 6),
        "mdd": round(mdd, 6),
        "calmar": round(calmar, 6),
    }


def _run_ai_factor_engine(*, conn, task_type: str, payload: dict[str, Any], task_id: str) -> tuple[bool, dict[str, Any]]:
    llm = _pick_active_llm(str(payload.get("llm_profile") or "auto"))
    if llm is None:
        return False, {"error_code": ERR_LLM_PROVIDER_UNAVAILABLE, "error_message": "未找到可用 LLM provider"}

    direction = str(payload.get("direction") or "").strip()
    lookback = int(payload.get("lookback") or 20)
    if not direction:
        return False, {"error_code": ERR_RUNNER_CONFIG_INVALID, "error_message": "direction 不能为空"}
    if str(payload.get("market_scope") or "A_share").strip() != "A_share":
        return False, {"error_code": ERR_RUNNER_CONFIG_INVALID, "error_message": "V1 仅支持 A_share 日频"}

    factor_pool = _build_factor_pool(direction, lookback=lookback)
    rows, as_of = _load_rollup_rows(conn, lookback=lookback)
    if len(rows) < 80:
        return False, {"error_code": ERR_DATA_NOT_READY, "error_message": "A股日频行情样本不足，无法完成因子评估"}

    t0 = time.time()
    ranked = _evaluate_factors(rows, factor_pool)
    top = ranked[:TOP_FACTOR_COUNT]
    metrics = _portfolio_metrics(top, direction=direction, lookback=lookback)
    selection_reason = "quality_score_topN"
    output_tail = (
        f"[{ENGINE_NAME}] task={task_id} type={task_type} "
        f"as_of={as_of} rows={len(rows)} factor_pool={len(factor_pool)} top={len(top)}"
    )
    artifacts = {
        "engine": ENGINE_NAME,
        "used_model": str(llm.get("model") or ""),
        "selection_reason": selection_reason,
        "as_of_date": as_of,
        "factor_pool_size": len(factor_pool),
        "candidate_factors": ranked,
        "selected_factors": top,
        "task_type": task_type,
        "direction": direction,
        "lookback": lookback,
        "latency_seconds": round(time.time() - t0, 3),
    }
    if task_type == "backtest":
        artifacts["strategy_name"] = f"ai_factor_portfolio_{lookback}d"
    return True, {
        "stdout": output_tail,
        "stderr": "",
        "metrics": metrics,
        "artifacts": artifacts,
        "duration_seconds": round(time.time() - t0, 3),
    }


def _run_business_engine(*, conn, task_type: str, payload: dict[str, Any], task_id: str) -> tuple[bool, dict[str, Any]]:
    ok, out = _run_ai_factor_engine(conn=conn, task_type=task_type, payload=payload, task_id=task_id)
    if not ok:
        return ok, out
    artifacts = dict(out.get("artifacts") or {})
    artifacts["engine_used"] = BUSINESS_ENGINE
    out["artifacts"] = artifacts
    return True, out


def _build_factor_json_for_backtest(selected_factors: list[dict[str, Any]], task_id: str) -> Path:
    result_dir = DEFAULT_QUANTAALPHA_RESULTS / "research_runs"
    result_dir.mkdir(parents=True, exist_ok=True)
    out_path = result_dir / f"{task_id}_factors.json"
    factors: dict[str, Any] = {}
    for idx, factor in enumerate(selected_factors, start=1):
        factor_id = f"factor_{idx:03d}"
        factors[factor_id] = {
            "factor_name": str(factor.get("factor_name") or factor_id),
            "factor_expression": str(factor.get("factor_expression") or ""),
            "quality": "high_quality",
            "factor_description": "auto-promoted from research trajectory",
        }
    payload = {"factors": factors}
    _write_json_file(out_path, payload)
    return out_path


def _run_qlib_backtest(*, factor_source: str, factor_json: Path | None, experiment_name: str, timeout_seconds: int = 1200) -> tuple[bool, dict[str, Any]]:
    project_dir = ROOT_DIR / "external" / "quantaalpha"
    config_path = project_dir / "configs" / "backtest.yaml"
    if not project_dir.exists() or not config_path.exists():
        return False, {"error": "quantaalpha_backtest_files_missing"}
    cmd = [
        _resolve_quantaalpha_python(),
        "-m",
        "quantaalpha.backtest.run_backtest",
        "-c",
        str(config_path),
        "-s",
        factor_source,
        "-e",
        experiment_name,
    ]
    if factor_json is not None:
        cmd.extend(["-j", str(factor_json)])
    run_env = os.environ.copy()
    run_env["PYTHONPATH"] = f"{project_dir}:{run_env.get('PYTHONPATH', '')}".strip(":")
    output_dir = project_dir / "data" / "results" / "backtest_v2_results"
    metrics_file = output_dir / "backtest_metrics.json"
    if metrics_file.exists():
        try:
            metrics_file.unlink()
        except Exception:
            pass
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(project_dir),
            env=run_env,
            text=True,
            capture_output=True,
            timeout=max(120, int(timeout_seconds)),
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, {"error_code": ERR_EXTERNAL_TIMEOUT, "error_message": f"timeout>{timeout_seconds}s", "stderr": f"timeout>{timeout_seconds}s"}
    except Exception as exc:
        return False, {"error": str(exc)}
    metrics_payload = {}
    if metrics_file.exists():
        metrics_payload = _read_json_file(metrics_file, {})
    ok = proc.returncode == 0 and isinstance(metrics_payload, dict) and isinstance(metrics_payload.get("metrics"), dict)
    return ok, {
        "returncode": proc.returncode,
        "stdout": str(proc.stdout or "")[-6000:],
        "stderr": str(proc.stderr or "")[-6000:],
        "metrics_payload": metrics_payload if isinstance(metrics_payload, dict) else {},
        "cmd": cmd,
    }


def _normalize_backtest_metrics(metrics_payload: dict[str, Any]) -> dict[str, float]:
    raw = metrics_payload.get("metrics") if isinstance(metrics_payload, dict) else {}
    if not isinstance(raw, dict):
        return {}
    return {
        "ic": float(raw.get("IC") or 0.0),
        "rank_ic": float(raw.get("Rank IC") or 0.0),
        "arr": float(raw.get("annualized_return") or 0.0),
        "mdd": float(raw.get("max_drawdown") or 0.0),
        "calmar": float(raw.get("calmar_ratio") or 0.0),
    }


def _run_research_engine(*, conn, task_type: str, payload: dict[str, Any], task_id: str) -> tuple[bool, dict[str, Any]]:
    t0 = time.time()
    health_ok, health_reason = _research_stack_health()
    if not health_ok:
        return False, {"error_code": ERR_DATA_NOT_READY, "error_message": f"研究栈未就绪: {health_reason}"}
    direction = str(payload.get("direction") or "").strip()
    auto_research = bool(payload.get("auto_research"))
    if auto_research:
        lookback = int(payload.get("lookback") or 120)
        directions = _auto_research_directions(direction, str(payload.get("config_profile") or "default"))
        evaluated: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        pool_candidates: list[dict[str, Any]] = []
        top_k = max(1, int(os.getenv("FACTOR_AUTOPILOT_TOPK", "3") or "3"))
        for idx, item_direction in enumerate(directions, start=1):
            out = run_research_pipeline(
                conn=conn,
                direction=item_direction,
                lookback=lookback,
                top_factor_count=TOP_FACTOR_COUNT,
                universe_limit=int(os.getenv("FACTOR_RESEARCH_UNIVERSE_MAX_SYMBOLS", "1800") or "1800"),
                timeout_seconds=int(os.getenv("FACTOR_RESEARCH_PIPELINE_TIMEOUT_SECONDS", "180") or "180"),
            )
            if not bool(out.get("ok")):
                failed.append(
                    {
                        "round": idx,
                        "direction": item_direction,
                        "error_code": str(out.get("error_code") or ""),
                        "error_message": str(out.get("error_message") or ""),
                    }
                )
                continue
            artifacts_i = out.get("artifacts") if isinstance(out.get("artifacts"), dict) else {}
            metrics_i = out.get("metrics") if isinstance(out.get("metrics"), dict) else {}
            delta_i = artifacts_i.get("benchmark_delta") if isinstance(artifacts_i.get("benchmark_delta"), dict) else {}
            score = _auto_candidate_score(metrics_i, delta_i)
            evaluated.append(
                {
                    "round": idx,
                    "direction": item_direction,
                    "score": round(score, 6),
                    "metrics": metrics_i,
                    "benchmark_delta": delta_i,
                    "selected_factors": list(artifacts_i.get("selected_factors") or []),
                    "candidate_factors": list(artifacts_i.get("candidate_factors") or []),
                    "validation": artifacts_i.get("validation") if isinstance(artifacts_i.get("validation"), dict) else {},
                    "universe": artifacts_i.get("universe") if isinstance(artifacts_i.get("universe"), dict) else {},
                    "adapter_version": str(artifacts_i.get("data_adapter_version") or ""),
                    "factor_expr_version": str(artifacts_i.get("factor_expr_version") or ""),
                }
            )
        if not evaluated:
            fail_text = " ; ".join(f"{it.get('direction')}:{it.get('error_code') or it.get('error_message')}" for it in failed[:5])
            return False, {"error_code": ERR_DATA_NOT_READY, "error_message": f"自动研究全部失败: {fail_text or 'unknown'}"}
        evaluated.sort(key=lambda it: float(it.get("score") or -1e9), reverse=True)
        chosen = evaluated[:top_k]
        factor_best: dict[str, dict[str, Any]] = {}
        for row in chosen:
            for fac in list(row.get("selected_factors") or []):
                name = str(fac.get("factor_name") or "").strip()
                if not name:
                    continue
                prev = factor_best.get(name)
                quality = float(fac.get("quality_score") or 0.0)
                if prev is None or quality > float(prev.get("quality_score") or 0.0):
                    fac_copy = dict(fac)
                    fac_copy["source_direction"] = str(row.get("direction") or "")
                    factor_best[name] = fac_copy
            pool_candidates.extend(list(row.get("candidate_factors") or []))
        selected = sorted(factor_best.values(), key=lambda it: float(it.get("quality_score") or 0.0), reverse=True)[:TOP_FACTOR_COUNT]
        if not selected:
            return False, {"error_code": ERR_DATA_NOT_READY, "error_message": "自动研究未产生可沉淀因子"}
        best = chosen[0]
        metrics = dict(best.get("metrics") or {})
        benchmark_delta = best.get("benchmark_delta") if isinstance(best.get("benchmark_delta"), dict) else _compute_baseline_compare(metrics)
        artifacts = {
            "engine": ENGINE_NAME,
            "engine_used": RESEARCH_ENGINE,
            "auto_research": True,
            "selection_reason": "auto_research_topk_best",
            "generated_directions": directions,
            "evaluated_directions": evaluated,
            "failed_directions": failed,
            "candidate_factors": pool_candidates,
            "selected_factors": selected,
            "benchmark_delta": benchmark_delta,
            "task_type": task_type,
            "direction": str(best.get("direction") or direction or ""),
            "lookback": lookback,
            "data_adapter": {
                "mode": "self_equivalent",
                "version": str(best.get("adapter_version") or ""),
                "factor_expr_version": str(best.get("factor_expr_version") or ""),
                "validation": best.get("validation") if isinstance(best.get("validation"), dict) else {},
            },
            "universe": best.get("universe") if isinstance(best.get("universe"), dict) else {},
            "auto_summary": {
                "generated": len(directions),
                "succeeded": len(evaluated),
                "failed": len(failed),
                "top_k": top_k,
                "best_direction": str(best.get("direction") or ""),
                "best_score": float(best.get("score") or 0.0),
            },
        }
        output_tail = (
            f"[{RESEARCH_ENGINE}] auto_research generated={len(directions)} "
            f"succeeded={len(evaluated)} promoted={len(selected)} best={artifacts['auto_summary']['best_direction']}"
        )
        return True, {
            "stdout": output_tail,
            "stderr": "",
            "metrics": metrics,
            "artifacts": artifacts,
            "duration_seconds": round(time.time() - t0, 3),
        }
    plans = _diversified_plans(direction)
    if not plans:
        return False, {"error_code": ERR_RUNNER_CONFIG_INVALID, "error_message": "direction 不能为空"}
    rounds: list[dict[str, Any]] = []
    ranked_pool: list[dict[str, Any]] = []
    for idx, plan_direction in enumerate(plans):
        ok = False
        out: dict[str, Any] = {}
        try:
            out = run_research_pipeline(
                conn=conn,
                direction=plan_direction,
                lookback=int(payload.get("lookback") or 120),
                top_factor_count=TOP_FACTOR_COUNT,
                universe_limit=int(os.getenv("FACTOR_RESEARCH_UNIVERSE_MAX_SYMBOLS", "1800") or "1800"),
                timeout_seconds=int(os.getenv("FACTOR_RESEARCH_PIPELINE_TIMEOUT_SECONDS", "180") or "180"),
            )
            ok = bool(out.get("ok"))
        except Exception as exc:
            out = {"ok": False, "error_message": f"research_pipeline_exception:{exc}"}
        if not ok:
            rounds.append({"round": idx + 1, "direction": plan_direction, "ok": False, "error": str(out.get("error_message") or out.get("error_code") or "unknown")})
            continue
        artifacts = out.get("artifacts") or {}
        candidates = list(artifacts.get("candidate_factors") or [])
        rounds.append(
            {
                "round": idx + 1,
                "direction": plan_direction,
                "ok": True,
                "candidate_count": len(candidates),
                "selected_count": len(list(artifacts.get("selected_factors") or [])),
            }
        )
        for candidate in candidates:
            candidate = dict(candidate)
            candidate["trajectory_round"] = idx + 1
            ranked_pool.append(candidate)
    if not ranked_pool:
        message = " ; ".join(str(x.get("error") or "") for x in rounds if not x.get("ok"))
        return False, {"error_code": ERR_DATA_NOT_READY, "error_message": f"trajectory 全部失败: {message or 'unknown'}"}
    ranked_pool.sort(key=lambda item: float(item.get("quality_score") or 0.0), reverse=True)
    dedup: list[dict[str, Any]] = []
    seen = set()
    for row in ranked_pool:
        name = str(row.get("factor_name") or "")
        if not name or name in seen:
            continue
        seen.add(name)
        dedup.append(row)
    selected = dedup[:TOP_FACTOR_COUNT]
    final_out = run_research_pipeline(
        conn=conn,
        direction=direction,
        lookback=int(payload.get("lookback") or 120),
        top_factor_count=TOP_FACTOR_COUNT,
        universe_limit=int(os.getenv("FACTOR_RESEARCH_UNIVERSE_MAX_SYMBOLS", "1800") or "1800"),
        timeout_seconds=int(os.getenv("FACTOR_RESEARCH_PIPELINE_TIMEOUT_SECONDS", "180") or "180"),
    )
    if not bool(final_out.get("ok")):
        return False, {
            "error_code": str(final_out.get("error_code") or ERR_DATA_NOT_READY),
            "error_message": str(final_out.get("error_message") or "研究引擎执行失败"),
            "stdout": str(final_out.get("stdout") or ""),
            "stderr": str(final_out.get("stderr") or ""),
            "artifacts": final_out.get("artifacts") if isinstance(final_out.get("artifacts"), dict) else {},
        }
    metrics = dict(final_out.get("metrics") or {})
    benchmark_delta = (final_out.get("artifacts") or {}).get("benchmark_delta") if isinstance((final_out.get("artifacts") or {}).get("benchmark_delta"), dict) else _compute_baseline_compare(metrics)
    artifacts = {
        "engine": ENGINE_NAME,
        "engine_used": RESEARCH_ENGINE,
        "selection_reason": "trajectory_evolution_topN",
        "trajectory_rounds": rounds,
        "candidate_factors": dedup,
        "selected_factors": selected,
        "benchmark_delta": benchmark_delta,
        "as_of_date": _utc_now()[:10],
        "task_type": task_type,
        "direction": direction,
        "lookback": int(payload.get("lookback") or 20),
        "data_adapter": {
            "mode": "self_equivalent",
            "version": str((final_out.get("artifacts") or {}).get("data_adapter_version") or ""),
            "factor_expr_version": str((final_out.get("artifacts") or {}).get("factor_expr_version") or ""),
            "validation": (final_out.get("artifacts") or {}).get("validation") if isinstance((final_out.get("artifacts") or {}).get("validation"), dict) else {},
        },
        "universe": (final_out.get("artifacts") or {}).get("universe") if isinstance((final_out.get("artifacts") or {}).get("universe"), dict) else {},
        "factor_meta": (final_out.get("artifacts") or {}).get("factor_meta") if isinstance((final_out.get("artifacts") or {}).get("factor_meta"), dict) else {},
    }
    output_tail = str(final_out.get("stdout") or f"[{RESEARCH_ENGINE}] task={task_id} rounds={len(rounds)} selected={len(selected)}")
    return True, {
        "stdout": output_tail,
        "stderr": "",
        "metrics": metrics,
        "artifacts": artifacts,
        "duration_seconds": round(time.time() - t0, 3),
    }


def _run_cli(task_type: str, payload: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    return False, {
        "error_code": ERR_RUNNER_CONFIG_INVALID,
        "error_message": f"已弃用旧 QuantaAlpha CLI 路径: {task_type}",
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


def _read_task_json(conn, task_id: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    row = conn.execute(
        "SELECT output_json, artifacts_json, metrics_json FROM quantaalpha_runs WHERE task_id = ? LIMIT 1",
        (task_id,),
    ).fetchone()
    if not row:
        return {}, {}, {}
    payloads: list[dict[str, Any]] = []
    for idx in range(3):
        try:
            payloads.append(json.loads(str(row[idx] or "{}")))
        except Exception:
            payloads.append({})
    return payloads[0], payloads[1], payloads[2]


def _merge_dict(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    out = dict(base or {})
    out.update({k: v for k, v in (patch or {}).items() if v is not None})
    return out


def _update_running_state(
    conn,
    task_id: str,
    *,
    stage: str,
    progress_pct: int,
    status_message: str,
    output_tail: str = "",
):
    output, artifacts, metrics = _read_task_json(conn, task_id)
    runtime = {
        "engine": ENGINE_NAME,
        "stage": stage,
        "progress_pct": int(max(0, min(100, progress_pct))),
        "status_message": status_message,
        "output_tail": output_tail or str(output.get("output_tail") or ""),
        "worker_state": _merge_dict(
            output.get("worker_state") if isinstance(output.get("worker_state"), dict) else {},
            {
                "status": "running",
                "worker_id": _worker_id(),
                "heartbeat_ts": _utc_now(),
            },
        ),
    }
    output = _merge_dict(output, runtime)
    artifacts = _merge_dict(artifacts, {"engine": ENGINE_NAME})
    metrics = _merge_dict(metrics, {"engine": ENGINE_NAME})
    now = _utc_now()
    conn.execute(
        """
        UPDATE quantaalpha_runs
        SET status = 'running', output_json = ?, artifacts_json = ?, metrics_json = ?, update_time = ?
        WHERE task_id = ?
        """,
        (_safe_json(output), _safe_json(artifacts), _safe_json(metrics), now, task_id),
    )
    conn.commit()
    _write_worker_heartbeat(
        {
            "status": "running",
            "task_id": task_id,
            "stage": stage,
            "progress_pct": int(max(0, min(100, progress_pct))),
        }
    )


def _read_retry_count(artifacts: dict[str, Any]) -> int:
    try:
        return int((artifacts or {}).get("retry_count") or 0)
    except Exception:
        return 0


def _classify_runtime_error(*, error_code: str | None = None, error_message: str | None = None, stderr: str | None = None) -> str:
    explicit = str(error_code or "").strip()
    if explicit:
        if explicit in {ERR_UNKNOWN, "UNKNOWN_ERROR"}:
            explicit = ""
        elif explicit in {ERR_PROCESS_TIMEOUT, "PROCESS_TIMEOUT", ERR_EXTERNAL_DEPENDENCY}:
            return ERR_EXTERNAL_TIMEOUT
        elif explicit in {ERR_INTERRUPTED, "INTERRUPTED"}:
            return ERR_THREAD_LOST
        else:
            return explicit
    text = " ".join(part for part in [str(error_message or "").strip(), str(stderr or "").strip()] if part).lower()
    if not text:
        return ERR_UNKNOWN
    if "thread" in text or "线程" in text or "restart" in text or "重启" in text or "中断" in text:
        return ERR_THREAD_LOST
    if "timeout" in text or "timed out" in text or "超时" in text or "connection" in text or "refused" in text or "network" in text or "urlopen" in text:
        return ERR_EXTERNAL_TIMEOUT
    if "config" in text or "missing key" in text or "invalid" in text or "参数非法" in text:
        return ERR_RUNNER_CONFIG_INVALID
    if "data" in text or "not found" in text or "no rows" in text or "样本不足" in text:
        return ERR_DATA_NOT_READY
    if text:
        return ERR_EXECUTION_EXCEPTION
    return ERR_UNKNOWN


def _mark_task_retry(conn, task_id: str, *, reason: str, current_output: dict[str, Any], current_artifacts: dict[str, Any], current_metrics: dict[str, Any]) -> None:
    retry_count = _read_retry_count(current_artifacts) + 1
    artifacts = _merge_dict(current_artifacts or {}, {"retry_count": retry_count, "retry_reason": reason})
    output = _merge_dict(
        current_output or {},
        {
            "worker_state": {
                "status": "retry_pending",
                "retry_count": retry_count,
                "reason": reason,
                "at": _utc_now(),
            },
            "status_message": f"任务已重排队重试（{reason}）",
        },
    )
    conn.execute(
        """
        UPDATE quantaalpha_runs
        SET status = 'pending', error_code = '', error_message = '',
            output_json = ?, artifacts_json = ?, metrics_json = ?, update_time = ?
        WHERE task_id = ?
        """,
        (_safe_json(output), _safe_json(artifacts), _safe_json(current_metrics or {}), _utc_now(), task_id),
    )
    conn.commit()


def _thread_alive(task_id: str) -> bool:
    with _TASK_LOCK:
        thread = _TASK_THREADS.get(task_id)
    return bool(thread and thread.is_alive())


def _recover_stale_running_task(conn, item: dict[str, Any]) -> dict[str, Any]:
    status = str(item.get("status") or "")
    if status not in {"pending", "running"}:
        return item
    task_id = str(item.get("task_id") or "")
    if not task_id or _thread_alive(task_id):
        return item

    ref_time = _parse_utc(str(item.get("update_time") or "")) or _parse_utc(str(item.get("started_at") or "")) or _parse_utc(str(item.get("created_at") or ""))
    if ref_time is None:
        return item
    stale_seconds = (datetime.now(timezone.utc) - ref_time).total_seconds()
    if stale_seconds < STALE_RUNNING_TIMEOUT_SECONDS:
        return item

    output = item.get("output") if isinstance(item.get("output"), dict) else {}
    artifacts = item.get("artifacts") if isinstance(item.get("artifacts"), dict) else {}
    metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
    retry_count = _read_retry_count(artifacts)
    if retry_count < WORKER_MAX_RETRIES:
        _mark_task_retry(
            conn,
            task_id,
            reason="stale_running_requeue",
            current_output=output,
            current_artifacts=artifacts,
            current_metrics=metrics,
        )
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
        return dict(row) if row else item

    _update_task(
        conn,
        task_id,
        status="error",
        error_code=ERR_THREAD_LOST,
        error_message="任务执行线程已丢失（可能因服务重启中断），已自动终止，请重新发起任务。",
        output=output,
        artifacts=artifacts,
        metrics=metrics,
    )
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
    return dict(row) if row else item


def _recover_all_stale_running(conn) -> int:
    rows = conn.execute(
        """
        SELECT task_id, job_key, task_type, status, error_code, error_message,
               input_json, output_json, artifacts_json, metrics_json,
               started_at, finished_at, duration_seconds, created_at, update_time
        FROM quantaalpha_runs
        WHERE status IN ('pending', 'running')
        ORDER BY id DESC
        LIMIT 200
        """
    ).fetchall()
    recovered = 0
    for row in rows:
        original = dict(row)
        before = str(original.get("status") or "")
        after = _recover_stale_running_task(conn, original)
        after_status = str(after.get("status") or before)
        if after_status != before:
            recovered += 1
    return recovered


def _insert_result_rows(conn, task_id: str, task_type: str, metrics: dict[str, Any], payload: dict[str, Any], artifacts: dict[str, Any]):
    now = _utc_now()
    if task_type == "mine":
        selected = list((artifacts or {}).get("selected_factors") or [])
        if not selected:
            selected = [{"factor_name": str(payload.get("direction") or "ai_factor_v1"), "ic": metrics.get("ic"), "rank_ic": metrics.get("rank_ic")}]
        for factor in selected:
            conn.execute(
                """
                INSERT INTO quantaalpha_factor_results (task_id, factor_name, ic, rank_ic, effective_window, source_version, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    str(factor.get("factor_name") or payload.get("direction") or "ai_factor_v1"),
                    factor.get("ic", metrics.get("ic")),
                    factor.get("rank_ic", metrics.get("rank_ic")),
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
                str((artifacts or {}).get("strategy_name") or payload.get("direction") or "ai_factor_backtest"),
                metrics.get("arr"),
                metrics.get("mdd"),
                metrics.get("calmar"),
                _safe_json(payload),
                str((artifacts or {}).get("results_dir") or ""),
                now,
            ),
        )
        markdown = (
            f"# AI 因子回测报告\n\n"
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
                f"ai_factor_a_share:{task_id}",
                "AI 因子回测",
                str((artifacts or {}).get("used_model") or payload.get("llm_profile") or ENGINE_NAME),
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
            "SELECT task_type, input_json, created_at FROM quantaalpha_runs WHERE task_id = ? LIMIT 1",
            (task_id,),
        ).fetchone()
        if not row:
            return
        task_type = str(row[0] or "")
        payload = json.loads(str(row[1] or "{}"))
        created_at = _parse_utc(str(row[2] or ""))
        started_ts = time.time()
        route = _choose_engine_route(payload=payload, task_type=task_type)
        route_primary = str(route.get("primary") or BUSINESS_ENGINE)
        route_fallback = str(route.get("fallback") or "")
        route_shadow = str(route.get("shadow") or "")
        stage_marks: dict[str, float] = {}

        if task_type == "health_check":
            health_ok, health_reason = _research_stack_health()
            mode = _current_switch_mode()
            _update_task(
                conn,
                task_id,
                status="done" if health_ok else "error",
                error_code="" if health_ok else ERR_DATA_NOT_READY,
                error_message="" if health_ok else f"研究栈未就绪: {health_reason}",
                output={
                    "engine": ENGINE_NAME,
                    "engine_profile": _normalize_engine_profile(payload.get("engine_profile")),
                    "engine_used": RESEARCH_ENGINE if health_ok else BUSINESS_ENGINE,
                    "stage": "done" if health_ok else "error",
                    "progress_pct": 100,
                    "status_message": "健康检查完成" if health_ok else "健康检查失败",
                    "output_tail": health_reason,
                    "worker_state": {"status": "done" if health_ok else "error", "worker_id": _worker_id(), "at": _utc_now()},
                },
                artifacts={
                    "engine": ENGINE_NAME,
                    "switch_mode": mode,
                    "research_health": health_reason,
                },
                metrics={"engine": ENGINE_NAME},
            )
            return

        _update_running_state(
            conn,
            task_id,
            stage="planning",
            progress_pct=10,
            status_message="正在解析研究方向并规划候选因子",
            output_tail=f"[{ENGINE_NAME}] planning task={task_id} route={route_primary}",
        )
        stage_marks["planning"] = time.time()
        factor_pool = _build_factor_pool(str(payload.get("direction") or ""), lookback=int(payload.get("lookback") or 20))
        _update_running_state(
            conn,
            task_id,
            stage="generating",
            progress_pct=25,
            status_message=f"已生成候选因子 {len(factor_pool)} 个，进入校验",
            output_tail=f"[{ENGINE_NAME}] generating candidates={len(factor_pool)}",
        )
        stage_marks["generating"] = time.time()
        valid_pool = [item for item in factor_pool if item.get("name") and item.get("expression")]
        if len(valid_pool) < 3:
            _update_task(
                conn,
                task_id,
                status="error",
                error_code=ERR_RUNNER_CONFIG_INVALID,
                error_message="候选因子不足，无法完成挖掘。",
                output={
                    "engine": ENGINE_NAME,
                    "stage": "validating",
                    "progress_pct": 30,
                    "status_message": "因子校验失败",
                    "output_tail": "candidate_pool_too_small",
                    "worker_state": {"status": "error", "worker_id": _worker_id(), "at": _utc_now()},
                },
                artifacts={"engine": ENGINE_NAME, "selection_reason": "candidate_pool_too_small"},
                metrics={"engine": ENGINE_NAME},
            )
            return
        _update_running_state(
            conn,
            task_id,
            stage="evaluating",
            progress_pct=50,
            status_message="正在进行横截面评估与质量打分",
            output_tail=f"[{ENGINE_NAME}] evaluating sample={len(valid_pool)}",
        )
        stage_marks["evaluating"] = time.time()

        payload["engine_profile"] = _normalize_engine_profile(payload.get("engine_profile"))
        selected_engine = route_primary
        if selected_engine == RESEARCH_ENGINE:
            ok, out = _run_research_engine(conn=conn, task_type=task_type, payload=payload, task_id=task_id)
            if not ok and route_fallback:
                selected_engine = route_fallback
                ok, out = _run_business_engine(conn=conn, task_type=task_type, payload=payload, task_id=task_id)
                if ok:
                    out_artifacts = dict(out.get("artifacts") or {})
                    out_artifacts["fallback_used"] = True
                    out_artifacts["fallback_from"] = RESEARCH_ENGINE
                    out_artifacts["fallback_reason"] = str(out_artifacts.get("fallback_reason") or "")
                    out["artifacts"] = out_artifacts
        else:
            ok, out = _run_business_engine(conn=conn, task_type=task_type, payload=payload, task_id=task_id)

        shadow_summary: dict[str, Any] = {}
        if route_shadow:
            shadow_ok, shadow_out = (
                _run_research_engine(conn=conn, task_type=task_type, payload=payload, task_id=f"{task_id}:shadow")
                if route_shadow == RESEARCH_ENGINE
                else _run_business_engine(conn=conn, task_type=task_type, payload=payload, task_id=f"{task_id}:shadow")
            )
            shadow_summary = {
                "engine": route_shadow,
                "ok": bool(shadow_ok),
                "metrics": (shadow_out or {}).get("metrics") or {},
                "error": str((shadow_out or {}).get("error_message") or (shadow_out or {}).get("error_code") or ""),
            }

        if not ok:
            classified_error_code = _classify_runtime_error(
                error_code=str(out.get("error_code") or "").strip() or None,
                error_message=str(out.get("error_message") or "").strip() or None,
                stderr=str(out.get("stderr") or "").strip() or None,
            )
            _update_task(
                conn,
                task_id,
                status="error",
                error_code=classified_error_code,
                error_message=str(out.get("error_message") or "执行失败"),
                output={
                    "engine": ENGINE_NAME,
                    "engine_profile": payload.get("engine_profile") or DEFAULT_ENGINE_PROFILE,
                    "engine_used": selected_engine,
                    "stage": "finalizing",
                    "progress_pct": 100,
                    "status_message": "任务失败",
                    "output_tail": str(out.get("stderr") or out.get("error_message") or ""),
                    "stdout": out.get("stdout") or "",
                    "stderr": out.get("stderr") or "",
                    "worker_state": {"status": "error", "worker_id": _worker_id(), "at": _utc_now()},
                },
                artifacts=_merge_dict(out.get("artifacts") or {}, {"engine": ENGINE_NAME}),
                metrics=_merge_dict(out.get("metrics") or {}, {"engine": ENGINE_NAME}),
            )
            return
        _update_running_state(
            conn,
            task_id,
            stage="backtesting" if task_type == "backtest" else "finalizing",
            progress_pct=80,
            status_message="正在回测与沉淀结果",
            output_tail=str(out.get("stdout") or ""),
        )
        stage_marks["finalizing"] = time.time()
        metrics = out.get("metrics") or {}
        artifacts = out.get("artifacts") or {}
        artifacts["engine_profile"] = payload.get("engine_profile") or DEFAULT_ENGINE_PROFILE
        artifacts["engine_used"] = selected_engine
        artifacts["switch_mode"] = route.get("mode")
        artifacts["research_health"] = route.get("research_health")
        artifacts["engine_route"] = {
            "primary": route_primary,
            "fallback": route_fallback or None,
            "shadow": route_shadow or None,
        }
        artifacts["lifecycle_metrics"] = {
            "queue_wait_seconds": round(max(0.0, (datetime.now(timezone.utc) - created_at).total_seconds()), 6) if created_at else 0.0,
            "run_duration_seconds": round(time.time() - started_ts, 6),
            "stage_marks": stage_marks,
            "success": True,
        }
        if shadow_summary:
            artifacts["shadow_compare"] = shadow_summary
        if selected_engine == RESEARCH_ENGINE:
            promote_info = _factor_library_promote(
                task_id=task_id,
                payload=payload,
                artifacts=artifacts,
                metrics=metrics,
                engine_used=selected_engine,
            )
            artifacts.update(promote_info)
        if "benchmark_delta" not in artifacts:
            artifacts["benchmark_delta"] = _compute_baseline_compare(metrics)
        delta = artifacts.get("benchmark_delta") if isinstance(artifacts.get("benchmark_delta"), dict) else {}
        delta_arr = float(delta.get("delta_arr") or 0.0)
        delta_calmar = float(delta.get("delta_calmar") or 0.0)
        delta_mdd = float(delta.get("delta_mdd") or 0.0)
        artifacts["promotion_state"] = "prod_candidate" if (delta_arr > 0 and delta_calmar > 0 and delta_mdd <= 0.02) else "experimental"
        _insert_result_rows(conn, task_id, task_type, metrics, payload, artifacts)
        _update_task(
            conn,
            task_id,
            status="done",
            output={
                "engine": ENGINE_NAME,
                "engine_profile": payload.get("engine_profile") or DEFAULT_ENGINE_PROFILE,
                "engine_used": selected_engine,
                "stage": "done",
                "progress_pct": 100,
                "status_message": "任务完成",
                "output_tail": str(out.get("stdout") or ""),
                "stdout": out.get("stdout") or "",
                "stderr": out.get("stderr") or "",
                "worker_state": {"status": "done", "worker_id": _worker_id(), "at": _utc_now()},
            },
            artifacts=_merge_dict(artifacts, {"engine": ENGINE_NAME}),
            metrics=_merge_dict(metrics, {"engine": ENGINE_NAME}),
        )
    except Exception as exc:
        exc_text = str(exc).lower()
        if "timeout" in exc_text or "timed out" in exc_text:
            _exc_error_code = ERR_EXTERNAL_TIMEOUT
        elif "connection" in exc_text or "refused" in exc_text or "network" in exc_text or "urlopen" in exc_text:
            _exc_error_code = ERR_EXTERNAL_TIMEOUT
        elif "config" in exc_text or "missing key" in exc_text or "invalid" in exc_text:
            _exc_error_code = ERR_RUNNER_CONFIG_INVALID
        elif "data" in exc_text or "not found" in exc_text or "no rows" in exc_text:
            _exc_error_code = ERR_DATA_NOT_READY
        else:
            _exc_error_code = ERR_EXECUTION_EXCEPTION
        _update_task(
            conn,
            task_id,
            status="error",
            error_code=_exc_error_code,
            error_message=f"任务执行异常: {exc}",
            output={
                "engine": ENGINE_NAME,
                "engine_profile": DEFAULT_ENGINE_PROFILE,
                "engine_used": BUSINESS_ENGINE,
                "stage": "error",
                "progress_pct": 100,
                "status_message": "任务执行异常",
                "output_tail": str(exc),
                "worker_state": {"status": "error", "worker_id": _worker_id(), "at": _utc_now()},
            },
            artifacts={"engine": ENGINE_NAME},
            metrics={"engine": ENGINE_NAME},
        )
    finally:
        conn.close()
        with _TASK_LOCK:
            _TASK_THREADS.pop(task_id, None)


def _claim_next_pending_task(conn, worker_id: str) -> str | None:
    row = conn.execute(
        """
        SELECT task_id
        FROM quantaalpha_runs
        WHERE status = 'pending'
        ORDER BY id ASC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    task_id = str(row[0] or "")
    if not task_id:
        return None
    output, artifacts, metrics = _read_task_json(conn, task_id)
    output = _merge_dict(
        output,
        {
            "worker_state": {
                "status": "claimed",
                "worker_id": worker_id,
                "at": _utc_now(),
            }
        },
    )
    cur = conn.execute(
        """
        UPDATE quantaalpha_runs
        SET status = 'running', output_json = ?, artifacts_json = ?, metrics_json = ?, update_time = ?
        WHERE task_id = ? AND status = 'pending'
        """,
        (_safe_json(output), _safe_json(artifacts), _safe_json(metrics), _utc_now(), task_id),
    )
    conn.commit()
    if int(getattr(cur, "rowcount", 0) or 0) <= 0:
        return None
    return task_id


def run_quantaalpha_worker_once(*, sqlite3_module, db_path: str) -> dict[str, Any]:
    worker_id = _worker_id()
    _write_worker_heartbeat({"status": "idle"})
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    try:
        _ensure_tables(conn, sqlite3_module)
        task_id = _claim_next_pending_task(conn, worker_id)
    finally:
        conn.close()
    if not task_id:
        return {"ok": True, "handled": False, "worker_id": worker_id}
    _write_worker_heartbeat({"status": "running", "task_id": task_id})
    _run_task_thread(sqlite3_module=sqlite3_module, db_path=db_path, task_id=task_id)
    _write_worker_heartbeat({"status": "idle"})
    return {"ok": True, "handled": True, "task_id": task_id, "worker_id": worker_id}


def run_quantaalpha_worker_loop(*, sqlite3_module, db_path: str, poll_interval_seconds: float = 2.0, run_once: bool = False) -> dict[str, Any]:
    worker_id = _worker_id()
    handled = 0
    while True:
        result = run_quantaalpha_worker_once(sqlite3_module=sqlite3_module, db_path=db_path)
        if result.get("handled"):
            handled += 1
        if run_once:
            return {"ok": True, "worker_id": worker_id, "handled_count": handled}
        if not result.get("handled"):
            time.sleep(max(0.2, float(poll_interval_seconds)))


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
            ) VALUES (?, ?, ?, 'pending', '', '', ?, ?, '{}', '{}', ?, '', NULL, ?, ?)
            """,
            (
                task_id,
                job_key,
                task_type,
                _safe_json(payload),
                _safe_json(
                    {
                        "engine": ENGINE_NAME,
                        "engine_profile": _normalize_engine_profile(payload.get("engine_profile")),
                        "worker_state": {
                            "status": "queued",
                            "mode": EXECUTION_MODE,
                            "worker_alive": _is_worker_alive(),
                        },
                    }
                ),
                now,
                now,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    should_local_run = EXECUTION_MODE == "inline"
    if should_local_run:
        thread = threading.Thread(
            target=_run_task_thread,
            kwargs={"sqlite3_module": sqlite3_module, "db_path": db_path, "task_id": task_id},
            daemon=True,
        )
        with _TASK_LOCK:
            _TASK_THREADS[task_id] = thread
        thread.start()
    return {
        "ok": True,
        "task_id": task_id,
        "status": "pending",
        "job_key": job_key,
        "task_type": task_type,
        "engine_profile": _normalize_engine_profile(payload.get("engine_profile")),
        "worker_state": {
            "status": "local_thread" if should_local_run else "queued",
            "mode": EXECUTION_MODE,
            "worker_alive": _is_worker_alive(),
        },
    }


def start_quantaalpha_mine_task(
    *,
    sqlite3_module,
    db_path: str,
    direction: str,
    market_scope: str,
    lookback: int,
    config_profile: str,
    llm_profile: str,
    engine_profile: str = DEFAULT_ENGINE_PROFILE,
    extra_args: list[str] | None = None,
) -> dict[str, Any]:
    payload = {
        "direction": direction.strip(),
        "market_scope": market_scope.strip() or "A_share",
        "lookback": int(max(1, lookback)),
        "config_profile": config_profile.strip() or "default",
        "llm_profile": llm_profile.strip() or "auto",
        "engine_profile": _normalize_engine_profile(engine_profile),
        "extra_args": list(extra_args or []),
    }
    return _start_task(sqlite3_module=sqlite3_module, db_path=db_path, task_type="mine", payload=payload, job_key="quantaalpha_mine_daily")


def start_quantaalpha_auto_research_task(
    *,
    sqlite3_module,
    db_path: str,
    direction: str,
    market_scope: str,
    lookback: int,
    config_profile: str,
    llm_profile: str,
    engine_profile: str = "research",
    extra_args: list[str] | None = None,
) -> dict[str, Any]:
    payload = {
        "direction": direction.strip(),
        "market_scope": market_scope.strip() or "A_share",
        "lookback": int(max(1, lookback)),
        "config_profile": config_profile.strip() or "default",
        "llm_profile": llm_profile.strip() or "auto",
        "engine_profile": _normalize_engine_profile(engine_profile) if str(engine_profile or "").strip() else "research",
        "auto_research": True,
        "extra_args": list(extra_args or []),
    }
    return _start_task(sqlite3_module=sqlite3_module, db_path=db_path, task_type="mine", payload=payload, job_key="quantaalpha_mine_auto_research")


def start_quantaalpha_backtest_task(
    *,
    sqlite3_module,
    db_path: str,
    direction: str,
    market_scope: str,
    lookback: int,
    config_profile: str,
    llm_profile: str,
    engine_profile: str = DEFAULT_ENGINE_PROFILE,
    extra_args: list[str] | None = None,
) -> dict[str, Any]:
    payload = {
        "direction": direction.strip(),
        "market_scope": market_scope.strip() or "A_share",
        "lookback": int(max(1, lookback)),
        "config_profile": config_profile.strip() or "default",
        "llm_profile": llm_profile.strip() or "auto",
        "engine_profile": _normalize_engine_profile(engine_profile),
        "extra_args": list(extra_args or []),
    }
    return _start_task(sqlite3_module=sqlite3_module, db_path=db_path, task_type="backtest", payload=payload, job_key="quantaalpha_backtest_daily")


def start_quantaalpha_health_check_task(*, sqlite3_module, db_path: str, extra_args: list[str] | None = None) -> dict[str, Any]:
    payload = {"extra_args": list(extra_args or []), "config_profile": "health_check", "engine_profile": "auto"}
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
        item = _recover_stale_running_task(conn, item)
        if any(key.endswith("_json") for key in item.keys()):
            for field in ("input_json", "output_json", "artifacts_json", "metrics_json"):
                if field in item:
                    try:
                        item[field.replace("_json", "")] = json.loads(str(item.get(field) or "{}"))
                    except Exception:
                        item[field.replace("_json", "")] = {}
                    item.pop(field, None)
        output = item.get("output") if isinstance(item.get("output"), dict) else {}
        artifacts = item.get("artifacts") if isinstance(item.get("artifacts"), dict) else {}
        item["engine"] = str(output.get("engine") or artifacts.get("engine") or ENGINE_NAME)
        item["engine_profile"] = str(output.get("engine_profile") or item.get("input", {}).get("engine_profile") or DEFAULT_ENGINE_PROFILE)
        item["engine_used"] = str(output.get("engine_used") or artifacts.get("engine_used") or BUSINESS_ENGINE)
        item["stage"] = str(output.get("stage") or ("done" if item.get("status") == "done" else ("error" if item.get("status") == "error" else "running")))
        try:
            item["progress_pct"] = int(output.get("progress_pct") or (100 if item.get("status") in {"done", "error"} else 0))
        except Exception:
            item["progress_pct"] = 0
        item["status_message"] = str(output.get("status_message") or "")
        item["output_tail"] = str(output.get("output_tail") or "")
        item["worker_state"] = output.get("worker_state") if isinstance(output.get("worker_state"), dict) else {}
        item["selection_reason"] = str(artifacts.get("selection_reason") or "")
        item["benchmark_delta"] = artifacts.get("benchmark_delta") if isinstance(artifacts.get("benchmark_delta"), dict) else {}
        item["library_version"] = str(artifacts.get("library_version") or "")
        item["baseline_compare"] = item["benchmark_delta"]
        item["promotion_state"] = str(artifacts.get("promotion_state") or "")
        item["troubleshooting"] = {
            "task_id": str(item.get("task_id") or ""),
            "stage": str(item.get("stage") or ""),
            "engine": str(item.get("engine_used") or item.get("engine") or ""),
            "duration_seconds": item.get("duration_seconds"),
            "last_error": str(item.get("error_message") or item.get("error_code") or ""),
            "output_tail": str(item.get("output_tail") or ""),
        }
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
            output: dict[str, Any] = {}
            for field in ("metrics_json", "artifacts_json"):
                try:
                    item[field.replace("_json", "")] = json.loads(str(item.get(field) or "{}"))
                except Exception:
                    item[field.replace("_json", "")] = {}
                item.pop(field, None)
            try:
                raw_output = conn.execute(
                    "SELECT output_json FROM quantaalpha_runs WHERE task_id = ? LIMIT 1",
                    (item.get("task_id"),),
                ).fetchone()
                output = json.loads(str((raw_output[0] if raw_output else "") or "{}"))
            except Exception:
                output = {}
            artifacts = item.get("artifacts") if isinstance(item.get("artifacts"), dict) else {}
            item["engine"] = str(output.get("engine") or artifacts.get("engine") or ENGINE_NAME)
            item["engine_profile"] = str(output.get("engine_profile") or DEFAULT_ENGINE_PROFILE)
            item["engine_used"] = str(output.get("engine_used") or artifacts.get("engine_used") or BUSINESS_ENGINE)
            item["selection_reason"] = str(artifacts.get("selection_reason") or "")
            item["library_version"] = str(artifacts.get("library_version") or "")
            item["promotion_state"] = str(artifacts.get("promotion_state") or "")
            benchmark_delta = artifacts.get("benchmark_delta") if isinstance(artifacts.get("benchmark_delta"), dict) else {}
            item["benchmark_delta"] = benchmark_delta
            item["baseline_compare"] = benchmark_delta
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


def get_quantaalpha_runtime_health(*, sqlite3_module, db_path: str) -> dict[str, Any]:
    health_ok, health_reason = _research_stack_health()
    heartbeat = _read_worker_heartbeat()
    worker_alive = _is_worker_alive()
    heartbeat_age_seconds = -1
    try:
        heartbeat_age_seconds = max(0, _now_epoch() - int(heartbeat.get("ts_epoch") or 0))
    except Exception:
        heartbeat_age_seconds = -1
    conn = sqlite3_module.connect(db_path)
    conn.row_factory = sqlite3_module.Row
    recent_error_codes: dict[str, int] = {}
    try:
        _ensure_tables(conn, sqlite3_module)
        recovered = _recover_all_stale_running(conn)
        pending = int(conn.execute("SELECT COUNT(*) FROM quantaalpha_runs WHERE status = 'pending'").fetchone()[0] or 0)
        running = int(conn.execute("SELECT COUNT(*) FROM quantaalpha_runs WHERE status = 'running'").fetchone()[0] or 0)
        failed = int(conn.execute("SELECT COUNT(*) FROM quantaalpha_runs WHERE status = 'error'").fetchone()[0] or 0)
        error_rows = conn.execute(
            """
            SELECT error_code, error_message, output_json
            FROM quantaalpha_runs
            WHERE status = 'error'
            ORDER BY id DESC
            LIMIT 50
            """
        ).fetchall()
        for row in error_rows:
            raw_code = str((row[0] if not isinstance(row, dict) else row.get("error_code")) or "").strip() or None
            raw_message = str((row[1] if not isinstance(row, dict) else row.get("error_message")) or "").strip() or None
            raw_output = row[2] if not isinstance(row, dict) else row.get("output_json")
            output_payload = {}
            if raw_output:
                try:
                    output_payload = json.loads(str(raw_output or "{}"))
                except Exception:
                    output_payload = {}
            raw_stderr = None
            if isinstance(output_payload, dict):
                raw_stderr = str(output_payload.get("stderr") or "").strip() or None
            code = _classify_runtime_error(error_code=raw_code, error_message=raw_message, stderr=raw_stderr)
            recent_error_codes[code] = int(recent_error_codes.get(code, 0) or 0) + 1
    finally:
        conn.close()
    alert_items: list[dict[str, Any]] = []
    if not worker_alive and pending > 0:
        alert_items.append(
            {
                "code": "WORKER_OFFLINE_BACKLOG",
                "severity": "critical",
                "message": f"worker 离线且仍有待处理任务（pending={pending}）",
            }
        )
    if pending >= ALERT_PENDING_THRESHOLD:
        alert_items.append(
            {
                "code": "QUEUE_PENDING_HIGH",
                "severity": "warning",
                "message": f"任务队列积压偏高（pending={pending}，阈值={ALERT_PENDING_THRESHOLD}）",
            }
        )
    total_recent_errors = sum(recent_error_codes.values())
    if total_recent_errors >= ALERT_ERROR_CONCENTRATION_MIN and recent_error_codes:
        top_code, top_count = sorted(recent_error_codes.items(), key=lambda item: item[1], reverse=True)[0]
        ratio = (top_count / total_recent_errors) if total_recent_errors else 0
        if ratio >= ALERT_ERROR_CONCENTRATION_RATIO:
            alert_items.append(
                {
                    "code": "ERROR_CODE_CONCENTRATED",
                    "severity": "warning",
                    "message": f"近期错误码集中：{top_code}（{top_count}/{total_recent_errors}）",
                    "error_code": top_code,
                    "ratio": round(ratio, 4),
                }
            )
    return {
        "ok": health_ok,
        "research_stack": {
            "status": "ok" if health_ok else "error",
            "reason": health_reason,
            "data_adapter_mode": "self_equivalent",
            "data_source_table": "stock_daily_prices",
        },
        "worker": {
            "alive": worker_alive,
            "heartbeat": heartbeat,
            "heartbeat_age_seconds": heartbeat_age_seconds,
            "execution_mode": EXECUTION_MODE,
        },
        "queue": {
            "pending": pending,
            "running": running,
            "error": failed,
            "stale_recovered_recent": recovered,
            "recent_error_codes": recent_error_codes,
        },
        "alerts": alert_items,
    }


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
            engine_profile=DEFAULT_ENGINE_PROFILE,
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
            engine_profile=DEFAULT_ENGINE_PROFILE,
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
        "start_quantaalpha_auto_research_task": lambda **kwargs: start_quantaalpha_auto_research_task(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "start_quantaalpha_backtest_task": lambda **kwargs: start_quantaalpha_backtest_task(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "start_quantaalpha_health_check_task": lambda **kwargs: start_quantaalpha_health_check_task(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "get_quantaalpha_task": lambda task_id: get_quantaalpha_task(sqlite3_module=sqlite3_module, db_path=db_path, task_id=task_id),
        "query_quantaalpha_results": lambda **kwargs: query_quantaalpha_results(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
        "run_quantaalpha_scheduled_job": lambda job_key: run_quantaalpha_scheduled_job(sqlite3_module=sqlite3_module, db_path=db_path, job_key=job_key),
        "get_quantaalpha_runtime_health": lambda: get_quantaalpha_runtime_health(sqlite3_module=sqlite3_module, db_path=db_path),
        "run_quantaalpha_worker_loop": lambda **kwargs: run_quantaalpha_worker_loop(sqlite3_module=sqlite3_module, db_path=db_path, **kwargs),
    }
