from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import db_compat as _db


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _row_to_dict(row) -> dict:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return {}


def _safe_str(val: Any, default: str = "") -> str:
    return str(val or default).strip()


def _parse_json_text(raw: Any) -> dict[str, Any]:
    try:
        import json

        parsed = json.loads(str(raw or ""))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _dedupe_keep_order(items: list[str], *, limit: int = 5) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        normalized = _safe_str(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
        if len(result) >= limit:
            break
    return result


def _first_existing_table(conn, table_names: list[str]) -> str:
    for table_name in table_names:
        if _db.table_exists(conn, table_name):
            return table_name
    return ""


def _conflict_row_identity(row: dict) -> tuple[str, str]:
    """用于合并多轮 lookback 拉取时的去重键。"""
    src = str(row.get("source") or "").strip()
    subj = str(
        row.get("subject")
        or row.get("theme_name")
        or row.get("subject_name")
        or row.get("ts_code")
        or row.get("summary_date")
        or "",
    ).strip()
    return (src, subj)


def _merge_distinct_conflict_rows(target: list[dict], extra: list[dict]) -> int:
    """将 extra 合并进 target，按 (source, subject) 去重。返回新增条数。"""
    seen = {_conflict_row_identity(r) for r in target}
    added = 0
    for r in extra:
        key = _conflict_row_identity(r)
        if key in seen or not key[0]:
            continue
        seen.add(key)
        target.append(dict(r))
        added += 1
    return added


def _hydrate_conflict_sources_for_arbitration(
    conn,
    conflict_source_rows: list[dict],
    *,
    base_lookback_hours: int,
) -> None:
    """
    叙事区块可能已从全表取到主题/日报，但 R30 裁决仅用 72h lookback，数据稍旧即无行 → 可信度 0%。
    在源过稀时自动放宽 lookback（168h → 720h）并合并去重，尽量让 composite / needs_review 有意义。
    """
    if len(conflict_source_rows) >= 4:
        return
    widen_steps = []
    h = max(int(base_lookback_hours or 72), 72)
    for mult in (2, 4, 8, 12):
        nxt = min(h * mult, 720)
        if nxt not in widen_steps and nxt > h:
            widen_steps.append(nxt)
    for widen in widen_steps:
        if len(conflict_source_rows) >= 8:
            break
        before = len(conflict_source_rows)
        _merge_distinct_conflict_rows(conflict_source_rows, _fetch_theme_hotspot_rows(conn, widen))
        _merge_distinct_conflict_rows(conflict_source_rows, _fetch_investment_signal_rows(conn, widen))
        _merge_distinct_conflict_rows(conflict_source_rows, _fetch_news_summary_rows(conn, widen))
        _merge_distinct_conflict_rows(conflict_source_rows, _fetch_stock_news_rows(conn, widen))
        _merge_distinct_conflict_rows(conflict_source_rows, _fetch_macro_series_rows(conn, widen))
        _merge_distinct_conflict_rows(conflict_source_rows, _fetch_risk_scenario_rows(conn, widen))
        if len(conflict_source_rows) <= before:
            continue


def _summary_markdown_excerpt(markdown: str, *, limit: int = 320) -> str:
    text = str(markdown or "").strip()
    if not text:
        return ""
    lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip().lstrip("#>*- ").strip()
        if not stripped:
            continue
        lines.append(stripped)
        if sum(len(part) for part in lines) >= limit:
            break
    return " ".join(lines).strip()[:limit].strip()


def _status_payload(
    *,
    status: str,
    status_reason: str,
    missing_inputs: list[str],
    generated_from: list[str],
) -> dict[str, Any]:
    return {
        "status": status,
        "status_reason": status_reason,
        "missing_inputs": _dedupe_keep_order(missing_inputs, limit=8),
        "generated_from": _dedupe_keep_order(generated_from, limit=8),
    }


# ---------------------------------------------------------------------------
# R30 conflict arbitration helpers (ported from worktree `myproj-backend`).
#
# Algorithm: grouped multi-source scoring with a composite of
#   composite = 0.45*best_strength + 0.20*recency + 0.20*ai_consistency + 0.15*risk_priority
# The winner is the group with the highest composite; groups whose composite
# trails the winner by >=0.15 are emitted as `dissenting_sources`. A winner
# whose composite <0.5 triggers `needs_review=True`.
# ---------------------------------------------------------------------------


def _safe_fetchall(conn, sql: str, params: tuple | None = None) -> list[dict]:
    try:
        cursor = conn.execute(sql, params) if params is not None else conn.execute(sql)
        rows = cursor.fetchall() or []
    except Exception:
        return []
    if not rows:
        return []
    # Build column list from cursor.description so plain-tuple rows can be
    # converted to dicts safely (sqlite3.Row / db_compat return raw tuples).
    columns: list[str] = []
    try:
        desc = getattr(cursor, "description", None) or []
        for col in desc:
            name = getattr(col, "name", None)
            if name is None:
                try:
                    name = col[0]
                except Exception:
                    name = None
            columns.append(str(name) if name is not None else "")
    except Exception:
        columns = []

    dict_rows: list[dict] = []
    for row in rows:
        if isinstance(row, dict):
            dict_rows.append(dict(row))
            continue
        if hasattr(row, "keys") and not isinstance(row, (list, tuple)):
            try:
                dict_rows.append({str(k): row[k] for k in row.keys()})
                continue
            except Exception:
                pass
        if columns and isinstance(row, (list, tuple)):
            dict_rows.append({columns[i]: row[i] for i in range(min(len(columns), len(row)))})
            continue
        try:
            dict_rows.append(dict(row))
        except Exception:
            dict_rows.append({})
    return dict_rows


def _fetch_theme_hotspot_rows(conn, lookback_hours: int) -> list[dict]:
    sql = f"""
        SELECT
            'theme_hotspots' AS source,
            theme_name AS subject,
            direction,
            heat_level,
            theme_strength,
            confidence,
            latest_evidence_time AS published_at
        FROM theme_hotspot_tracker
        WHERE CAST(NULLIF(latest_evidence_time, '') AS timestamptz)
              >= NOW() - INTERVAL '{int(lookback_hours)} hours'
        ORDER BY COALESCE(theme_strength, 0) DESC,
                 COALESCE(confidence, 0) DESC,
                 latest_evidence_time DESC
        LIMIT 12
    """
    return _safe_fetchall(conn, sql)


def _fetch_investment_signal_rows(conn, lookback_hours: int) -> list[dict]:
    sql = f"""
        SELECT
            'investment_signals' AS source,
            subject_name AS subject,
            direction,
            signal_strength,
            confidence,
            COALESCE(NULLIF(update_time, ''), NULLIF(latest_signal_date, '')) AS updated_at
        FROM investment_signal_tracker
        WHERE CAST(COALESCE(NULLIF(update_time, ''), NULLIF(latest_signal_date, '')) AS timestamptz)
              >= NOW() - INTERVAL '{int(lookback_hours)} hours'
        ORDER BY COALESCE(signal_strength, 0) DESC,
                 COALESCE(confidence, 0) DESC,
                 update_time DESC
        LIMIT 12
    """
    return _safe_fetchall(conn, sql)


def _fetch_news_summary_rows(conn, lookback_hours: int) -> list[dict]:
    sql = f"""
        SELECT
            'news_daily_summaries' AS source,
            summary_date AS subject,
            source_filter,
            news_count,
            summary_markdown,
            created_at AS updated_at
        FROM news_daily_summaries
        WHERE CAST(NULLIF(created_at, '') AS timestamptz)
              >= NOW() - INTERVAL '{int(lookback_hours)} hours'
        ORDER BY created_at DESC
        LIMIT 8
    """
    return _safe_fetchall(conn, sql)


def _fetch_stock_news_rows(conn, lookback_hours: int) -> list[dict]:
    sql = f"""
        SELECT
            'stock_news_items' AS source,
            COALESCE(NULLIF(company_name, ''), NULLIF(ts_code, ''), title) AS subject,
            llm_finance_impact_score,
            llm_system_score,
            llm_sentiment_label,
            llm_sentiment_confidence,
            COALESCE(NULLIF(update_time, ''), NULLIF(pub_time, '')) AS published_at
        FROM stock_news_items
        WHERE CAST(COALESCE(NULLIF(update_time, ''), NULLIF(pub_time, '')) AS timestamptz)
              >= NOW() - INTERVAL '{int(lookback_hours)} hours'
        ORDER BY COALESCE(llm_finance_impact_score, 0) DESC,
                 COALESCE(llm_system_score, 0) DESC,
                 pub_time DESC
        LIMIT 12
    """
    return _safe_fetchall(conn, sql)


def _fetch_macro_series_rows(conn, lookback_hours: int) -> list[dict]:
    sql = f"""
        SELECT
            'macro_series' AS source,
            COALESCE(NULLIF(indicator_name, ''), indicator_code) AS subject,
            indicator_code,
            indicator_name,
            value,
            COALESCE(NULLIF(update_time, ''), NULLIF(publish_date, '')) AS updated_at
        FROM macro_series
        WHERE CAST(COALESCE(NULLIF(update_time, ''), NULLIF(publish_date, '')) AS timestamptz)
              >= NOW() - INTERVAL '{int(lookback_hours)} hours'
        ORDER BY COALESCE(update_time, publish_date) DESC
        LIMIT 8
    """
    return _safe_fetchall(conn, sql)


def _fetch_risk_scenario_rows(conn, lookback_hours: int) -> list[dict]:
    sql = f"""
        SELECT
            'risk_scenarios' AS source,
            COALESCE(NULLIF(ts_code, ''), scenario_name) AS subject,
            scenario_name,
            pnl_impact,
            max_drawdown,
            var_95,
            cvar_95,
            horizon,
            COALESCE(
                NULLIF(update_time, ''),
                to_char(
                    to_timestamp(NULLIF(scenario_date, ''), 'YYYYMMDD'),
                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                )
            ) AS updated_at
        FROM risk_scenarios
        WHERE COALESCE(
            CAST(NULLIF(update_time, '') AS timestamptz),
            to_timestamp(NULLIF(scenario_date, ''), 'YYYYMMDD')
        ) >= NOW() - INTERVAL '{int(lookback_hours)} hours'
        ORDER BY COALESCE(ABS(cvar_95), ABS(max_drawdown), ABS(var_95), ABS(pnl_impact), 0) DESC,
                 update_time DESC
        LIMIT 8
    """
    return _safe_fetchall(conn, sql)


def _group_source_rows(source_rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for row in source_rows:
        source = str(row.get("source") or "").strip()
        if not source:
            continue
        grouped.setdefault(source, []).append(dict(row))
    return grouped


def _score_source_group(
    source_name: str,
    rows: list[dict],
    *,
    lookback_hours: int,
    now: datetime,
) -> dict | None:
    if not rows:
        return None

    normalized_rows: list[dict] = []
    for row in rows:
        item = dict(row)
        item["direction"] = _resolve_direction(source_name, item)
        item["signal_strength"] = _resolve_signal_strength(source_name, item)
        item["data_age_hours"] = _resolve_data_age_hours(item, now)
        normalized_rows.append(item)

    weighted_direction = _pick_direction(normalized_rows)
    ai_consistency = _estimate_ai_consistency([row.get("direction") for row in normalized_rows])
    best_strength = max(float(row.get("signal_strength") or 0.0) for row in normalized_rows)
    min_age_hours = min(float(row.get("data_age_hours") or lookback_hours) for row in normalized_rows)
    recency_score = _clamp(1.0 - (min_age_hours / max(float(lookback_hours), 1.0)))
    risk_priority = _resolve_risk_priority(source_name, normalized_rows, weighted_direction)
    composite = _clamp(
        0.45 * best_strength
        + 0.20 * recency_score
        + 0.20 * ai_consistency
        + 0.15 * risk_priority
    )

    return {
        "source": source_name,
        "direction": weighted_direction,
        "signal_strength": round(best_strength, 4),
        "data_age_hours": round(min_age_hours, 4),
        "ai_consistency": round(ai_consistency, 4),
        "recency_score": round(recency_score, 4),
        "risk_priority": round(risk_priority, 4),
        "composite": round(composite, 4),
        "row_count": len(normalized_rows),
    }


def _resolve_direction(source_name: str, row: dict) -> str:
    explicit = str(row.get("direction") or "").strip()
    if explicit in {"看多", "看空", "中性"}:
        return explicit

    if source_name == "news_daily_summaries":
        return _direction_from_text(str(row.get("summary_markdown") or ""))
    if source_name == "stock_news_items":
        return _direction_from_text(str(row.get("llm_sentiment_label") or ""))
    if source_name == "risk_scenarios":
        risk_values = [
            _as_float(row.get("pnl_impact")),
            _as_float(row.get("max_drawdown")),
            _as_float(row.get("var_95")),
            _as_float(row.get("cvar_95")),
        ]
        if any(value < 0 for value in risk_values):
            return "看空"
        if any(value > 0 for value in risk_values):
            return "看多"
        return "中性"
    if source_name == "macro_series":
        return _direction_from_text(
            f"{row.get('indicator_name') or ''} {row.get('indicator_code') or ''}"
        )
    return "中性"


def _resolve_signal_strength(source_name: str, row: dict) -> float:
    if source_name == "theme_hotspots":
        return _clamp(
            max(
                _heat_level_score(str(row.get("heat_level") or "")),
                _as_float(row.get("theme_strength")),
                _as_float(row.get("confidence")),
            )
        )
    if source_name == "investment_signals":
        return _clamp(max(_as_float(row.get("signal_strength")), _as_float(row.get("confidence"))))
    if source_name == "news_daily_summaries":
        return _clamp(
            _as_float(
                row.get("summary_score"),
                _as_float(row.get("score"), _as_float(row.get("news_count")) / 20.0),
            )
        )
    if source_name == "stock_news_items":
        return _clamp(
            max(
                _as_float(row.get("llm_finance_impact_score")) / 100.0,
                _as_float(row.get("llm_system_score")) / 100.0,
                _as_float(row.get("llm_sentiment_confidence")),
            )
        )
    if source_name == "macro_series":
        return _clamp(min(abs(_as_float(row.get("value"))) / 10.0, 0.8))
    if source_name == "risk_scenarios":
        return _clamp(
            max(
                abs(_as_float(row.get("max_drawdown"))) / 0.20,
                abs(_as_float(row.get("cvar_95"))) / 0.15,
                abs(_as_float(row.get("var_95"))) / 0.12,
                abs(_as_float(row.get("pnl_impact"))) / 0.10,
            )
        )
    return _clamp(_as_float(row.get("signal_strength")))


def _resolve_data_age_hours(row: dict, now: datetime) -> float:
    for key in (
        "updated_at",
        "published_at",
        "update_time",
        "pub_time",
        "created_at",
        "latest_signal_date",
        "latest_evidence_time",
    ):
        value = _parse_dt(row.get(key))
        if value is not None:
            return max((now - value).total_seconds() / 3600.0, 0.0)
    return 999.0


def _resolve_risk_priority(source_name: str, rows: list[dict], direction: str) -> float:
    if source_name != "risk_scenarios":
        return 0.0
    severity = max(
        _clamp(
            max(
                abs(_as_float(row.get("max_drawdown"))) / 0.20,
                abs(_as_float(row.get("cvar_95"))) / 0.15,
                abs(_as_float(row.get("var_95"))) / 0.12,
                abs(_as_float(row.get("pnl_impact"))) / 0.10,
            )
        )
        for row in rows
    )
    if direction == "看空":
        return max(severity, 0.85)
    if direction == "看多":
        return severity * 0.35
    return severity * 0.5


def _pick_direction(rows: list[dict]) -> str:
    bullish = 0.0
    bearish = 0.0
    neutral = 0.0
    for row in rows:
        strength = _clamp(_as_float(row.get("signal_strength")))
        direction = str(row.get("direction") or "中性")
        if direction == "看多":
            bullish += strength
        elif direction == "看空":
            bearish += strength
        else:
            neutral += max(strength, 0.25)
    if bullish > bearish and bullish >= neutral:
        return "看多"
    if bearish > bullish and bearish >= neutral:
        return "看空"
    return "中性"


def _estimate_ai_consistency(directions: list[object]) -> float:
    normalized = [str(item or "").strip() or "中性" for item in directions]
    if not normalized:
        return 0.5
    if len(normalized) == 1:
        return 1.0

    total = 0.0
    pair_count = 0
    for idx in range(len(normalized)):
        for jdx in range(idx + 1, len(normalized)):
            pair_count += 1
            total += _pair_consistency(normalized[idx], normalized[jdx])
    if pair_count <= 0:
        return 0.5
    return _clamp(total / pair_count)


def _pair_consistency(left: str, right: str) -> float:
    if left == right:
        if left == "中性":
            return 0.5
        return 1.0
    if "中性" in {left, right}:
        return 0.5
    return 0.0


def _direction_from_text(text: str) -> str:
    content = str(text or "").strip().lower()
    if not content:
        return "中性"
    bullish_terms = ("看多", "利多", "偏多", "positive", "bull", "上涨", "改善", "走强")
    bearish_terms = ("看空", "利空", "偏空", "negative", "bear", "下行", "走弱", "恶化")
    bullish = any(term in content for term in bullish_terms)
    bearish = any(term in content for term in bearish_terms)
    if bullish and not bearish:
        return "看多"
    if bearish and not bullish:
        return "看空"
    return "中性"


def _heat_level_score(value: str) -> float:
    mapping = {
        "极高": 1.0,
        "高": 0.85,
        "中高": 0.7,
        "中": 0.55,
        "低": 0.35,
    }
    return _clamp(mapping.get(str(value or "").strip(), 0.0))


def _parse_dt(value) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if len(text) == 10:
        text = f"{text}T00:00:00+00:00"
    if len(text) == 8 and text.isdigit():
        text = f"{text[0:4]}-{text[4:6]}-{text[6:8]}T00:00:00+00:00"
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_now(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _as_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return minimum
    if numeric < minimum:
        return minimum
    if numeric > maximum:
        return maximum
    return numeric


def _score_conflict_resolution(
    source_rows: list[dict],
    *,
    lookback_hours: int = 72,
    now: datetime | None = None,
) -> dict:
    now = _normalize_now(now)
    grouped = _group_source_rows(source_rows)
    scored_sources: list[dict] = []
    for source_name, rows in grouped.items():
        scored = _score_source_group(source_name, rows, lookback_hours=lookback_hours, now=now)
        if scored is not None:
            scored_sources.append(scored)

    scored_sources.sort(
        key=lambda item: (
            -float(item.get("composite") or 0.0),
            -float(item.get("risk_priority") or 0.0),
            float(item.get("data_age_hours") or 0.0),
            str(item.get("source") or ""),
        )
    )

    if not scored_sources:
        return {
            "confidence": 0.0,
            "direction": "中性",
            "winning_source": "",
            "winner_source": "",
            "score_breakdown": {"composite": 0.0, "winner": None, "sources": []},
            "resolution_basis": "无足够数据源参与裁决",
            "needs_review": True,
            "dissenting_sources": [],
        }

    winner = scored_sources[0]
    winner_direction = str(winner.get("direction") or "中性")
    winner_composite = float(winner.get("composite") or 0.0)
    winner_source = str(winner.get("source") or "")

    dissenting_sources = [
        str(item.get("source") or "")
        for item in scored_sources[1:]
        if float(item.get("composite") or 0.0) <= winner_composite - 0.15
    ]
    needs_review = winner_composite < 0.5

    if len(scored_sources) == 1:
        basis = f"单一来源 {winner_source}，无裁决冲突"
    else:
        runner_up = scored_sources[1]
        margin = winner_composite - float(runner_up.get("composite") or 0.0)
        if float(winner.get("recency_score") or 0.0) > 0.8:
            basis = f"{winner_source} 胜出：时效优势（{margin:.2f} 分差）"
        elif float(winner.get("risk_priority") or 0.0) >= 0.85:
            basis = f"{winner_source} 胜出：风险信号优先原则（安全优先）"
        elif float(winner.get("signal_strength") or 0.0) > 0.7:
            basis = f"{winner_source} 胜出：信号强度领先（{float(winner.get('signal_strength') or 0.0):.2f}）"
        else:
            basis = f"{winner_source} 胜出：综合评分 {winner_composite:.2f}（信号强度>时效>AI一致性>风险优先）"

    return {
        "confidence": round(winner_composite, 4),
        "direction": winner_direction,
        "winning_source": winner_source,
        "winner_source": winner_source,
        "score_breakdown": {
            "composite": round(winner_composite, 4),
            "winner": winner,
            "sources": scored_sources,
        },
        "resolution_basis": basis,
        "needs_review": needs_review,
        "dissenting_sources": dissenting_sources,
    }


def query_market_conclusion_from_conn(
    conn,
    *,
    lookback_hours: int = 72,
    now: datetime | None = None,
) -> dict:
    now = _normalize_now(now)
    source_rows: list[dict] = []
    source_rows.extend(_fetch_theme_hotspot_rows(conn, lookback_hours))
    source_rows.extend(_fetch_investment_signal_rows(conn, lookback_hours))
    source_rows.extend(_fetch_news_summary_rows(conn, lookback_hours))
    source_rows.extend(_fetch_stock_news_rows(conn, lookback_hours))
    source_rows.extend(_fetch_macro_series_rows(conn, lookback_hours))
    source_rows.extend(_fetch_risk_scenario_rows(conn, lookback_hours))

    conflict_resolution = _score_conflict_resolution(source_rows, lookback_hours=lookback_hours, now=now)
    return {
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "conflict_resolution": conflict_resolution,
        "sources_considered": len(source_rows),
    }


def _get_market_conclusion(lookback_hours: int = 72) -> dict[str, Any]:
    generated_at_dt = datetime.now(timezone.utc)
    generated_at = generated_at_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    global_news_count = 0
    cn_news_count = 0
    macro_count = 0
    theme_count = 0
    signal_count = 0
    risk_count = 0

    trading_theme = ""
    main_risks: list[str] = []
    benefiting_sectors: list[str] = []
    pressured_sectors: list[str] = []
    candidate_directions: list[dict] = []
    conflict_note = ""
    missing_inputs: list[str] = []
    generated_from: list[str] = []
    sources: list[dict[str, Any]] = []
    conflict_source_rows: list[dict] = []

    try:
        conn = _db.connect()
        try:
            _db.apply_row_factory(conn)
            summary_table = "news_daily_summaries" if _db.table_exists(conn, "news_daily_summaries") else ""
            signal_table = _first_existing_table(
                conn,
                [
                    "investment_signal_tracker_7d",
                    "investment_signal_tracker_1d",
                    "investment_signal_tracker",
                ],
            )
            theme_table = "theme_hotspot_tracker" if _db.table_exists(conn, "theme_hotspot_tracker") else ""
            macro_table = "macro_series" if _db.table_exists(conn, "macro_series") else ""
            risk_table = "risk_scenarios" if _db.table_exists(conn, "risk_scenarios") else ""
            stock_news_table = "stock_news_items" if _db.table_exists(conn, "stock_news_items") else ""
            signal_state_table = "signal_state_tracker" if _db.table_exists(conn, "signal_state_tracker") else ""

            if not summary_table:
                missing_inputs.append("news_daily_summaries")
            if not macro_table:
                missing_inputs.append("macro_series")
            if not signal_table:
                missing_inputs.append("investment_signal_tracker")
            if not theme_table:
                missing_inputs.append("theme_hotspot_tracker")
            if not risk_table:
                missing_inputs.append("risk_scenarios")
            if not stock_news_table:
                missing_inputs.append("stock_news_items")

            # Narrative: trading theme + source summary from news_daily_summaries
            latest_summary_text = ""
            if summary_table:
                latest_row = conn.execute(
                    """
                    SELECT summary_date, source_filter, news_count, summary_markdown, created_at
                    FROM news_daily_summaries
                    ORDER BY summary_date DESC, id DESC
                    LIMIT 1
                    """
                ).fetchone()
                count_row = conn.execute("SELECT COUNT(*) AS cnt FROM news_daily_summaries").fetchone()
                total_summaries = int((_row_to_dict(count_row).get("cnt") if count_row else 0) or 0)
                global_news_count = total_summaries
                cn_news_count = total_summaries
                if latest_row:
                    latest = _row_to_dict(latest_row)
                    latest_summary_text = _summary_markdown_excerpt(latest.get("summary_markdown"))
                    if latest_summary_text:
                        trading_theme = latest_summary_text
                        generated_from.append("news_daily_summaries")
                sources.append({
                    "key": "news_daily_summaries",
                    "label": "新闻日报总结",
                    "count": total_summaries,
                    "status": "ready" if total_summaries > 0 else "empty",
                })

            if macro_table:
                row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {macro_table}").fetchone()
                macro_count = int((_row_to_dict(row).get("cnt") if row else 0) or 0)
                sources.append({
                    "key": "macro_series",
                    "label": "宏观序列",
                    "count": macro_count,
                    "status": "ready" if macro_count > 0 else "empty",
                })
                if macro_count > 0:
                    generated_from.append("macro_series")

            if theme_table:
                theme_rows = conn.execute(
                    f"""
                    SELECT theme_name, direction, heat_level, theme_strength, confidence
                    FROM {theme_table}
                    ORDER BY COALESCE(theme_strength, 0) DESC,
                             COALESCE(confidence, 0) DESC
                    LIMIT 10
                    """
                ).fetchall()
                theme_count = len(theme_rows) if theme_rows else 0
                for raw in theme_rows or []:
                    item = _row_to_dict(raw)
                    name = _safe_str(item.get("theme_name"))
                    direction = _safe_str(item.get("direction")).lower()
                    if not name:
                        continue
                    if direction in {"up", "bullish", "看多", "涨", "多"}:
                        benefiting_sectors.append(name)
                    elif direction in {"down", "bearish", "看空", "跌", "空"}:
                        pressured_sectors.append(name)
                sources.append({
                    "key": "theme_hotspot_tracker",
                    "label": "主题热点",
                    "count": theme_count,
                    "status": "ready" if theme_count > 0 else "empty",
                })
                if theme_count > 0:
                    generated_from.append("theme_hotspot_tracker")

            if signal_table:
                rows = conn.execute(
                    f"""
                    SELECT ts_code, subject_name, signal_strength, direction, signal_status
                    FROM {signal_table}
                    WHERE COALESCE(ts_code, '') <> ''
                      AND direction IN ('看多', 'bullish', 'up', '多', '涨')
                    ORDER BY signal_strength DESC, confidence DESC, latest_signal_date DESC
                    LIMIT 5
                    """
                ).fetchall()
                signal_count = len(rows or [])
                for raw in rows or []:
                    item = _row_to_dict(raw)
                    ts_code = _safe_str(item.get("ts_code")).upper()
                    if not ts_code:
                        continue
                    candidate_directions.append({
                        "ts_code": ts_code,
                        "name": _safe_str(item.get("subject_name"), ts_code),
                        "reason": (
                            f"信号强度 {float(item.get('signal_strength') or 0.0):.2f} · "
                            f"状态 {_safe_str(item.get('signal_status'), '未知')}"
                        ),
                        "signal_strength": float(item.get("signal_strength") or 0.0),
                    })
                sources.append({
                    "key": signal_table,
                    "label": "投资信号追踪",
                    "count": signal_count,
                    "status": "ready" if signal_count > 0 else "empty",
                })
                if candidate_directions:
                    generated_from.append(signal_table)

            if stock_news_table:
                count_row = conn.execute("SELECT COUNT(*) AS cnt FROM stock_news_items").fetchone()
                news_items_count = int((_row_to_dict(count_row).get("cnt") if count_row else 0) or 0)
                sources.append({
                    "key": "stock_news_items",
                    "label": "个股新闻",
                    "count": news_items_count,
                    "status": "ready" if news_items_count > 0 else "empty",
                })
                if news_items_count > 0:
                    generated_from.append("stock_news_items")

            if risk_table:
                latest_risk_date_row = conn.execute(
                    "SELECT MAX(scenario_date) AS dt FROM risk_scenarios"
                ).fetchone()
                latest_risk_date = _safe_str(_row_to_dict(latest_risk_date_row).get("dt") if latest_risk_date_row else "")
                risk_rows: list = []
                if latest_risk_date:
                    risk_rows = conn.execute(
                        """
                        SELECT scenario_name, max_drawdown, pnl_impact, horizon
                        FROM risk_scenarios
                        WHERE scenario_date = ?
                        ORDER BY COALESCE(max_drawdown, 0) ASC,
                                 COALESCE(pnl_impact, 0) ASC
                        LIMIT 5
                        """,
                        (latest_risk_date,),
                    ).fetchall()
                # 仅有 MAX(scenario_date) 但当日无行时（稀疏写入 / 日期口径漂移），回退为最近情景，避免「主要风险」整块只剩占位句
                if not risk_rows:
                    if _db.using_postgres():
                        risk_rows = conn.execute(
                            """
                            SELECT scenario_name, max_drawdown, pnl_impact, horizon
                            FROM risk_scenarios
                            ORDER BY scenario_date DESC NULLS LAST, id DESC
                            LIMIT 5
                            """
                        ).fetchall()
                    else:
                        risk_rows = conn.execute(
                            """
                            SELECT scenario_name, max_drawdown, pnl_impact, horizon
                            FROM risk_scenarios
                            ORDER BY scenario_date DESC, id DESC
                            LIMIT 5
                            """
                        ).fetchall()
                risk_count = len(risk_rows or [])
                if risk_count > 0:
                    generated_from.append("risk_scenarios")
                for raw in risk_rows or []:
                    item = _row_to_dict(raw)
                    main_risks.append(
                        f"{_safe_str(item.get('scenario_name'), '压力情景')}："
                        f"{_safe_str(item.get('horizon'), '默认周期')} "
                        f"最大回撤 {float(item.get('max_drawdown') or 0.0):.2%}，"
                        f"收益冲击 {float(item.get('pnl_impact') or 0.0):.2%}"
                    )
                sources.append({
                    "key": "risk_scenarios",
                    "label": "风险情景",
                    "count": risk_count,
                    "status": "ready" if risk_count > 0 else "empty",
                })

            if signal_state_table and len(main_risks) < 5:
                try:
                    rows = conn.execute(
                        """
                        SELECT signal_key, current_state, event_summary
                        FROM signal_state_tracker
                        WHERE COALESCE(current_state, '') <> ''
                        ORDER BY updated_at DESC, id DESC
                        LIMIT 8
                        """
                    ).fetchall()
                    for raw in rows or []:
                        item = _row_to_dict(raw)
                        current_state = _safe_str(item.get("current_state")).lower()
                        if current_state in {"risk_rising", "weakening", "failing", "failed", "crowded"}:
                            main_risks.append(
                                _safe_str(item.get("event_summary"))
                                or f"{_safe_str(item.get('signal_key'), '信号')} 当前处于 {current_state} 阶段"
                            )
                except Exception:
                    pass

            # Real-data conflict arbitration: collect typed rows from each table
            conflict_source_rows.extend(_fetch_theme_hotspot_rows(conn, lookback_hours))
            conflict_source_rows.extend(_fetch_investment_signal_rows(conn, lookback_hours))
            conflict_source_rows.extend(_fetch_news_summary_rows(conn, lookback_hours))
            conflict_source_rows.extend(_fetch_stock_news_rows(conn, lookback_hours))
            conflict_source_rows.extend(_fetch_macro_series_rows(conn, lookback_hours))
            conflict_source_rows.extend(_fetch_risk_scenario_rows(conn, lookback_hours))
            _hydrate_conflict_sources_for_arbitration(conn, conflict_source_rows, base_lookback_hours=lookback_hours)

        finally:
            conn.close()
    except Exception as exc:
        conflict_note = f"数据聚合异常: {exc}"

    main_risks = _dedupe_keep_order(main_risks, limit=5)
    benefiting_sectors = _dedupe_keep_order(benefiting_sectors, limit=5)
    pressured_sectors = _dedupe_keep_order(pressured_sectors, limit=5)

    if not trading_theme and candidate_directions:
        first = candidate_directions[0]
        trading_theme = (
            f"当前更偏向信号驱动：优先关注 "
            f"{first.get('name') or first.get('ts_code') or '-'} 等高强度机会。"
        )
    if not trading_theme:
        missing_inputs.append("trading_theme")

    if not main_risks and risk_count == 0:
        main_risks = [
            "当前缺少显式风险情景或风险状态数据，建议结合信号总览继续核对风险侧证据。"
        ]

    if not benefiting_sectors and candidate_directions:
        benefiting_sectors = _dedupe_keep_order(
            [str(item.get("name") or item.get("ts_code") or "") for item in candidate_directions],
            limit=5,
        )

    resolution = _score_conflict_resolution(
        conflict_source_rows,
        lookback_hours=lookback_hours,
        now=generated_at_dt,
    )
    if (
        not conflict_note
        and resolution.get("winning_source")
        and len(resolution.get("score_breakdown", {}).get("sources", [])) > 1
    ):
        conflict_note = (
            "已按信号强度 > 时效 > AI一致性 > 风险优先完成裁决，"
            f"当前主导来源为 {resolution['winning_source']}。"
        )

    if trading_theme and (main_risks or benefiting_sectors or candidate_directions):
        status = "ready"
        status_reason = "已形成市场主线、风险和候选方向的基础结论。"
    elif trading_theme or candidate_directions or benefiting_sectors:
        status = "insufficient_evidence"
        status_reason = "部分数据已到位，但仍缺少完整风险、行业或候选支撑，当前为降级结论。"
    elif missing_inputs:
        status = "not_initialized"
        status_reason = "关键数据源尚未准备完成，当前无法形成稳定市场结论。"
    else:
        status = "empty"
        status_reason = "当前没有命中有效市场结论数据。"

    return {
        "generated_at": generated_at,
        "trading_theme": trading_theme,
        "main_risks": main_risks,
        "benefiting_sectors": benefiting_sectors,
        "pressured_sectors": pressured_sectors,
        "candidate_directions": candidate_directions,
        "conflict_note": conflict_note,
        "resolution_basis": resolution["resolution_basis"],
        "conflict_resolution": {
            "winning_source": resolution["winning_source"],
            "winner_source": resolution.get("winner_source") or resolution["winning_source"],
            "direction": resolution["direction"],
            "confidence": resolution["confidence"],
            "score_breakdown": resolution["score_breakdown"],
            "needs_review": resolution["needs_review"],
            "dissenting_sources": resolution["dissenting_sources"],
        },
        "sources_considered": len(conflict_source_rows),
        "sources": sources or [{"key": "none", "label": "暂无可用来源", "count": 0, "status": "empty"}],
        "source_summary": {
            "global_news_count": global_news_count,
            "cn_news_count": cn_news_count,
            "macro_series": macro_count,
            "theme_signals": theme_count,
            "investment_signals": signal_count,
            "risk_scenarios": risk_count,
        },
        **_status_payload(
            status=status,
            status_reason=status_reason,
            missing_inputs=missing_inputs,
            generated_from=generated_from,
        ),
    }


def dispatch_get(handler, parsed, deps: dict) -> bool:
    if not parsed.path.startswith("/api/market"):
        return False

    if parsed.path == "/api/market/conclusion":
        from urllib.parse import parse_qs

        params = parse_qs(parsed.query or "")
        try:
            lookback_hours = int(params.get("lookback_hours", ["72"])[0] or 72)
        except ValueError:
            handler._send_json({"ok": False, "error": "lookback_hours 必须是整数"}, status=400)
            return True

        try:
            payload = _get_market_conclusion(lookback_hours=lookback_hours)
        except Exception as exc:  # pragma: no cover
            handler._send_json({"ok": False, "error": f"市场结论聚合失败: {exc}"}, status=500)
            return True
        handler._send_json({"ok": True, **payload})
        return True

    return False
